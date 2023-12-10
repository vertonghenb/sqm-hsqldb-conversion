package org.hsqldb;
import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.result.Result;
public class StatementSchemaDefinition extends StatementSchema {
    StatementSchema[] statements;
    StatementSchemaDefinition(StatementSchema[] statements) {
        super(StatementTypes.CREATE_SCHEMA,
              StatementTypes.X_SQL_SCHEMA_DEFINITION);
        this.statements = statements;
    }
    public Result execute(Session session) {
        Result result;
        try {
            result = getResult(session);
        } catch (Throwable t) {
            result = Result.newErrorResult(t, null);
        }
        if (result.isError()) {
            result.getException().setStatementType(group, type);
        }
        return result;
    }
    Result getResult(Session session) {
        HsqlName schemaDefinitionName = statements[0].getSchemaName();
        if (this.isExplain) {
            return Result.newSingleColumnStringResult("OPERATION",
                    describe(session));
        }
        StatementSchema cs;
        Result          result      = statements[0].execute(session);
        HsqlArrayList   constraints = new HsqlArrayList();
        StatementSchema log = new StatementSchema(null,
            StatementTypes.LOG_SCHEMA_STATEMENT);
        if (statements.length == 1 || result.isError()) {
            return result;
        }
        HsqlName oldSessionSchema = session.getCurrentSchemaHsqlName();
        for (int i = 1; i < statements.length; i++) {
            try {
                session.setSchema(schemaDefinitionName.name);
            } catch (HsqlException e) {}
            statements[i].setSchemaHsqlName(schemaDefinitionName);
            session.parser.reset(statements[i].getSQL());
            try {
                session.parser.read();
                switch (statements[i].getType()) {
                    case StatementTypes.GRANT :
                    case StatementTypes.GRANT_ROLE :
                        result = statements[i].execute(session);
                        break;
                    case StatementTypes.CREATE_TABLE :
                        cs                    = session.parser.compileCreate();
                        cs.isSchemaDefinition = true;
                        cs.setSchemaHsqlName(schemaDefinitionName);
                        if (session.parser.token.tokenType
                                != Tokens.X_ENDPARSE) {
                            throw session.parser.unexpectedToken();
                        }
                        cs.isLogged = false;
                        result      = cs.execute(session);
                        HsqlName name = ((Table) cs.arguments[0]).getName();
                        Table table =
                            (Table) session.database.schemaManager
                                .getSchemaObject(name);
                        constraints.addAll((HsqlArrayList) cs.arguments[1]);
                        ((HsqlArrayList) cs.arguments[1]).clear();
                        log.sql = table.getSQL();
                        log.execute(session);
                        break;
                    case StatementTypes.CREATE_ROLE :
                    case StatementTypes.CREATE_SEQUENCE :
                    case StatementTypes.CREATE_TYPE :
                    case StatementTypes.CREATE_CHARACTER_SET :
                    case StatementTypes.CREATE_COLLATION :
                        result = statements[i].execute(session);
                        break;
                    case StatementTypes.CREATE_INDEX :
                    case StatementTypes.CREATE_TRIGGER :
                    case StatementTypes.CREATE_VIEW :
                    case StatementTypes.CREATE_DOMAIN :
                    case StatementTypes.CREATE_ROUTINE :
                        cs                    = session.parser.compileCreate();
                        cs.isSchemaDefinition = true;
                        cs.setSchemaHsqlName(schemaDefinitionName);
                        if (session.parser.token.tokenType
                                != Tokens.X_ENDPARSE) {
                            throw session.parser.unexpectedToken();
                        }
                        result = cs.execute(session);
                        break;
                    case StatementTypes.CREATE_ASSERTION :
                    case StatementTypes.CREATE_TRANSFORM :
                    case StatementTypes.CREATE_TRANSLATION :
                    case StatementTypes.CREATE_CAST :
                    case StatementTypes.CREATE_ORDERING :
                        throw session.parser.unsupportedFeature();
                    default :
                        throw Error.runtimeError(ErrorCode.U_S0500, "");
                }
                if (result.isError()) {
                    break;
                }
            } catch (HsqlException e) {
                result = Result.newErrorResult(e, statements[i].getSQL());
                break;
            }
        }
        if (!result.isError()) {
            try {
                for (int i = 0; i < constraints.size(); i++) {
                    Constraint c = (Constraint) constraints.get(i);
                    Table table =
                        session.database.schemaManager.getUserTable(session,
                            c.core.refTableName);
                    ParserDDL.addForeignKey(session, table, c, null);
                    log.sql = c.getSQL();
                    log.execute(session);
                }
            } catch (HsqlException e) {
                result = Result.newErrorResult(e, sql);
            }
        }
        if (result.isError()) {
            try {
                session.database.schemaManager.dropSchema(session,
                        schemaDefinitionName.name, true);
                session.database.logger.writeOtherStatement(session,
                        getDropSchemaStatement(schemaDefinitionName));
            } catch (HsqlException e) {}
        }
        session.setCurrentSchemaHsqlName(oldSessionSchema);
        return result;
    }
    String getDropSchemaStatement(HsqlName schema) {
        return "DROP SCHEMA " + schema.statementName + " " + Tokens.T_CASCADE;
    }
    public boolean isAutoCommitStatement() {
        return true;
    }
}