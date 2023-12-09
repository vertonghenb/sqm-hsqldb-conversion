


package org.hsqldb;

import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.OrderedHashSet;
import org.hsqldb.result.Result;


public class StatementSimple extends Statement {

    String   sqlState;
    String   message;
    HsqlName label;

    
    ColumnSchema[] variables;
    int[]          variableIndexes;

    StatementSimple(int type, HsqlName label) {

        super(type, StatementTypes.X_SQL_CONTROL);

        references             = new OrderedHashSet();
        isTransactionStatement = false;
        this.label             = label;
    }

    StatementSimple(int type, String sqlState, String message) {

        super(type, StatementTypes.X_SQL_CONTROL);

        references             = new OrderedHashSet();
        isTransactionStatement = false;
        this.sqlState          = sqlState;
        this.message           = message;
    }

    public String getSQL() {

        StringBuffer sb = new StringBuffer();

        switch (type) {

            case StatementTypes.SIGNAL :
                sb.append(Tokens.T_SIGNAL).append(' ');
                sb.append(Tokens.T_SQLSTATE);
                sb.append(' ').append('\'').append(sqlState).append('\'');
                break;

            case StatementTypes.RESIGNAL :
                sb.append(Tokens.T_RESIGNAL).append(' ');
                sb.append(Tokens.T_SQLSTATE);
                sb.append(' ').append('\'').append(sqlState).append('\'');
                break;

            case StatementTypes.ITERATE :
                sb.append(Tokens.T_ITERATE).append(' ').append(label);
                break;

            case StatementTypes.LEAVE :
                sb.append(Tokens.T_LEAVE).append(' ').append(label);
                break;
        }

        return sb.toString();
    }

    protected String describe(Session session, int blanks) {

        StringBuffer sb = new StringBuffer();

        sb.append('\n');

        for (int i = 0; i < blanks; i++) {
            sb.append(' ');
        }

        sb.append(Tokens.T_STATEMENT);

        return sb.toString();
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

        switch (type) {

            
            case StatementTypes.SIGNAL :
            case StatementTypes.RESIGNAL :
                HsqlException ex = Error.error(message, sqlState);

                return Result.newErrorResult(ex);

            case StatementTypes.ITERATE :
            case StatementTypes.LEAVE :
                return Result.newPSMResult(type, label.name, null);

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "");
        }
    }

    public void resolve(Session session) {

        boolean resolved = false;

        switch (type) {

            case StatementTypes.SIGNAL :
            case StatementTypes.RESIGNAL :
                resolved = true;
                break;

            case StatementTypes.ITERATE : {
                StatementCompound statement = parent;

                while (statement != null) {
                    if (statement.isLoop) {
                        if (label == null) {
                            resolved = true;

                            break;
                        }

                        if (statement.label != null
                                && label.name.equals(statement.label.name)) {
                            resolved = true;

                            break;
                        }
                    }

                    statement = statement.parent;
                }

                break;
            }
            case StatementTypes.LEAVE :
                resolved = true;
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "");
        }

        if (!resolved) {
            throw Error.error(ErrorCode.X_42602);
        }
    }

    public String describe(Session session) {
        return "";
    }

    public boolean isCatalogLock() {
        return false;
    }

    public boolean isCatalogChange() {
        return false;
    }
}
