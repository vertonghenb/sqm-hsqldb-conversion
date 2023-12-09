


package org.hsqldb;

import org.hsqldb.ParserDQL.CompileContext;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.OrderedHashSet;
import org.hsqldb.result.Result;
import org.hsqldb.result.ResultMetaData;
import org.hsqldb.result.ResultProperties;


public class StatementQuery extends StatementDMQL {

    StatementQuery(Session session, QueryExpression queryExpression,
                   CompileContext compileContext) {

        super(StatementTypes.SELECT_CURSOR, StatementTypes.X_SQL_DATA,
              session.getCurrentSchemaHsqlName());

        this.statementReturnType = StatementTypes.RETURN_RESULT;
        this.queryExpression     = queryExpression;

        setDatabseObjects(session, compileContext);
        checkAccessRights(session);
    }

    Result getResult(Session session) {

        Result result = queryExpression.getResult(session,
            session.getMaxRows());

        result.setStatement(this);

        return result;
    }

    public ResultMetaData getResultMetaData() {

        switch (type) {

            case StatementTypes.SELECT_CURSOR :
                return queryExpression.getMetaData();

            case StatementTypes.SELECT_SINGLE :
                return queryExpression.getMetaData();

            default :
                throw Error.runtimeError(ErrorCode.U_S0500,
                                         "StatementQuery.getResultMetaData()");
        }
    }

    void collectTableNamesForRead(OrderedHashSet set) {

        queryExpression.getBaseTableNames(set);

        for (int i = 0; i < subqueries.length; i++) {
            if (subqueries[i].queryExpression != null) {
                subqueries[i].queryExpression.getBaseTableNames(set);
            }
        }

        for (int i = 0; i < routines.length; i++) {
            set.addAll(routines[i].getTableNamesForRead());
        }
    }

    void collectTableNamesForWrite(OrderedHashSet set) {

        if (queryExpression.isUpdatable) {
            queryExpression.getBaseTableNames(set);
        }
    }

    public int getResultProperties() {
        return ResultProperties.defaultPropsValue;
    }
}
