package org.hsqldb;
import java.sql.Connection;
import org.hsqldb.ParserDQL.CompileContext;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.ArraySort;
import org.hsqldb.lib.OrderedHashSet;
import org.hsqldb.navigator.RowSetNavigator;
import org.hsqldb.navigator.RowSetNavigatorData;
import org.hsqldb.result.Result;
import org.hsqldb.result.ResultMetaData;
import org.hsqldb.store.ValuePool;
import org.hsqldb.types.Type;
public class StatementProcedure extends StatementDMQL {
    Expression expression;
    Routine procedure;
    Expression[]   arguments = Expression.emptyArray;
    ResultMetaData resultMetaData;
    StatementProcedure(Session session, Expression expression,
                       CompileContext compileContext) {
        super(StatementTypes.CALL, StatementTypes.X_SQL_DATA,
              session.getCurrentSchemaHsqlName());
        statementReturnType = StatementTypes.RETURN_RESULT;
        if (expression.opType == OpTypes.FUNCTION) {
            FunctionSQLInvoked f = (FunctionSQLInvoked) expression;
            if (f.routine.returnsTable) {
                this.procedure = f.routine;
                this.arguments = f.nodes;
            } else {
                this.expression = expression;
            }
        } else {
            this.expression = expression;
        }
        setDatabseObjects(session, compileContext);
        checkAccessRights(session);
        if (procedure != null) {
            session.getGrantee().checkAccess(procedure);
        }
    }
    StatementProcedure(Session session, Routine procedure,
                       Expression[] arguments, CompileContext compileContext) {
        super(StatementTypes.CALL, StatementTypes.X_SQL_DATA,
              session.getCurrentSchemaHsqlName());
        if (procedure.maxDynamicResults > 0) {
            statementReturnType = StatementTypes.RETURN_ANY;
        }
        this.procedure = procedure;
        this.arguments = arguments;
        setDatabseObjects(session, compileContext);
        checkAccessRights(session);
        session.getGrantee().checkAccess(procedure);
    }
    Result getResult(Session session) {
        Result result = expression == null ? getProcedureResult(session)
                                           : getExpressionResult(session);
        result.setStatementType(statementReturnType);
        return result;
    }
    Result getProcedureResult(Session session) {
        Object[] data = ValuePool.emptyObjectArray;
        int      argLength;
        if (procedure.isPSM()) {
            argLength = arguments.length;
            if (procedure.getMaxDynamicResults() > 0) {
                argLength++;
            }
        } else {
            argLength = procedure.javaMethod.getParameterTypes().length;
            if (procedure.javaMethodWithConnection) {
                argLength--;
            }
        }
        if (argLength > 0) {
            data = new Object[argLength];
        }
        for (int i = 0; i < arguments.length; i++) {
            Expression e     = arguments[i];
            Object     value = e.getValue(session);
            if (e != null) {
                Type targetType = procedure.getParameter(i).getDataType();
                data[i] = targetType.convertToType(session, value,
                                                   e.getDataType());
            }
        }
        session.sessionContext.push();
        session.sessionContext.routineArguments = data;
        session.sessionContext.routineVariables = ValuePool.emptyObjectArray;
        Result result = Result.updateZeroResult;
        if (procedure.isPSM()) {
            result = executePSMProcedure(session);
        } else {
            Connection connection = session.getInternalConnection();
            result = executeJavaProcedure(session, connection);
        }
        Object[] callArguments = session.sessionContext.routineArguments;
        session.sessionContext.pop();
        if (!procedure.isPSM()) {
            session.releaseInternalConnection();
        }
        if (result.isError()) {
            return result;
        }
        for (int i = 0; i < procedure.getParameterCount(); i++) {
            ColumnSchema param = procedure.getParameter(i);
            int          mode  = param.getParameterMode();
            if (mode != SchemaObject.ParameterModes.PARAM_IN) {
                if (this.arguments[i].isDynamicParam()) {
                    int paramIndex = arguments[i].parameterIndex;
                    session.sessionContext.dynamicArguments[paramIndex] =
                        callArguments[i];
                } else {
                    int varIndex = arguments[i].getColumnIndex();
                    session.sessionContext.routineVariables[varIndex] =
                        callArguments[i];
                }
            }
        }
        Result r = result;
        result = Result.newCallResponse(
            this.getParametersMetaData().getParameterTypes(), this.id,
            session.sessionContext.dynamicArguments);
        if (procedure.returnsTable()) {
            result.addChainedResult(r);
        } else if (callArguments.length > arguments.length) {
            r = (Result) callArguments[arguments.length];
            result.addChainedResult(r);
        }
        return result;
    }
    Result executePSMProcedure(Session session) {
        int variableCount = procedure.getVariableCount();
        session.sessionContext.routineVariables = new Object[variableCount];
        Result result = procedure.statement.execute(session);
        if (result.isError()) {
            return result;
        }
        if (procedure.returnsTable()) {
            RowSetNavigator resultNavigator = result.getNavigator();
            RowSetNavigatorData navigator = new RowSetNavigatorData(session,
                resultNavigator);
            result.setNavigator(navigator);
        }
        return result;
    }
    Result executeJavaProcedure(Session session, Connection connection) {
        Result   result        = Result.updateZeroResult;
        Object[] callArguments = session.sessionContext.routineArguments;
        Object[] data = procedure.convertArgsToJava(session, callArguments);
        if (procedure.javaMethodWithConnection) {
            data[0] = connection;
        }
        result = procedure.invokeJavaMethod(session, data);
        procedure.convertArgsToSQL(session, callArguments, data);
        return result;
    }
    Result getExpressionResult(Session session) {
        Object o;    
        Result r;
        session.sessionData.startRowProcessing();
        o = expression.getValue(session);
        if (resultMetaData == null) {
            getResultMetaData();
        }
        r = Result.newSingleColumnResult(resultMetaData);
        Object[] row;
        if (expression.getDataType().isArrayType()) {
            row    = new Object[1];
            row[0] = o;
        } else if (o instanceof Object[]) {
            row = (Object[]) o;
        } else {
            row    = new Object[1];
            row[0] = o;
        }
        r.getNavigator().add(row);
        return r;
    }
    SubQuery[] getSubqueries(Session session) {
        OrderedHashSet subQueries = null;
        if (expression != null) {
            subQueries = expression.collectAllSubqueries(subQueries);
        }
        for (int i = 0; i < arguments.length; i++) {
            subQueries = arguments[i].collectAllSubqueries(subQueries);
        }
        if (subQueries == null || subQueries.size() == 0) {
            return SubQuery.emptySubqueryArray;
        }
        SubQuery[] subQueryArray = new SubQuery[subQueries.size()];
        subQueries.toArray(subQueryArray);
        ArraySort.sort(subQueryArray, 0, subQueryArray.length,
                       subQueryArray[0]);
        for (int i = 0; i < subqueries.length; i++) {
            subQueryArray[i].prepareTable(session);
        }
        return subQueryArray;
    }
    public ResultMetaData getResultMetaData() {
        if (resultMetaData != null) {
            return resultMetaData;
        }
        switch (type) {
            case StatementTypes.CALL : {
                if (expression == null) {
                    return ResultMetaData.emptyResultMetaData;
                }
                ResultMetaData md = ResultMetaData.newResultMetaData(1);
                ColumnBase column =
                    new ColumnBase(null, null, null,
                                   StatementDMQL.RETURN_COLUMN_NAME);
                column.setType(expression.getDataType());
                md.columns[0] = column;
                md.prepareData();
                resultMetaData = md;
                return md;
            }
            default :
                throw Error.runtimeError(ErrorCode.U_S0500,
                                         "StatementProcedure");
        }
    }
    public ResultMetaData getParametersMetaData() {
        return super.getParametersMetaData();
    }
    void collectTableNamesForRead(OrderedHashSet set) {
        if (expression == null) {
            set.addAll(procedure.getTableNamesForRead());
        } else {
            for (int i = 0; i < subqueries.length; i++) {
                if (subqueries[i].queryExpression != null) {
                    subqueries[i].queryExpression.getBaseTableNames(set);
                }
            }
            for (int i = 0; i < routines.length; i++) {
                set.addAll(routines[i].getTableNamesForRead());
            }
        }
    }
    void collectTableNamesForWrite(OrderedHashSet set) {
        if (expression == null) {
            set.addAll(procedure.getTableNamesForWrite());
        }
    }
}