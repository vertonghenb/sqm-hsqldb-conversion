package org.hsqldb;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.ArrayListIdentity;
import org.hsqldb.lib.HsqlList;
import org.hsqldb.lib.Set;
import org.hsqldb.result.Result;
import org.hsqldb.store.ValuePool;
import org.hsqldb.types.Type;
public class FunctionSQLInvoked extends Expression {
    RoutineSchema routineSchema;
    Routine       routine;
    Expression    condition = Expression.EXPR_TRUE;    
    FunctionSQLInvoked(RoutineSchema routineSchema) {
        super(routineSchema.isAggregate() ? OpTypes.USER_AGGREGATE
                                          : OpTypes.FUNCTION);
        this.routineSchema = routineSchema;
    }
    public void setArguments(Expression[] newNodes) {
        this.nodes = newNodes;
    }
    public HsqlList resolveColumnReferences(Session session,
            RangeVariable[] rangeVarArray, int rangeCount,
            HsqlList unresolvedSet, boolean acceptsSequences) {
        HsqlList conditionSet = condition.resolveColumnReferences(session,
            rangeVarArray, rangeCount, null, false);
        if (conditionSet != null) {
            ExpressionColumn.checkColumnsResolved(conditionSet);
        }
        if (isSelfAggregate()) {
            if (unresolvedSet == null) {
                unresolvedSet = new ArrayListIdentity();
            }
            unresolvedSet.add(this);
            return unresolvedSet;
        } else {
            return super.resolveColumnReferences(session, rangeVarArray,
                                                 rangeCount, unresolvedSet,
                                                 acceptsSequences);
        }
    }
    public void resolveTypes(Session session, Expression parent) {
        Type[] types = new Type[nodes.length];
        for (int i = 0; i < nodes.length; i++) {
            Expression e = nodes[i];
            e.resolveTypes(session, this);
            types[i] = e.dataType;
        }
        routine = routineSchema.getSpecificRoutine(types);
        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i].dataType == null) {
                nodes[i].dataType = routine.getParameterTypes()[i];
            }
        }
        dataType = routine.getReturnType();
        condition.resolveTypes(session, null);
    }
    private Object getValueInternal(Session session, Object[] aggregateData) {
        boolean  isValue       = false;
        int      variableCount = routine.getVariableCount();
        Result   result;
        int      extraArg = routine.javaMethodWithConnection ? 1
                                                             : 0;
        Object[] data     = ValuePool.emptyObjectArray;
        boolean  push     = true;
        if (extraArg + nodes.length > 0) {
            if (opType == OpTypes.USER_AGGREGATE) {
                data = new Object[routine.getParameterCount()];
                for (int i = 0; i < aggregateData.length; i++) {
                    data[i + 1] = aggregateData[i];
                }
            } else {
                data = new Object[nodes.length + extraArg];
            }
            if (!routine.isPSM()) {
                Object connection = session.getInternalConnection();
                if (extraArg > 0) {
                    data[0] = connection;
                }
            }
        }
        Type[] dataTypes = routine.getParameterTypes();
        for (int i = 0; i < nodes.length; i++) {
            Expression e     = nodes[i];
            Object     value = e.getValue(session, dataTypes[i]);
            if (value == null) {
                if (routine.isNullInputOutput()) {
                    return null;
                }
                if (!routine.getParameter(i).isNullable()) {
                    return Result.newErrorResult(
                        Error.error(ErrorCode.X_39004));
                }
            }
            if (routine.isPSM()) {
                data[i] = value;
            } else {
                data[i + extraArg] = e.dataType.convertSQLToJava(session,
                        value);
            }
        }
        result = routine.invoke(session, data, aggregateData, push);
        session.releaseInternalConnection();
        if (result.isError()) {
            throw result.getException();
        }
        if (isValue) {
            return result.valueData;
        } else {
            return result;
        }
    }
    public Object getValue(Session session) {
        if (opType == OpTypes.SIMPLE_COLUMN) {
            Object value =
                session.sessionContext.rangeIterators[rangePosition]
                    .getCurrent(columnIndex);
            return value;
        }
        Object returnValue = getValueInternal(session, null);
        if (returnValue instanceof Result) {
            Result result = (Result) returnValue;
            if (result.isError()) {
                throw result.getException();
            } else if (result.isSimpleValue()) {
                returnValue = result.getValueObject();
            } else if (result.isData()) {
                returnValue = result;
            } else {
                throw Error.error(ErrorCode.X_2F005, routine.getName().name);
            }
        }
        return returnValue;
    }
    public Result getResult(Session session) {
        Object value = getValueInternal(session, null);
        if (value instanceof Result) {
            return (Result) value;
        }
        return Result.newPSMResult(value);
    }
    void collectObjectNames(Set set) {
        set.add(routine.getSpecificName());
    }
    public String getSQL() {
        StringBuffer sb = new StringBuffer();
        sb.append(routineSchema.getName().getSchemaQualifiedStatementName());
        sb.append('(');
        int nodeCount = nodes.length;
        if (opType == OpTypes.USER_AGGREGATE) {
            nodeCount = 1;
        }
        for (int i = 0; i < nodeCount; i++) {
            if (i != 0) {
                sb.append(',');
            }
            sb.append(nodes[i].getSQL());
        }
        sb.append(')');
        return sb.toString();
    }
    public String describe(Session session, int blanks) {
        return super.describe(session, blanks);
    }
    boolean isSelfAggregate() {
        return routineSchema.isAggregate();
    }
    public boolean isDeterministic() {
        return routine.isDeterministic();
    }
    public boolean equals(Expression other) {
        if (!(other instanceof FunctionSQLInvoked)) {
            return false;
        }
        FunctionSQLInvoked o = (FunctionSQLInvoked) other;
        if (opType == other.opType && routineSchema == o.routineSchema
                && routine == o.routine && condition.equals(o.condition)) {
            return super.equals(other);
        }
        return false;
    }
    public Object updateAggregatingValue(Session session, Object currValue) {
        if (!condition.testCondition(session)) {
            return currValue;
        }
        Object[] array = (Object[]) currValue;
        if (array == null) {
            array = new Object[3];
        }
        array[0] = Boolean.FALSE;
        getValueInternal(session, array);
        return array;
    }
    public Object getAggregatedValue(Session session, Object currValue) {
        Object[] array = (Object[]) currValue;
        if (array == null) {
            array = new Object[3];
        }
        array[0] = Boolean.TRUE;
        Result result = (Result) getValueInternal(session, array);
        Object returnValue;
        if (result.isError()) {
            throw result.getException();
        } else {
            return result.getValueObject();
        }
    }
    public Expression getCondition() {
        return condition;
    }
    public boolean hasCondition() {
        return condition != null && condition != Expression.EXPR_TRUE;
    }
    public void setCondition(Expression e) {
        condition = e;
    }
}