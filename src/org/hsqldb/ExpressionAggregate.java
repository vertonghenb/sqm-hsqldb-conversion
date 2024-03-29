package org.hsqldb;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.ArrayListIdentity;
import org.hsqldb.lib.HsqlList;
import org.hsqldb.store.ValuePool;
import org.hsqldb.types.ArrayType;
public class ExpressionAggregate extends Expression {
    boolean    isDistinctAggregate;
    Expression condition = Expression.EXPR_TRUE;
    ArrayType  arrayType;
    ExpressionAggregate(int type, boolean distinct, Expression e) {
        super(type);
        nodes               = new Expression[UNARY];
        isDistinctAggregate = distinct;
        nodes[LEFT]         = e;
    }
    boolean isSelfAggregate() {
        return true;
    }
    public String getSQL() {
        StringBuffer sb   = new StringBuffer(64);
        String       left = getContextSQL(nodes.length > 0 ? nodes[LEFT]
                                                           : null);
        switch (opType) {
            case OpTypes.COUNT :
                sb.append(' ').append(Tokens.T_COUNT).append('(');
                break;
            case OpTypes.SUM :
                sb.append(' ').append(Tokens.T_SUM).append('(');
                sb.append(left).append(')');
                break;
            case OpTypes.MIN :
                sb.append(' ').append(Tokens.T_MIN).append('(');
                sb.append(left).append(')');
                break;
            case OpTypes.MAX :
                sb.append(' ').append(Tokens.T_MAX).append('(');
                sb.append(left).append(')');
                break;
            case OpTypes.AVG :
                sb.append(' ').append(Tokens.T_AVG).append('(');
                sb.append(left).append(')');
                break;
            case OpTypes.EVERY :
                sb.append(' ').append(Tokens.T_EVERY).append('(');
                sb.append(left).append(')');
                break;
            case OpTypes.SOME :
                sb.append(' ').append(Tokens.T_SOME).append('(');
                sb.append(left).append(')');
                break;
            case OpTypes.STDDEV_POP :
                sb.append(' ').append(Tokens.T_STDDEV_POP).append('(');
                sb.append(left).append(')');
                break;
            case OpTypes.STDDEV_SAMP :
                sb.append(' ').append(Tokens.T_STDDEV_SAMP).append('(');
                sb.append(left).append(')');
                break;
            case OpTypes.VAR_POP :
                sb.append(' ').append(Tokens.T_VAR_POP).append('(');
                sb.append(left).append(')');
                break;
            case OpTypes.VAR_SAMP :
                sb.append(' ').append(Tokens.T_VAR_SAMP).append('(');
                sb.append(left).append(')');
                break;
            default :
                throw Error.runtimeError(ErrorCode.U_S0500,
                                         "ExpressionAggregate");
        }
        return sb.toString();
    }
    protected String describe(Session session, int blanks) {
        StringBuffer sb = new StringBuffer(64);
        sb.append('\n');
        for (int i = 0; i < blanks; i++) {
            sb.append(' ');
        }
        switch (opType) {
            case OpTypes.COUNT :
                sb.append(Tokens.T_COUNT).append(' ');
                break;
            case OpTypes.SUM :
                sb.append(Tokens.T_SUM).append(' ');
                break;
            case OpTypes.MIN :
                sb.append(Tokens.T_MIN).append(' ');
                break;
            case OpTypes.MAX :
                sb.append(Tokens.T_MAX).append(' ');
                break;
            case OpTypes.AVG :
                sb.append(Tokens.T_AVG).append(' ');
                break;
            case OpTypes.EVERY :
                sb.append(Tokens.T_EVERY).append(' ');
                break;
            case OpTypes.SOME :
                sb.append(Tokens.T_SOME).append(' ');
                break;
            case OpTypes.STDDEV_POP :
                sb.append(Tokens.T_STDDEV_POP).append(' ');
                break;
            case OpTypes.STDDEV_SAMP :
                sb.append(Tokens.T_STDDEV_SAMP).append(' ');
                break;
            case OpTypes.VAR_POP :
                sb.append(Tokens.T_VAR_POP).append(' ');
                break;
            case OpTypes.VAR_SAMP :
                sb.append(Tokens.T_VAR_SAMP).append(' ');
                break;
        }
        if (getLeftNode() != null) {
            sb.append(" arg=[");
            sb.append(nodes[LEFT].describe(session, blanks + 1));
            sb.append(']');
        }
        return sb.toString();
    }
    public HsqlList resolveColumnReferences(Session session,
            RangeVariable[] rangeVarArray, int rangeCount,
            HsqlList unresolvedSet, boolean acceptsSequences) {
        HsqlList conditionSet = condition.resolveColumnReferences(session,
            rangeVarArray, rangeCount, null, false);
        if (conditionSet != null) {
            ExpressionColumn.checkColumnsResolved(conditionSet);
        }
        if (unresolvedSet == null) {
            unresolvedSet = new ArrayListIdentity();
        }
        unresolvedSet.add(this);
        return unresolvedSet;
    }
    public void resolveTypes(Session session, Expression parent) {
        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] != null) {
                nodes[i].resolveTypes(session, this);
            }
        }
        if (nodes[LEFT].isUnresolvedParam()) {
            throw Error.error(ErrorCode.X_42567);
        }
        if (isDistinctAggregate) {
            if (nodes[LEFT].dataType.isLobType()) {
                throw Error.error(ErrorCode.X_42534);
            }
            if (nodes[LEFT].dataType.isCharacterType()) {
                arrayType = new ArrayType(nodes[LEFT].dataType,
                                          Integer.MAX_VALUE);
            }
        }
        dataType = SetFunction.getType(session, opType, nodes[LEFT].dataType);
        condition.resolveTypes(session, null);
    }
    public boolean equals(Expression other) {
        if (!(other instanceof ExpressionAggregate)) {
            return false;
        }
        ExpressionAggregate o = (ExpressionAggregate) other;
        if (opType == other.opType && exprSubType == other.exprSubType
                && isDistinctAggregate == o.isDistinctAggregate
                && condition.equals(o.condition)) {
            return super.equals(other);
        }
        return false;
    }
    public Object updateAggregatingValue(Session session, Object currValue) {
        if (!condition.testCondition(session)) {
            return currValue;
        }
        if (currValue == null) {
            currValue = new SetFunction(opType, nodes[LEFT].dataType,
                                        dataType, isDistinctAggregate,
                                        arrayType);
        }
        Object newValue = nodes[LEFT].opType == OpTypes.ASTERISK
                          ? ValuePool.INTEGER_1
                          : nodes[LEFT].getValue(session);
        ((SetFunction) currValue).add(session, newValue);
        return currValue;
    }
    public Object getAggregatedValue(Session session, Object currValue) {
        if (currValue == null) {
            return opType == OpTypes.COUNT ? ValuePool.INTEGER_0
                                           : null;
        }
        return ((SetFunction) currValue).getValue(session);
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