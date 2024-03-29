package org.hsqldb;
import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.HsqlNameManager.SimpleName;
import org.hsqldb.ParserDQL.CompileContext;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.index.Index;
import org.hsqldb.lib.ArrayListIdentity;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.HsqlList;
import org.hsqldb.lib.OrderedHashSet;
import org.hsqldb.lib.OrderedIntHashSet;
import org.hsqldb.lib.Set;
import org.hsqldb.navigator.RowSetNavigatorData;
import org.hsqldb.persist.PersistentStore;
import org.hsqldb.result.Result;
import org.hsqldb.types.ArrayType;
import org.hsqldb.types.CharacterType;
import org.hsqldb.types.Collation;
import org.hsqldb.types.NullType;
import org.hsqldb.types.Type;
import org.hsqldb.types.Types;
public class Expression implements Cloneable {
    public static final int LEFT    = 0;
    public static final int RIGHT   = 1;
    public static final int UNARY   = 1;
    public static final int BINARY  = 2;
    public static final int TERNARY = 3;
    static final Expression[] emptyArray = new Expression[]{};
    static final Expression EXPR_TRUE  = new ExpressionLogical(true);
    static final Expression EXPR_FALSE = new ExpressionLogical(false);
    static final OrderedIntHashSet aggregateFunctionSet =
        new OrderedIntHashSet();
    static {
        aggregateFunctionSet.add(OpTypes.COUNT);
        aggregateFunctionSet.add(OpTypes.SUM);
        aggregateFunctionSet.add(OpTypes.MIN);
        aggregateFunctionSet.add(OpTypes.MAX);
        aggregateFunctionSet.add(OpTypes.AVG);
        aggregateFunctionSet.add(OpTypes.EVERY);
        aggregateFunctionSet.add(OpTypes.SOME);
        aggregateFunctionSet.add(OpTypes.STDDEV_POP);
        aggregateFunctionSet.add(OpTypes.STDDEV_SAMP);
        aggregateFunctionSet.add(OpTypes.VAR_POP);
        aggregateFunctionSet.add(OpTypes.VAR_SAMP);
        aggregateFunctionSet.add(OpTypes.GROUP_CONCAT);
        aggregateFunctionSet.add(OpTypes.ARRAY_AGG);
        aggregateFunctionSet.add(OpTypes.MEDIAN);
        aggregateFunctionSet.add(OpTypes.USER_AGGREGATE);
    }
    static final OrderedIntHashSet columnExpressionSet =
        new OrderedIntHashSet();
    static {
        columnExpressionSet.add(OpTypes.COLUMN);
    }
    static final OrderedIntHashSet subqueryExpressionSet =
        new OrderedIntHashSet();
    static {
        subqueryExpressionSet.add(OpTypes.ROW_SUBQUERY);
        subqueryExpressionSet.add(OpTypes.TABLE_SUBQUERY);
    }
    static final OrderedIntHashSet subqueryAggregateExpressionSet =
        new OrderedIntHashSet();
    static {
        subqueryAggregateExpressionSet.add(OpTypes.COUNT);
        subqueryAggregateExpressionSet.add(OpTypes.SUM);
        subqueryAggregateExpressionSet.add(OpTypes.MIN);
        subqueryAggregateExpressionSet.add(OpTypes.MAX);
        subqueryAggregateExpressionSet.add(OpTypes.AVG);
        subqueryAggregateExpressionSet.add(OpTypes.EVERY);
        subqueryAggregateExpressionSet.add(OpTypes.SOME);
        subqueryAggregateExpressionSet.add(OpTypes.STDDEV_POP);
        subqueryAggregateExpressionSet.add(OpTypes.STDDEV_SAMP);
        subqueryAggregateExpressionSet.add(OpTypes.VAR_POP);
        subqueryAggregateExpressionSet.add(OpTypes.VAR_SAMP);
        subqueryAggregateExpressionSet.add(OpTypes.GROUP_CONCAT);
        subqueryAggregateExpressionSet.add(OpTypes.ARRAY_AGG);
        subqueryAggregateExpressionSet.add(OpTypes.MEDIAN);
        subqueryAggregateExpressionSet.add(OpTypes.USER_AGGREGATE);
        subqueryAggregateExpressionSet.add(OpTypes.TABLE_SUBQUERY);
        subqueryAggregateExpressionSet.add(OpTypes.ROW_SUBQUERY);
    }
    static final OrderedIntHashSet functionExpressionSet =
        new OrderedIntHashSet();
    static {
        functionExpressionSet.add(OpTypes.SQL_FUNCTION);
        functionExpressionSet.add(OpTypes.FUNCTION);
    }
    static final OrderedIntHashSet emptyExpressionSet =
        new OrderedIntHashSet();
    protected int opType;
    protected int exprSubType;
    SimpleName alias;
    private boolean isAggregate;
    protected Object       valueData;
    protected Expression[] nodes;
    Type[]                 nodeDataTypes;
    SubQuery subQuery;
    boolean isCorrelated;
    int columnIndex = -1;
    protected Type dataType;
    int queryTableColumnIndex = -1;    
    int parameterIndex = -1;
    int rangePosition = -1;
    boolean isColumnEqual;
    boolean isSingleColumnCondition;
    Collation collation;
    Expression(int type) {
        opType = type;
        nodes  = emptyArray;
    }
    Expression(int type, SubQuery sq) {
        switch (type) {
            case OpTypes.ARRAY :
                opType = OpTypes.ARRAY;
                break;
            case OpTypes.ARRAY_SUBQUERY :
                opType = OpTypes.ARRAY_SUBQUERY;
                break;
            case OpTypes.TABLE_SUBQUERY :
                opType = OpTypes.TABLE_SUBQUERY;
                break;
            case OpTypes.ROW_SUBQUERY :
            case OpTypes.SCALAR_SUBQUERY :
                opType = OpTypes.ROW_SUBQUERY;
                break;
            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "Expression");
        }
        nodes    = emptyArray;
        subQuery = sq;
    }
    Expression(int type, Expression[] list) {
        this(type);
        this.nodes = list;
    }
    static String getContextSQL(Expression expression) {
        if (expression == null) {
            return null;
        }
        String ddl = expression.getSQL();
        switch (expression.opType) {
            case OpTypes.VALUE :
            case OpTypes.COLUMN :
            case OpTypes.ROW :
            case OpTypes.FUNCTION :
            case OpTypes.SQL_FUNCTION :
            case OpTypes.ALTERNATIVE :
            case OpTypes.CASEWHEN :
            case OpTypes.CAST :
                return ddl;
        }
        StringBuffer sb = new StringBuffer();
        ddl = sb.append('(').append(ddl).append(')').toString();
        return ddl;
    }
    public String getSQL() {
        StringBuffer sb = new StringBuffer(64);
        switch (opType) {
            case OpTypes.VALUE :
                if (valueData == null) {
                    return Tokens.T_NULL;
                }
                return dataType.convertToSQLString(valueData);
            case OpTypes.ROW :
                sb.append('(');
                for (int i = 0; i < nodes.length; i++) {
                    if (i > 0) {
                        sb.append(',');
                    }
                    sb.append(nodes[i].getSQL());
                }
                sb.append(')');
                return sb.toString();
            case OpTypes.VALUELIST :
                for (int i = 0; i < nodes.length; i++) {
                    if (i > 0) {
                        sb.append(',');
                    }
                    sb.append(nodes[i].getSQL());
                }
                return sb.toString();
        }
        switch (opType) {
            case OpTypes.ARRAY :
                sb.append(Tokens.T_ARRAY).append('[');
                for (int i = 0; i < nodes.length; i++) {
                    if (i > 0) {
                        sb.append(',');
                    }
                    sb.append(nodes[i].getSQL());
                }
                sb.append(']');
                break;
            case OpTypes.ARRAY_SUBQUERY :
            case OpTypes.ROW_SUBQUERY :
            case OpTypes.TABLE_SUBQUERY :
                sb.append('(');
                sb.append(')');
                break;
            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "Expression");
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
            case OpTypes.VALUE :
                sb.append("VALUE = ").append(valueData);
                sb.append(", TYPE = ").append(dataType.getNameString());
                return sb.toString();
            case OpTypes.ARRAY :
                sb.append("ARRAY ");
                return sb.toString();
            case OpTypes.ARRAY_SUBQUERY :
                sb.append("ARRAY SUBQUERY");
                return sb.toString();
            case OpTypes.ROW_SUBQUERY :
            case OpTypes.TABLE_SUBQUERY :
                sb.append("QUERY ");
                sb.append(subQuery.queryExpression.describe(session, blanks));
                return sb.toString();
            case OpTypes.ROW :
                sb.append("ROW = ");
                for (int i = 0; i < nodes.length; i++) {
                    sb.append(nodes[i].describe(session, blanks + 1));
                    sb.append(' ');
                }
                break;
            case OpTypes.VALUELIST :
                sb.append("VALUELIST ");
                for (int i = 0; i < nodes.length; i++) {
                    sb.append(nodes[i].describe(session, blanks + 1));
                    sb.append(' ');
                }
                break;
        }
        return sb.toString();
    }
    void setDataType(Session session, Type type) {
        if (opType == OpTypes.VALUE) {
            valueData = type.convertToType(session, valueData, dataType);
        }
        dataType = type;
    }
    public boolean equals(Expression other) {
        if (other == this) {
            return true;
        }
        if (other == null) {
            return false;
        }
        if (opType != other.opType || exprSubType != other.exprSubType
                || !equals(dataType, other.dataType)) {
            return false;
        }
        switch (opType) {
            case OpTypes.SIMPLE_COLUMN :
                return this.columnIndex == other.columnIndex;
            case OpTypes.VALUE :
                return equals(valueData, other.valueData);
            case OpTypes.ARRAY :
            case OpTypes.ARRAY_SUBQUERY :
            case OpTypes.ROW_SUBQUERY :
            case OpTypes.TABLE_SUBQUERY :
                return (subQuery.queryExpression.isEquivalent(
                    other.subQuery.queryExpression));
            default :
                return equals(nodes, other.nodes)
                       && equals(subQuery, other.subQuery);
        }
    }
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if (other instanceof Expression) {
            return equals((Expression) other);
        }
        return false;
    }
    public int hashCode() {
        int val = opType + exprSubType;
        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] != null) {
                val += nodes[i].hashCode();
            }
        }
        return val;
    }
    static boolean equals(Object o1, Object o2) {
        if (o1 == o2) {
            return true;
        }
        return (o1 == null) ? o2 == null
                            : o1.equals(o2);
    }
    static boolean equals(Expression[] row1, Expression[] row2) {
        if (row1 == row2) {
            return true;
        }
        if (row1.length != row2.length) {
            return false;
        }
        int len = row1.length;
        for (int i = 0; i < len; i++) {
            Expression e1     = row1[i];
            Expression e2     = row2[i];
            boolean    equals = (e1 == null) ? e2 == null
                                             : e1.equals(e2);
            if (!equals) {
                return false;
            }
        }
        return true;
    }
    boolean isComposedOf(Expression exprList[], int start, int end,
                         OrderedIntHashSet excludeSet) {
        if (opType == OpTypes.VALUE) {
            return true;
        }
        if (excludeSet.contains(opType)) {
            return true;
        }
        for (int i = start; i < end; i++) {
            if (equals(exprList[i])) {
                return true;
            }
        }
        switch (opType) {
            case OpTypes.LIKE :
            case OpTypes.MATCH_SIMPLE :
            case OpTypes.MATCH_PARTIAL :
            case OpTypes.MATCH_FULL :
            case OpTypes.MATCH_UNIQUE_SIMPLE :
            case OpTypes.MATCH_UNIQUE_PARTIAL :
            case OpTypes.MATCH_UNIQUE_FULL :
            case OpTypes.UNIQUE :
            case OpTypes.EXISTS :
            case OpTypes.ARRAY :
            case OpTypes.ARRAY_SUBQUERY :
            case OpTypes.TABLE_SUBQUERY :
            case OpTypes.ROW_SUBQUERY :
            case OpTypes.COUNT :
            case OpTypes.SUM :
            case OpTypes.MIN :
            case OpTypes.MAX :
            case OpTypes.AVG :
            case OpTypes.EVERY :
            case OpTypes.SOME :
            case OpTypes.STDDEV_POP :
            case OpTypes.STDDEV_SAMP :
            case OpTypes.VAR_POP :
            case OpTypes.VAR_SAMP :
                return false;
        }
        if (nodes.length == 0) {
            return false;
        }
        boolean result = true;
        for (int i = 0; i < nodes.length; i++) {
            result &= (nodes[i] == null
                       || nodes[i].isComposedOf(exprList, start, end,
                                                excludeSet));
        }
        return result;
    }
    boolean isComposedOf(OrderedHashSet expressions,
                         OrderedIntHashSet excludeSet) {
        if (opType == OpTypes.VALUE || opType == OpTypes.DYNAMIC_PARAM
                || opType == OpTypes.PARAMETER || opType == OpTypes.VARIABLE) {
            return true;
        }
        if (excludeSet.contains(opType)) {
            return true;
        }
        for (int i = 0; i < expressions.size(); i++) {
            if (equals(expressions.get(i))) {
                return true;
            }
        }
        switch (opType) {
            case OpTypes.COUNT :
            case OpTypes.SUM :
            case OpTypes.MIN :
            case OpTypes.MAX :
            case OpTypes.AVG :
            case OpTypes.EVERY :
            case OpTypes.SOME :
            case OpTypes.STDDEV_POP :
            case OpTypes.STDDEV_SAMP :
            case OpTypes.VAR_POP :
            case OpTypes.VAR_SAMP :
                return false;
        }
        if (nodes.length == 0) {
            return false;
        }
        boolean result = true;
        for (int i = 0; i < nodes.length; i++) {
            result &= (nodes[i] == null
                       || nodes[i].isComposedOf(expressions, excludeSet));
        }
        return result;
    }
    Expression replaceColumnReferences(RangeVariable range,
                                       Expression[] list) {
        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] == null) {
                continue;
            }
            nodes[i] = nodes[i].replaceColumnReferences(range, list);
        }
        if (subQuery != null && subQuery.queryExpression != null) {
            subQuery.queryExpression.replaceColumnReference(range, list);
        }
        return this;
    }
    void replaceRangeVariables(RangeVariable[] ranges,
                               RangeVariable[] newRanges) {
        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] == null) {
                continue;
            }
            nodes[i].replaceRangeVariables(ranges, newRanges);
        }
        if (subQuery != null && subQuery.queryExpression != null) {
            subQuery.queryExpression.replaceRangeVariables(ranges, newRanges);
        }
    }
    void resetColumnReferences() {
        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] == null) {
                continue;
            }
            nodes[i].resetColumnReferences();
        }
    }
    void convertToSimpleColumn(OrderedHashSet expressions,
                               OrderedHashSet replacements) {
        if (opType == OpTypes.VALUE) {
            return;
        }
        if (opType == OpTypes.SIMPLE_COLUMN) {
            return;
        }
        int index = expressions.getIndex(this);
        if (index != -1) {
            Expression e = (Expression) replacements.get(index);
            nodes         = emptyArray;
            opType        = OpTypes.SIMPLE_COLUMN;
            columnIndex   = e.columnIndex;
            rangePosition = e.rangePosition;
            return;
        }
        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] == null) {
                continue;
            }
            nodes[i].convertToSimpleColumn(expressions, replacements);
        }
    }
    boolean isAggregate() {
        return isAggregate;
    }
    void setAggregate() {
        isAggregate = true;
    }
    boolean isSelfAggregate() {
        return false;
    }
    void setAlias(SimpleName name) {
        alias = name;
    }
    String getAlias() {
        if (alias != null) {
            return alias.name;
        }
        return "";
    }
    SimpleName getSimpleName() {
        if (alias != null) {
            return alias;
        }
        return null;
    }
    public int getType() {
        return opType;
    }
    Expression getLeftNode() {
        return nodes.length > 0 ? nodes[LEFT]
                                : null;
    }
    Expression getRightNode() {
        return nodes.length > 1 ? nodes[RIGHT]
                                : null;
    }
    void setLeftNode(Expression e) {
        nodes[LEFT] = e;
    }
    void setRightNode(Expression e) {
        nodes[RIGHT] = e;
    }
    void setSubType(int subType) {
        exprSubType = subType;
    }
    RangeVariable getRangeVariable() {
        return null;
    }
    Expression replaceAliasInOrderBy(Expression[] columns, int length) {
        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] == null) {
                continue;
            }
            nodes[i] = nodes[i].replaceAliasInOrderBy(columns, length);
        }
        return this;
    }
    int findMatchingRangeVariableIndex(RangeVariable[] rangeVarArray) {
        return -1;
    }
    void collectRangeVariables(RangeVariable[] rangeVariables, Set set) {
        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] != null) {
                nodes[i].collectRangeVariables(rangeVariables, set);
            }
        }
        if (subQuery != null && subQuery.queryExpression != null) {
            HsqlList unresolvedExpressions =
                subQuery.queryExpression.getUnresolvedExpressions();
            if (unresolvedExpressions != null) {
                for (int i = 0; i < unresolvedExpressions.size(); i++) {
                    Expression e = (Expression) unresolvedExpressions.get(i);
                    e.collectRangeVariables(rangeVariables, set);
                }
            }
        }
    }
    void collectObjectNames(Set set) {
        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] != null) {
                nodes[i].collectObjectNames(set);
            }
        }
        if (subQuery != null) {
            if (subQuery.queryExpression != null) {
                subQuery.queryExpression.collectObjectNames(set);
            }
        }
    }
    boolean hasReference(RangeVariable range) {
        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] != null) {
                if (nodes[i].hasReference(range)) {
                    return true;
                }
            }
        }
        if (subQuery != null && subQuery.queryExpression != null) {
            if (subQuery.queryExpression.hasReference(range)) {
                return true;
            }
        }
        return false;
    }
    public HsqlList resolveColumnReferences(Session session,
            RangeVariable[] rangeVarArray, HsqlList unresolvedSet) {
        return resolveColumnReferences(session, rangeVarArray,
                                       rangeVarArray.length, unresolvedSet,
                                       true);
    }
    public HsqlList resolveColumnReferences(Session session,
            RangeVariable[] rangeVarArray, int rangeCount,
            HsqlList unresolvedSet, boolean acceptsSequences) {
        if (opType == OpTypes.VALUE) {
            return unresolvedSet;
        }
        switch (opType) {
            case OpTypes.TABLE :
            case OpTypes.VALUELIST : {
                HsqlList localSet = null;
                for (int i = 0; i < nodes.length; i++) {
                    if (nodes[i] == null) {
                        continue;
                    }
                    localSet = nodes[i].resolveColumnReferences(session,
                            RangeVariable.emptyArray, localSet);
                }
                if (localSet != null) {
                    isCorrelated = true;
                    if (subQuery != null) {
                        subQuery.setCorrelated();
                    }
                    unresolvedSet = Expression.resolveColumnSet(session,
                            rangeVarArray, rangeCount, localSet,
                            unresolvedSet);
                }
                return unresolvedSet;
            }
        }
        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] == null) {
                continue;
            }
            unresolvedSet = nodes[i].resolveColumnReferences(session,
                    rangeVarArray, rangeCount, unresolvedSet,
                    acceptsSequences);
        }
        switch (opType) {
            case OpTypes.ARRAY :
                break;
            case OpTypes.ARRAY_SUBQUERY :
            case OpTypes.ROW_SUBQUERY :
            case OpTypes.TABLE_SUBQUERY : {
                QueryExpression queryExpression = subQuery.queryExpression;
                if (!queryExpression.areColumnsResolved()) {
                    isCorrelated = true;
                    subQuery.setCorrelated();
                    if (unresolvedSet == null) {
                        unresolvedSet = new ArrayListIdentity();
                    }
                    unresolvedSet.addAll(
                        queryExpression.getUnresolvedExpressions());
                }
                break;
            }
            default :
        }
        return unresolvedSet;
    }
    public OrderedHashSet getUnkeyedColumns(OrderedHashSet unresolvedSet) {
        if (opType == OpTypes.VALUE) {
            return unresolvedSet;
        }
        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] == null) {
                continue;
            }
            unresolvedSet = nodes[i].getUnkeyedColumns(unresolvedSet);
        }
        switch (opType) {
            case OpTypes.ARRAY :
            case OpTypes.ARRAY_SUBQUERY :
            case OpTypes.ROW_SUBQUERY :
            case OpTypes.TABLE_SUBQUERY :
                if (subQuery != null) {
                    if (unresolvedSet == null) {
                        unresolvedSet = new OrderedHashSet();
                    }
                    unresolvedSet.add(this);
                }
                break;
        }
        return unresolvedSet;
    }
    public void resolveTypes(Session session, Expression parent) {
        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] != null) {
                nodes[i].resolveTypes(session, this);
            }
        }
        switch (opType) {
            case OpTypes.VALUE :
                break;
            case OpTypes.VALUELIST :
                break;
            case OpTypes.ROW :
                nodeDataTypes = new Type[nodes.length];
                for (int i = 0; i < nodes.length; i++) {
                    if (nodes[i] != null) {
                        nodeDataTypes[i] = nodes[i].dataType;
                    }
                }
                break;
            case OpTypes.ARRAY : {
                boolean hasUndefined = false;
                Type    nodeDataType = null;
                for (int i = 0; i < nodes.length; i++) {
                    if (nodes[i].dataType == null) {
                        hasUndefined = true;
                    } else {
                        nodeDataType =
                            Type.getAggregateType(nodeDataType,
                                                  nodes[i].dataType);
                    }
                }
                if (hasUndefined) {
                    for (int i = 0; i < nodes.length; i++) {
                        if (nodes[i].dataType == null) {
                            nodes[i].dataType = nodeDataType;
                        }
                    }
                }
                dataType = new ArrayType(nodeDataType, nodes.length);
                return;
            }
            case OpTypes.ARRAY_SUBQUERY : {
                QueryExpression queryExpression = subQuery.queryExpression;
                queryExpression.resolveTypes(session);
                subQuery.prepareTable(session);
                nodeDataTypes = queryExpression.getColumnTypes();
                dataType      = nodeDataTypes[0];
                if (nodeDataTypes.length > 1) {
                    throw Error.error(ErrorCode.X_42564);
                }
                dataType = new ArrayType(dataType, nodes.length);
                break;
            }
            case OpTypes.ROW_SUBQUERY :
            case OpTypes.TABLE_SUBQUERY : {
                QueryExpression queryExpression = subQuery.queryExpression;
                queryExpression.resolveTypes(session);
                subQuery.prepareTable(session);
                nodeDataTypes = queryExpression.getColumnTypes();
                dataType      = nodeDataTypes[0];
                break;
            }
            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "Expression");
        }
    }
    void setAsConstantValue(Session session) {
        valueData = getValue(session);
        opType    = OpTypes.VALUE;
        nodes     = emptyArray;
    }
    void setAsConstantValue(Object value) {
        valueData = value;
        opType    = OpTypes.VALUE;
        nodes     = emptyArray;
    }
    void prepareTable(Session session, Expression row, int degree) {
        if (nodeDataTypes != null) {
            return;
        }
        for (int i = 0; i < nodes.length; i++) {
            Expression e = nodes[i];
            if (e.opType == OpTypes.ROW) {
                if (degree != e.nodes.length) {
                    throw Error.error(ErrorCode.X_42564);
                }
            } else if (degree == 1) {
                nodes[i]       = new Expression(OpTypes.ROW);
                nodes[i].nodes = new Expression[]{ e };
            } else {
                throw Error.error(ErrorCode.X_42564);
            }
        }
        nodeDataTypes = new Type[degree];
        for (int j = 0; j < degree; j++) {
            Type    type                  = row == null ? null
                                                        : row.nodes[j]
                                                            .dataType;
            boolean hasUresolvedParameter = row == null ? false
                                                        : row.nodes[j]
                                                            .isUnresolvedParam();
            for (int i = 0; i < nodes.length; i++) {
                type = Type.getAggregateType(nodes[i].nodes[j].dataType, type);
                hasUresolvedParameter |= nodes[i].nodes[j].isUnresolvedParam();
            }
            if (type == null) {
                type = Type.SQL_VARCHAR_DEFAULT;
            }
            int typeCode = type.typeCode;
            if (hasUresolvedParameter && type.isCharacterType()) {
                if (typeCode == Types.SQL_CHAR
                        || type.precision
                           < Type.SQL_VARCHAR_DEFAULT.precision) {
                    if (typeCode == Types.SQL_CHAR) {
                        typeCode = Types.SQL_VARCHAR;
                    }
                    long precision =
                        Math.max(Type.SQL_VARCHAR_DEFAULT.precision,
                                 type.precision);
                    type = CharacterType.getCharacterType(typeCode, precision,
                                                          type.getCollation());
                }
            }
            nodeDataTypes[j] = type;
            if (row != null && row.nodes[j].isUnresolvedParam()) {
                row.nodes[j].dataType = type;
            }
            for (int i = 0; i < nodes.length; i++) {
                if (nodes[i].nodes[j].isUnresolvedParam()) {
                    nodes[i].nodes[j].dataType = nodeDataTypes[j];
                    continue;
                }
                if (nodes[i].nodes[j].opType == OpTypes.VALUE) {
                    if (nodes[i].nodes[j].valueData == null) {
                        nodes[i].nodes[j].dataType = nodeDataTypes[j];
                    }
                }
            }
            if (nodeDataTypes[j].isCharacterType()
                    && !((CharacterType) nodeDataTypes[j])
                        .isEqualIdentical()) {
            }
        }
    }
    void insertValuesIntoSubqueryTable(Session session,
                                       PersistentStore store) {
        for (int i = 0; i < nodes.length; i++) {
            Object[] data = nodes[i].getRowValue(session);
            for (int j = 0; j < nodeDataTypes.length; j++) {
                data[j] = nodeDataTypes[j].convertToType(session, data[j],
                        nodes[i].nodes[j].dataType);
            }
            Row row = (Row) store.getNewCachedObject(session, data, false);
            try {
                store.indexRow(session, row);
            } catch (HsqlException e) {}
        }
    }
    String getColumnName() {
        return getAlias();
    }
    ColumnSchema getColumn() {
        return null;
    }
    int getColumnIndex() {
        return columnIndex;
    }
    Type getDataType() {
        return dataType;
    }
    byte getNullability() {
        return SchemaObject.Nullability.NULLABLE_UNKNOWN;
    }
    Type getNodeDataType(int i) {
        if (nodeDataTypes == null) {
            if (i > 0) {
                throw Error.runtimeError(ErrorCode.U_S0500, "Expression");
            }
            return dataType;
        } else {
            return nodeDataTypes[i];
        }
    }
    Type[] getNodeDataTypes() {
        if (nodeDataTypes == null) {
            return new Type[]{ dataType };
        } else {
            return nodeDataTypes;
        }
    }
    int getDegree() {
        switch (opType) {
            case OpTypes.ROW :
                return nodes.length;
            case OpTypes.TABLE :
            case OpTypes.ROW_SUBQUERY :
            case OpTypes.TABLE_SUBQUERY :
                if (subQuery == null) {
                    return nodeDataTypes.length;
                }
                return subQuery.queryExpression.getColumnCount();
            default :
                return 1;
        }
    }
    public Table getTable() {
        return subQuery == null ? null
                                : subQuery.getTable();
    }
    public void materialise(Session session) {
        if (subQuery == null) {
            return;
        }
        if (subQuery.isCorrelated()) {
            subQuery.materialiseCorrelated(session);
        } else {
            subQuery.materialise(session);
        }
    }
    Object getValue(Session session, Type type) {
        Object o = getValue(session);
        if (o == null || dataType == type) {
            return o;
        }
        return type.convertToType(session, o, dataType);
    }
    public Object getConstantValueNoCheck(Session session) {
        try {
            return getValue(session);
        } catch (HsqlException e) {
            return null;
        }
    }
    public Object[] getRowValue(Session session) {
        switch (opType) {
            case OpTypes.ROW : {
                Object[] data = new Object[nodes.length];
                for (int i = 0; i < nodes.length; i++) {
                    data[i] = nodes[i].getValue(session);
                }
                return data;
            }
            case OpTypes.ROW_SUBQUERY :
            case OpTypes.TABLE_SUBQUERY : {
                return subQuery.queryExpression.getValues(session);
            }
            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "Expression");
        }
    }
    public Object getValue(Session session) {
        switch (opType) {
            case OpTypes.VALUE :
                return valueData;
            case OpTypes.SIMPLE_COLUMN : {
                Object value =
                    session.sessionContext.rangeIterators[rangePosition]
                        .getCurrent(columnIndex);
                return value;
            }
            case OpTypes.ROW : {
                if (nodes.length == 1) {
                    return nodes[0].getValue(session);
                }
                Object[] row = new Object[nodes.length];
                for (int i = 0; i < nodes.length; i++) {
                    row[i] = nodes[i].getValue(session);
                }
                return row;
            }
            case OpTypes.ARRAY : {
                Object[] array = new Object[nodes.length];
                for (int i = 0; i < nodes.length; i++) {
                    array[i] = nodes[i].getValue(session);
                }
                return array;
            }
            case OpTypes.ARRAY_SUBQUERY : {
                subQuery.materialiseCorrelated(session);
                RowSetNavigatorData nav   = subQuery.getNavigator(session);
                int                 size  = nav.getSize();
                Object[]            array = new Object[size];
                nav.beforeFirst();
                for (int i = 0; nav.hasNext(); i++) {
                    Object[] data = nav.getNextRowData();
                    array[i] = data[0];
                }
                return array;
            }
            case OpTypes.TABLE_SUBQUERY :
            case OpTypes.ROW_SUBQUERY : {
                subQuery.materialiseCorrelated(session);
                Object[] value = subQuery.getValues(session);
                if (value.length == 1) {
                    return ((Object[]) value)[0];
                }
                return value;
            }
            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "Expression");
        }
    }
    public Result getResult(Session session) {
        switch (opType) {
            case OpTypes.ARRAY : {
                RowSetNavigatorData navigator = subQuery.getNavigator(session);
                Object[]            array = new Object[navigator.getSize()];
                navigator.beforeFirst();
                for (int i = 0; navigator.hasNext(); i++) {
                    Object[] data = navigator.getNext();
                    array[i] = data[0];
                }
                return Result.newPSMResult(array);
            }
            case OpTypes.TABLE_SUBQUERY : {
                subQuery.materialiseCorrelated(session);
                RowSetNavigatorData navigator = subQuery.getNavigator(session);
                Result              result    = Result.newResult(navigator);
                result.metaData = subQuery.queryExpression.getMetaData();
                return result;
            }
            default : {
                Object value = getValue(session);
                return Result.newPSMResult(value);
            }
        }
    }
    public boolean testCondition(Session session) {
        return Boolean.TRUE.equals(getValue(session));
    }
    static int countNulls(Object[] a) {
        int nulls = 0;
        for (int i = 0; i < a.length; i++) {
            if (a[i] == null) {
                nulls++;
            }
        }
        return nulls;
    }
    public boolean isIndexable(RangeVariable range) {
        return false;
    }
    static void convertToType(Session session, Object[] data, Type[] dataType,
                              Type[] newType) {
        for (int i = 0; i < data.length; i++) {
            data[i] = newType[i].convertToType(session, data[i], dataType[i]);
        }
    }
    static QuerySpecification getCheckSelect(Session session, Table t,
            Expression e) {
        CompileContext compileContext = new CompileContext(session, null);
        compileContext.reset(0);
        QuerySpecification s = new QuerySpecification(compileContext);
        RangeVariable[] ranges = new RangeVariable[]{
            new RangeVariable(t, null, null, null, compileContext) };
        e.resolveCheckOrGenExpression(session, ranges, true);
        s.exprColumns    = new Expression[1];
        s.exprColumns[0] = EXPR_TRUE;
        s.rangeVariables = ranges;
        if (Type.SQL_BOOLEAN != e.getDataType()) {
            throw Error.error(ErrorCode.X_42568);
        }
        Expression condition = new ExpressionLogical(OpTypes.NOT, e);
        s.queryCondition = condition;
        s.resolveReferences(session, RangeVariable.emptyArray);
        s.resolveTypes(session);
        return s;
    }
    public void resolveCheckOrGenExpression(Session session,
            RangeVariable[] ranges, boolean isCheck) {
        boolean        nonDeterministic = false;
        OrderedHashSet set              = new OrderedHashSet();
        HsqlList unresolved = resolveColumnReferences(session, ranges, null);
        ExpressionColumn.checkColumnsResolved(unresolved);
        resolveTypes(session, null);
        collectAllExpressions(set, Expression.subqueryAggregateExpressionSet,
                              Expression.emptyExpressionSet);
        if (!set.isEmpty()) {
            throw Error.error(ErrorCode.X_42512);
        }
        collectAllExpressions(set, Expression.functionExpressionSet,
                              Expression.emptyExpressionSet);
        for (int i = 0; i < set.size(); i++) {
            Expression current = (Expression) set.get(i);
            if (current.opType == OpTypes.FUNCTION) {
                if (!((FunctionSQLInvoked) current).isDeterministic()) {
                    throw Error.error(ErrorCode.X_42512);
                }
            }
            if (current.opType == OpTypes.SQL_FUNCTION) {
                if (!((FunctionSQL) current).isDeterministic()) {
                    if (isCheck) {
                        nonDeterministic = true;
                        continue;
                    }
                    throw Error.error(ErrorCode.X_42512);
                }
            }
        }
        if (isCheck && nonDeterministic) {
            HsqlArrayList list = new HsqlArrayList();
            RangeVariableResolver.decomposeAndConditions(session, this, list);
            for (int i = 0; i < list.size(); i++) {
                nonDeterministic = true;
                Expression e = (Expression) list.get(i);
                Expression e1;
                if (e instanceof ExpressionLogical) {
                    boolean b = ((ExpressionLogical) e).convertToSmaller();
                    if (!b) {
                        break;
                    }
                    e1 = e.getRightNode();
                    e  = e.getLeftNode();
                    if (!e.dataType.isDateTimeType()) {
                        nonDeterministic = true;
                        break;
                    }
                    if (e.hasNonDeterministicFunction()) {
                        nonDeterministic = true;
                        break;
                    }
                    if (e1 instanceof ExpressionArithmetic) {
                        if (opType == OpTypes.ADD) {
                            if (e1.getRightNode()
                                    .hasNonDeterministicFunction()) {
                                e1.swapLeftAndRightNodes();
                            }
                        } else if (opType == OpTypes.SUBTRACT) {}
                        else {
                            break;
                        }
                        if (e1.getRightNode().hasNonDeterministicFunction()) {
                            break;
                        }
                        e1 = e1.getLeftNode();
                    }
                    if (e1.opType == OpTypes.SQL_FUNCTION) {
                        FunctionSQL function = (FunctionSQL) e1;
                        switch (function.funcType) {
                            case FunctionSQL.FUNC_CURRENT_DATE :
                            case FunctionSQL.FUNC_CURRENT_TIMESTAMP :
                            case FunctionSQL.FUNC_LOCALTIMESTAMP :
                                nonDeterministic = false;
                                continue;
                            default :
                                break;
                        }
                        break;
                    }
                    break;
                } else {
                    break;
                }
            }
            if (nonDeterministic) {
                throw Error.error(ErrorCode.X_42512);
            }
        }
        set.clear();
        collectObjectNames(set);
        for (int i = 0; i < set.size(); i++) {
            HsqlName name = (HsqlName) set.get(i);
            switch (name.type) {
                case SchemaObject.COLUMN : {
                    if (isCheck) {
                        break;
                    }
                    int colIndex = ranges[0].rangeTable.findColumn(name.name);
                    ColumnSchema column =
                        ranges[0].rangeTable.getColumn(colIndex);
                    if (column.isGenerated()) {
                        throw Error.error(ErrorCode.X_42512);
                    }
                    break;
                }
                case SchemaObject.SEQUENCE : {
                    throw Error.error(ErrorCode.X_42512);
                }
                case SchemaObject.SPECIFIC_ROUTINE : {
                    Routine routine =
                        (Routine) session.database.schemaManager
                            .getSchemaObject(name);
                    if (!routine.isDeterministic()) {
                        throw Error.error(ErrorCode.X_42512);
                    }
                    int impact = routine.getDataImpact();
                    if (impact == Routine.READS_SQL
                            || impact == Routine.MODIFIES_SQL) {
                        throw Error.error(ErrorCode.X_42512);
                    }
                }
            }
        }
        set.clear();
    }
    boolean isUnresolvedParam() {
        return false;
    }
    boolean isDynamicParam() {
        return false;
    }
    boolean hasNonDeterministicFunction() {
        OrderedHashSet list = null;
        list = collectAllExpressions(list, Expression.functionExpressionSet,
                                     Expression.emptyExpressionSet);
        if (list == null) {
            return false;
        }
        for (int j = 0; j < list.size(); j++) {
            Expression current = (Expression) list.get(j);
            if (current.opType == OpTypes.FUNCTION) {
                if (!((FunctionSQLInvoked) current).isDeterministic()) {
                    return true;
                }
            } else if (current.opType == OpTypes.SQL_FUNCTION) {
                if (!((FunctionSQL) current).isDeterministic()) {
                    return true;
                }
            }
        }
        return false;
    }
    void swapLeftAndRightNodes() {
        Expression temp = nodes[LEFT];
        nodes[LEFT]  = nodes[RIGHT];
        nodes[RIGHT] = temp;
    }
    void setAttributesAsColumn(ColumnSchema column, boolean isWritable) {
        throw Error.runtimeError(ErrorCode.U_S0500, "Expression");
    }
    String getValueClassName() {
        Type type = dataType == null ? NullType.getNullType()
                                     : dataType;
        return type.getJDBCClassName();
    }
    OrderedHashSet collectAllExpressions(OrderedHashSet set,
                                         OrderedIntHashSet typeSet,
                                         OrderedIntHashSet stopAtTypeSet) {
        if (stopAtTypeSet.contains(opType)) {
            return set;
        }
        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] != null) {
                set = nodes[i].collectAllExpressions(set, typeSet,
                                                     stopAtTypeSet);
            }
        }
        if (typeSet.contains(opType)) {
            if (set == null) {
                set = new OrderedHashSet();
            }
            set.add(this);
        }
        if (subQuery != null && subQuery.queryExpression != null) {
            set = subQuery.queryExpression.collectAllExpressions(set, typeSet,
                    stopAtTypeSet);
        }
        return set;
    }
    public OrderedHashSet getSubqueries() {
        return collectAllSubqueries(null);
    }
    OrderedHashSet collectAllSubqueries(OrderedHashSet set) {
        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] != null) {
                set = nodes[i].collectAllSubqueries(set);
            }
        }
        if (subQuery != null) {
            if (set == null) {
                set = new OrderedHashSet();
            }
            set.add(subQuery);
            if (subQuery.queryExpression != null) {
                OrderedHashSet tempSet =
                    subQuery.queryExpression.getSubqueries();
                set = OrderedHashSet.addAll(set, tempSet);
            }
        }
        return set;
    }
    public boolean isCorrelated() {
        if (subQuery == null) {
            return false;
        }
        return subQuery.isCorrelated();
    }
    public void checkValidCheckConstraint() {
        OrderedHashSet set = null;
        set = collectAllExpressions(set, subqueryAggregateExpressionSet,
                                    emptyExpressionSet);
        if (set != null && !set.isEmpty()) {
            throw Error.error(ErrorCode.X_0A000,
                              "subquery in check constraint");
        }
    }
    static HsqlList resolveColumnSet(Session session,
                                     RangeVariable[] rangeVars,
                                     int rangeCount, HsqlList sourceSet,
                                     HsqlList targetSet) {
        if (sourceSet == null) {
            return targetSet;
        }
        for (int i = 0; i < sourceSet.size(); i++) {
            Expression e = (Expression) sourceSet.get(i);
            targetSet = e.resolveColumnReferences(session, rangeVars,
                                                  rangeCount, targetSet,
                                                  false);
        }
        return targetSet;
    }
    boolean isTargetRangeVariables(RangeVariable range) {
        return false;
    }
    RangeVariable[] getJoinRangeVariables(RangeVariable[] ranges) {
        return RangeVariable.emptyArray;
    }
    double costFactor(Session session, RangeVariable range, int operation) {
        return Index.minimumSelectivity;
    }
    Expression getIndexableExpression(RangeVariable rangeVar) {
        return null;
    }
    public Expression duplicate() {
        Expression e = null;
        try {
            e       = (Expression) super.clone();
            e.nodes = (Expression[]) nodes.clone();
            for (int i = 0; i < nodes.length; i++) {
                if (nodes[i] != null) {
                    e.nodes[i] = nodes[i].duplicate();
                }
            }
        } catch (CloneNotSupportedException ex) {
            throw Error.runtimeError(ErrorCode.U_S0500, "Expression");
        }
        return e;
    }
    void replaceNode(Expression existing, Expression replacement) {
        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] == existing) {
                replacement.alias = nodes[i].alias;
                nodes[i]          = replacement;
                return;
            }
        }
        throw Error.runtimeError(ErrorCode.U_S0500, "Expression");
    }
    public Object updateAggregatingValue(Session session, Object currValue) {
        throw Error.runtimeError(ErrorCode.U_S0500, "Expression");
    }
    public Object getAggregatedValue(Session session, Object currValue) {
        throw Error.runtimeError(ErrorCode.U_S0500, "Expression");
    }
    public Expression getCondition() {
        return null;
    }
    public boolean hasCondition() {
        return false;
    }
    public void setCondition(Expression e) {
        throw Error.runtimeError(ErrorCode.U_S0500, "Expression");
    }
    public void setCollation(Collation collation) {
        this.collation = collation;
    }
}