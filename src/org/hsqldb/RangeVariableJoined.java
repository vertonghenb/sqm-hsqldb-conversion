package org.hsqldb;
import org.hsqldb.lib.OrderedHashSet;
import org.hsqldb.HsqlNameManager.SimpleName;
import org.hsqldb.RangeVariable.RangeIteratorMain;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.error.Error;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.HashSet;
import org.hsqldb.index.Index;
import org.hsqldb.store.ValuePool;
import org.hsqldb.ParserDQL.CompileContext;
public class RangeVariableJoined extends RangeVariable {
    RangeVariable[] rangeArray;
    RangeVariableJoined(Table table, SimpleName alias,
                        OrderedHashSet columnList,
                        SimpleName[] columnNameList,
                        CompileContext compileContext) {
        super(table, alias, columnList, columnNameList, compileContext);
        QuerySpecification qs =
            (QuerySpecification) this.rangeTable.getQueryExpression();
        this.rangeArray = qs.rangeVariables;
    }
    public void setRangeTableVariables() {
        super.setRangeTableVariables();
    }
    public RangeVariable duplicate() {
        RangeVariable r = null;
        try {
            r = (RangeVariable) super.clone();
        } catch (CloneNotSupportedException ex) {
            throw Error.runtimeError(ErrorCode.U_S0500, "RangeVariable");
        }
        r.resetConditions();
        return r;
    }
    void setJoinType(boolean isLeft, boolean isRight) {
        super.setJoinType(isLeft, isRight);
    }
    public void addNamedJoinColumns(OrderedHashSet columns) {
        super.addNamedJoinColumns(columns);
    }
    public void addColumn(int columnIndex) {
        super.addColumn(columnIndex);
    }
    public void addAllColumns() {
        super.addAllColumns();
    }
    void addNamedJoinColumnExpression(String name, Expression e) {
        super.addNamedJoinColumnExpression(name, e);
    }
    ExpressionColumn getColumnExpression(String name) {
        return super.getColumnExpression(name);
    }
    Table getTable() {
        return super.getTable();
    }
    boolean hasSingleIndexCondition() {
        return super.hasSingleIndexCondition();
    }
    boolean setDistinctColumnsOnIndex(int[] colMap) {
        return super.setDistinctColumnsOnIndex(colMap);
    }
    Index getSortIndex() {
        return super.getSortIndex();
    }
    boolean setSortIndex(Index index, boolean reversed) {
        return super.setSortIndex(index, reversed);
    }
    boolean reverseOrder() {
        return super.reverseOrder();
    }
    public OrderedHashSet getColumnNames() {
        return super.getColumnNames();
    }
    public OrderedHashSet getUniqueColumnNameSet() {
        return super.getUniqueColumnNameSet();
    }
    public int findColumn(ExpressionColumn e) {
        if (tableAlias != null) {
            return super.findColumn(e);
        }
        int count = 0;
        for (int i = 0; i < rangeArray.length; i++) {
            int colIndex = rangeArray[i].findColumn(e);
            if (colIndex > -1) {
                return count + colIndex;
            }
            count += rangeArray[i].rangeTable.getColumnCount();
        }
        return -1;
    }
    public int findColumn(String columnName) {
        return super.findColumn(columnName);
    }
    ColumnSchema getColumn(String columnName) {
        return super.getColumn(columnName);
    }
    ColumnSchema getColumn(int i) {
        return super.getColumn(i);
    }
    public SimpleName getColumnAlias(int i) {
        return super.getColumnAlias(i);
    }
    boolean hasColumnAlias() {
        return super.hasColumnAlias();
    }
    SimpleName getTableAlias() {
        return super.getTableAlias();
    }
    boolean resolvesTableName(ExpressionColumn e) {
        if (tableAlias != null) {
            return super.resolvesTableName(e);
        }
        for (int i = 0; i < rangeArray.length; i++) {
            if (rangeArray[i].resolvesTableName(e)) {
                return true;
            }
        }
        return false;
    }
    public boolean resolvesTableName(String name) {
        if (tableAlias != null) {
            return super.resolvesTableName(name);
        }
        for (int i = 0; i < rangeArray.length; i++) {
            if (rangeArray[i].resolvesTableName(name)) {
                return true;
            }
        }
        return false;
    }
    boolean resolvesSchemaName(String name) {
        return super.resolvesSchemaName(name);
    }
    void addTableColumns(HsqlArrayList exprList) {
        super.addTableColumns(exprList);
    }
    int addTableColumns(HsqlArrayList exprList, int position,
                        HashSet exclude) {
        return super.addTableColumns(exprList, position, exclude);
    }
    void addTableColumns(Expression expression, HashSet exclude) {
        super.addTableColumns(expression, exclude);
    }
    void setForCheckConstraint() {
        super.setForCheckConstraint();
    }
    Expression getJoinCondition() {
        return super.getJoinCondition();
    }
    void addJoinCondition(Expression e) {
        super.addJoinCondition(e);
    }
    void resetConditions() {
        super.resetConditions();
    }
    OrderedHashSet getSubqueries() {
        return super.getSubqueries();
    }
    public void replaceColumnReference(RangeVariable range,
                                       Expression[] list) {}
    public void replaceRangeVariables(RangeVariable[] ranges,
                                      RangeVariable[] newRanges) {
        super.replaceRangeVariables(ranges, newRanges);
    }
    public void resolveRangeTable(Session session,
                                  RangeVariable[] rangeVariables,
                                  int rangeCount,
                                  RangeVariable[] outerRanges) {
        super.resolveRangeTable(session, rangeVariables, rangeCount,
                                RangeVariable.emptyArray);
    }
    public String describe(Session session, int blanks) {
        RangeVariableConditions[] conditionsArray = joinConditions;
        StringBuffer              sb;
        String b = ValuePool.spaceString.substring(0, blanks);
        sb = new StringBuffer();
        String temp = "INNER";
        if (isLeftJoin) {
            temp = "LEFT OUTER";
            if (isRightJoin) {
                temp = "FULL";
            }
        } else if (isRightJoin) {
            temp = "RIGHT OUTER";
        }
        sb.append(b).append("join type=").append(temp).append("\n");
        sb.append(b).append("table=").append(rangeTable.getName().name).append(
            "\n");
        if (tableAlias != null) {
            sb.append(b).append("alias=").append(tableAlias.name).append("\n");
        }
        boolean fullScan = !conditionsArray[0].hasIndexCondition();
        sb.append(b).append("access=").append(fullScan ? "FULL SCAN"
                                                       : "INDEX PRED").append(
                                                       "\n");
        for (int i = 0; i < conditionsArray.length; i++) {
            RangeVariableConditions conditions = this.joinConditions[i];
            if (i > 0) {
                sb.append(b).append("OR condition = [");
            } else {
                sb.append(b).append("condition = [");
            }
            sb.append(conditions.describe(session, blanks + 2));
            sb.append(b).append("]\n");
        }
        return sb.toString();
    }
    public RangeIteratorMain getIterator(Session session) {
        return super.getIterator(session);
    }
}