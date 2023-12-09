


package org.hsqldb;

import org.hsqldb.lib.HsqlList;
import org.hsqldb.lib.OrderedHashSet;
import org.hsqldb.lib.Set;
import org.hsqldb.types.Type;


public class ExpressionColumnAccessor extends Expression {

    ColumnSchema column;

    ExpressionColumnAccessor(ColumnSchema column) {

        super(OpTypes.COLUMN);

        this.column   = column;
        this.dataType = column.getDataType();
    }

    String getAlias() {
        return column.getNameString();
    }

    void collectObjectNames(Set set) {

        set.add(column.getName());

        if (column.getName().parent != null) {
            set.add(column.getName().parent);
        }
    }

    String getColumnName() {
        return column.getNameString();
    }

    ColumnSchema getColumn() {
        return column;
    }

    RangeVariable getRangeVariable() {
        return null;
    }

    public HsqlList resolveColumnReferences(Session session,
            RangeVariable[] rangeVarArray, int rangeCount,
            HsqlList unresolvedSet, boolean acceptsSequences) {
        return unresolvedSet;
    }

    public void resolveTypes(Session session, Expression parent) {}

    public Object getValue(Session session) {
        return null;
    }

    public String getSQL() {
        return column.getName().statementName;
    }

    protected String describe(Session session, int blanks) {
        return column.getName().name;
    }

    public OrderedHashSet getUnkeyedColumns(OrderedHashSet unresolvedSet) {
        return unresolvedSet;
    }

    
    void collectRangeVariables(RangeVariable[] rangeVariables, Set set) {}

    Expression replaceAliasInOrderBy(Expression[] columns, int length) {
        return this;
    }

    Expression replaceColumnReferences(RangeVariable range,
                                       Expression[] list) {
        return this;
    }

    int findMatchingRangeVariableIndex(RangeVariable[] rangeVarArray) {
        return -1;
    }

    
    boolean hasReference(RangeVariable range) {
        return false;
    }

    
    public boolean equals(Expression other) {

        if (other == this) {
            return true;
        }

        if (other == null) {
            return false;
        }

        if (opType != ((Expression) other).opType) {
            return false;
        }

        return column == ((Expression) other).getColumn();
    }

    void replaceRangeVariables(RangeVariable[] ranges,
                               RangeVariable[] newRanges) {}

    void resetColumnReferences() {}

    public boolean isIndexable(RangeVariable range) {
        return false;
    }

    public boolean isUnresolvedParam() {
        return false;
    }

    boolean isDynamicParam() {
        return false;
    }

    public Type getDataType() {
        return column.getDataType();
    }
}
