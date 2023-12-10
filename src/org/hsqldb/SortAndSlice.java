package org.hsqldb;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.index.Index;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.types.Collation;
import org.hsqldb.types.Type;
public final class SortAndSlice {
    static final SortAndSlice noSort        = new SortAndSlice();
    static final int[]        defaultLimits = new int[] {
        0, Integer.MAX_VALUE, Integer.MAX_VALUE
    };
    public int[]       sortOrder;
    public boolean[]   sortDescending;
    public boolean[]   sortNullsLast;
    public Collation[] collations;
    boolean            sortUnion;
    HsqlArrayList      exprList = new HsqlArrayList();
    ExpressionOp       limitCondition;
    int                columnCount;
    boolean            hasNullsLast;
    boolean            strictLimit;
    boolean            zeroLimit;
    boolean            usingIndex;
    boolean            allDescending;
    public boolean     skipSort       = false;    
    public boolean     skipFullResult = false;    
    public Index   index;
    public Table   primaryTable;
    public Index   primaryTableIndex;
    public int[]   colIndexes;
    public boolean isGenerated;
    SortAndSlice() {}
    public HsqlArrayList getExpressionList() {
        return exprList;
    }
    public boolean hasOrder() {
        return exprList.size() != 0;
    }
    public boolean hasLimit() {
        return limitCondition != null;
    }
    public int getOrderLength() {
        return exprList.size();
    }
    public void addOrderExpression(Expression e) {
        exprList.add(e);
    }
    public void addLimitCondition(ExpressionOp expression) {
        limitCondition = expression;
    }
    public void setStrictLimit() {
        strictLimit = true;
    }
    public void setZeroLimit() {
        zeroLimit = true;
    }
    public void setUsingIndex() {
        usingIndex = true;
    }
    public void prepareSingleColumn(int colIndex) {
        sortOrder      = new int[1];
        sortDescending = new boolean[1];
        sortNullsLast  = new boolean[1];
        sortOrder[0]   = colIndex;
    }
    public void prepare(int degree) {
        columnCount = exprList.size();
        if (columnCount == 0) {
            return;
        }
        sortOrder      = new int[columnCount + degree];
        sortDescending = new boolean[columnCount + degree];
        sortNullsLast  = new boolean[columnCount + degree];
        ArrayUtil.fillSequence(sortOrder);
        for (int i = 0; i < columnCount; i++) {
            ExpressionOrderBy sort = (ExpressionOrderBy) exprList.get(i);
            sortDescending[i] = sort.isDescending();
            sortNullsLast[i]  = sort.isNullsLast();
            hasNullsLast      |= sortNullsLast[i];
        }
    }
    public void prepare(QuerySpecification select) {
        columnCount = exprList.size();
        if (columnCount == 0) {
            return;
        }
        sortOrder      = new int[columnCount];
        sortDescending = new boolean[columnCount];
        sortNullsLast  = new boolean[columnCount];
        for (int i = 0; i < columnCount; i++) {
            ExpressionOrderBy sort = (ExpressionOrderBy) exprList.get(i);
            if (sort.getLeftNode().queryTableColumnIndex == -1) {
                sortOrder[i] = select.indexStartOrderBy + i;
            } else {
                sortOrder[i] = sort.getLeftNode().queryTableColumnIndex;
            }
            sortDescending[i] = sort.isDescending();
            sortNullsLast[i]  = sort.isNullsLast();
            hasNullsLast      |= sortNullsLast[i];
            if (sort.collation != null) {
                if (collations == null) {
                    collations = new Collation[columnCount];
                }
                collations[i] = sort.collation;
            }
        }
    }
    void setSortIndex(QuerySpecification select) {
        if (isGenerated) {
            return;
        }
        for (int i = 0; i < columnCount; i++) {
            ExpressionOrderBy sort     = (ExpressionOrderBy) exprList.get(i);
            Type              dataType = sort.getLeftNode().getDataType();
            if (dataType.isArrayType() || dataType.isLobType()) {
                throw Error.error(ErrorCode.X_42534);
            }
        }
        if (select.isDistinctSelect || select.isGrouped
                || select.isAggregated) {
            return;
        }
        if (columnCount == 0) {
            if (limitCondition == null) {
                return;
            }
            skipFullResult = true;
            return;
        }
        if (select == null || hasNullsLast) {
            return;
        }
        if (collations != null) {
            return;
        }
        colIndexes = new int[columnCount];
        for (int i = 0; i < columnCount; i++) {
            Expression e = ((Expression) exprList.get(i)).getLeftNode();
            if (e.getType() != OpTypes.COLUMN) {
                return;
            }
            if (((ExpressionColumn) e).getRangeVariable()
                    != select.rangeVariables[0]) {
                return;
            }
            colIndexes[i] = e.columnIndex;
        }
        int count = ArrayUtil.countTrueElements(sortDescending);
        allDescending = count == columnCount;
        if (!allDescending && count > 0) {
            return;
        }
        primaryTable      = select.rangeVariables[0].getTable();
        primaryTableIndex = primaryTable.getFullIndexForColumns(colIndexes);
    }
    void setSortRange(QuerySpecification select) {
        if (primaryTableIndex == null) {
            return;
        }
        Index rangeIndex = select.rangeVariables[0].getSortIndex();
        if (rangeIndex == null) {
            return;
        }
        if (primaryTable != select.rangeVariables[0].rangeTable) {
            return;
        }
        if (rangeIndex == primaryTableIndex) {
            if (allDescending) {
                boolean reversed = select.rangeVariables[0].reverseOrder();
                if (!reversed) {
                    return;
                }
            }
            skipSort       = true;
            skipFullResult = true;
        } else if (!select.rangeVariables[0].joinConditions[0]
                .hasIndexCondition()) {
            if (select.rangeVariables[0].setSortIndex(primaryTableIndex,
                    allDescending)) {
                skipSort       = true;
                skipFullResult = true;
            }
        }
    }
    public boolean prepareSpecial(Session session, QuerySpecification select) {
        Expression e      = select.exprColumns[select.indexStartAggregates];
        int        opType = e.getType();
        e = e.getLeftNode();
        if (e.getType() != OpTypes.COLUMN) {
            return false;
        }
        if (((ExpressionColumn) e).getRangeVariable()
                != select.rangeVariables[0]) {
            return false;
        }
        Index rangeIndex = select.rangeVariables[0].getSortIndex();
        if (rangeIndex == null) {
            return false;
        }
        if (select.rangeVariables[0].hasSingleIndexCondition()) {
            int[] colIndexes = rangeIndex.getColumns();
            if (colIndexes[0] != ((ExpressionColumn) e).getColumnIndex()) {
                return false;
            }
            if (opType == OpTypes.MAX) {
                select.rangeVariables[0].reverseOrder();
            }
        } else if (select.rangeVariables[0].hasAnyIndexCondition()) {
            return false;
        } else {
            Table table = select.rangeVariables[0].getTable();
            Index index = table.getIndexForColumn(
                session, ((ExpressionColumn) e).getColumnIndex());
            if (index == null) {
                return false;
            }
            if (!select.rangeVariables[0].setSortIndex(index,
                    opType == OpTypes.MAX)) {
                return false;
            }
        }
        columnCount    = 1;
        sortOrder      = new int[columnCount];
        sortDescending = new boolean[columnCount];
        sortNullsLast  = new boolean[columnCount];
        skipSort       = true;
        skipFullResult = true;
        return true;
    }
    int[] getLimits(Session session, QueryExpression qe, int maxRows) {
        int     skipRows   = 0;
        int     limitRows  = Integer.MAX_VALUE;
        int     limitFetch = Integer.MAX_VALUE;
        boolean hasLimits  = false;
        if (hasLimit()) {
            Integer value =
                (Integer) limitCondition.getLeftNode().getValue(session);
            if (value == null || value.intValue() < 0) {
                throw Error.error(ErrorCode.X_2201X);
            }
            skipRows  = value.intValue();
            hasLimits = skipRows != 0;
            if (limitCondition.getRightNode() != null) {
                value =
                    (Integer) limitCondition.getRightNode().getValue(session);
                if (value == null || value.intValue() < 0
                        || (strictLimit && value.intValue() == 0)) {
                    throw Error.error(ErrorCode.X_2201W);
                }
                if (value.intValue() == 0 && !zeroLimit) {
                    limitRows = Integer.MAX_VALUE;
                } else {
                    limitRows = value.intValue();
                    hasLimits = true;
                }
            }
        }
        if (maxRows != 0) {
            if (maxRows < limitRows) {
                limitRows = maxRows;
            }
            hasLimits = true;
        }
        boolean simpleLimit = false;
        if (qe instanceof QuerySpecification) {
            QuerySpecification qs = (QuerySpecification) qe;
            if (!qs.isDistinctSelect && !qs.isGrouped) {
                simpleLimit = true;
            }
        }
        if (hasLimits) {
            if (simpleLimit && (!hasOrder() || skipSort)
                    && (!hasLimit() || skipFullResult)) {
                if (limitFetch - skipRows > limitRows) {
                    limitFetch = skipRows + limitRows;
                }
            }
            return new int[] {
                skipRows, limitRows, limitFetch
            };
        }
        return defaultLimits;
    }
    public void setIndex(Session session, TableBase table) {
        index = getNewIndex(session, table);
    }
    public Index getNewIndex(Session session, TableBase table) {
        if (hasOrder()) {
            Index orderIndex = table.createAndAddIndexStructure(null, sortOrder,
                sortDescending, sortNullsLast, false, false, false);
            if (collations != null) {
                for (int i = 0; i < columnCount; i++) {
                    if (collations[i] != null) {
                        Type type = orderIndex.getColumnTypes()[i];
                        type = Type.getType(type.typeCode,
                                            type.getCharacterSet(),
                                            collations[i], type.precision,
                                            type.scale);
                        orderIndex.getColumnTypes()[i] = type;
                    }
                }
            }
            return orderIndex;
        }
        return null;
    }
}