package org.hsqldb;
import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.HsqlNameManager.SimpleName;
import org.hsqldb.ParserDQL.CompileContext;
import org.hsqldb.RangeVariable.RangeIteratorRight;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.index.Index;
import org.hsqldb.lib.ArrayListIdentity;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.lib.HashMappedList;
import org.hsqldb.lib.HashSet;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.HsqlList;
import org.hsqldb.lib.IntValueHashMap;
import org.hsqldb.lib.OrderedHashSet;
import org.hsqldb.lib.OrderedIntHashSet;
import org.hsqldb.lib.Set;
import org.hsqldb.navigator.RangeIterator;
import org.hsqldb.navigator.RowSetNavigatorData;
import org.hsqldb.navigator.RowSetNavigatorDataTable;
import org.hsqldb.persist.PersistentStore;
import org.hsqldb.result.Result;
import org.hsqldb.result.ResultMetaData;
import org.hsqldb.result.ResultProperties;
import org.hsqldb.store.ValuePool;
import org.hsqldb.types.Type;
import org.hsqldb.types.Types;
public class QuerySpecification extends QueryExpression {
    public int            resultRangePosition;
    public boolean        isValueList;
    public boolean        isDistinctSelect;
    public boolean        isAggregated;
    public boolean        isGrouped;
    RangeVariable[]       rangeVariables;
    private HsqlArrayList rangeVariableList;
    int                   startInnerRange = -1;
    int                   endInnerRange   = -1;
    Expression            queryCondition;
    Expression            checkQueryCondition;
    private Expression    havingCondition;
    Expression            rowExpression;
    Expression[]          exprColumns;
    HsqlArrayList         exprColumnList;
    public int            indexLimitVisible;
    private int           indexLimitRowId;
    private int           groupByColumnCount;    
    private int           havingColumnCount;     
    private int           indexStartHaving;
    public int            indexStartOrderBy;
    public int            indexStartAggregates;
    private int           indexLimitExpressions;
    public int            indexLimitData;
    private boolean       hasRowID;
    private boolean       isSimpleCount;
    private boolean       hasMemoryRow;
    public boolean isUniqueResultRows;
    Type[]                    columnTypes;
    private ArrayListIdentity aggregateSet;
    private ArrayListIdentity resolvedSubqueryExpressions = null;
    private boolean[] aggregateCheck;
    private OrderedHashSet tempSet = new OrderedHashSet();
    int[]                  columnMap;
    private Table          baseTable;
    private OrderedHashSet conditionTables;      
    public Index groupIndex;
    QuerySpecification(Session session, Table table,
                       CompileContext compileContext, boolean isValueList) {
        this(compileContext);
        this.isValueList = isValueList;
        RangeVariable range = new RangeVariable(table, null, null, null,
            compileContext);
        range.addTableColumns(exprColumnList, 0, null);
        indexLimitVisible = exprColumnList.size();
        addRangeVariable(range);
        isMergeable = false;
        resolveReferences(session, RangeVariable.emptyArray);
        resolveTypes(session);
        sortAndSlice = SortAndSlice.noSort;
    }
    QuerySpecification(CompileContext compileContext) {
        super(compileContext);
        resultRangePosition = compileContext.getNextRangeVarIndex();
        rangeVariableList   = new HsqlArrayList();
        exprColumnList      = new HsqlArrayList();
        sortAndSlice        = SortAndSlice.noSort;
        isMergeable         = true;
    }
    void addRangeVariable(RangeVariable rangeVar) {
        rangeVariableList.add(rangeVar);
    }
    private void resolveRangeVariables(Session session,
                                       RangeVariable[] outerRanges) {
        if (rangeVariables == null
                || rangeVariables.length < rangeVariableList.size()) {
            rangeVariables = new RangeVariable[rangeVariableList.size()];
            rangeVariableList.toArray(rangeVariables);
        }
        for (int i = 0; i < rangeVariables.length; i++) {
            rangeVariables[i].resolveRangeTable(session, rangeVariables, i,
                                                outerRanges);
            rangeVariables[i].rangePositionInJoin = i;
            if (rangeVariables[i].isLeftJoin) {
                if (endInnerRange == -1) {
                    endInnerRange = i;
                }
            }
            if (rangeVariables[i].isRightJoin) {
                startInnerRange = i;
            }
        }
        if (startInnerRange < 0) {
            startInnerRange = 0;
        }
        if (endInnerRange < 0) {
            endInnerRange = rangeVariables.length;
        }
        if (startInnerRange > endInnerRange) {
            endInnerRange = rangeVariables.length;
        }
    }
    void addSelectColumnExpression(Expression e) {
        if (e.getType() == OpTypes.ROW) {
            throw Error.error(ErrorCode.X_42564);
        }
        if (indexLimitVisible > 0) {
            if (e.opType == OpTypes.MULTICOLUMN) {
                if (((ExpressionColumn) e).getTableName() == null) {
                    throw Error.error(ErrorCode.X_42578);
                }
            }
            Expression first = ((Expression) exprColumnList.get(0));
            if (first.opType == OpTypes.MULTICOLUMN
                    && ((ExpressionColumn) first).getTableName() == null) {
                throw Error.error(ErrorCode.X_42578);
            }
        }
        exprColumnList.add(e);
        indexLimitVisible++;
    }
    void addQueryCondition(Expression e) {
        queryCondition = e;
    }
    void addGroupByColumnExpression(Expression e) {
        if (e.getType() == OpTypes.ROW) {
            throw Error.error(ErrorCode.X_42564);
        }
        exprColumnList.add(e);
        isGrouped = true;
        groupByColumnCount++;
    }
    void addHavingExpression(Expression e) {
        exprColumnList.add(e);
        havingCondition   = e;
        havingColumnCount = 1;
    }
    void addSortAndSlice(SortAndSlice sortAndSlice) {
        this.sortAndSlice = sortAndSlice;
    }
    public void resolveReferences(Session session,
                                  RangeVariable[] outerRanges) {
        resolveRangeVariables(session, outerRanges);
        resolveColumnReferencesForAsterisk();
        finaliseColumns();
        resolveColumnReferences(session, outerRanges);
        unionColumnTypes = new Type[indexLimitVisible];
        setReferenceableColumns();
    }
    public boolean hasReference(RangeVariable range) {
        if (unresolvedExpressions == null) {
            return false;
        }
        for (int i = 0; i < unresolvedExpressions.size(); i++) {
            if (((Expression) unresolvedExpressions.get(i)).hasReference(
                    range)) {
                return true;
            }
        }
        return false;
    }
    public boolean areColumnsResolved() {
        return unresolvedExpressions == null
               || unresolvedExpressions.isEmpty();
    }
    public void resolveTypes(Session session) {
        if (isResolved) {
            return;
        }
        resolveTypesPartOne(session);
        resolveTypesPartTwo(session);
        ArrayUtil.copyArray(resultTable.colTypes, unionColumnTypes,
                            unionColumnTypes.length);
    }
    void resolveTypesPartOne(Session session) {
        resolveExpressionTypes(session, rowExpression);
        resolveAggregates();
        for (int i = 0; i < unionColumnTypes.length; i++) {
            unionColumnTypes[i] = Type.getAggregateType(unionColumnTypes[i],
                    exprColumns[i].getDataType());
        }
    }
    void resolveTypesPartTwo(Session session) {
        resolveGroups();
        for (int i = 0; i < unionColumnTypes.length; i++) {
            Type type = unionColumnTypes[i];
            if (type == null) {
                if (session.database.sqlEnforceTypes) {
                    throw Error.error(ErrorCode.X_42567);
                }
                type                = Type.SQL_VARCHAR_DEFAULT;
                unionColumnTypes[i] = type;
            }
            exprColumns[i].setDataType(session, type);
            if (exprColumns[i].dataType.isArrayType()
                    && exprColumns[i].dataType.collectionBaseType() == null) {
                throw Error.error(ErrorCode.X_42567);
            }
        }
        for (int i = indexLimitVisible; i < indexStartHaving; i++) {
            if (exprColumns[i].dataType == null) {
                throw Error.error(ErrorCode.X_42567);
            }
        }
        checkLobUsage();
        setMergeability();
        setUpdatability();
        createResultMetaData(session);
        createTable(session);
        if (isMergeable) {
            mergeQuery();
        }
        sortAndSlice.setSortIndex(this);
        setRangeVariableConditions(session);
        setDistinctConditions(session);
        setAggregateConditions(session);
        sortAndSlice.setSortRange(this);
        isResolved = true;
    }
    private void resolveColumnReferences(Session session,
                                         RangeVariable[] outerRanges) {
        if (isDistinctSelect || isGrouped) {
            acceptsSequences = false;
        }
        for (int i = 0; i < rangeVariables.length; i++) {
            Expression e = rangeVariables[i].getJoinCondition();
            if (e == null) {
                continue;
            }
            resolveColumnReferencesAndAllocate(session, e, i + 1, false);
        }
        resolveColumnReferencesAndAllocate(session, queryCondition,
                                           rangeVariables.length, false);
        if (resolvedSubqueryExpressions != null) {
            resolvedSubqueryExpressions.setSize(0);
        }
        for (int i = 0; i < indexLimitVisible; i++) {
            resolveColumnReferencesAndAllocate(session, exprColumns[i],
                                               rangeVariables.length,
                                               acceptsSequences);
        }
        for (int i = indexLimitVisible; i < indexStartHaving; i++) {
            exprColumns[i] = resolveColumnReferencesInGroupBy(session,
                    exprColumns[i]);
        }
        for (int i = indexStartHaving; i < indexStartOrderBy; i++) {
            resolveColumnReferencesAndAllocate(session, exprColumns[i],
                                               rangeVariables.length, false);
        }
        resolveColumnRefernecesInOrderBy(session, outerRanges, sortAndSlice);
    }
    void resolveColumnRefernecesInOrderBy(Session session,
                                          RangeVariable[] outerRanges,
                                          SortAndSlice sortAndSlice) {
        int orderCount = sortAndSlice.getOrderLength();
        for (int i = 0; i < orderCount; i++) {
            ExpressionOrderBy e =
                (ExpressionOrderBy) sortAndSlice.exprList.get(i);
            replaceColumnIndexInOrderBy(e);
            if (e.getLeftNode().queryTableColumnIndex != -1) {
                continue;
            }
            if (sortAndSlice.sortUnion) {
                if (e.getLeftNode().getType() != OpTypes.COLUMN) {
                    throw Error.error(ErrorCode.X_42576);
                }
            }
            e.replaceAliasInOrderBy(exprColumns, indexLimitVisible);
            resolveColumnReferencesAndAllocate(session, e,
                                               rangeVariables.length, false);
            if (isAggregated || isGrouped) {
                boolean check = e.getLeftNode().isComposedOf(exprColumns, 0,
                    indexLimitVisible + groupByColumnCount,
                    Expression.aggregateFunctionSet);
                if (!check) {
                    throw Error.error(ErrorCode.X_42576);
                }
            }
        }
        if (sortAndSlice.limitCondition != null) {
            sortAndSlice.limitCondition.resolveColumnReferences(session,
                    outerRanges, unresolvedExpressions);
        }
        sortAndSlice.prepare(this);
    }
    private boolean resolveColumnReferences(Session session, Expression e,
            int rangeCount, boolean withSequences) {
        if (e == null) {
            return true;
        }
        int oldSize = unresolvedExpressions == null ? 0
                                                    : unresolvedExpressions
                                                        .size();
        unresolvedExpressions = e.resolveColumnReferences(session,
                rangeVariables, rangeCount, unresolvedExpressions,
                withSequences);
        int newSize = unresolvedExpressions == null ? 0
                                                    : unresolvedExpressions
                                                        .size();
        return oldSize == newSize;
    }
    private void resolveColumnReferencesForAsterisk() {
        for (int pos = 0; pos < indexLimitVisible; ) {
            Expression e = (Expression) (exprColumnList.get(pos));
            if (e.getType() == OpTypes.MULTICOLUMN) {
                exprColumnList.remove(pos);
                String tablename = ((ExpressionColumn) e).getTableName();
                if (tablename == null) {
                    addAllJoinedColumns(e);
                } else {
                    int rangeIndex =
                        e.findMatchingRangeVariableIndex(rangeVariables);
                    if (rangeIndex == -1) {
                        throw Error.error(ErrorCode.X_42501, tablename);
                    }
                    RangeVariable range   = rangeVariables[rangeIndex];
                    HashSet       exclude = getAllNamedJoinColumns();
                    range.addTableColumns(e, exclude);
                }
                for (int i = 0; i < e.nodes.length; i++) {
                    exprColumnList.add(pos, e.nodes[i]);
                    pos++;
                }
                indexLimitVisible += e.nodes.length - 1;
            } else {
                pos++;
            }
        }
    }
    private void resolveColumnReferencesAndAllocate(Session session,
            Expression expression, int count, boolean withSequences) {
        if (expression == null) {
            return;
        }
        HsqlList list = expression.resolveColumnReferences(session,
            rangeVariables, count, null, withSequences);
        if (list != null) {
            for (int i = 0; i < list.size(); i++) {
                Expression e        = (Expression) list.get(i);
                boolean    resolved = true;
                if (e.isSelfAggregate()) {
                    for (int j = 0; j < e.nodes.length; j++) {
                        HsqlList colList =
                            e.nodes[j].resolveColumnReferences(session,
                                                               rangeVariables,
                                                               count, null,
                                                               false);
                        resolved &= colList == null;
                    }
                } else {
                    resolved = resolveColumnReferences(session, e, count,
                                                       withSequences);
                }
                if (resolved) {
                    if (e.isSelfAggregate()) {
                        if (aggregateSet == null) {
                            aggregateSet = new ArrayListIdentity();
                        }
                        aggregateSet.add(e);
                        isAggregated = true;
                        expression.setAggregate();
                    }
                    if (resolvedSubqueryExpressions == null) {
                        resolvedSubqueryExpressions = new ArrayListIdentity();
                    }
                    resolvedSubqueryExpressions.add(e);
                } else {
                    if (unresolvedExpressions == null) {
                        unresolvedExpressions = new ArrayListIdentity();
                    }
                    unresolvedExpressions.add(e);
                }
            }
        }
    }
    private Expression resolveColumnReferencesInGroupBy(Session session,
            Expression expression) {
        if (expression == null) {
            return null;
        }
        HsqlList list = expression.resolveColumnReferences(session,
            rangeVariables, rangeVariables.length, null, false);
        if (list != null) {
            if (expression.getType() == OpTypes.COLUMN) {
                Expression resolved =
                    expression.replaceAliasInOrderBy(exprColumns,
                                                     indexLimitVisible);
                if (resolved != expression) {
                    return resolved;
                }
            }
            resolveColumnReferencesAndAllocate(session, expression,
                                               rangeVariables.length, false);
        }
        return expression;
    }
    private HashSet getAllNamedJoinColumns() {
        HashSet set = null;
        for (int i = 0; i < rangeVariableList.size(); i++) {
            RangeVariable range = (RangeVariable) rangeVariableList.get(i);
            if (range.namedJoinColumns != null) {
                if (set == null) {
                    set = new HashSet();
                }
                set.addAll(range.namedJoinColumns);
            }
        }
        return set;
    }
    public Expression getEquiJoinExpressions(OrderedHashSet nameSet,
            RangeVariable rightRange, boolean fullList) {
        HashSet        set             = new HashSet();
        Expression     result          = null;
        OrderedHashSet joinColumnNames = new OrderedHashSet();
        for (int i = 0; i < rangeVariableList.size(); i++) {
            RangeVariable  range = (RangeVariable) rangeVariableList.get(i);
            HashMappedList columnList = range.rangeTable.columnList;
            for (int j = 0; j < columnList.size(); j++) {
                ColumnSchema column       = (ColumnSchema) columnList.get(j);
                String       name         = range.getColumnAlias(j).name;
                boolean      columnInList = nameSet.contains(name);
                boolean namedJoin = range.namedJoinColumns != null
                                    && range.namedJoinColumns.contains(name);
                boolean repeated = !namedJoin && !set.add(name);
                if (repeated && (!fullList || columnInList)) {
                    throw Error.error(ErrorCode.X_42578, name);
                }
                if (!columnInList) {
                    continue;
                }
                joinColumnNames.add(name);
                int leftPosition =
                    range.rangeTable.getColumnIndex(column.getNameString());
                int rightPosition = rightRange.rangeTable.getColumnIndex(name);
                Expression e = new ExpressionLogical(range, leftPosition,
                                                     rightRange,
                                                     rightPosition);
                result = ExpressionLogical.andExpressions(result, e);
                ExpressionColumn col = range.getColumnExpression(name);
                if (col == null) {
                    col = new ExpressionColumn(new Expression[] {
                        e.getLeftNode(), e.getRightNode()
                    }, name);
                    range.addNamedJoinColumnExpression(name, col);
                } else {
                    col.nodes = (Expression[]) ArrayUtil.resizeArray(col.nodes,
                            col.nodes.length + 1);
                    col.nodes[col.nodes.length - 1] = e.getRightNode();
                }
                rightRange.addNamedJoinColumnExpression(name, col);
            }
        }
        if (fullList && !joinColumnNames.containsAll(nameSet)) {
            throw Error.error(ErrorCode.X_42501);
        }
        rightRange.addNamedJoinColumns(joinColumnNames);
        return result;
    }
    private void addAllJoinedColumns(Expression e) {
        HsqlArrayList list = new HsqlArrayList();
        for (int i = 0; i < rangeVariables.length; i++) {
            rangeVariables[i].addTableColumns(list);
        }
        Expression[] nodes = new Expression[list.size()];
        list.toArray(nodes);
        e.nodes = nodes;
    }
    private void finaliseColumns() {
        indexLimitRowId   = indexLimitVisible;
        indexStartHaving  = indexLimitRowId + groupByColumnCount;
        indexStartOrderBy = indexStartHaving + havingColumnCount;
        indexStartAggregates = indexStartOrderBy
                               + sortAndSlice.getOrderLength();
        indexLimitData = indexLimitExpressions = indexStartAggregates;
        exprColumns    = new Expression[indexLimitExpressions];
        exprColumnList.toArray(exprColumns);
        for (int i = 0; i < indexLimitVisible; i++) {
            exprColumns[i].queryTableColumnIndex = i;
        }
        if (sortAndSlice.hasOrder()) {
            for (int i = 0; i < sortAndSlice.getOrderLength(); i++) {
                exprColumns[indexStartOrderBy + i] =
                    (Expression) sortAndSlice.exprList.get(i);
            }
        }
        rowExpression = new Expression(OpTypes.ROW, exprColumns);
    }
    private void replaceColumnIndexInOrderBy(Expression orderBy) {
        Expression e = orderBy.getLeftNode();
        if (e.getType() != OpTypes.VALUE) {
            return;
        }
        Type type = e.getDataType();
        if (type != null && type.typeCode == Types.SQL_INTEGER) {
            int i = ((Integer) e.getValue(null)).intValue();
            if (0 < i && i <= indexLimitVisible) {
                orderBy.setLeftNode(exprColumns[i - 1]);
                return;
            }
        }
        throw Error.error(ErrorCode.X_42576);
    }
    void collectRangeVariables(RangeVariable[] rangeVars, Set set) {
        for (int i = 0; i < indexStartAggregates; i++) {
            exprColumns[i].collectRangeVariables(rangeVars, set);
        }
        if (queryCondition != null) {
            queryCondition.collectRangeVariables(rangeVars, set);
        }
        if (havingCondition != null) {
            havingCondition.collectRangeVariables(rangeVars, set);
        }
    }
    public void resolveExpressionTypes(Session session, Expression parent) {
        for (int i = 0; i < indexStartAggregates; i++) {
            Expression e = exprColumns[i];
            e.resolveTypes(session, parent);
            if (e.getType() == OpTypes.ROW) {
                throw Error.error(ErrorCode.X_42565);
            }
            if (e.getDataType() != null
                    && e.getDataType().typeCode == Types.SQL_ROW) {
                throw Error.error(ErrorCode.X_42565);
            }
        }
        for (int i = 0, len = rangeVariables.length; i < len; i++) {
            Expression e = rangeVariables[i].getJoinCondition();
            if (e != null) {
                e.resolveTypes(session, null);
                if (e.getDataType() != Type.SQL_BOOLEAN) {
                    throw Error.error(ErrorCode.X_42568);
                }
            }
        }
        if (queryCondition != null) {
            queryCondition.resolveTypes(session, null);
            if (queryCondition.getDataType() != Type.SQL_BOOLEAN) {
                throw Error.error(ErrorCode.X_42568);
            }
        }
        if (havingCondition != null) {
            havingCondition.resolveTypes(session, null);
            if (havingCondition.getDataType() != Type.SQL_BOOLEAN) {
                throw Error.error(ErrorCode.X_42568);
            }
        }
        if (sortAndSlice.limitCondition != null) {
            sortAndSlice.limitCondition.resolveTypes(session, null);
        }
    }
    private void resolveAggregates() {
        tempSet.clear();
        if (isAggregated) {
            aggregateCheck = new boolean[indexStartAggregates];
            tempSet.addAll(aggregateSet);
            indexLimitData = indexLimitExpressions = exprColumns.length
                    + tempSet.size();
            exprColumns = (Expression[]) ArrayUtil.resizeArray(exprColumns,
                    indexLimitExpressions);
            for (int i = indexStartAggregates, j = 0;
                    i < indexLimitExpressions; i++, j++) {
                Expression e = (Expression) tempSet.get(j);
                exprColumns[i]          = e.duplicate();
                exprColumns[i].nodes    = e.nodes;    
                exprColumns[i].dataType = e.dataType;
            }
            tempSet.clear();
        }
    }
    private void setRangeVariableConditions(Session session) {
        RangeVariableResolver rangeResolver = new RangeVariableResolver(this);
        rangeResolver.processConditions(session);
        rangeVariables = rangeResolver.rangeVariables;
    }
    private void setDistinctConditions(Session session) {
        if (!isDistinctSelect && !isGrouped) {
            return;
        }
        if (isAggregated) {
            return;
        }
        RangeVariable range = null;
        tempSet.clear();
        for (int i = 0; i < indexLimitVisible; i++) {
            if (exprColumns[i].getType() != OpTypes.COLUMN) {
                return;
            }
            if (i == 0) {
                range = exprColumns[i].getRangeVariable();
            } else {
                if (range != exprColumns[i].getRangeVariable()) {
                    return;
                }
            }
            tempSet.add(exprColumns[i].getColumn().getName().name);
        }
        int[] colMap;
        if (!range.hasAnyIndexCondition()) {
            colMap = range.rangeTable.getColumnIndexes(tempSet);
            Index index = range.rangeTable.getFullIndexForColumns(colMap);
            if (index != null) {
                range.setSortIndex(index, false);
            }
            return;
        }
        colMap = range.rangeTable.getColumnIndexes(tempSet);
        range.setDistinctColumnsOnIndex(colMap);
    }
    private void setAggregateConditions(Session session) {
        if (!isAggregated) {
            return;
        }
        if (isGrouped) {
            setGroupedAggregateConditions(session);
        } else if (!sortAndSlice.hasOrder() && !sortAndSlice.hasLimit()
                   && aggregateSet.size() == 1 && indexLimitVisible == 1) {
            Expression e      = exprColumns[indexStartAggregates];
            int        opType = e.getType();
            switch (opType) {
                case OpTypes.MAX :
                case OpTypes.MIN : {
                    if (e.hasCondition()) {
                        break;
                    }
                    SortAndSlice slice = new SortAndSlice();
                    slice.isGenerated = true;
                    slice.addLimitCondition(ExpressionOp.limitOneExpression);
                    if (slice.prepareSpecial(session, this)) {
                        this.sortAndSlice = slice;
                    }
                    break;
                }
                case OpTypes.COUNT : {
                    if (e.hasCondition()) {
                        break;
                    }
                    if (rangeVariables.length == 1 && queryCondition == null) {
                        Expression expr = e.getLeftNode();
                        if (expr.getType() == OpTypes.ASTERISK) {
                            isSimpleCount = true;
                        } else if (expr.getNullability()
                                   == SchemaObject.Nullability.NO_NULLS) {
                            if (((ExpressionAggregate) e)
                                    .isDistinctAggregate) {
                                if (expr.opType == OpTypes.COLUMN) {
                                    Table t =
                                        expr.getRangeVariable().getTable();
                                    if (t.getPrimaryKey().length == 1) {
                                        if (t.getColumn(t.getPrimaryKey()[0])
                                                == expr.getColumn()) {
                                            isSimpleCount = true;
                                        }
                                    }
                                }
                            } else {
                                isSimpleCount = true;
                            }
                        }
                    }
                    break;
                }
                default :
            }
        }
    }
    private void setGroupedAggregateConditions(Session session) {
    }
    void checkLobUsage() {
        if (!isDistinctSelect && !isGrouped) {
            return;
        }
        for (int i = 0; i < indexStartHaving; i++) {
            if (exprColumns[i].dataType.isLobType()) {
                throw Error.error(ErrorCode.X_42534);
            }
        }
    }
    private void resolveGroups() {
        tempSet.clear();
        if (isGrouped) {
            for (int i = indexLimitVisible;
                    i < indexLimitVisible + groupByColumnCount; i++) {
                exprColumns[i].collectAllExpressions(
                    tempSet, Expression.aggregateFunctionSet,
                    Expression.subqueryExpressionSet);
                if (!tempSet.isEmpty()) {
                    throw Error.error(ErrorCode.X_42572,
                                      ((Expression) tempSet.get(0)).getSQL());
                }
            }
            for (int i = 0; i < indexLimitVisible; i++) {
                if (!exprColumns[i].isComposedOf(
                        exprColumns, indexLimitVisible,
                        indexLimitVisible + groupByColumnCount,
                        Expression.subqueryAggregateExpressionSet)) {
                    tempSet.add(exprColumns[i]);
                }
            }
            if (!tempSet.isEmpty() && !resolveForGroupBy(tempSet)) {
                throw Error.error(ErrorCode.X_42574,
                                  ((Expression) tempSet.get(0)).getSQL());
            }
        } else if (isAggregated) {
            for (int i = 0; i < indexLimitVisible; i++) {
                exprColumns[i].collectAllExpressions(
                    tempSet, Expression.columnExpressionSet,
                    Expression.aggregateFunctionSet);
                if (!tempSet.isEmpty()) {
                    throw Error.error(ErrorCode.X_42574,
                                      ((Expression) tempSet.get(0)).getSQL());
                }
            }
        }
        tempSet.clear();
        if (havingCondition != null) {
            if (unresolvedExpressions != null) {
                tempSet.addAll(unresolvedExpressions);
            }
            for (int i = indexLimitVisible;
                    i < indexLimitVisible + groupByColumnCount; i++) {
                tempSet.add(exprColumns[i]);
            }
            if (!havingCondition.isComposedOf(
                    tempSet, Expression.subqueryAggregateExpressionSet)) {
                throw Error.error(ErrorCode.X_42573);
            }
            tempSet.clear();
        }
        if (isDistinctSelect) {
            int orderCount = sortAndSlice.getOrderLength();
            for (int i = 0; i < orderCount; i++) {
                Expression e = (Expression) sortAndSlice.exprList.get(i);
                if (e.queryTableColumnIndex != -1) {
                    continue;
                }
                if (!e.isComposedOf(exprColumns, 0, indexLimitVisible,
                                    Expression.emptyExpressionSet)) {
                    throw Error.error(ErrorCode.X_42576);
                }
            }
        }
        if (isGrouped) {
            int orderCount = sortAndSlice.getOrderLength();
            for (int i = 0; i < orderCount; i++) {
                Expression e = (Expression) sortAndSlice.exprList.get(i);
                if (e.queryTableColumnIndex != -1) {
                    continue;
                }
                if (!e.isAggregate()
                        && !e.isComposedOf(
                            exprColumns, 0,
                            indexLimitVisible + groupByColumnCount,
                            Expression.emptyExpressionSet)) {
                    throw Error.error(ErrorCode.X_42576);
                }
            }
        }
        if (!isAggregated) {
            return;
        }
        OrderedHashSet expressions       = new OrderedHashSet();
        OrderedHashSet columnExpressions = new OrderedHashSet();
        for (int i = indexStartAggregates; i < indexLimitExpressions; i++) {
            Expression e = exprColumns[i];
            Expression c = new ExpressionColumn(e, i, resultRangePosition);
            expressions.add(e);
            columnExpressions.add(c);
        }
        for (int i = 0; i < indexStartHaving; i++) {
            if (exprColumns[i].isAggregate()) {
                continue;
            }
            Expression e = exprColumns[i];
            if (expressions.add(e)) {
                Expression c = new ExpressionColumn(e, i, resultRangePosition);
                columnExpressions.add(c);
            }
        }
        int orderCount = sortAndSlice.getOrderLength();
        for (int i = 0; i < orderCount; i++) {
            Expression e = (Expression) sortAndSlice.exprList.get(i);
            if (e.getLeftNode().isAggregate()) {
                e.setAggregate();
            }
        }
        for (int i = indexStartOrderBy; i < indexStartAggregates; i++) {
            if (exprColumns[i].getLeftNode().isAggregate()) {
                exprColumns[i].setAggregate();
            }
        }
        for (int i = 0; i < indexStartAggregates; i++) {
            Expression e = exprColumns[i];
            if (!e.isAggregate() && !e.isCorrelated()) {
                continue;
            }
            aggregateCheck[i] = true;
            if (e.isAggregate()) {
                e.convertToSimpleColumn(expressions, columnExpressions);
            }
        }
        for (int i = 0; i < aggregateSet.size(); i++) {
            Expression e = (Expression) aggregateSet.get(i);
            e.convertToSimpleColumn(expressions, columnExpressions);
        }
        if (resolvedSubqueryExpressions != null) {
            for (int i = 0; i < resolvedSubqueryExpressions.size(); i++) {
                Expression e = (Expression) resolvedSubqueryExpressions.get(i);
                e.convertToSimpleColumn(expressions, columnExpressions);
            }
        }
    }
    boolean resolveForGroupBy(HsqlList unresolvedSet) {
        for (int i = indexLimitVisible;
                i < indexLimitVisible + groupByColumnCount; i++) {
            Expression e = exprColumns[i];
            if (e.getType() == OpTypes.COLUMN) {
                RangeVariable range    = e.getRangeVariable();
                int           colIndex = e.getColumnIndex();
                range.columnsInGroupBy[colIndex] = true;
            }
        }
        for (int i = 0; i < rangeVariables.length; i++) {
            RangeVariable range = rangeVariables[i];
            range.hasKeyedColumnInGroupBy =
                range.rangeTable.getUniqueNotNullColumnGroup(
                    range.columnsInGroupBy) != null;
        }
        OrderedHashSet set = null;
        for (int i = 0; i < unresolvedSet.size(); i++) {
            Expression e = (Expression) unresolvedSet.get(i);
            set = e.getUnkeyedColumns(set);
        }
        return set == null;
    }
    Result getResult(Session session, int maxrows) {
        Result r = getSingleResult(session, maxrows);
        r.getNavigator().reset();
        return r;
    }
    private Result getSingleResult(Session session, int maxRows) {
        int[] limits = sortAndSlice.getLimits(session, this, maxRows);
        Result              r         = buildResult(session, limits);
        RowSetNavigatorData navigator = (RowSetNavigatorData) r.getNavigator();
        if (isDistinctSelect) {
            navigator.removeDuplicates(session);
        }
        if (sortAndSlice.hasOrder()) {
            navigator.sortOrder(session);
        }
        if (limits != SortAndSlice.defaultLimits
                && !sortAndSlice.skipFullResult) {
            navigator.trim(limits[0], limits[1]);
        }
        return r;
    }
    private Result buildResult(Session session, int[] limits) {
        RowSetNavigatorData navigator = new RowSetNavigatorData(session,
            (QuerySpecification) this);
        Result result = Result.newResult(navigator);
        result.metaData = resultMetaData;
        if (isUpdatable) {
            result.rsProperties = ResultProperties.updatablePropsValue;
        }
        int skipCount  = 0;
        int limitCount = limits[2];
        if (sortAndSlice.skipFullResult) {
            skipCount = limits[0];
            limitCount = limits[1];
        }
        if (this.isSimpleCount) {
            Object[]        data  = new Object[indexLimitData];
            Table           table = rangeVariables[0].getTable();
            PersistentStore store = table.getRowStore(session);
            long            count = store.elementCount(session);
            data[0] = data[indexStartAggregates] = ValuePool.getLong(count);
            navigator.add(data);
            return result;
        }
        int fullJoinIndex = 0;
        RangeIterator[] rangeIterators =
            new RangeIterator[rangeVariables.length];
        for (int i = 0; i < rangeVariables.length; i++) {
            rangeIterators[i] = rangeVariables[i].getIterator(session);
        }
        session.sessionContext.rownum = 1;
        for (int currentIndex = 0; ; ) {
            if (currentIndex < fullJoinIndex) {
                boolean end = true;
                for (int i = fullJoinIndex + 1; i < rangeVariables.length;
                        i++) {
                    if (rangeVariables[i].isRightJoin) {
                        fullJoinIndex = i;
                        currentIndex  = i;
                        end           = false;
                        ((RangeIteratorRight) rangeIterators[i])
                            .setOnOuterRows();
                        break;
                    }
                }
                if (end) {
                    break;
                }
            }
            RangeIterator it = rangeIterators[currentIndex];
            if (it.next()) {
                if (currentIndex < rangeVariables.length - 1) {
                    currentIndex++;
                    continue;
                }
            } else {
                it.reset();
                currentIndex--;
                continue;
            }
            if (limitCount == 0) {
                break;
            }
            session.sessionData.startRowProcessing();
            Object[] data = new Object[indexLimitData];
            for (int i = 0; i < indexStartAggregates; i++) {
                if (isAggregated && aggregateCheck[i]) {
                    continue;
                } else {
                    data[i] = exprColumns[i].getValue(session);
                }
            }
            for (int i = indexLimitVisible; i < indexLimitRowId; i++) {
                if (i == indexLimitVisible) {
                    data[i] = it.getRowidObject();
                } else {
                    data[i] = it.getCurrentRow();
                }
            }
            session.sessionContext.rownum++;
            if (skipCount > 0) {
                skipCount--;
                continue;
            }
            Object[] groupData = null;
            if (isAggregated || isGrouped) {
                groupData = navigator.getGroupData(data);
                if (groupData != null) {
                    data = groupData;
                }
            }
            for (int i = indexStartAggregates; i < indexLimitExpressions;
                    i++) {
                data[i] = exprColumns[i].updateAggregatingValue(session,
                        data[i]);
            }
            if (groupData == null) {
                navigator.add(data);
            } else if (isAggregated) {
                navigator.update(groupData, data);
            }
            int rowCount = navigator.getSize();
            if (rowCount == session.resultMaxMemoryRows && !isAggregated
                    && !hasMemoryRow) {
                navigator = new RowSetNavigatorDataTable(session, this,
                        navigator);
                result.setNavigator(navigator);
            }
            if (isAggregated || isGrouped) {
                if (!sortAndSlice.isGenerated) {
                    continue;
                }
            }
            if (rowCount >= limitCount) {
                break;
            }
        }
        navigator.reset();
        for (int i = 0; i < rangeVariables.length; i++) {
            rangeIterators[i].reset();
        }
        if (!isGrouped && !isAggregated) {
            return result;
        }
        if (isAggregated) {
            if (!isGrouped && navigator.getSize() == 0) {
                Object[] data = new Object[exprColumns.length];
                for (int i = 0; i < indexStartAggregates; i++) {
                    if (!aggregateCheck[i]) {
                        data[i] = exprColumns[i].getValue(session);
                    }
                }
                navigator.add(data);
            }
            navigator.reset();
            session.sessionContext.setRangeIterator(navigator);
            while (navigator.next()) {
                Object[] data = navigator.getCurrent();
                for (int i = indexStartAggregates; i < indexLimitExpressions;
                        i++) {
                    data[i] = exprColumns[i].getAggregatedValue(session,
                            data[i]);
                }
                for (int i = 0; i < indexStartAggregates; i++) {
                    if (aggregateCheck[i]) {
                        data[i] = exprColumns[i].getValue(session);
                    }
                }
            }
        }
        navigator.reset();
        if (havingCondition != null) {
            while (navigator.hasNext()) {
                Object[] data = (Object[]) navigator.getNext();
                if (!Boolean.TRUE.equals(
                        data[indexLimitVisible + groupByColumnCount])) {
                    navigator.remove();
                }
            }
            navigator.reset();
        }
        return result;
    }
    void setReferenceableColumns() {
        accessibleColumns = new boolean[indexLimitVisible];
        IntValueHashMap aliases = new IntValueHashMap();
        for (int i = 0; i < indexLimitVisible; i++) {
            Expression expression = exprColumns[i];
            String     alias      = expression.getAlias();
            if (alias.length() == 0) {
                SimpleName name = HsqlNameManager.getAutoColumnName(i);
                expression.setAlias(name);
                continue;
            }
            int index = aliases.get(alias, -1);
            if (index == -1) {
                aliases.put(alias, i);
                accessibleColumns[i] = true;
            } else {
                accessibleColumns[index] = false;
            }
        }
    }
    void setColumnAliases(SimpleName[] names) {
        if (names.length != indexLimitVisible) {
            throw Error.error(ErrorCode.X_42593);
        }
        for (int i = 0; i < indexLimitVisible; i++) {
            exprColumns[i].setAlias(names[i]);
        }
    }
    private void createResultMetaData(Session session) {
        columnTypes = new Type[indexLimitData];
        for (int i = 0; i < indexStartAggregates; i++) {
            Expression e = exprColumns[i];
            columnTypes[i] = e.getDataType();
        }
        for (int i = indexLimitVisible; i < indexLimitRowId; i++) {
            if (i == indexLimitVisible) {
                columnTypes[i] = Type.SQL_BIGINT;
            } else {
                columnTypes[i] = Type.SQL_ALL_TYPES;
            }
        }
        for (int i = indexLimitRowId; i < indexLimitData; i++) {
            Expression e = exprColumns[i];
            columnTypes[i] = e.getDataType();
        }
        resultMetaData = ResultMetaData.newResultMetaData(columnTypes,
                columnMap, indexLimitVisible, indexLimitRowId);
        for (int i = 0; i < indexLimitVisible; i++) {
            byte nullability = SchemaObject.Nullability.NULLABLE_UNKNOWN;
            Expression   e           = exprColumns[i];
            ColumnSchema tableColumn = null;
            ColumnBase   column;
            resultMetaData.columnTypes[i] = e.getDataType();
            switch (e.getType()) {
                case OpTypes.COLUMN :
                    tableColumn = e.getColumn();
                    if (tableColumn != null) {
                        RangeVariable range = e.getRangeVariable();
                        if (range != null && range
                                .rangePositionInJoin >= startInnerRange && range
                                .rangePositionInJoin < endInnerRange) {
                            resultMetaData.columns[i]      = tableColumn;
                            resultMetaData.columnLabels[i] = e.getAlias();
                            continue;
                        }
                    }
                    break;
                case OpTypes.SEQUENCE :
                case OpTypes.COALESCE :
                case OpTypes.ROWNUM :
                    nullability = SchemaObject.Nullability.NO_NULLS;
                    break;
                case OpTypes.VALUE :
                    nullability = e.valueData == null
                                  ? SchemaObject.Nullability.NULLABLE
                                  : SchemaObject.Nullability.NO_NULLS;
                    break;
            }
            if (tableColumn == null) {
                column = new ColumnBase();
            } else {
                column = new ColumnBase(session.database.getCatalogName().name,
                                        tableColumn.getSchemaNameString(),
                                        tableColumn.getTableNameString(),
                                        tableColumn.getNameString());
            }
            column.setType(e.getDataType());
            column.setNullability(nullability);
            resultMetaData.columns[i]      = column;
            resultMetaData.columnLabels[i] = e.getAlias();
        }
    }
    void createTable(Session session) {
        createResultTable(session);
        mainIndex = resultTable.getPrimaryIndex();
        if (sortAndSlice.hasOrder() && !sortAndSlice.skipSort) {
            orderIndex = sortAndSlice.getNewIndex(session, resultTable);
        }
        if (isDistinctSelect || isFullOrder) {
            createFullIndex(session);
        }
        if (isGrouped) {
            int[] groupCols = new int[groupByColumnCount];
            for (int i = 0; i < groupByColumnCount; i++) {
                groupCols[i] = indexLimitVisible + i;
            }
            groupIndex = resultTable.createAndAddIndexStructure(null, groupCols,
                    null, null, false, false, false);
        } else if (isAggregated) {
            groupIndex = mainIndex;
        }
        if (isUpdatable && view == null) {
            int[] idCols = new int[]{ indexLimitVisible };
            idIndex = resultTable.createAndAddIndexStructure(null, idCols,
                    null, null, false, false, false);
        }
    }
    void createFullIndex(Session session) {
        int[] fullCols = new int[indexLimitVisible];
        ArrayUtil.fillSequence(fullCols);
        fullIndex = resultTable.createAndAddIndexStructure(null, fullCols,
                null, null, false, false, false);
        resultTable.fullIndex = fullIndex;
    }
    void createResultTable(Session session) {
        HsqlName       tableName;
        HashMappedList columnList;
        int            tableType;
        tableName = session.database.nameManager.getSubqueryTableName();
        tableType = persistenceScope == TableBase.SCOPE_STATEMENT
                    ? TableBase.SYSTEM_SUBQUERY
                    : TableBase.RESULT_TABLE;
        columnList = new HashMappedList();
        for (int i = 0; i < indexLimitVisible; i++) {
            Expression e          = exprColumns[i];
            SimpleName simpleName = e.getSimpleName();
            String     nameString = simpleName.name;
            HsqlName name =
                session.database.nameManager.newColumnSchemaHsqlName(tableName,
                    simpleName);
            if (!accessibleColumns[i]) {
                nameString = HsqlNameManager.getAutoNoNameColumnString(i);
            }
            ColumnSchema column = new ColumnSchema(name, e.dataType, true,
                                                   false, null);
            columnList.add(nameString, column);
        }
        try {
            resultTable = new TableDerived(session.database, tableName,
                                           tableType, columnTypes, columnList
                                           );
        } catch (Exception e) {}
    }
    public String getSQL() {
        StringBuffer sb = new StringBuffer();
        int          limit;
        sb.append(Tokens.T_SELECT).append(' ');
        limit = indexLimitVisible;
        for (int i = 0; i < limit; i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(exprColumns[i].getSQL());
        }
        sb.append(Tokens.T_FROM);
        limit = rangeVariables.length;
        for (int i = 0; i < limit; i++) {
            RangeVariable rangeVar = rangeVariables[i];
            if (i > 0) {
                if (rangeVar.isLeftJoin && rangeVar.isRightJoin) {
                    sb.append(Tokens.T_FULL).append(' ');
                } else if (rangeVar.isLeftJoin) {
                    sb.append(Tokens.T_LEFT).append(' ');
                } else if (rangeVar.isRightJoin) {
                    sb.append(Tokens.T_RIGHT).append(' ');
                }
                sb.append(Tokens.T_JOIN).append(' ');
            }
            sb.append(rangeVar.getTable().getName().statementName);
        }
        if (isGrouped) {
            sb.append(' ').append(Tokens.T_GROUP).append(' ').append(
                Tokens.T_BY);
            limit = indexLimitVisible + groupByColumnCount;
            for (int i = indexLimitVisible; i < limit; i++) {
                sb.append(exprColumns[i].getSQL());
                if (i < limit - 1) {
                    sb.append(',');
                }
            }
        }
        if (havingCondition != null) {
            sb.append(' ').append(Tokens.T_HAVING).append(' ');
            sb.append(havingCondition.getSQL());
        }
        if (sortAndSlice.hasOrder()) {
            limit = indexStartOrderBy + sortAndSlice.getOrderLength();
            sb.append(' ').append(Tokens.T_ORDER).append(Tokens.T_BY).append(
                ' ');
            for (int i = indexStartOrderBy; i < limit; i++) {
                sb.append(exprColumns[i].getSQL());
                if (i < limit - 1) {
                    sb.append(',');
                }
            }
        }
        if (sortAndSlice.hasLimit()) {
            sb.append(sortAndSlice.limitCondition.getLeftNode().getSQL());
        }
        return sb.toString();
    }
    public ResultMetaData getMetaData() {
        return resultMetaData;
    }
    public String describe(Session session, int blanks) {
        StringBuffer sb;
        String       temp;
        String       b = ValuePool.spaceString.substring(0, blanks);
        sb = new StringBuffer();
        sb.append(b).append("isDistinctSelect=[").append(
            isDistinctSelect).append("]\n");
        sb.append(b).append("isGrouped=[").append(isGrouped).append("]\n");
        sb.append(b).append("isAggregated=[").append(isAggregated).append(
            "]\n");
        sb.append(b).append("columns=[");
        for (int i = 0; i < indexLimitVisible; i++) {
            int index = i;
            if (exprColumns[i].getType() == OpTypes.SIMPLE_COLUMN) {
                index = exprColumns[i].columnIndex;
            }
            sb.append(b).append(exprColumns[index].describe(session, 2));
            if (resultMetaData.columns[i].getNullability()
                    == SchemaObject.Nullability.NO_NULLS) {
                sb.append(" not nullable");
            } else {
                sb.append(" nullable");
            }
        }
        sb.append("\n");
        sb.append(b).append("]\n");
        for (int i = 0; i < rangeVariables.length; i++) {
            sb.append(b).append("[");
            sb.append("range variable ").append(i + 1).append("\n");
            sb.append(rangeVariables[i].describe(session, blanks + 2));
            sb.append(b).append("]");
        }
        sb.append(b).append("]\n");
        temp = queryCondition == null ? "null"
                                      : queryCondition.describe(session,
                                      blanks);
        if (isGrouped) {
            sb.append(b).append("groupColumns=[");
            for (int i = indexLimitRowId;
                    i < indexLimitRowId + groupByColumnCount; i++) {
                int index = i;
                if (exprColumns[i].getType() == OpTypes.SIMPLE_COLUMN) {
                    index = exprColumns[i].columnIndex;
                }
                sb.append(exprColumns[index].describe(session, blanks));
            }
            sb.append(b).append("]\n");
        }
        if (havingCondition != null) {
            temp = havingCondition.describe(session, blanks);
            sb.append(b).append("havingCondition=[").append(temp).append(
                "]\n");
        }
        if (sortAndSlice.hasOrder()) {
            sb.append(b).append("order by=[\n");
            for (int i = 0; i < sortAndSlice.exprList.size(); i++) {
                sb.append(b).append(
                    ((Expression) sortAndSlice.exprList.get(i)).describe(
                        session, blanks));
            }
            sb.append(b).append("]\n");
        }
        if (sortAndSlice.hasLimit()) {
            if (sortAndSlice.limitCondition.getLeftNode() != null) {
                sb.append(b).append("offset=[").append(
                    sortAndSlice.limitCondition.getLeftNode().describe(
                        session, 0)).append("]\n");
            }
            if (sortAndSlice.limitCondition.getRightNode() != null) {
                sb.append(b).append("limit=[").append(
                    sortAndSlice.limitCondition.getRightNode().describe(
                        session, 0)).append("]\n");
            }
        }
        return sb.toString();
    }
    void setMergeability() {
        if (isGrouped || isDistinctSelect) {
            isMergeable = false;
            return;
        }
        if (sortAndSlice.hasLimit() || sortAndSlice.hasOrder()) {
            isMergeable = false;
            return;
        }
        if (rangeVariables.length != 1) {
            isMergeable = false;
            return;
        }
    }
    void setUpdatability() {
        if (!isUpdatable) {
            return;
        }
        isUpdatable = false;
        if (!isMergeable) {
            return;
        }
        if (!isTopLevel) {
            return;
        }
        if (isAggregated) {
            return;
        }
        if (sortAndSlice.hasLimit() || sortAndSlice.hasOrder()) {
            return;
        }
        RangeVariable rangeVar  = rangeVariables[0];
        Table         table     = rangeVar.getTable();
        Table         baseTable = table.getBaseTable();
        if (baseTable == null) {
            return;
        }
        isInsertable = table.isInsertable();
        isUpdatable  = table.isUpdatable();
        if (!isInsertable && !isUpdatable) {
            return;
        }
        IntValueHashMap columns = new IntValueHashMap();
        boolean[]       checkList;
        int[]           baseColumnMap = table.getBaseTableColumnMap();
        int[]           columnMap     = new int[indexLimitVisible];
        if (queryCondition != null) {
            tempSet.clear();
            collectSubQueriesAndReferences(tempSet, queryCondition);
            if (tempSet.contains(table.getName())
                    || tempSet.contains(baseTable.getName())) {
                isUpdatable  = false;
                isInsertable = false;
                return;
            }
        }
        for (int i = 0; i < indexLimitVisible; i++) {
            Expression expression = exprColumns[i];
            if (expression.getType() == OpTypes.COLUMN) {
                String name = expression.getColumn().getName().name;
                if (columns.containsKey(name)) {
                    columns.put(name, 1);
                    continue;
                }
                columns.put(name, 0);
            } else {
                tempSet.clear();
                collectSubQueriesAndReferences(tempSet, expression);
                if (tempSet.contains(table.getName())) {
                    isUpdatable  = false;
                    isInsertable = false;
                    return;
                }
            }
        }
        isUpdatable = false;
        for (int i = 0; i < indexLimitVisible; i++) {
            if (accessibleColumns[i]) {
                Expression expression = exprColumns[i];
                if (expression.getType() == OpTypes.COLUMN) {
                    String name = expression.getColumn().getName().name;
                    if (columns.get(name) == 0) {
                        int index = table.findColumn(name);
                        columnMap[i] = baseColumnMap[index];
                        if (columnMap[i] != -1) {
                            isUpdatable = true;
                        }
                        continue;
                    }
                }
            }
            columnMap[i] = -1;
            isInsertable = false;
        }
        if (isInsertable) {
            checkList = baseTable.getColumnCheckList(columnMap);
            for (int i = 0; i < checkList.length; i++) {
                if (checkList[i]) {
                    continue;
                }
                ColumnSchema column = baseTable.getColumn(i);
                if (column.isIdentity() || column.isGenerated()
                        || column.hasDefault() || column.isNullable()) {}
                else {
                    isInsertable = false;
                    break;
                }
            }
        }
        if (!isUpdatable) {
            isInsertable = false;
        }
        if (isUpdatable) {
            this.columnMap = columnMap;
            this.baseTable = baseTable;
            if (view != null) {
                return;
            }
            indexLimitRowId++;
            hasRowID = true;
            if (!baseTable.isFileBased()) {
                indexLimitRowId++;
                hasMemoryRow = true;
            }
            indexLimitData = indexLimitRowId;
        }
    }
    void mergeQuery() {
        RangeVariable rangeVar            = rangeVariables[0];
        Table         table               = rangeVar.getTable();
        Expression    localQueryCondition = queryCondition;
        if (table instanceof TableDerived) {
            QueryExpression baseQueryExpression =
                ((TableDerived) table).getQueryExpression();
            if (baseQueryExpression == null
                    || !baseQueryExpression.isMergeable) {
                isMergeable = false;
                return;
            }
            QuerySpecification baseSelect =
                baseQueryExpression.getMainSelect();
            if (baseSelect.rangeVariables.length != 1) {
                isMergeable = false;
                return;
            }
            if (baseQueryExpression.view == null) {
                rangeVariables[0] = baseSelect.rangeVariables[0];
                rangeVariables[0].resetConditions();
                Expression[] newExprColumns = new Expression[indexLimitData];
                for (int i = 0; i < indexLimitData; i++) {
                    Expression e = exprColumns[i];
                    newExprColumns[i] = e.replaceColumnReferences(rangeVar,
                            baseSelect.exprColumns);
                }
                exprColumns = newExprColumns;
                if (localQueryCondition != null) {
                    localQueryCondition =
                        localQueryCondition.replaceColumnReferences(rangeVar,
                            baseSelect.exprColumns);
                }
                Expression baseQueryCondition = baseSelect.queryCondition;
                checkQueryCondition = baseSelect.checkQueryCondition;
                queryCondition =
                    ExpressionLogical.andExpressions(baseQueryCondition,
                                                     localQueryCondition);
            } else {
                RangeVariable[] newRangeVariables = new RangeVariable[1];
                newRangeVariables[0] =
                    baseSelect.rangeVariables[0].duplicate();
                newRangeVariables[0].rangePosition =
                    rangeVariables[0].rangePosition;
                Expression[] newBaseExprColumns =
                    new Expression[baseSelect.indexLimitData];
                for (int i = 0; i < baseSelect.indexLimitData; i++) {
                    Expression e = baseSelect.exprColumns[i].duplicate();
                    newBaseExprColumns[i] = e;
                    e.replaceRangeVariables(baseSelect.rangeVariables,
                                            newRangeVariables);
                }
                for (int i = 0; i < indexLimitData; i++) {
                    Expression e = exprColumns[i];
                    exprColumns[i] = e.replaceColumnReferences(rangeVar,
                            newBaseExprColumns);
                }
                Expression baseQueryCondition = baseSelect.queryCondition;
                if (baseQueryCondition != null) {
                    baseQueryCondition = baseQueryCondition.duplicate();
                    baseQueryCondition.replaceRangeVariables(
                        baseSelect.rangeVariables, newRangeVariables);
                }
                if (localQueryCondition != null) {
                    localQueryCondition =
                        localQueryCondition.replaceColumnReferences(rangeVar,
                            newBaseExprColumns);
                }
                checkQueryCondition = baseSelect.checkQueryCondition;
                if (checkQueryCondition != null) {
                    checkQueryCondition = checkQueryCondition.duplicate();
                    checkQueryCondition.replaceRangeVariables(
                        baseSelect.rangeVariables, newRangeVariables);
                }
                queryCondition =
                    ExpressionLogical.andExpressions(baseQueryCondition,
                                                     localQueryCondition);
                rangeVariables = newRangeVariables;
            }
        }
        if (view != null) {
            switch (view.getCheckOption()) {
                case SchemaObject.ViewCheckModes.CHECK_LOCAL :
                    if (!isUpdatable) {
                        throw Error.error(ErrorCode.X_42537);
                    }
                    checkQueryCondition = localQueryCondition;
                    break;
                case SchemaObject.ViewCheckModes.CHECK_CASCADE :
                    if (!isUpdatable) {
                        throw Error.error(ErrorCode.X_42537);
                    }
                    checkQueryCondition = queryCondition;
                    break;
            }
        }
        if (isAggregated) {
            isMergeable = false;
        }
    }
    static void collectSubQueriesAndReferences(OrderedHashSet set,
            Expression expression) {
        expression.collectAllExpressions(set,
                                         Expression.subqueryExpressionSet,
                                         Expression.emptyExpressionSet);
        int size = set.size();
        for (int i = 0; i < size; i++) {
            Expression e = (Expression) set.get(i);
            e.collectObjectNames(set);
        }
    }
    public OrderedHashSet getSubqueries() {
        OrderedHashSet set = null;
        for (int i = 0; i < indexLimitExpressions; i++) {
            set = exprColumns[i].collectAllSubqueries(set);
        }
        if (queryCondition != null) {
            set = queryCondition.collectAllSubqueries(set);
        }
        if (havingCondition != null) {
            set = havingCondition.collectAllSubqueries(set);
        }
        for (int i = 0; i < rangeVariables.length; i++) {
            OrderedHashSet temp = rangeVariables[i].getSubqueries();
            set = OrderedHashSet.addAll(set, temp);
        }
        return set;
    }
    public Table getBaseTable() {
        return baseTable;
    }
    public OrderedHashSet collectAllSubqueries(OrderedHashSet set) {
        return set;
    }
    public OrderedHashSet collectAllExpressions(OrderedHashSet set,
            OrderedIntHashSet typeSet, OrderedIntHashSet stopAtTypeSet) {
        for (int i = 0; i < indexStartAggregates; i++) {
            set = exprColumns[i].collectAllExpressions(set, typeSet,
                    stopAtTypeSet);
        }
        if (queryCondition != null) {
            set = queryCondition.collectAllExpressions(set, typeSet,
                    stopAtTypeSet);
        }
        if (havingCondition != null) {
            set = havingCondition.collectAllExpressions(set, typeSet,
                    stopAtTypeSet);
        }
        return set;
    }
    public void collectObjectNames(Set set) {
        for (int i = 0; i < indexStartAggregates; i++) {
            exprColumns[i].collectObjectNames(set);
        }
        if (queryCondition != null) {
            queryCondition.collectObjectNames(set);
        }
        if (havingCondition != null) {
            havingCondition.collectObjectNames(set);
        }
        for (int i = 0, len = rangeVariables.length; i < len; i++) {
            HsqlName name = rangeVariables[i].getTable().getName();
            set.add(name);
        }
    }
    public void replaceColumnReference(RangeVariable range,
                                       Expression[] list) {
        for (int i = 0; i < indexStartAggregates; i++) {
            exprColumns[i].replaceColumnReferences(range, list);
        }
        if (queryCondition != null) {
            queryCondition.replaceColumnReferences(range, list);
        }
        if (havingCondition != null) {
            havingCondition.replaceColumnReferences(range, list);
        }
        for (int i = 0, len = rangeVariables.length; i < len; i++) {
        }
    }
    public void replaceRangeVariables(RangeVariable[] ranges,
                                      RangeVariable[] newRanges) {
        for (int i = 0; i < indexStartAggregates; i++) {
            exprColumns[i].replaceRangeVariables(ranges, newRanges);
        }
        if (queryCondition != null) {
            queryCondition.replaceRangeVariables(ranges, newRanges);
        }
        if (havingCondition != null) {
            havingCondition.replaceRangeVariables(ranges, newRanges);
        }
        for (int i = 0, len = rangeVariables.length; i < len; i++) {
            rangeVariables[i].getSubqueries();
        }
    }
    public void setReturningResult() {
        setReturningResultSet();
        acceptsSequences = true;
        isTopLevel       = true;
    }
    void setReturningResultSet() {
        persistenceScope = TableBase.SCOPE_SESSION;
    }
    public boolean isSingleColumn() {
        return indexLimitVisible == 1;
    }
    public String[] getColumnNames() {
        String[] names = new String[indexLimitVisible];
        for (int i = 0; i < indexLimitVisible; i++) {
            names[i] = exprColumns[i].getAlias();
        }
        return names;
    }
    public Type[] getColumnTypes() {
        if (columnTypes.length == indexLimitVisible) {
            return columnTypes;
        }
        Type[] types = new Type[indexLimitVisible];
        ArrayUtil.copyArray(columnTypes, types, types.length);
        return types;
    }
    public int getColumnCount() {
        return indexLimitVisible;
    }
    public int[] getBaseTableColumnMap() {
        return columnMap;
    }
    public Expression getCheckCondition() {
        return queryCondition;
    }
    void getBaseTableNames(OrderedHashSet set) {
        for (int i = 0; i < rangeVariables.length; i++) {
            Table    rangeTable = rangeVariables[i].rangeTable;
            HsqlName name       = rangeTable.getName();
            if (rangeTable.isReadOnly() || rangeTable.isTemp()) {
                continue;
            }
            if (name.schema == SqlInvariants.SYSTEM_SCHEMA_HSQLNAME) {
                continue;
            }
            set.add(name);
        }
    }
    boolean isEquivalent(QueryExpression other) {
        if (!(other instanceof QuerySpecification)) {
            return false;
        }
        QuerySpecification otherSpec = (QuerySpecification) other;
        if (!Expression.equals(exprColumns, otherSpec.exprColumns)) {
            return false;
        }
        if (!Expression.equals(queryCondition, otherSpec.queryCondition)) {
            return false;
        }
        if (rangeVariables.length != otherSpec.rangeVariables.length) {
            return false;
        }
        for (int i = 0; i < rangeVariables.length; i++) {
            if (rangeVariables[i].getTable()
                    != otherSpec.rangeVariables[i].getTable()) {
                return false;
            }
        }
        return true;
    }
}