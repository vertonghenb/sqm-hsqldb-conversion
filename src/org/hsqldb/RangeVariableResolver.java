package org.hsqldb;
import org.hsqldb.ParserDQL.CompileContext;
import org.hsqldb.RangeVariable.RangeVariableConditions;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.index.Index;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.IntKeyIntValueHashMap;
import org.hsqldb.lib.Iterator;
import org.hsqldb.lib.MultiValueHashMap;
import org.hsqldb.lib.OrderedHashSet;
import org.hsqldb.lib.OrderedIntHashSet;
public class RangeVariableResolver {
    Session         session;
    RangeVariable[] rangeVariables;
    Expression      conditions;
    OrderedHashSet  rangeVarSet = new OrderedHashSet();
    CompileContext  compileContext;
    SortAndSlice    sortAndSlice = SortAndSlice.noSort;
    HsqlArrayList[] tempJoinExpressions;
    HsqlArrayList[] joinExpressions;
    HsqlArrayList[] whereExpressions;
    HsqlArrayList   queryExpressions = new HsqlArrayList();
    Expression[] inExpressions;
    boolean[]    inInJoin;
    int          inExpressionCount  = 0;
    boolean      expandInExpression = true;
    boolean hasOuterJoin = false;
    int     firstLeftJoinIndex;
    int     firstRightJoinIndex;
    OrderedIntHashSet     colIndexSetEqual = new OrderedIntHashSet();
    IntKeyIntValueHashMap colIndexSetOther = new IntKeyIntValueHashMap();
    OrderedHashSet        tempSet          = new OrderedHashSet();
    MultiValueHashMap     tempMap          = new MultiValueHashMap();
    RangeVariableResolver(QuerySpecification select) {
        this.rangeVariables = select.rangeVariables;
        this.conditions     = select.queryCondition;
        this.compileContext = select.compileContext;
        this.sortAndSlice   = select.sortAndSlice;
        initialise();
    }
    RangeVariableResolver(RangeVariable[] rangeVariables,
                          Expression conditions,
                          CompileContext compileContext) {
        this.rangeVariables = rangeVariables;
        this.conditions     = conditions;
        this.compileContext = compileContext;
        initialise();
    }
    private void initialise() {
        firstLeftJoinIndex  = rangeVariables.length;
        firstRightJoinIndex = rangeVariables.length;
        for (int i = 0; i < rangeVariables.length; i++) {
            RangeVariable range = rangeVariables[i];
            if (range.isLeftJoin) {
                if (firstLeftJoinIndex == rangeVariables.length) {
                    firstLeftJoinIndex = i;
                }
                hasOuterJoin = true;
            }
            if (range.isRightJoin) {
                if (firstRightJoinIndex == rangeVariables.length) {
                    firstRightJoinIndex = i;
                }
                hasOuterJoin = true;
            }
        }
        inExpressions       = new Expression[rangeVariables.length];
        inInJoin            = new boolean[rangeVariables.length];
        tempJoinExpressions = new HsqlArrayList[rangeVariables.length];
        for (int i = 0; i < rangeVariables.length; i++) {
            tempJoinExpressions[i] = new HsqlArrayList();
        }
        joinExpressions = new HsqlArrayList[rangeVariables.length];
        for (int i = 0; i < rangeVariables.length; i++) {
            joinExpressions[i] = new HsqlArrayList();
        }
        whereExpressions = new HsqlArrayList[rangeVariables.length];
        for (int i = 0; i < rangeVariables.length; i++) {
            whereExpressions[i] = new HsqlArrayList();
        }
    }
    void processConditions(Session session) {
        this.session = session;
        decomposeAndConditions(session, conditions, queryExpressions);
        for (int i = 0; i < rangeVariables.length; i++) {
            if (rangeVariables[i].joinCondition == null) {
                continue;
            }
            decomposeAndConditions(session, rangeVariables[i].joinCondition,
                                   tempJoinExpressions[i]);
        }
        conditions = null;
        if (!sortAndSlice.usingIndex
                || sortAndSlice.primaryTableIndex == null) {
            for (int i = 0; i < rangeVariables.length; i++) {
                rangeVarSet.add(rangeVariables[i]);
            }
            reorder();
            rangeVarSet.clear();
        }
        for (int i = 0; i < rangeVariables.length; i++) {
            rangeVarSet.add(rangeVariables[i]);
        }
        assignToLists();
        expandConditions();
        assignToRangeVariables();
    }
    void reorder() {
        if (rangeVariables.length == 1 || firstLeftJoinIndex == 1
                || firstRightJoinIndex != rangeVariables.length) {
            return;
        }
        for (int i = 0; i < rangeVariables.length; i++) {
            if (!rangeVariables[i].rangeTable.isSchemaBaseTable()) {
                return;
            }
        }
        HsqlArrayList joins  = new HsqlArrayList();
        HsqlArrayList starts = new HsqlArrayList();
        HsqlArrayList others = new HsqlArrayList();
        for (int i = 0; i < firstLeftJoinIndex; i++) {
            HsqlArrayList tempJoins = tempJoinExpressions[i];
            for (int j = 0; j < tempJoins.size(); j++) {
                Expression e = (Expression) tempJoins.get(j);
                if (e.isColumnEqual) {
                    joins.add(e);
                } else if (e.isSingleColumnCondition) {
                    starts.add(e);
                } else {
                    others.add(e);
                }
            }
        }
        for (int i = 0; i < queryExpressions.size(); i++) {
            Expression      e      = (Expression) queryExpressions.get(i);
            RangeVariable[] ranges = e.getJoinRangeVariables(rangeVariables);
            int count =
                ArrayUtil.countCommonElements((Object[]) rangeVariables,
                                              firstLeftJoinIndex,
                                              (Object[]) ranges);
            if (count != ranges.length) {
                continue;
            }
            if (e.isColumnEqual) {
                joins.add(e);
            } else if (e.isSingleColumnCondition) {
                starts.add(e);
            } else {
                others.add(e);
            }
        }
        if (starts.size() == 0) {
            return;
        }
        Expression    start    = null;
        int           position = 0;
        RangeVariable range    = null;
        double cost =
            rangeVariables[0].rangeTable.getRowStore(session).elementCount();
        if (cost < Index.minimumSelectivity) {
            cost = Index.minimumSelectivity;
        }
        if (rangeVariables[0].rangeTable.getTableType()
                == TableBase.CACHED_TABLE) {
            cost *= Index.cachedFactor;
        }
        for (int i = 0; i < starts.size(); i++) {
            Expression e = (Expression) starts.get(i);
            range = e.getJoinRangeVariables(rangeVariables)[0];
            double currentCost = e.costFactor(session, range, OpTypes.EQUAL);
            if (range == rangeVariables[0]) {
                start = null;
                break;
            }
            if (currentCost < cost) {
                start = e;
            }
        }
        if (start == null) {
            return;
        }
        position = ArrayUtil.find(rangeVariables, range);
        if (position <= 0) {
            return;
        }
        RangeVariable[] newRanges = new RangeVariable[rangeVariables.length];
        ArrayUtil.copyArray(rangeVariables, newRanges, rangeVariables.length);
        newRanges[position] = newRanges[0];
        newRanges[0]        = range;
        position            = 1;
        for (; position < firstLeftJoinIndex; position++) {
            boolean found = false;
            for (int i = 0; i < joins.size(); i++) {
                Expression e = (Expression) joins.get(i);
                if (e == null) {
                    continue;
                }
                int newPosition = getJoinedRangePosition(e, position,
                    newRanges);
                if (newPosition >= position) {
                    range                  = newRanges[position];
                    newRanges[position]    = newRanges[newPosition];
                    newRanges[newPosition] = range;
                    joins.set(i, null);
                    found = true;
                    break;
                }
            }
            if (!found) {
                break;
            }
        }
        if (position != firstLeftJoinIndex) {
            return;
        }
        ArrayUtil.copyArray(newRanges, rangeVariables, rangeVariables.length);
        joins.clear();
        for (int i = 0; i < firstLeftJoinIndex; i++) {
            HsqlArrayList tempJoins = tempJoinExpressions[i];
            joins.addAll(tempJoins);
            tempJoins.clear();
        }
        tempJoinExpressions[firstLeftJoinIndex - 1].addAll(joins);
    }
    int getJoinedRangePosition(Expression e, int position,
                               RangeVariable[] currentRanges) {
        int             found  = -1;
        RangeVariable[] ranges = e.getJoinRangeVariables(currentRanges);
        for (int i = 0; i < ranges.length; i++) {
            for (int j = 0; j < currentRanges.length; j++) {
                if (ranges[i] == currentRanges[j]) {
                    if (j >= position) {
                        if (found > 0) {
                            return -1;
                        } else {
                            found = j;
                        }
                    }
                }
            }
        }
        return found;
    }
    static Expression decomposeAndConditions(Session session, Expression e,
            HsqlArrayList conditions) {
        if (e == null) {
            return Expression.EXPR_TRUE;
        }
        Expression arg1 = e.getLeftNode();
        Expression arg2 = e.getRightNode();
        int        type = e.getType();
        if (type == OpTypes.AND) {
            arg1 = decomposeAndConditions(session, arg1, conditions);
            arg2 = decomposeAndConditions(session, arg2, conditions);
            if (arg1 == Expression.EXPR_TRUE) {
                return arg2;
            }
            if (arg2 == Expression.EXPR_TRUE) {
                return arg1;
            }
            e.setLeftNode(arg1);
            e.setRightNode(arg2);
            return e;
        } else if (type == OpTypes.EQUAL) {
            if (arg1.getType() == OpTypes.ROW
                    && arg2.getType() == OpTypes.ROW) {
                for (int i = 0; i < arg1.nodes.length; i++) {
                    Expression part = new ExpressionLogical(arg1.nodes[i],
                        arg2.nodes[i]);
                    part.resolveTypes(session, null);
                    conditions.add(part);
                }
                return Expression.EXPR_TRUE;
            }
        }
        if (e != Expression.EXPR_TRUE) {
            conditions.add(e);
        }
        return Expression.EXPR_TRUE;
    }
    static Expression decomposeOrConditions(Expression e,
            HsqlArrayList conditions) {
        if (e == null) {
            return Expression.EXPR_FALSE;
        }
        Expression arg1 = e.getLeftNode();
        Expression arg2 = e.getRightNode();
        int        type = e.getType();
        if (type == OpTypes.OR) {
            arg1 = decomposeOrConditions(arg1, conditions);
            arg2 = decomposeOrConditions(arg2, conditions);
            if (arg1 == Expression.EXPR_FALSE) {
                return arg2;
            }
            if (arg2 == Expression.EXPR_FALSE) {
                return arg1;
            }
            e = new ExpressionLogical(OpTypes.OR, arg1, arg2);
            return e;
        }
        if (e != Expression.EXPR_FALSE) {
            conditions.add(e);
        }
        return Expression.EXPR_FALSE;
    }
    void assignToLists() {
        int lastBoundary   = 0;
        int lastOuterIndex = -1;
        int lastRightIndex = -1;
        for (int i = 0; i < rangeVariables.length; i++) {
            if (rangeVariables[i].isLeftJoin) {
                lastOuterIndex = i;
            }
            if (rangeVariables[i].isRightJoin) {
                lastOuterIndex = i;
                lastRightIndex = i;
            }
            if (rangeVariables[i].isBoundary) {
                lastBoundary = i;
            }
            if (lastOuterIndex == i) {
                joinExpressions[i].addAll(tempJoinExpressions[i]);
            } else {
                int start = lastOuterIndex + 1;
                if (lastBoundary > start) {
                    start = lastBoundary;
                }
                for (int j = 0; j < tempJoinExpressions[i].size(); j++) {
                    assignToJoinLists(
                        (Expression) tempJoinExpressions[i].get(j),
                        joinExpressions, start);
                }
            }
        }
        for (int i = 0; i < queryExpressions.size(); i++) {
            assignToJoinLists((Expression) queryExpressions.get(i),
                              whereExpressions, lastRightIndex);
        }
    }
    void assignToJoinLists(Expression e, HsqlArrayList[] expressionLists,
                           int first) {
        tempSet.clear();
        e.collectRangeVariables(rangeVariables, tempSet);
        int index = rangeVarSet.getLargestIndex(tempSet);
        if (index == -1) {
            index = 0;
        }
        if (index < first) {
            index = first;
        }
        if (e instanceof ExpressionLogical) {
            if (((ExpressionLogical) e).isTerminal) {
                index = expressionLists.length - 1;
            }
        }
        expressionLists[index].add(e);
    }
    void expandConditions() {
        if (hasOuterJoin) {
            return;
        }
        expandConditions(joinExpressions, true);
        expandConditions(whereExpressions, false);
    }
    void expandConditions(HsqlArrayList[] array, boolean isJoin) {
        for (int i = 0; i < rangeVariables.length; i++) {
            HsqlArrayList list = array[i];
            tempMap.clear();
            tempSet.clear();
            boolean hasChain = false;
            for (int j = 0; j < list.size(); j++) {
                Expression e = (Expression) list.get(j);
                if (!e.isColumnEqual) {
                    continue;
                }
                if (e.getLeftNode().getRangeVariable()
                        == e.getRightNode().getRangeVariable()) {
                    continue;
                }
                if (e.getLeftNode().getRangeVariable() == rangeVariables[i]) {
                    tempMap.put(e.getLeftNode().getColumn(), e.getRightNode());
                    if (!tempSet.add(e.getLeftNode().getColumn())) {
                        hasChain = true;
                    }
                } else if (e.getRightNode().getRangeVariable()
                           == rangeVariables[i]) {
                    tempMap.put(e.getRightNode().getColumn(), e.getLeftNode());
                    if (!tempSet.add(e.getRightNode().getColumn())) {
                        hasChain = true;
                    }
                }
            }
            if (hasChain) {
                Iterator keyIt = tempMap.keySet().iterator();
                while (keyIt.hasNext()) {
                    Object   key = keyIt.next();
                    Iterator it  = tempMap.get(key);
                    tempSet.clear();
                    while (it.hasNext()) {
                        tempSet.add(it.next());
                    }
                    while (tempSet.size() > 1) {
                        Expression e1 =
                            (Expression) tempSet.remove(tempSet.size() - 1);
                        for (int j = 0; j < tempSet.size(); j++) {
                            Expression e2 = (Expression) tempSet.get(j);
                            closeJoinChain(array, e1, e2);
                        }
                    }
                }
            }
        }
    }
    void closeJoinChain(HsqlArrayList[] array, Expression e1, Expression e2) {
        int idx1  = rangeVarSet.getIndex(e1.getRangeVariable());
        int idx2  = rangeVarSet.getIndex(e2.getRangeVariable());
        int index = idx1 > idx2 ? idx1
                                : idx2;
        if (idx1 == -1 || idx2 == -1) {
            return;
        }
        Expression e = new ExpressionLogical(e1, e2);
        for (int i = 0; i < array[index].size(); i++) {
            if (e.equals(array[index].get(i))) {
                return;
            }
        }
        array[index].add(e);
    }
    void assignToRangeVariables() {
        for (int i = 0; i < rangeVariables.length; i++) {
            boolean                 hasIndex = false;
            RangeVariableConditions conditions;
            if (i < firstLeftJoinIndex
                    && firstRightJoinIndex == rangeVariables.length) {
                conditions = rangeVariables[i].joinConditions[0];
                joinExpressions[i].addAll(whereExpressions[i]);
                assignToRangeVariable(rangeVariables[i], conditions, i,
                                      joinExpressions[i]);
                assignToRangeVariable(conditions, joinExpressions[i]);
            } else {
                conditions = rangeVariables[i].joinConditions[0];
                assignToRangeVariable(rangeVariables[i], conditions, i,
                                      joinExpressions[i]);
                conditions = rangeVariables[i].joinConditions[0];
                if (conditions.hasIndex()) {
                    hasIndex = true;
                }
                assignToRangeVariable(conditions, joinExpressions[i]);
                conditions = rangeVariables[i].whereConditions[0];
                for (int j = i + 1; j < rangeVariables.length; j++) {
                    if (rangeVariables[j].isRightJoin) {
                        assignToRangeVariable(
                            rangeVariables[j].whereConditions[0],
                            whereExpressions[i]);
                    }
                }
                if (!hasIndex) {
                    assignToRangeVariable(rangeVariables[i], conditions, i,
                                          whereExpressions[i]);
                }
                assignToRangeVariable(conditions, whereExpressions[i]);
            }
        }
        if (expandInExpression && inExpressionCount != 0) {
            setInConditionsAsTables();
        }
    }
    void assignToRangeVariable(RangeVariableConditions conditions,
                               HsqlArrayList exprList) {
        for (int j = 0, size = exprList.size(); j < size; j++) {
            Expression e = (Expression) exprList.get(j);
            conditions.addCondition(e);
        }
    }
    Expression getIndexableColumn(HsqlArrayList exprList, int start) {
        for (int j = start, size = exprList.size(); j < size; j++) {
            Expression e = (Expression) exprList.get(j);
            if (e.getType() != OpTypes.EQUAL) {
                continue;
            }
            if (e.exprSubType == OpTypes.ALL_QUANTIFIED) {
                continue;
            }
            if (e.exprSubType == OpTypes.ANY_QUANTIFIED) {
                continue;
            }
            tempSet.clear();
            e.collectRangeVariables(rangeVariables, tempSet);
            if (tempSet.size() != 1) {
                continue;
            }
            RangeVariable range = (RangeVariable) tempSet.get(0);
            e = e.getIndexableExpression(range);
            if (e == null) {
                continue;
            }
            e = e.getLeftNode();
            if (e.getType() != OpTypes.COLUMN) {
                continue;
            }
            int colIndex = e.getColumnIndex();
            if (range.rangeTable.indexTypeForColumn(session, colIndex)
                    != Index.INDEX_NONE) {
                return e;
            }
        }
        return null;
    }
    void assignToRangeVariable(RangeVariable rangeVar,
                               RangeVariableConditions conditions,
                               int rangeVarIndex, HsqlArrayList exprList) {
        if (exprList.isEmpty()) {
            return;
        }
        setIndexConditions(conditions, exprList, rangeVarIndex, true);
    }
    private void setIndexConditions(RangeVariableConditions conditions,
                                    HsqlArrayList exprList, int rangeVarIndex,
                                    boolean includeOr) {
        boolean hasIndex;
        colIndexSetEqual.clear();
        colIndexSetOther.clear();
        for (int j = 0, size = exprList.size(); j < size; j++) {
            Expression e = (Expression) exprList.get(j);
            if (e == null) {
                continue;
            }
            if (!e.isIndexable(conditions.rangeVar)) {
                continue;
            }
            int type = e.getType();
            switch (type) {
                case OpTypes.OR : {
                    continue;
                }
                case OpTypes.COLUMN : {
                    continue;
                }
                case OpTypes.EQUAL : {
                    if (e.exprSubType == OpTypes.ANY_QUANTIFIED
                            || e.exprSubType == OpTypes.ALL_QUANTIFIED) {
                        continue;
                    }
                    if (e.getLeftNode().getRangeVariable()
                            != conditions.rangeVar) {
                        continue;
                    }
                    int colIndex = e.getLeftNode().getColumnIndex();
                    colIndexSetEqual.add(colIndex);
                    break;
                }
                case OpTypes.IS_NULL : {
                    if (e.getLeftNode().getRangeVariable()
                            != conditions.rangeVar) {
                        continue;
                    }
                    if (conditions.rangeVar.isLeftJoin) {
                        continue;
                    }
                    int colIndex = e.getLeftNode().getColumnIndex();
                    colIndexSetEqual.add(colIndex);
                    break;
                }
                case OpTypes.NOT : {
                    if (e.getLeftNode().getLeftNode().getRangeVariable()
                            != conditions.rangeVar) {
                        continue;
                    }
                    if (conditions.rangeVar.isLeftJoin) {
                        continue;
                    }
                    int colIndex =
                        e.getLeftNode().getLeftNode().getColumnIndex();
                    int count = colIndexSetOther.get(colIndex, 0);
                    colIndexSetOther.put(colIndex, count + 1);
                    break;
                }
                case OpTypes.SMALLER :
                case OpTypes.SMALLER_EQUAL :
                case OpTypes.GREATER :
                case OpTypes.GREATER_EQUAL : {
                    if (e.getLeftNode().getRangeVariable()
                            != conditions.rangeVar) {
                        continue;
                    }
                    int colIndex = e.getLeftNode().getColumnIndex();
                    int count    = colIndexSetOther.get(colIndex, 0);
                    colIndexSetOther.put(colIndex, count + 1);
                    break;
                }
                default : {
                    Error.runtimeError(ErrorCode.U_S0500,
                                       "RangeVariableResolver");
                }
            }
        }
        setEqualityConditions(conditions, exprList, rangeVarIndex);
        hasIndex = conditions.hasIndex();
        if (!hasIndex) {
            setNonEqualityConditions(conditions, exprList, rangeVarIndex);
        }
        if (rangeVarIndex == 0 && sortAndSlice.usingIndex) {
            hasIndex = true;
        } else {
            hasIndex = conditions.hasIndex();
        }
        boolean isOR = false;
        if (!hasIndex && includeOr) {
            for (int j = 0, size = exprList.size(); j < size; j++) {
                Expression e = (Expression) exprList.get(j);
                if (e == null) {
                    continue;
                }
                if (e.getType() == OpTypes.OR) {
                    hasIndex = ((ExpressionLogical) e).isIndexable(
                        conditions.rangeVar);
                    if (hasIndex) {
                        hasIndex = setOrConditions(conditions,
                                                   (ExpressionLogical) e,
                                                   rangeVarIndex);
                    }
                    if (hasIndex) {
                        exprList.set(j, null);
                        isOR = true;
                        break;
                    }
                } else if (e.getType() == OpTypes.EQUAL
                           && e.exprSubType == OpTypes.ANY_QUANTIFIED) {
                    if (rangeVarIndex >= firstLeftJoinIndex
                            || firstRightJoinIndex != rangeVariables.length) {
                        continue;
                    }
                    if (e.getRightNode().isCorrelated()) {
                        continue;
                    }
                    OrderedIntHashSet set = new OrderedIntHashSet();
                    ((ExpressionLogical) e).addLeftColumnsForAllAny(
                        conditions.rangeVar, set);
                    Index index =
                        conditions.rangeVar.rangeTable.getIndexForColumns(
                            session, set, false);
                    if (index != null
                            && inExpressions[rangeVarIndex] == null) {
                        inExpressions[rangeVarIndex] = e;
                        inInJoin[rangeVarIndex]      = conditions.isJoin;
                        inExpressionCount++;
                        exprList.set(j, null);
                        break;
                    }
                }
            }
        }
        for (int i = 0, size = exprList.size(); i < size; i++) {
            Expression e = (Expression) exprList.get(i);
            if (e == null) {
                continue;
            }
            if (isOR) {
                for (int j = 0; j < conditions.rangeVar.joinConditions.length;
                        j++) {
                    if (conditions.isJoin) {
                        conditions.rangeVar.joinConditions[j]
                            .nonIndexCondition =
                                ExpressionLogical
                                    .andExpressions(e, conditions.rangeVar
                                        .joinConditions[j].nonIndexCondition);
                    } else {
                        conditions.rangeVar.whereConditions[j]
                            .nonIndexCondition =
                                ExpressionLogical
                                    .andExpressions(e, conditions.rangeVar
                                        .whereConditions[j].nonIndexCondition);
                    }
                }
            } else {
                conditions.addCondition(e);
            }
        }
    }
    private boolean setOrConditions(RangeVariableConditions conditions,
                                    ExpressionLogical orExpression,
                                    int rangeVarIndex) {
        HsqlArrayList orExprList = new HsqlArrayList();
        decomposeOrConditions(orExpression, orExprList);
        RangeVariableConditions[] conditionsArray =
            new RangeVariableConditions[orExprList.size()];
        for (int i = 0; i < orExprList.size(); i++) {
            HsqlArrayList exprList = new HsqlArrayList();
            Expression    e        = (Expression) orExprList.get(i);
            decomposeAndConditions(session, e, exprList);
            RangeVariableConditions c =
                new RangeVariableConditions(conditions);
            setIndexConditions(c, exprList, rangeVarIndex, false);
            conditionsArray[i] = c;
            if (!c.hasIndex()) {
                return false;
            }
        }
        Expression exclude = null;
        for (int i = 0; i < conditionsArray.length; i++) {
            RangeVariableConditions c = conditionsArray[i];
            conditionsArray[i].excludeConditions = exclude;
            if (i == conditionsArray.length - 1) {
                break;
            }
            Expression e = null;
            if (c.indexCond != null) {
                for (int k = 0; k < c.indexedColumnCount; k++) {
                    e = ExpressionLogical.andExpressions(e, c.indexCond[k]);
                }
            }
            e       = ExpressionLogical.andExpressions(e, c.indexEndCondition);
            e       = ExpressionLogical.andExpressions(e, c.nonIndexCondition);
            exclude = ExpressionLogical.orExpressions(e, exclude);
        }
        if (exclude != null) {
        }
        if (conditions.isJoin) {
            conditions.rangeVar.joinConditions = conditionsArray;
            conditionsArray = new RangeVariableConditions[orExprList.size()];
            ArrayUtil.fillArray(conditionsArray,
                                conditions.rangeVar.whereConditions[0]);
            conditions.rangeVar.whereConditions = conditionsArray;
        } else {
            conditions.rangeVar.whereConditions = conditionsArray;
            conditionsArray = new RangeVariableConditions[orExprList.size()];
            ArrayUtil.fillArray(conditionsArray,
                                conditions.rangeVar.joinConditions[0]);
            conditions.rangeVar.joinConditions = conditionsArray;
        }
        return true;
    }
    private void setEqualityConditions(RangeVariableConditions conditions,
                                       HsqlArrayList exprList,
                                       int rangeVarIndex) {
        Index idx = null;
        if (rangeVarIndex == 0 && sortAndSlice.usingIndex) {
            idx = sortAndSlice.primaryTableIndex;
            if (idx != null) {
                conditions.rangeIndex = idx;
            }
        }
        if (idx == null) {
            idx = conditions.rangeVar.rangeTable.getIndexForColumns(session,
                    colIndexSetEqual, false);
        }
        if (idx == null) {
            return;
        }
        int[]        cols                = idx.getColumns();
        int          colCount            = cols.length;
        Expression[] firstRowExpressions = new Expression[cols.length];
        for (int j = 0; j < exprList.size(); j++) {
            Expression e = (Expression) exprList.get(j);
            if (e == null) {
                continue;
            }
            int type = e.getType();
            if (type == OpTypes.EQUAL || type == OpTypes.IS_NULL) {
                if (e.getLeftNode().getRangeVariable()
                        != conditions.rangeVar) {
                    continue;
                }
                if (!e.isIndexable(conditions.rangeVar)) {
                    continue;
                }
                int offset = ArrayUtil.find(cols,
                                            e.getLeftNode().getColumnIndex());
                if (offset != -1 && firstRowExpressions[offset] == null) {
                    firstRowExpressions[offset] = e;
                    exprList.set(j, null);
                    continue;
                }
            }
        }
        boolean hasNull = false;
        for (int i = 0; i < firstRowExpressions.length; i++) {
            Expression e = firstRowExpressions[i];
            if (e == null) {
                if (colCount == cols.length) {
                    colCount = i;
                }
                hasNull = true;
                continue;
            }
            if (hasNull) {
                conditions.addCondition(e);
                firstRowExpressions[i] = null;
            }
        }
        if (colCount > 0) {
            conditions.addIndexCondition(firstRowExpressions, idx, colCount);
        }
    }
    private void setNonEqualityConditions(RangeVariableConditions conditions,
                                          HsqlArrayList exprList,
                                          int rangeVarIndex) {
        if (colIndexSetOther.isEmpty()) {
            return;
        }
        int      currentCount = 0;
        int      currentIndex = 0;
        Iterator it           = colIndexSetOther.keySet().iterator();
        while (it.hasNext()) {
            int colIndex = it.nextInt();
            int colCount = colIndexSetOther.get(colIndex);
            if (colCount > currentCount) {
                currentIndex = colIndex;
            }
        }
        Index idx = null;
        if (rangeVarIndex == 0 && sortAndSlice.usingIndex) {
            idx = sortAndSlice.primaryTableIndex;
        }
        if (idx == null) {
            idx = conditions.rangeVar.rangeTable.getIndexForColumn(session,
                    currentIndex);
        }
        if (idx == null) {
            it = colIndexSetOther.keySet().iterator();
            while (it.hasNext()) {
                int colIndex = it.nextInt();
                if (colIndex != currentIndex) {
                    idx = conditions.rangeVar.rangeTable.getIndexForColumn(
                        session, colIndex);
                    if (idx != null) {
                        break;
                    }
                }
            }
        }
        if (idx == null) {
            return;
        }
        int[] cols = idx.getColumns();
        for (int j = 0; j < exprList.size(); j++) {
            Expression e = (Expression) exprList.get(j);
            if (e == null) {
                continue;
            }
            boolean isIndexed = false;
            switch (e.getType()) {
                case OpTypes.NOT : {
                    if (e.getLeftNode().getType() == OpTypes.IS_NULL
                            && cols[0]
                               == e.getLeftNode().getLeftNode()
                                   .getColumnIndex()) {
                        isIndexed = true;
                    }
                    break;
                }
                case OpTypes.SMALLER :
                case OpTypes.SMALLER_EQUAL :
                case OpTypes.GREATER :
                case OpTypes.GREATER_EQUAL : {
                    if (cols[0] == e.getLeftNode().getColumnIndex()) {
                        if (e.getRightNode() != null
                                && !e.getRightNode().isCorrelated()) {
                            isIndexed = true;
                        }
                    }
                    break;
                }
            }
            if (isIndexed) {
                Expression[] firstRowExpressions =
                    new Expression[idx.getColumnCount()];
                firstRowExpressions[0] = e;
                conditions.addIndexCondition(firstRowExpressions, idx, 1);
                exprList.set(j, null);
                break;
            }
        }
    }
    void setInConditionsAsTables() {
        for (int i = rangeVariables.length - 1; i >= 0; i--) {
            RangeVariable     rangeVar = rangeVariables[i];
            ExpressionLogical in       = (ExpressionLogical) inExpressions[i];
            if (in != null) {
                OrderedIntHashSet set = new OrderedIntHashSet();
                in.addLeftColumnsForAllAny(rangeVar, set);
                Index index = rangeVar.rangeTable.getIndexForColumns(session,
                    set, false);
                int indexedColCount = 0;
                for (int j = 0; j < index.getColumnCount(); j++) {
                    if (set.contains(index.getColumns()[j])) {
                        indexedColCount++;
                    } else {
                        break;
                    }
                }
                RangeVariable newRangeVar =
                    new RangeVariable(in.getRightNode().getTable(), null,
                                      null, null, compileContext);
                newRangeVar.isGenerated = true;
                RangeVariable[] newList =
                    new RangeVariable[rangeVariables.length + 1];
                ArrayUtil.copyAdjustArray(rangeVariables, newList,
                                          newRangeVar, i, 1);
                rangeVariables = newList;
                Expression[] exprList = new Expression[index.getColumnCount()];
                for (int j = 0; j < indexedColCount; j++) {
                    int leftIndex  = index.getColumns()[j];
                    int rightIndex = set.getIndex(leftIndex);
                    Expression e = new ExpressionLogical(rangeVar, leftIndex,
                                                         newRangeVar,
                                                         rightIndex);
                    exprList[j] = e;
                }
                boolean isOuter = rangeVariables[i].isLeftJoin
                                  || rangeVariables[i].isRightJoin;
                RangeVariableConditions conditions =
                    !inInJoin[i] && isOuter ? rangeVar.whereConditions[0]
                                            : rangeVar.joinConditions[0];
                conditions.addIndexCondition(exprList, index, indexedColCount);
                for (int j = 0; j < set.size(); j++) {
                    int leftIndex  = set.get(j);
                    int rightIndex = j;
                    Expression e = new ExpressionLogical(rangeVar, leftIndex,
                                                         newRangeVar,
                                                         rightIndex);
                    conditions.addCondition(e);
                }
            }
        }
    }
}