package org.hsqldb;
import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.HsqlList;
import org.hsqldb.lib.LongDeque;
import org.hsqldb.lib.OrderedHashSet;
import org.hsqldb.types.Type;
public class ParserDML extends ParserDQL {
    ParserDML(Session session, Scanner t) {
        super(session, t);
    }
    StatementDMQL compileInsertStatement(RangeVariable[] outerRanges) {
        read();
        readThis(Tokens.INTO);
        boolean[]     columnCheckList;
        int[]         columnMap;
        int           colCount;
        Table         table;
        RangeVariable range;
        boolean       overridingUser    = false;
        boolean       overridingSystem  = false;
        boolean       assignsToIdentity = false;
        range           = readSimpleRangeVariable(StatementTypes.INSERT);
        table           = range.getTable();
        columnCheckList = null;
        columnMap       = table.getColumnMap();
        colCount        = table.getColumnCount();
        int   position  = getPosition();
        Table baseTable = table.isTriggerInsertable() ? table
                                                      : table.getBaseTable();
        switch (token.tokenType) {
            case Tokens.DEFAULT : {
                read();
                readThis(Tokens.VALUES);
                Expression insertExpression = new Expression(OpTypes.ROW,
                    new Expression[]{});
                insertExpression = new Expression(OpTypes.VALUELIST,
                                                  new Expression[]{
                                                      insertExpression });
                columnCheckList = table.getNewColumnCheckList();
                for (int i = 0; i < table.colDefaults.length; i++) {
                    if (table.colDefaults[i] == null
                            && table.identityColumn != i) {
                        if (!table.getColumn(i).isGenerated()) {
                            throw Error.error(ErrorCode.X_42544);
                        }
                    }
                }
                StatementDMQL cs = new StatementInsert(session, table,
                                                       columnMap,
                                                       insertExpression,
                                                       columnCheckList,
                                                       compileContext);
                return cs;
            }
            case Tokens.OPENBRACKET : {
                int brackets = readOpenBrackets();
                if (brackets == 1) {
                    boolean isQuery = false;
                    switch (token.tokenType) {
                        case Tokens.WITH :
                        case Tokens.SELECT :
                        case Tokens.TABLE : {
                            rewind(position);
                            isQuery = true;
                            break;
                        }
                        default :
                    }
                    if (isQuery) {
                        break;
                    }
                    OrderedHashSet columnNames = new OrderedHashSet();
                    boolean        withPrefix  = database.sqlSyntaxOra;
                    readSimpleColumnNames(columnNames, range, withPrefix);
                    readThis(Tokens.CLOSEBRACKET);
                    colCount  = columnNames.size();
                    columnMap = table.getColumnIndexes(columnNames);
                    if (token.tokenType != Tokens.VALUES
                            && token.tokenType != Tokens.OVERRIDING) {
                        break;
                    }
                } else {
                    rewind(position);
                    break;
                }
            }
            case Tokens.OVERRIDING : {
                if (token.tokenType == Tokens.OVERRIDING) {
                    read();
                    if (token.tokenType == Tokens.USER) {
                        read();
                        overridingUser = true;
                    } else if (token.tokenType == Tokens.SYSTEM) {
                        read();
                        overridingSystem = true;
                    } else {
                        unexpectedToken();
                    }
                    readThis(Tokens.VALUE);
                    if (token.tokenType != Tokens.VALUES) {
                        break;
                    }
                }
            }
            case Tokens.VALUES : {
                read();
                columnCheckList = table.getColumnCheckList(columnMap);
                Expression insertExpressions =
                    XreadContextuallyTypedTable(colCount);
                HsqlList unresolved =
                    insertExpressions.resolveColumnReferences(session,
                        outerRanges, null);
                ExpressionColumn.checkColumnsResolved(unresolved);
                insertExpressions.resolveTypes(session, null);
                setParameterTypes(insertExpressions, table, columnMap);
                if (table != baseTable) {
                    int[] baseColumnMap = table.getBaseTableColumnMap();
                    int[] newColumnMap  = new int[columnMap.length];
                    ArrayUtil.projectRow(baseColumnMap, columnMap,
                                         newColumnMap);
                    columnMap = newColumnMap;
                }
                Expression[] rowList = insertExpressions.nodes;
                for (int j = 0; j < rowList.length; j++) {
                    Expression[] rowArgs = rowList[j].nodes;
                    for (int i = 0; i < rowArgs.length; i++) {
                        Expression e = rowArgs[i];
                        ColumnSchema column =
                            baseTable.getColumn(columnMap[i]);
                        if (column.isIdentity()) {
                            assignsToIdentity = true;
                            if (e.getType() != OpTypes.DEFAULT) {
                                if (baseTable.identitySequence.isAlways()) {
                                    if (!overridingUser && !overridingSystem) {
                                        throw Error.error(ErrorCode.X_42543);
                                    }
                                }
                                if (overridingUser) {
                                    rowArgs[i] =
                                        new ExpressionColumn(OpTypes.DEFAULT);
                                }
                            }
                        } else if (column.hasDefault()) {}
                        else if (column.isGenerated()) {
                            if (e.getType() != OpTypes.DEFAULT) {
                                throw Error.error(ErrorCode.X_42541);
                            }
                        } else {
                            if (e.getType() == OpTypes.DEFAULT) {
                                throw Error.error(ErrorCode.X_42544);
                            }
                        }
                        if (e.isUnresolvedParam()) {
                            e.setAttributesAsColumn(column, true);
                        }
                    }
                }
                if (!assignsToIdentity
                        && (overridingUser || overridingSystem)) {
                    unexpectedTokenRequire(Tokens.T_OVERRIDING);
                }
                StatementDMQL cs = new StatementInsert(session, table,
                                                       columnMap,
                                                       insertExpressions,
                                                       columnCheckList,
                                                       compileContext);
                return cs;
            }
            case Tokens.WITH :
            case Tokens.SELECT :
            case Tokens.TABLE : {
                break;
            }
            default : {
                throw unexpectedToken();
            }
        }
        columnCheckList = table.getColumnCheckList(columnMap);
        if (baseTable != null && table != baseTable) {
            int[] baseColumnMap = table.getBaseTableColumnMap();
            int[] newColumnMap  = new int[columnMap.length];
            ArrayUtil.projectRow(baseColumnMap, columnMap, newColumnMap);
            columnMap = newColumnMap;
        }
        int enforcedDefaultIndex = baseTable.getIdentityColumnIndex();
        int overrideIndex        = -1;
        if (enforcedDefaultIndex != -1
                && ArrayUtil.find(columnMap, enforcedDefaultIndex) > -1) {
            if (baseTable.identitySequence.isAlways()) {
                if (!overridingUser && !overridingSystem) {
                    throw Error.error(ErrorCode.X_42543);
                }
            }
            if (overridingUser) {
                overrideIndex = enforcedDefaultIndex;
            }
        } else if (overridingUser || overridingSystem) {
            unexpectedTokenRequire(Tokens.T_OVERRIDING);
        }
        Type[] types = new Type[columnMap.length];
        ArrayUtil.projectRow(baseTable.getColumnTypes(), columnMap, types);
        QueryExpression queryExpression = XreadQueryExpression(outerRanges);
        queryExpression.setReturningResult();
        queryExpression.resolve(session, outerRanges, types);
        if (colCount != queryExpression.getColumnCount()) {
            throw Error.error(ErrorCode.X_42546);
        }
        StatementDMQL cs = new StatementInsert(session, table, columnMap,
                                               columnCheckList,
                                               queryExpression,
                                               compileContext, overrideIndex);
        return cs;
    }
    private static void setParameterTypes(Expression tableExpression,
                                          Table table, int[] columnMap) {
        for (int i = 0; i < tableExpression.nodes.length; i++) {
            Expression[] list = tableExpression.nodes[i].nodes;
            for (int j = 0; j < list.length; j++) {
                if (list[j].isUnresolvedParam()) {
                    list[j].setAttributesAsColumn(
                        table.getColumn(columnMap[j]), true);
                }
            }
        }
    }
    Statement compileTruncateStatement() {
        boolean         isTable         = false;
        boolean         withCommit      = false;
        boolean         noCheck         = false;
        boolean         restartIdentity = false;
        HsqlName        objectName      = null;
        RangeVariable[] rangeVariables  = null;
        Table           table           = null;
        HsqlName[]      writeTableNames = null;
        readThis(Tokens.TRUNCATE);
        if (token.tokenType == Tokens.TABLE) {
            readThis(Tokens.TABLE);
            rangeVariables = new RangeVariable[]{
                readSimpleRangeVariable(StatementTypes.TRUNCATE) };
            table      = rangeVariables[0].getTable();
            objectName = table.getName();
            isTable    = true;
        } else {
            readThis(Tokens.SCHEMA);
            objectName = readSchemaName();
        }
        switch (token.tokenType) {
            case Tokens.CONTINUE : {
                read();
                readThis(Tokens.IDENTITY);
                break;
            }
            case Tokens.RESTART : {
                read();
                readThis(Tokens.IDENTITY);
                restartIdentity = true;
                break;
            }
        }
        if (!isTable) {
            checkIsThis(Tokens.AND);
        }
        if (readIfThis(Tokens.AND)) {
            readThis(Tokens.COMMIT);
            withCommit = true;
            if (readIfThis(Tokens.NO)) {
                readThis(Tokens.CHECK);
                noCheck = true;
            }
        }
        if (isTable) {
            writeTableNames = new HsqlName[]{ table.getName() };
        } else {
            writeTableNames =
                session.database.schemaManager.getCatalogAndBaseTableNames();
        }
        if (withCommit) {
            Object[] args = new Object[] {
                objectName, restartIdentity, noCheck
            };
            return new StatementCommand(StatementTypes.TRUNCATE, args, null,
                                        writeTableNames);
        }
        Statement cs = new StatementDML(session, table, rangeVariables,
                                        compileContext, restartIdentity,
                                        StatementTypes.TRUNCATE);
        return cs;
    }
    Statement compileDeleteStatement(RangeVariable[] outerRanges) {
        Expression condition       = null;
        boolean    restartIdentity = false;
        readThis(Tokens.DELETE);
        readThis(Tokens.FROM);
        RangeVariable[] rangeVariables = null;
        Table           table          = null;
        rangeVariables = new RangeVariable[]{
            readSimpleRangeVariable(StatementTypes.DELETE_WHERE) };
        table = rangeVariables[0].getTable();
        if (table.isTriggerDeletable()) {
            rangeVariables[0].resetViewRageTableAsSubquery();
        }
        if (token.tokenType == Tokens.WHERE) {
            read();
            condition = XreadBooleanValueExpression();
            HsqlList unresolved = condition.resolveColumnReferences(session,
                outerRanges, null);
            unresolved = Expression.resolveColumnSet(session, rangeVariables,
                    rangeVariables.length, unresolved, null);
            ExpressionColumn.checkColumnsResolved(unresolved);
            condition.resolveTypes(session, null);
            if (condition.isUnresolvedParam()) {
                condition.dataType = Type.SQL_BOOLEAN;
            }
            if (condition.getDataType() != Type.SQL_BOOLEAN) {
                throw Error.error(ErrorCode.X_42568);
            }
        }
        Table baseTable = table.isTriggerDeletable() ? table
                                                     : table.getBaseTable();
        if (table != baseTable) {
            QuerySpecification baseSelect =
                ((TableDerived) table).getQueryExpression().getMainSelect();
            RangeVariable[] newRangeVariables =
                (RangeVariable[]) ArrayUtil.duplicateArray(
                    baseSelect.rangeVariables);
            newRangeVariables[0] = baseSelect.rangeVariables[0].duplicate();
            Expression[] newBaseExprColumns =
                new Expression[baseSelect.indexLimitData];
            for (int i = 0; i < baseSelect.indexLimitData; i++) {
                Expression e = baseSelect.exprColumns[i].duplicate();
                newBaseExprColumns[i] = e;
                e.replaceRangeVariables(baseSelect.rangeVariables,
                                        newRangeVariables);
            }
            Expression baseQueryCondition = baseSelect.queryCondition;
            if (baseQueryCondition != null) {
                baseQueryCondition = baseQueryCondition.duplicate();
                baseQueryCondition.replaceRangeVariables(rangeVariables,
                        newRangeVariables);
            }
            if (condition != null) {
                condition =
                    condition.replaceColumnReferences(rangeVariables[0],
                                                      newBaseExprColumns);
            }
            rangeVariables = newRangeVariables;
            condition = ExpressionLogical.andExpressions(baseQueryCondition,
                    condition);
        }
        if (condition != null) {
            rangeVariables[0].addJoinCondition(condition);
            RangeVariableResolver resolver =
                new RangeVariableResolver(rangeVariables, null,
                                          compileContext);
            resolver.processConditions(session);
            rangeVariables = resolver.rangeVariables;
        }
        Statement cs = new StatementDML(session, table, rangeVariables,
                                        compileContext, restartIdentity,
                                        StatementTypes.DELETE_WHERE);
        return cs;
    }
    StatementDMQL compileUpdateStatement(RangeVariable[] outerRanges) {
        read();
        Expression[]   updateExpressions;
        int[]          columnMap;
        boolean[]      columnCheckList;
        OrderedHashSet targetSet    = new OrderedHashSet();
        LongDeque      colIndexList = new LongDeque();
        HsqlArrayList  exprList     = new HsqlArrayList();
        RangeVariable[] rangeVariables = {
            readSimpleRangeVariable(StatementTypes.UPDATE_WHERE) };
        Table table     = rangeVariables[0].rangeTable;
        Table baseTable = table.isTriggerUpdatable() ? table
                                                     : table.getBaseTable();
        if (table.isTriggerUpdatable()) {
            rangeVariables[0].resetViewRageTableAsSubquery();
        }
        readThis(Tokens.SET);
        readSetClauseList(rangeVariables, targetSet, colIndexList, exprList);
        columnMap = new int[colIndexList.size()];
        colIndexList.toArray(columnMap);
        Expression[] targets = new Expression[targetSet.size()];
        targetSet.toArray(targets);
        for (int i = 0; i < targets.length; i++) {
            this.resolveOuterReferencesAndTypes(outerRanges, targets[i]);
        }
        columnCheckList   = table.getColumnCheckList(columnMap);
        updateExpressions = new Expression[exprList.size()];
        exprList.toArray(updateExpressions);
        Expression condition = null;
        if (token.tokenType == Tokens.WHERE) {
            read();
            condition = XreadBooleanValueExpression();
            HsqlList unresolved = condition.resolveColumnReferences(session,
                outerRanges, null);
            unresolved = Expression.resolveColumnSet(session, rangeVariables,
                    rangeVariables.length, unresolved, null);
            ExpressionColumn.checkColumnsResolved(unresolved);
            condition.resolveTypes(session, null);
            if (condition.isUnresolvedParam()) {
                condition.dataType = Type.SQL_BOOLEAN;
            }
            if (condition.getDataType() != Type.SQL_BOOLEAN) {
                throw Error.error(ErrorCode.X_42568);
            }
        }
        resolveUpdateExpressions(table, rangeVariables, columnMap,
                                 updateExpressions, outerRanges);
        if (table != baseTable) {
            QuerySpecification baseSelect =
                ((TableDerived) table).getQueryExpression().getMainSelect();
            RangeVariable[] newRangeVariables =
                (RangeVariable[]) ArrayUtil.duplicateArray(
                    baseSelect.rangeVariables);
            newRangeVariables[0] = baseSelect.rangeVariables[0].duplicate();
            Expression[] newBaseExprColumns =
                new Expression[baseSelect.indexLimitData];
            for (int i = 0; i < baseSelect.indexLimitData; i++) {
                Expression e = baseSelect.exprColumns[i].duplicate();
                newBaseExprColumns[i] = e;
                e.replaceRangeVariables(baseSelect.rangeVariables,
                                        newRangeVariables);
            }
            Expression baseQueryCondition = baseSelect.queryCondition;
            if (baseQueryCondition != null) {
                baseQueryCondition = baseQueryCondition.duplicate();
                baseQueryCondition.replaceRangeVariables(rangeVariables,
                        newRangeVariables);
            }
            if (condition != null) {
                condition =
                    condition.replaceColumnReferences(rangeVariables[0],
                                                      newBaseExprColumns);
            }
            for (int i = 0; i < updateExpressions.length; i++) {
                updateExpressions[i] =
                    updateExpressions[i].replaceColumnReferences(
                        rangeVariables[0], newBaseExprColumns);
            }
            rangeVariables = newRangeVariables;
            condition = ExpressionLogical.andExpressions(baseQueryCondition,
                    condition);
        }
        if (condition != null) {
            rangeVariables[0].addJoinCondition(condition);
            RangeVariableResolver resolver =
                new RangeVariableResolver(rangeVariables, null,
                                          compileContext);
            resolver.processConditions(session);
            rangeVariables = resolver.rangeVariables;
        }
        if (table != baseTable) {
            int[] baseColumnMap = table.getBaseTableColumnMap();
            int[] newColumnMap  = new int[columnMap.length];
            ArrayUtil.projectRow(baseColumnMap, columnMap, newColumnMap);
            columnMap = newColumnMap;
            for (int i = 0; i < columnMap.length; i++) {
                if (baseTable.colGenerated[columnMap[i]]) {
                    throw Error.error(ErrorCode.X_42513);
                }
            }
        }
        StatementDMQL cs = new StatementDML(session, targets, table,
                                            rangeVariables, columnMap,
                                            updateExpressions,
                                            columnCheckList, compileContext);
        return cs;
    }
    void resolveUpdateExpressions(Table targetTable,
                                  RangeVariable[] rangeVariables,
                                  int[] columnMap,
                                  Expression[] colExpressions,
                                  RangeVariable[] outerRanges) {
        HsqlList unresolved           = null;
        int      enforcedDefaultIndex = -1;
        if (targetTable.hasIdentityColumn()
                && targetTable.identitySequence.isAlways()) {
            enforcedDefaultIndex = targetTable.getIdentityColumnIndex();
        }
        for (int i = 0, ix = 0; i < columnMap.length; ix++) {
            Expression expr = colExpressions[ix];
            Expression e;
            if (targetTable.colGenerated[columnMap[i]]) {
                throw Error.error(ErrorCode.X_42513);
            }
            if (expr.getType() == OpTypes.ROW) {
                Expression[] elements = expr.nodes;
                for (int j = 0; j < elements.length; j++, i++) {
                    e = elements[j];
                    if (enforcedDefaultIndex == columnMap[i]) {
                        if (e.getType() != OpTypes.DEFAULT) {
                            throw Error.error(ErrorCode.X_42541);
                        }
                    }
                    if (e.isUnresolvedParam()) {
                        e.setAttributesAsColumn(
                            targetTable.getColumn(columnMap[i]), true);
                    } else if (e.getType() == OpTypes.DEFAULT) {
                        if (targetTable.colDefaults[columnMap[i]] == null
                                && targetTable.identityColumn
                                   != columnMap[i]) {
                            throw Error.error(ErrorCode.X_42544);
                        }
                    } else {
                        unresolved = expr.resolveColumnReferences(session,
                                outerRanges, null);
                        unresolved = Expression.resolveColumnSet(session,
                                rangeVariables, rangeVariables.length,
                                unresolved, null);
                        ExpressionColumn.checkColumnsResolved(unresolved);
                        unresolved = null;
                        e.resolveTypes(session, null);
                    }
                }
            } else if (expr.getType() == OpTypes.ROW_SUBQUERY) {
                unresolved = expr.resolveColumnReferences(session,
                        outerRanges, null);
                unresolved = Expression.resolveColumnSet(session,
                        rangeVariables, rangeVariables.length, unresolved,
                        null);
                ExpressionColumn.checkColumnsResolved(unresolved);
                expr.resolveTypes(session, null);
                int count = expr.subQuery.queryExpression.getColumnCount();
                for (int j = 0; j < count; j++, i++) {
                    if (enforcedDefaultIndex == columnMap[i]) {
                        throw Error.error(ErrorCode.X_42541);
                    }
                }
            } else {
                e = expr;
                if (enforcedDefaultIndex == columnMap[i]) {
                    if (e.getType() != OpTypes.DEFAULT) {
                        throw Error.error(ErrorCode.X_42541);
                    }
                }
                if (e.isUnresolvedParam()) {
                    e.setAttributesAsColumn(
                        targetTable.getColumn(columnMap[i]), true);
                } else if (e.getType() == OpTypes.DEFAULT) {
                    if (targetTable.colDefaults[columnMap[i]] == null
                            && targetTable.identityColumn != columnMap[i]) {
                        throw Error.error(ErrorCode.X_42544);
                    }
                } else {
                    unresolved = expr.resolveColumnReferences(session,
                            outerRanges, null);
                    unresolved = Expression.resolveColumnSet(session,
                            rangeVariables, rangeVariables.length, unresolved,
                            null);
                    ExpressionColumn.checkColumnsResolved(unresolved);
                    e.resolveTypes(session, null);
                }
                i++;
            }
        }
    }
    void readSetClauseList(RangeVariable[] rangeVars, OrderedHashSet targets,
                           LongDeque colIndexList, HsqlArrayList expressions) {
        while (true) {
            int degree;
            if (token.tokenType == Tokens.OPENBRACKET) {
                read();
                int oldCount = targets.size();
                readTargetSpecificationList(targets, rangeVars, colIndexList);
                degree = targets.size() - oldCount;
                readThis(Tokens.CLOSEBRACKET);
            } else {
                Expression target = XreadTargetSpecification(rangeVars,
                    colIndexList);
                if (!targets.add(target)) {
                    ColumnSchema col = target.getColumn();
                    throw Error.error(ErrorCode.X_42579, col.getName().name);
                }
                degree = 1;
            }
            readThis(Tokens.EQUALS);
            int position = getPosition();
            int brackets = readOpenBrackets();
            if (token.tokenType == Tokens.SELECT) {
                rewind(position);
                SubQuery sq = XreadSubqueryBody(RangeVariable.emptyArray,
                                                OpTypes.ROW_SUBQUERY);
                if (degree != sq.queryExpression.getColumnCount()) {
                    throw Error.error(ErrorCode.X_42546);
                }
                Expression e = new Expression(OpTypes.ROW_SUBQUERY, sq);
                expressions.add(e);
                if (token.tokenType == Tokens.COMMA) {
                    read();
                    continue;
                }
                break;
            }
            if (brackets > 0) {
                rewind(position);
            }
            if (degree > 1) {
                readThis(Tokens.OPENBRACKET);
                Expression e = readRow();
                readThis(Tokens.CLOSEBRACKET);
                int rowDegree = e.getType() == OpTypes.ROW ? e.nodes.length
                                                           : 1;
                if (degree != rowDegree) {
                    throw Error.error(ErrorCode.X_42546);
                }
                expressions.add(e);
            } else {
                Expression e = XreadValueExpressionWithContext();
                expressions.add(e);
            }
            if (token.tokenType == Tokens.COMMA) {
                read();
                continue;
            }
            break;
        }
    }
    void readGetClauseList(RangeVariable[] rangeVars, OrderedHashSet targets,
                           LongDeque colIndexList, HsqlArrayList expressions) {
        while (true) {
            Expression target = XreadTargetSpecification(rangeVars,
                colIndexList);
            if (!targets.add(target)) {
                ColumnSchema col = target.getColumn();
                throw Error.error(ErrorCode.X_42579, col.getName().name);
            }
            readThis(Tokens.EQUALS);
            switch (token.tokenType) {
                case Tokens.ROW_COUNT :
                case Tokens.MORE :
                    int columnIndex =
                        ExpressionColumn.diagnosticsList.getIndex(
                            token.tokenString);
                    Expression e =
                        new ExpressionColumn(OpTypes.DIAGNOSTICS_VARIABLE,
                                             columnIndex);
                    expressions.add(e);
                    read();
                    break;
            }
            if (token.tokenType == Tokens.COMMA) {
                read();
                continue;
            }
            break;
        }
    }
    StatementDMQL compileMergeStatement(RangeVariable[] outerRanges) {
        boolean[]     insertColumnCheckList;
        int[]         insertColumnMap = null;
        int[]         updateColumnMap = null;
        int[]         baseUpdateColumnMap;
        Table         table;
        RangeVariable targetRange;
        RangeVariable sourceRange;
        Expression    mergeCondition;
        Expression[]  targets           = null;
        HsqlArrayList updateList        = new HsqlArrayList();
        Expression[]  updateExpressions = Expression.emptyArray;
        HsqlArrayList insertList        = new HsqlArrayList();
        Expression    insertExpression  = null;
        read();
        readThis(Tokens.INTO);
        targetRange = readSimpleRangeVariable(StatementTypes.MERGE);
        table       = targetRange.rangeTable;
        readThis(Tokens.USING);
        sourceRange = readTableOrSubquery(outerRanges);
        readThis(Tokens.ON);
        mergeCondition = XreadBooleanValueExpression();
        if (mergeCondition.getDataType() != Type.SQL_BOOLEAN) {
            throw Error.error(ErrorCode.X_42568);
        }
        RangeVariable[] fullRangeVars   = new RangeVariable[] {
            sourceRange, targetRange
        };
        RangeVariable[] sourceRangeVars = new RangeVariable[]{ sourceRange };
        RangeVariable[] targetRangeVars = new RangeVariable[]{ targetRange };
        insertColumnMap       = table.getColumnMap();
        insertColumnCheckList = table.getNewColumnCheckList();
        OrderedHashSet updateTargetSet    = new OrderedHashSet();
        OrderedHashSet insertColNames     = new OrderedHashSet();
        LongDeque      updateColIndexList = new LongDeque();
        readMergeWhen(updateColIndexList, insertColNames, updateTargetSet,
                      insertList, updateList, targetRangeVars, sourceRange);
        if (insertList.size() > 0) {
            int colCount = insertColNames.size();
            if (colCount != 0) {
                insertColumnMap = table.getColumnIndexes(insertColNames);
                insertColumnCheckList =
                    table.getColumnCheckList(insertColumnMap);
            }
            insertExpression = (Expression) insertList.get(0);
            setParameterTypes(insertExpression, table, insertColumnMap);
        }
        if (updateList.size() > 0) {
            targets = new Expression[updateTargetSet.size()];
            updateTargetSet.toArray(targets);
            for (int i = 0; i < targets.length; i++) {
                this.resolveOuterReferencesAndTypes(outerRanges, targets[i]);
            }
            updateExpressions = new Expression[updateList.size()];
            updateList.toArray(updateExpressions);
            updateColumnMap = new int[updateColIndexList.size()];
            updateColIndexList.toArray(updateColumnMap);
        }
        if (updateExpressions.length != 0) {
            Table baseTable = table.isTriggerUpdatable() ? table
                                                         : table.getBaseTable();
            baseUpdateColumnMap = updateColumnMap;
            if (table != baseTable) {
                baseUpdateColumnMap = new int[updateColumnMap.length];
                ArrayUtil.projectRow(table.getBaseTableColumnMap(),
                                     updateColumnMap, baseUpdateColumnMap);
            }
            resolveUpdateExpressions(table, fullRangeVars, updateColumnMap,
                                     updateExpressions, outerRanges);
        }
        HsqlList unresolved = null;
        unresolved = mergeCondition.resolveColumnReferences(session,
                fullRangeVars, null);
        unresolved = Expression.resolveColumnSet(session, outerRanges,
                outerRanges.length, unresolved, null);
        ExpressionColumn.checkColumnsResolved(unresolved);
        mergeCondition.resolveTypes(session, null);
        if (mergeCondition.isUnresolvedParam()) {
            mergeCondition.dataType = Type.SQL_BOOLEAN;
        }
        if (mergeCondition.getDataType() != Type.SQL_BOOLEAN) {
            throw Error.error(ErrorCode.X_42568);
        }
        fullRangeVars[1].addJoinCondition(mergeCondition);
        RangeVariableResolver resolver =
            new RangeVariableResolver(fullRangeVars, null, compileContext);
        resolver.processConditions(session);
        fullRangeVars = resolver.rangeVariables;
        if (insertExpression != null) {
            unresolved = insertExpression.resolveColumnReferences(session,
                    sourceRangeVars, null);
            unresolved = Expression.resolveColumnSet(session, outerRanges,
                    outerRanges.length, unresolved, null);
            ExpressionColumn.checkColumnsResolved(unresolved);
            insertExpression.resolveTypes(session, null);
        }
        StatementDMQL cs = new StatementDML(session, targets, fullRangeVars,
                                            insertColumnMap, updateColumnMap,
                                            insertColumnCheckList,
                                            mergeCondition, insertExpression,
                                            updateExpressions, compileContext);
        return cs;
    }
    private void readMergeWhen(LongDeque updateColIndexList,
                               OrderedHashSet insertColumnNames,
                               OrderedHashSet updateTargetSet,
                               HsqlArrayList insertExpressions,
                               HsqlArrayList updateExpressions,
                               RangeVariable[] targetRangeVars,
                               RangeVariable sourceRangeVar) {
        Table table       = targetRangeVars[0].rangeTable;
        int   columnCount = table.getColumnCount();
        readThis(Tokens.WHEN);
        if (token.tokenType == Tokens.MATCHED) {
            if (updateExpressions.size() != 0) {
                throw Error.error(ErrorCode.X_42547);
            }
            read();
            readThis(Tokens.THEN);
            readThis(Tokens.UPDATE);
            readThis(Tokens.SET);
            readSetClauseList(targetRangeVars, updateTargetSet,
                              updateColIndexList, updateExpressions);
        } else if (token.tokenType == Tokens.NOT) {
            if (insertExpressions.size() != 0) {
                throw Error.error(ErrorCode.X_42548);
            }
            read();
            readThis(Tokens.MATCHED);
            readThis(Tokens.THEN);
            readThis(Tokens.INSERT);
            int brackets = readOpenBrackets();
            if (brackets == 1) {
                boolean withPrefix = database.sqlSyntaxOra;
                readSimpleColumnNames(insertColumnNames, targetRangeVars[0],
                                      withPrefix);
                columnCount = insertColumnNames.size();
                readThis(Tokens.CLOSEBRACKET);
                brackets = 0;
            }
            readThis(Tokens.VALUES);
            Expression e = XreadContextuallyTypedTable(columnCount);
            if (e.nodes.length != 1) {
                throw Error.error(ErrorCode.X_21000);
            }
            insertExpressions.add(e);
        } else {
            throw unexpectedToken();
        }
        if (token.tokenType == Tokens.WHEN) {
            readMergeWhen(updateColIndexList, insertColumnNames,
                          updateTargetSet, insertExpressions,
                          updateExpressions, targetRangeVars, sourceRangeVar);
        }
    }
    StatementDMQL compileCallStatement(RangeVariable[] outerRanges,
                                       boolean isStrictlyProcedure) {
        read();
        if (isIdentifier()) {
            checkValidCatalogName(token.namePrePrefix);
            RoutineSchema routineSchema =
                (RoutineSchema) database.schemaManager.findSchemaObject(
                    token.tokenString,
                    session.getSchemaName(token.namePrefix),
                    SchemaObject.PROCEDURE);
            if (routineSchema != null) {
                read();
                HsqlArrayList list = new HsqlArrayList();
                readThis(Tokens.OPENBRACKET);
                if (token.tokenType == Tokens.CLOSEBRACKET) {
                    read();
                } else {
                    while (true) {
                        Expression e = XreadValueExpression();
                        list.add(e);
                        if (token.tokenType == Tokens.COMMA) {
                            read();
                        } else {
                            readThis(Tokens.CLOSEBRACKET);
                            break;
                        }
                    }
                }
                Expression[] arguments = new Expression[list.size()];
                list.toArray(arguments);
                Routine routine =
                    routineSchema.getSpecificRoutine(arguments.length);
                compileContext.addProcedureCall(routine);
                HsqlList unresolved = null;
                for (int i = 0; i < arguments.length; i++) {
                    Expression e = arguments[i];
                    if (e.isUnresolvedParam()) {
                        e.setAttributesAsColumn(
                            routine.getParameter(i),
                            routine.getParameter(i).isWriteable());
                    } else {
                        int paramMode =
                            routine.getParameter(i).getParameterMode();
                        unresolved =
                            arguments[i].resolveColumnReferences(session,
                                outerRanges, unresolved);
                        if (paramMode
                                != SchemaObject.ParameterModes.PARAM_IN) {
                            if (e.getType() != OpTypes.VARIABLE) {
                                throw Error.error(ErrorCode.X_42603);
                            }
                        }
                    }
                }
                ExpressionColumn.checkColumnsResolved(unresolved);
                for (int i = 0; i < arguments.length; i++) {
                    arguments[i].resolveTypes(session, null);
                    if (!routine.getParameter(
                            i).getDataType().canBeAssignedFrom(
                            arguments[i].getDataType())) {
                        throw Error.error(ErrorCode.X_42561);
                    }
                }
                StatementDMQL cs = new StatementProcedure(session, routine,
                    arguments, compileContext);
                return cs;
            }
        }
        if (isStrictlyProcedure) {
            throw Error.error(ErrorCode.X_42501, token.tokenString);
        }
        Expression expression = this.XreadValueExpression();
        HsqlList unresolved = expression.resolveColumnReferences(session,
            outerRanges, null);
        ExpressionColumn.checkColumnsResolved(unresolved);
        expression.resolveTypes(session, null);
        StatementDMQL cs = new StatementProcedure(session, expression,
            compileContext);
        return cs;
    }
    void resolveOuterReferencesAndTypes(RangeVariable[] rangeVars,
                                        Expression e) {
        HsqlList unresolved = e.resolveColumnReferences(session, rangeVars,
            rangeVars.length, null, false);
        unresolved = Expression.resolveColumnSet(session, rangeVars,
                rangeVars.length, unresolved, null);
        ExpressionColumn.checkColumnsResolved(unresolved);
        e.resolveTypes(session, null);
    }
}