package org.hsqldb;
import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.HsqlNameManager.SimpleName;
import org.hsqldb.ParserDQL.CompileContext;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.ArraySort;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.lib.HashSet;
import org.hsqldb.lib.OrderedHashSet;
import org.hsqldb.result.Result;
import org.hsqldb.result.ResultMetaData;
import org.hsqldb.rights.Grantee;
import org.hsqldb.store.ValuePool;
public abstract class StatementDMQL extends Statement {
    public static final String PCOL_PREFIX        = "@p";
    static final String        RETURN_COLUMN_NAME = "@p0";
    Table targetTable;
    Table baseTable;
    int[]           baseColumnMap;
    RangeVariable[] targetRangeVariables = RangeVariable.emptyArray;
    Table sourceTable;
    Expression condition;
    boolean restartIdentity;
    int[] insertColumnMap = ValuePool.emptyIntArray;
    int[] updateColumnMap     = ValuePool.emptyIntArray;
    int[] baseUpdateColumnMap = ValuePool.emptyIntArray;
    Expression[] updateExpressions = Expression.emptyArray;
    Expression[][] multiColumnValues;
    Expression insertExpression;
    boolean[] insertCheckColumns;
    boolean[] updateCheckColumns;
    Expression    updatableTableCheck;
    RangeVariable checkRangeVariable;
    QueryExpression queryExpression;
    SimpleName cursorName;
    ExpressionColumn[] parameters;
    ResultMetaData parameterMetaData;
    SubQuery[] subqueries = SubQuery.emptySubqueryArray;
    int rangeIteratorCount;
    NumberSequence[] sequences;
    Routine[]        routines;
    RangeVariable[]  rangeVariables;
    StatementDMQL(int type, int group, HsqlName schemaName) {
        super(type, group);
        this.schemaName             = schemaName;
        this.isTransactionStatement = true;
    }
    void setBaseIndexColumnMap() {
        if (targetTable != baseTable) {
            baseColumnMap = targetTable.getBaseTableColumnMap();
        }
    }
    public void setCursorName(SimpleName name) {
        cursorName = name;
    }
    public SimpleName getCursorName() {
        return cursorName;
    }
    public Result execute(Session session) {
        Result result;
        if (targetTable != null && session.isReadOnly()
                && !targetTable.isTemp()) {
            HsqlException e = Error.error(ErrorCode.X_25006);
            return Result.newErrorResult(e);
        }
        if (isExplain) {
            return getExplainResult(session);
        }
        try {
            if (subqueries.length > 0) {
                materializeSubQueries(session);
            }
            result = getResult(session);
            clearStructures(session);
        } catch (Throwable t) {
            clearStructures(session);
            result = Result.newErrorResult(t, null);
            result.getException().setStatementType(group, type);
        }
        return result;
    }
    private Result getExplainResult(Session session) {
        Result result = Result.newSingleColumnStringResult("OPERATION",
            describe(session));
        OrderedHashSet set = getReferences();
        result.navigator.add(new Object[]{ "Object References" });
        for (int i = 0; i < set.size(); i++) {
            HsqlName name = (HsqlName) set.get(i);
            result.navigator.add(new Object[]{
                name.getSchemaQualifiedStatementName() });
        }
        result.navigator.add(new Object[]{ "Read Locks" });
        for (int i = 0; i < readTableNames.length; i++) {
            HsqlName name = readTableNames[i];
            result.navigator.add(new Object[]{
                name.getSchemaQualifiedStatementName() });
        }
        result.navigator.add(new Object[]{ "WriteLocks" });
        for (int i = 0; i < writeTableNames.length; i++) {
            HsqlName name = writeTableNames[i];
            result.navigator.add(new Object[]{
                name.getSchemaQualifiedStatementName() });
        }
        return result;
    }
    abstract Result getResult(Session session);
    abstract void collectTableNamesForRead(OrderedHashSet set);
    abstract void collectTableNamesForWrite(OrderedHashSet set);
    boolean[] getInsertOrUpdateColumnCheckList() {
        switch (type) {
            case StatementTypes.INSERT :
                return insertCheckColumns;
            case StatementTypes.UPDATE_WHERE :
                return updateCheckColumns;
            case StatementTypes.MERGE :
                boolean[] check =
                    (boolean[]) ArrayUtil.duplicateArray(insertCheckColumns);
                ArrayUtil.orBooleanArray(updateCheckColumns, check);
                return check;
        }
        return null;
    }
    private void setParameters() {
        for (int i = 0; i < parameters.length; i++) {
            parameters[i].parameterIndex = i;
        }
    }
    void materializeSubQueries(Session session) {
        HashSet subqueryPopFlags = new HashSet();
        for (int i = 0; i < subqueries.length; i++) {
            SubQuery sq = subqueries[i];
            if (!subqueryPopFlags.add(sq)) {
                continue;
            }
            if (!sq.isCorrelated()) {
                sq.materialise(session);
            }
        }
    }
    SubQuery[] getSubqueries(Session session) {
        OrderedHashSet subQueries = null;
        for (int i = 0; i < targetRangeVariables.length; i++) {
            if (targetRangeVariables[i] == null) {
                continue;
            }
            OrderedHashSet set = targetRangeVariables[i].getSubqueries();
            subQueries = OrderedHashSet.addAll(subQueries, set);
        }
        for (int i = 0; i < updateExpressions.length; i++) {
            subQueries = updateExpressions[i].collectAllSubqueries(subQueries);
        }
        if (insertExpression != null) {
            subQueries = insertExpression.collectAllSubqueries(subQueries);
        }
        if (condition != null) {
            subQueries = condition.collectAllSubqueries(subQueries);
        }
        if (queryExpression != null) {
            OrderedHashSet set = queryExpression.getSubqueries();
            subQueries = OrderedHashSet.addAll(subQueries, set);
        }
        if (updatableTableCheck != null) {
            OrderedHashSet set = updatableTableCheck.getSubqueries();
            subQueries = OrderedHashSet.addAll(subQueries, set);
        }
        if (subQueries == null || subQueries.size() == 0) {
            return SubQuery.emptySubqueryArray;
        }
        SubQuery[] subQueryArray = new SubQuery[subQueries.size()];
        subQueries.toArray(subQueryArray);
        ArraySort.sort(subQueryArray, 0, subQueryArray.length,
                       subQueryArray[0]);
        for (int i = 0; i < subQueryArray.length; i++) {
            subQueryArray[i].prepareTable(session);
        }
        return subQueryArray;
    }
    void setDatabseObjects(Session session, CompileContext compileContext) {
        parameters = compileContext.getParameters();
        setParameters();
        setParameterMetaData();
        subqueries         = getSubqueries(session);
        rangeIteratorCount = compileContext.getRangeVarCount();
        rangeVariables     = compileContext.getRangeVariables();
        sequences          = compileContext.getSequences();
        routines           = compileContext.getRoutines();
        OrderedHashSet set = new OrderedHashSet();
        collectTableNamesForWrite(set);
        if (set.size() > 0) {
            writeTableNames = new HsqlName[set.size()];
            set.toArray(writeTableNames);
            set.clear();
        }
        collectTableNamesForRead(set);
        set.removeAll(writeTableNames);
        if (set.size() > 0) {
            readTableNames = new HsqlName[set.size()];
            set.toArray(readTableNames);
        }
        references = compileContext.getSchemaObjectNames();
        if (targetTable != null) {
            references.add(targetTable.getName());
        }
    }
    void checkAccessRights(Session session) {
        if (targetTable != null && !targetTable.isTemp()) {
            if (!session.isProcessingScript) {
                targetTable.checkDataReadOnly();
            }
            Grantee owner = targetTable.getOwner();
            if (owner != null && owner.isSystem()) {
                if (!session.getUser().isSystem()) {
                    throw Error.error(ErrorCode.X_42501,
                                      targetTable.getName().name);
                }
            }
            session.checkReadWrite();
        }
        if (session.isAdmin()) {
            return;
        }
        for (int i = 0; i < sequences.length; i++) {
            session.getGrantee().checkAccess(sequences[i]);
        }
        for (int i = 0; i < routines.length; i++) {
            if (routines[i].isLibraryRoutine()) {
                continue;
            }
            session.getGrantee().checkAccess(routines[i]);
        }
        for (int i = 0; i < rangeVariables.length; i++) {
            RangeVariable range = rangeVariables[i];
            if (range.rangeTable.getSchemaName()
                    == SqlInvariants.SYSTEM_SCHEMA_HSQLNAME) {
                continue;
            }
            if (range.rangeTable.getSchemaName()
                    == SqlInvariants.SYSTEM_SUBQUERY_HSQLNAME) {
                continue;
            }
            session.getGrantee().checkSelect(range.rangeTable,
                                             range.usedColumns);
        }
        switch (type) {
            case StatementTypes.CALL : {
                break;
            }
            case StatementTypes.INSERT : {
                session.getGrantee().checkInsert(targetTable,
                                                 insertCheckColumns);
                break;
            }
            case StatementTypes.SELECT_CURSOR :
                break;
            case StatementTypes.DELETE_WHERE : {
                session.getGrantee().checkDelete(targetTable);
                break;
            }
            case StatementTypes.UPDATE_WHERE : {
                session.getGrantee().checkUpdate(targetTable,
                                                 updateCheckColumns);
                break;
            }
            case StatementTypes.MERGE : {
                session.getGrantee().checkInsert(targetTable,
                                                 insertCheckColumns);
                session.getGrantee().checkUpdate(targetTable,
                                                 updateCheckColumns);
                break;
            }
        }
    }
    Result getWriteAccessResult(Session session) {
        try {
            if (targetTable != null && !targetTable.isTemp()) {
                session.checkReadWrite();
            }
        } catch (HsqlException e) {
            return Result.newErrorResult(e);
        }
        return null;
    }
    public ResultMetaData getResultMetaData() {
        switch (type) {
            case StatementTypes.DELETE_WHERE :
            case StatementTypes.INSERT :
            case StatementTypes.UPDATE_WHERE :
            case StatementTypes.MERGE :
                return ResultMetaData.emptyResultMetaData;
            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "StatementDMQL");
        }
    }
    public ResultMetaData getParametersMetaData() {
        return parameterMetaData;
    }
    void setParameterMetaData() {
        int     offset;
        int     idx;
        boolean hasReturnValue;
        offset = 0;
        if (parameters.length == 0) {
            parameterMetaData = ResultMetaData.emptyParamMetaData;
            return;
        }
        parameterMetaData =
            ResultMetaData.newParameterMetaData(parameters.length);
        for (int i = 0; i < parameters.length; i++) {
            idx = i + offset;
            parameterMetaData.columnLabels[idx] = StatementDMQL.PCOL_PREFIX
                                                  + (i + 1);
            parameterMetaData.columnTypes[idx] = parameters[i].dataType;
            if (parameters[i].dataType == null) {
                throw Error.error(ErrorCode.X_42567);
            }
            byte parameterMode = SchemaObject.ParameterModes.PARAM_IN;
            if (parameters[i].column != null
                    && parameters[i].column.getParameterMode()
                       != SchemaObject.ParameterModes.PARAM_UNKNOWN) {
                parameterMode = parameters[i].column.getParameterMode();
            }
            parameterMetaData.paramModes[idx] = parameterMode;
            parameterMetaData.paramNullable[idx] =
                parameters[i].column == null
                ? SchemaObject.Nullability.NULLABLE
                : parameters[i].column.getNullability();
        }
    }
    public String describe(Session session) {
        try {
            return describeImpl(session);
        } catch (Throwable e) {
            e.printStackTrace();
            return e.toString();
        }
    }
    String describeImpl(Session session) throws Exception {
        StringBuffer sb;
        sb = new StringBuffer();
        int blanks = 0;
        switch (type) {
            case StatementTypes.SELECT_CURSOR : {
                sb.append(queryExpression.describe(session, 0));
                appendParms(sb).append('\n');
                appendSubqueries(session, sb, 2);
                return sb.toString();
            }
            case StatementTypes.INSERT : {
                if (queryExpression == null) {
                    sb.append("INSERT VALUES");
                    sb.append('[').append('\n');
                    appendMultiColumns(sb, insertColumnMap).append('\n');
                    appendTable(sb).append('\n');
                    appendParms(sb).append('\n');
                    appendSubqueries(session, sb, 2).append(']');
                    return sb.toString();
                } else {
                    sb.append("INSERT SELECT");
                    sb.append('[').append('\n');
                    appendColumns(sb, insertColumnMap).append('\n');
                    appendTable(sb).append('\n');
                    sb.append(queryExpression.describe(session,
                                                       blanks)).append('\n');
                    appendParms(sb).append('\n');
                    appendSubqueries(session, sb, 2).append(']');
                    return sb.toString();
                }
            }
            case StatementTypes.UPDATE_WHERE : {
                sb.append("UPDATE");
                sb.append('[').append('\n');
                appendColumns(sb, updateColumnMap).append('\n');
                appendTable(sb).append('\n');
                appendCondition(session, sb);
                for (int i = 0; i < targetRangeVariables.length; i++) {
                    sb.append(targetRangeVariables[i].describe(session,
                            blanks)).append('\n');
                }
                appendParms(sb).append('\n');
                appendSubqueries(session, sb, 2).append(']');
                return sb.toString();
            }
            case StatementTypes.DELETE_WHERE : {
                sb.append("DELETE");
                sb.append('[').append('\n');
                appendTable(sb).append('\n');
                appendCondition(session, sb);
                for (int i = 0; i < targetRangeVariables.length; i++) {
                    sb.append(targetRangeVariables[i].describe(session,
                            blanks)).append('\n');
                }
                appendParms(sb).append('\n');
                appendSubqueries(session, sb, 2).append(']');
                return sb.toString();
            }
            case StatementTypes.CALL : {
                sb.append("CALL");
                sb.append('[').append(']');
                return sb.toString();
            }
            case StatementTypes.MERGE : {
                sb.append("MERGE");
                sb.append('[').append('\n');
                appendMultiColumns(sb, insertColumnMap).append('\n');
                appendColumns(sb, updateColumnMap).append('\n');
                appendTable(sb).append('\n');
                appendCondition(session, sb);
                for (int i = 0; i < targetRangeVariables.length; i++) {
                    sb.append(targetRangeVariables[i].describe(session,
                            blanks)).append('\n');
                }
                appendParms(sb).append('\n');
                appendSubqueries(session, sb, 2).append(']');
                return sb.toString();
            }
            default : {
                return "UNKNOWN";
            }
        }
    }
    private StringBuffer appendSubqueries(Session session, StringBuffer sb,
                                          int blanks) {
        sb.append("SUBQUERIES[");
        for (int i = 0; i < subqueries.length; i++) {
            sb.append("\n[level=").append(subqueries[i].level).append('\n');
            if (subqueries[i].queryExpression == null) {
                for (int j = 0; j < blanks; j++) {
                    sb.append(' ');
                }
                sb.append("value expression");
            } else {
                sb.append(subqueries[i].queryExpression.describe(session,
                        blanks));
            }
            sb.append("]");
        }
        sb.append(']');
        return sb;
    }
    private StringBuffer appendTable(StringBuffer sb) {
        sb.append("TABLE[").append(targetTable.getName().name).append(']');
        return sb;
    }
    private StringBuffer appendSourceTable(StringBuffer sb) {
        sb.append("SOURCE TABLE[").append(sourceTable.getName().name).append(
            ']');
        return sb;
    }
    private StringBuffer appendColumns(StringBuffer sb, int[] columnMap) {
        if (columnMap == null || updateExpressions.length == 0) {
            return sb;
        }
        sb.append("COLUMNS=[");
        for (int i = 0; i < columnMap.length; i++) {
            sb.append('\n').append(columnMap[i]).append(':').append(
                ' ').append(
                targetTable.getColumn(columnMap[i]).getNameString());
        }
        for (int i = 0; i < updateExpressions.length; i++) {
            sb.append('[').append(updateExpressions[i]).append(']');
        }
        sb.append(']');
        return sb;
    }
    private StringBuffer appendMultiColumns(StringBuffer sb, int[] columnMap) {
        if (columnMap == null || multiColumnValues == null) {
            return sb;
        }
        sb.append("COLUMNS=[");
        for (int j = 0; j < multiColumnValues.length; j++) {
            for (int i = 0; i < columnMap.length; i++) {
                sb.append('\n').append(columnMap[i]).append(':').append(
                    ' ').append(
                    targetTable.getColumn(columnMap[i]).getName().name).append(
                    '[').append(multiColumnValues[j][i]).append(']');
            }
        }
        sb.append(']');
        return sb;
    }
    private StringBuffer appendParms(StringBuffer sb) {
        sb.append("PARAMETERS=[");
        for (int i = 0; i < parameters.length; i++) {
            sb.append('\n').append('@').append(i).append('[').append(
                parameters[i].describe(null, 0)).append(']');
        }
        sb.append(']');
        return sb;
    }
    private StringBuffer appendCondition(Session session, StringBuffer sb) {
        return condition == null ? sb.append("CONDITION[]\n")
                                 : sb.append("CONDITION[").append(
                                     condition.describe(session, 0)).append(
                                     "]\n");
    }
    public void resolve(Session session) {}
    public RangeVariable[] getRangeVariables() {
        return rangeVariables;
    }
    public final boolean isCatalogLock() {
        return false;
    }
    public boolean isCatalogChange() {
        return false;
    }
}