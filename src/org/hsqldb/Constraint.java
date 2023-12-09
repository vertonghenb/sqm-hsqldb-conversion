


package org.hsqldb;

import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.RangeVariable.RangeIteratorBase;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.index.Index;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.lib.OrderedHashSet;
import org.hsqldb.navigator.RowIterator;
import org.hsqldb.persist.PersistentStore;
import org.hsqldb.result.Result;
import org.hsqldb.rights.Grantee;
import org.hsqldb.types.Type;


public final class Constraint implements SchemaObject {

    ConstraintCore   core;
    private HsqlName name;
    int              constType;
    boolean          isForward;

    
    Expression      check;
    private boolean isNotNull;
    int             notNullColumnIndex;
    RangeVariable   rangeVariable;

    
    OrderedHashSet mainColSet;
    OrderedHashSet refColSet;

    
    public static final Constraint[] emptyArray = new Constraint[]{};

    private Constraint() {}

    
    public Constraint(HsqlName name, Table t, Index index, int type) {

        this.name      = name;
        constType      = type;
        core           = new ConstraintCore();
        core.mainTable = t;
        core.mainIndex = index;
        core.mainCols  = index.getColumns();

        for (int i = 0; i < core.mainCols.length; i++) {
            Type dataType = t.getColumn(core.mainCols[i]).getDataType();

            if (dataType.isLobType()) {
                throw Error.error(ErrorCode.X_42534);
            }
        }
    }

    public Constraint(HsqlName name, Table table, int[] cols, int type) {

        this.name      = name;
        constType      = type;
        core           = new ConstraintCore();
        core.mainTable = table;
        core.mainCols  = cols;
    }

    
    public Constraint(HsqlName name, Constraint fkconstraint) {

        this.name = name;
        constType = SchemaObject.ConstraintTypes.MAIN;
        core      = fkconstraint.core;
    }

    
    public Constraint(HsqlName name, HsqlName refTableName,
                      OrderedHashSet refCols, HsqlName mainTableName,
                      OrderedHashSet mainCols, int type, int deleteAction,
                      int updateAction, int matchType) {

        this.name          = name;
        constType          = type;
        mainColSet         = mainCols;
        refColSet          = refCols;
        core               = new ConstraintCore();
        core.refTableName  = refTableName;
        core.mainTableName = mainTableName;
        core.deleteAction  = deleteAction;
        core.updateAction  = updateAction;
        core.matchType     = matchType;

        switch (core.deleteAction) {

            case SchemaObject.ReferentialAction.CASCADE :
            case SchemaObject.ReferentialAction.SET_DEFAULT :
            case SchemaObject.ReferentialAction.SET_NULL :
                core.hasDeleteAction = true;
        }

        switch (core.updateAction) {

            case SchemaObject.ReferentialAction.CASCADE :
            case SchemaObject.ReferentialAction.SET_DEFAULT :
            case SchemaObject.ReferentialAction.SET_NULL :
                core.hasUpdateAction = true;
        }
    }

    public Constraint(HsqlName name, OrderedHashSet mainCols, int type) {

        this.name  = name;
        constType  = type;
        mainColSet = mainCols;
        core       = new ConstraintCore();
    }

    public Constraint(HsqlName uniqueName, HsqlName mainName,
                      HsqlName refName, Table mainTable, Table refTable,
                      int[] mainCols, int[] refCols, Index mainIndex,
                      Index refIndex, int deleteAction,
                      int updateAction) throws HsqlException {

        this.name         = refName;
        constType         = SchemaObject.ConstraintTypes.FOREIGN_KEY;
        core              = new ConstraintCore();
        core.uniqueName   = uniqueName;
        core.mainName     = mainName;
        core.refName      = refName;
        core.mainTable    = mainTable;
        core.refTable     = refTable;
        core.mainCols     = mainCols;
        core.refCols      = refCols;
        core.mainIndex    = mainIndex;
        core.refIndex     = refIndex;
        core.deleteAction = deleteAction;
        core.updateAction = updateAction;
    }

    Constraint duplicate() {

        Constraint copy = new Constraint();

        copy.core      = core.duplicate();
        copy.name      = name;
        copy.constType = constType;
        copy.isForward = isForward;

        
        copy.check              = check;
        copy.isNotNull          = isNotNull;
        copy.notNullColumnIndex = notNullColumnIndex;
        copy.rangeVariable      = rangeVariable;

        return copy;
    }

    void setColumnsIndexes(Table table) {

        if (constType == SchemaObject.ConstraintTypes.FOREIGN_KEY) {
            if (mainColSet == null) {
                core.mainCols = core.mainTable.getPrimaryKey();

                if (core.mainCols == null) {
                    throw Error.error(ErrorCode.X_42581);
                }
            } else if (core.mainCols == null) {
                core.mainCols = core.mainTable.getColumnIndexes(mainColSet);
            }

            if (core.refCols == null) {
                core.refCols = table.getColumnIndexes(refColSet);
            }

            for (int i = 0; i < core.refCols.length; i++) {
                Type dataType = table.getColumn(core.refCols[i]).getDataType();

                if (dataType.isLobType()) {
                    throw Error.error(ErrorCode.X_42534);
                }
            }
        } else if (mainColSet != null) {
            core.mainCols = table.getColumnIndexes(mainColSet);

            for (int i = 0; i < core.mainCols.length; i++) {
                Type dataType =
                    table.getColumn(core.mainCols[i]).getDataType();

                if (dataType.isLobType()) {
                    throw Error.error(ErrorCode.X_42534);
                }
            }
        }
    }

    public int getType() {
        return SchemaObject.CONSTRAINT;
    }

    
    public HsqlName getName() {
        return name;
    }

    public HsqlName getCatalogName() {
        return name.schema.schema;
    }

    public HsqlName getSchemaName() {
        return name.schema;
    }

    public Grantee getOwner() {
        return name.schema.owner;
    }

    public OrderedHashSet getReferences() {

        switch (constType) {

            case SchemaObject.ConstraintTypes.CHECK :
                OrderedHashSet refs = new OrderedHashSet();

                check.collectObjectNames(refs);

                for (int j = refs.size() - 1; j >= 0; j--) {
                    HsqlName name = (HsqlName) refs.get(j);

                    if (name.type == SchemaObject.COLUMN
                            || name.type == SchemaObject.TABLE) {
                        refs.remove(j);
                    }
                }

                return refs;

            case SchemaObject.ConstraintTypes.FOREIGN_KEY :
                OrderedHashSet set = new OrderedHashSet();

                set.add(core.uniqueName);

                return set;
        }

        return new OrderedHashSet();
    }

    public OrderedHashSet getComponents() {
        return null;
    }

    public void compile(Session session, SchemaObject parentObject) {}

    public String getSQL() {

        StringBuffer sb = new StringBuffer();

        switch (getConstraintType()) {

            case SchemaObject.ConstraintTypes.PRIMARY_KEY :
                if (getMainColumns().length > 1
                        || (getMainColumns().length == 1
                            && !getName().isReservedName())) {
                    if (!getName().isReservedName()) {
                        sb.append(Tokens.T_CONSTRAINT).append(' ');
                        sb.append(getName().statementName).append(' ');
                    }

                    sb.append(Tokens.T_PRIMARY).append(' ').append(
                        Tokens.T_KEY);
                    sb.append(
                        getMain().getColumnListSQL(
                            getMainColumns(), getMainColumns().length));
                }
                break;

            case SchemaObject.ConstraintTypes.UNIQUE :
                if (!getName().isReservedName()) {
                    sb.append(Tokens.T_CONSTRAINT).append(' ');
                    sb.append(getName().statementName);
                    sb.append(' ');
                }

                sb.append(Tokens.T_UNIQUE);

                int[] col = getMainColumns();

                sb.append(getMain().getColumnListSQL(col, col.length));
                break;

            case SchemaObject.ConstraintTypes.FOREIGN_KEY :
                if (isForward) {
                    sb.append(Tokens.T_ALTER).append(' ').append(
                        Tokens.T_TABLE).append(' ');
                    sb.append(
                        getRef().getName().getSchemaQualifiedStatementName());
                    sb.append(' ').append(Tokens.T_ADD).append(' ');
                    getFKStatement(sb);
                } else {
                    getFKStatement(sb);
                }
                break;

            case SchemaObject.ConstraintTypes.CHECK :
                if (isNotNull()) {
                    break;
                }

                if (!getName().isReservedName()) {
                    sb.append(Tokens.T_CONSTRAINT).append(' ');
                    sb.append(getName().statementName).append(' ');
                }

                sb.append(Tokens.T_CHECK).append('(');
                sb.append(check.getSQL());
                sb.append(')');

                
                break;
        }

        return sb.toString();
    }

    public long getChangeTimestamp() {
        return 0;
    }

    
    private void getFKStatement(StringBuffer sb) {

        if (!getName().isReservedName()) {
            sb.append(Tokens.T_CONSTRAINT).append(' ');
            sb.append(getName().statementName);
            sb.append(' ');
        }

        sb.append(Tokens.T_FOREIGN).append(' ').append(Tokens.T_KEY);

        int[] col = getRefColumns();

        sb.append(getRef().getColumnListSQL(col, col.length));
        sb.append(' ').append(Tokens.T_REFERENCES).append(' ');
        sb.append(getMain().getName().getSchemaQualifiedStatementName());

        col = getMainColumns();

        sb.append(getMain().getColumnListSQL(col, col.length));

        if (getDeleteAction() != SchemaObject.ReferentialAction.NO_ACTION) {
            sb.append(' ').append(Tokens.T_ON).append(' ').append(
                Tokens.T_DELETE).append(' ');
            sb.append(getDeleteActionString());
        }

        if (getUpdateAction() != SchemaObject.ReferentialAction.NO_ACTION) {
            sb.append(' ').append(Tokens.T_ON).append(' ').append(
                Tokens.T_UPDATE).append(' ');
            sb.append(getUpdateActionString());
        }
    }

    public HsqlName getMainTableName() {
        return core.mainTableName;
    }

    public HsqlName getMainName() {
        return core.mainName;
    }

    public HsqlName getRefName() {
        return core.refName;
    }

    public HsqlName getUniqueName() {
        return core.uniqueName;
    }

    
    public int getConstraintType() {
        return constType;
    }

    
    public Table getMain() {
        return core.mainTable;
    }

    
    Index getMainIndex() {
        return core.mainIndex;
    }

    
    public Table getRef() {
        return core.refTable;
    }

    
    Index getRefIndex() {
        return core.refIndex;
    }

    
    private static String getActionString(int action) {

        switch (action) {

            case SchemaObject.ReferentialAction.RESTRICT :
                return Tokens.T_RESTRICT;

            case SchemaObject.ReferentialAction.CASCADE :
                return Tokens.T_CASCADE;

            case SchemaObject.ReferentialAction.SET_DEFAULT :
                return Tokens.T_SET + ' ' + Tokens.T_DEFAULT;

            case SchemaObject.ReferentialAction.SET_NULL :
                return Tokens.T_SET + ' ' + Tokens.T_NULL;

            default :
                return Tokens.T_NO + ' ' + Tokens.T_ACTION;
        }
    }

    
    public int getDeleteAction() {
        return core.deleteAction;
    }

    public String getDeleteActionString() {
        return getActionString(core.deleteAction);
    }

    
    public int getUpdateAction() {
        return core.updateAction;
    }

    public String getUpdateActionString() {
        return getActionString(core.updateAction);
    }

    public boolean hasTriggeredAction() {

        if (constType == SchemaObject.ConstraintTypes.FOREIGN_KEY) {
            switch (core.deleteAction) {

                case SchemaObject.ReferentialAction.CASCADE :
                case SchemaObject.ReferentialAction.SET_DEFAULT :
                case SchemaObject.ReferentialAction.SET_NULL :
                    return true;
            }

            switch (core.updateAction) {

                case SchemaObject.ReferentialAction.CASCADE :
                case SchemaObject.ReferentialAction.SET_DEFAULT :
                case SchemaObject.ReferentialAction.SET_NULL :
                    return true;
            }
        }

        return false;
    }

    public int getDeferability() {
        return SchemaObject.Deferable.NOT_DEFERRABLE;
    }

    
    public int[] getMainColumns() {
        return core.mainCols;
    }

    
    public int[] getRefColumns() {
        return core.refCols;
    }

    
    public String getCheckSQL() {
        return check.getSQL();
    }

    
    public boolean isNotNull() {
        return isNotNull;
    }

    boolean hasColumnOnly(int colIndex) {

        switch (constType) {

            case SchemaObject.ConstraintTypes.CHECK :
                return rangeVariable.usedColumns[colIndex] && ArrayUtil
                    .countTrueElements(rangeVariable.usedColumns) == 1;

            case SchemaObject.ConstraintTypes.PRIMARY_KEY :
            case SchemaObject.ConstraintTypes.UNIQUE :
                return core.mainCols.length == 1
                       && core.mainCols[0] == colIndex;

            case SchemaObject.ConstraintTypes.MAIN :
                return core.mainCols.length == 1
                       && core.mainCols[0] == colIndex
                       && core.mainTable == core.refTable;

            case SchemaObject.ConstraintTypes.FOREIGN_KEY :
                return core.refCols.length == 1 && core.refCols[0] == colIndex
                       && core.mainTable == core.refTable;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "Constraint");
        }
    }

    boolean hasColumnPlus(int colIndex) {

        switch (constType) {

            case SchemaObject.ConstraintTypes.CHECK :
                return rangeVariable.usedColumns[colIndex] && ArrayUtil
                    .countTrueElements(rangeVariable.usedColumns) > 1;

            case SchemaObject.ConstraintTypes.PRIMARY_KEY :
            case SchemaObject.ConstraintTypes.UNIQUE :
                return core.mainCols.length != 1
                       && ArrayUtil.find(core.mainCols, colIndex) != -1;

            case SchemaObject.ConstraintTypes.MAIN :
                return ArrayUtil.find(core.mainCols, colIndex) != -1
                       && (core.mainCols.length != 1
                           || core.mainTable != core.refTable);

            case SchemaObject.ConstraintTypes.FOREIGN_KEY :
                return ArrayUtil.find(core.refCols, colIndex) != -1
                       && (core.mainCols.length != 1
                           || core.mainTable != core.refTable);

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "Constraint");
        }
    }

    boolean hasColumn(int colIndex) {

        switch (constType) {

            case SchemaObject.ConstraintTypes.CHECK :
                return rangeVariable.usedColumns[colIndex];

            case SchemaObject.ConstraintTypes.PRIMARY_KEY :
            case SchemaObject.ConstraintTypes.UNIQUE :
            case SchemaObject.ConstraintTypes.MAIN :
                return ArrayUtil.find(core.mainCols, colIndex) != -1;

            case SchemaObject.ConstraintTypes.FOREIGN_KEY :
                return ArrayUtil.find(core.refCols, colIndex) != -1;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "Constraint");
        }
    }

    
    boolean isUniqueWithColumns(int[] cols) {

        switch (constType) {

            case SchemaObject.ConstraintTypes.PRIMARY_KEY :
            case SchemaObject.ConstraintTypes.UNIQUE :
                if (core.mainCols.length == cols.length) {
                    return ArrayUtil.haveEqualSets(core.mainCols, cols,
                                                   cols.length);
                }
        }

        return false;
    }

    
    boolean isEquivalent(Table mainTable, int[] mainCols, Table refTable,
                         int[] refCols) {

        switch (constType) {

            case SchemaObject.ConstraintTypes.MAIN :
            case SchemaObject.ConstraintTypes.FOREIGN_KEY :
                if (mainTable != core.mainTable || refTable != core.refTable) {
                    return false;
                }

                if (core.mainCols.length == mainCols.length
                        && core.refCols.length == refCols.length) {
                    return ArrayUtil.areEqualSets(core.mainCols, mainCols)
                           && ArrayUtil.areEqualSets(core.refCols, refCols);
                }
        }

        return false;
    }

    
    void updateTable(Session session, Table oldTable, Table newTable,
                     int colIndex, int adjust) {

        if (oldTable == core.mainTable) {
            core.mainTable = newTable;

            if (core.mainIndex != null) {
                core.mainIndex =
                    core.mainTable.getIndex(core.mainIndex.getName().name);
                core.mainCols = ArrayUtil.toAdjustedColumnArray(core.mainCols,
                        colIndex, adjust);

                core.mainIndex.setTable(newTable);
            }
        }

        if (oldTable == core.refTable) {
            core.refTable = newTable;

            if (core.refIndex != null) {
                core.refIndex =
                    core.refTable.getIndex(core.refIndex.getName().name);
                core.refCols = ArrayUtil.toAdjustedColumnArray(core.refCols,
                        colIndex, adjust);

                core.refIndex.setTable(newTable);
            }
        }

        
        if (constType == SchemaObject.ConstraintTypes.CHECK) {
            recompile(session, newTable);
        }
    }

    
    void checkInsert(Session session, Table table, Object[] data,
                     boolean isNew) {

        switch (constType) {

            case SchemaObject.ConstraintTypes.CHECK :
                if (!isNotNull) {
                    checkCheckConstraint(session, table, data);
                }

                return;

            case SchemaObject.ConstraintTypes.FOREIGN_KEY :
                PersistentStore store = core.mainTable.getRowStore(session);

                if (ArrayUtil.hasNull(data, core.refCols)) {
                    if (core.matchType == OpTypes.MATCH_SIMPLE) {
                        return;
                    }

                    if (core.refCols.length == 1) {
                        return;
                    }

                    if (ArrayUtil.hasAllNull(data, core.refCols)) {
                        return;
                    }

                    
                } else if (core.mainIndex.existsParent(session, store, data,
                                                       core.refCols)) {
                    return;
                }

                throw getException(data);
        }
    }

    
    void checkCheckConstraint(Session session, Table table, Object[] data) {


        RangeIteratorBase it =
            session.sessionContext.getCheckIterator(rangeVariable);

        it.currentData = data;

        boolean nomatch = Boolean.FALSE.equals(check.getValue(session));

        it.currentData = null;

        if (nomatch) {
            String[] info = new String[] {
                name.name, table.getName().name
            };

            throw Error.error(null, ErrorCode.X_23513, ErrorCode.CONSTRAINT,
                              info);
        }
    }

    void checkCheckConstraint(Session session, Table table,
                              ColumnSchema column, Object data) {

        session.sessionData.currentValue = data;

        boolean nomatch = Boolean.FALSE.equals(check.getValue(session));

        session.sessionData.currentValue = null;

        if (nomatch) {
            String[] info = new String[] {
                name.statementName,
                table == null ? ""
                              : table.getName().statementName,
                column == null ? ""
                               : column.getName().statementName,
            };

            throw Error.error(null, ErrorCode.X_23513,
                              ErrorCode.COLUMN_CONSTRAINT, info);
        }
    }

    public HsqlException getException(Object[] data) {

        switch (this.constType) {

            case SchemaObject.ConstraintTypes.CHECK : {
                String[] info = new String[]{ name.statementName };

                return Error.error(null, ErrorCode.X_23513,
                                   ErrorCode.CONSTRAINT, info);
            }
            case SchemaObject.ConstraintTypes.FOREIGN_KEY : {
                StringBuffer sb = new StringBuffer();

                for (int i = 0; i < core.refCols.length; i++) {
                    Object o = data[core.refCols[i]];

                    sb.append(core.refTable.getColumnTypes()[core.refCols[i]]
                        .convertToString(o));
                    sb.append(',');
                }

                String[] info = new String[] {
                    name.statementName, core.refTable.getName().statementName,
                    sb.toString()
                };

                return Error.error(null, ErrorCode.X_23503,
                                   ErrorCode.CONSTRAINT, info);
            }
            case SchemaObject.ConstraintTypes.PRIMARY_KEY :
            case SchemaObject.ConstraintTypes.UNIQUE : {
                StringBuffer sb = new StringBuffer();

                for (int i = 0; i < core.mainCols.length; i++) {
                    Object o = data[core.mainCols[i]];

                    sb.append(core.mainTable.colTypes[core.mainCols[i]]
                        .convertToString(o));
                    sb.append(',');
                }

                return Error.error(null, ErrorCode.X_23505,
                                   ErrorCode.CONSTRAINT, new String[] {
                    name.statementName, core.mainTable.getName().statementName,
                    sb.toString()
                });
            }
            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "Constraint");
        }
    }



    
    RowIterator findFkRef(Session session, Object[] row) {

        if (row == null || ArrayUtil.hasNull(row, core.mainCols)) {
            return core.refIndex.emptyIterator();
        }

        PersistentStore store = core.refTable.getRowStore(session);

        return core.refIndex.findFirstRow(session, store, row, core.mainCols);
    }

    
    void checkReferencedRows(Session session, Table table) {

        RowIterator it = table.rowIterator(session);

        while (true) {
            Row row = it.getNextRow();

            if (row == null) {
                break;
            }

            Object[] rowData = row.getData();

            checkInsert(session, table, rowData, false);
        }
    }

    public Expression getCheckExpression() {
        return check;
    }

    public OrderedHashSet getCheckColumnExpressions() {

        OrderedHashSet set = new OrderedHashSet();

        check.collectAllExpressions(set, Expression.columnExpressionSet,
                                    Expression.emptyExpressionSet);

        return set;
    }

    void recompile(Session session, Table newTable) {

        String    ddl     = check.getSQL();
        Scanner   scanner = new Scanner(ddl);
        ParserDQL parser  = new ParserDQL(session, scanner);

        parser.compileContext.reset(0);
        parser.read();

        parser.isCheckOrTriggerCondition = true;

        Expression condition = parser.XreadBooleanValueExpression();

        check = condition;

        
        QuerySpecification s = Expression.getCheckSelect(session, newTable,
            check);

        rangeVariable = s.rangeVariables[0];

        rangeVariable.setForCheckConstraint();
    }

    void prepareCheckConstraint(Session session, Table table,
                                boolean checkValues) {

        
        check.checkValidCheckConstraint();

        if (table == null) {
            check.resolveTypes(session, null);
        } else {
            QuerySpecification s = Expression.getCheckSelect(session, table,
                check);
            Result r = s.getResult(session, 1);

            if (r.getNavigator().getSize() != 0) {
                String[] info = new String[] {
                    name.statementName, table.getName().statementName
                };

                throw Error.error(null, ErrorCode.X_23513,
                                  ErrorCode.CONSTRAINT, info);
            }

            rangeVariable = s.rangeVariables[0];

            
            rangeVariable.setForCheckConstraint();
        }

        if (check.getType() == OpTypes.NOT
                && check.getLeftNode().getType() == OpTypes.IS_NULL
                && check.getLeftNode().getLeftNode().getType()
                   == OpTypes.COLUMN) {
            notNullColumnIndex =
                check.getLeftNode().getLeftNode().getColumnIndex();
            isNotNull = true;
        }
    }
}
