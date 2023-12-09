


package org.hsqldb;

import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.HsqlDeque;
import org.hsqldb.lib.OrderedHashSet;
import org.hsqldb.lib.StringConverter;
import org.hsqldb.rights.Grantee;










public class TriggerDef implements Runnable, SchemaObject {

    static final int OLD_ROW     = 0;
    static final int NEW_ROW     = 1;
    static final int RANGE_COUNT = 2;
    static final int OLD_TABLE   = 2;
    static final int NEW_TABLE   = 3;
    static final int BEFORE      = 4;
    static final int AFTER       = 5;
    static final int INSTEAD     = 6;

    
    static final int NUM_TRIGGER_OPS = 3;                      
    static final int NUM_TRIGS       = NUM_TRIGGER_OPS * 3;    

    
    static final TriggerDef[] emptyArray = new TriggerDef[]{};
    Table[]                   transitions;
    RangeVariable[]           rangeVars;
    Expression                condition;
    boolean                   hasTransitionTables;
    boolean                   hasTransitionRanges;
    String                    conditionSQL;
    Routine                   routine;
    int[]                     updateColumns;

    
    private HsqlName name;
    long             changeTimestamp;
    int              actionTiming;
    int              operationType;
    boolean          isSystem;
    boolean          forEachRow;
    boolean          nowait;                                   
    int              maxRowsQueued;                            
    Table            table;
    Trigger          trigger;
    String           triggerClassName;
    int              triggerType;
    Thread           thread;

    
    protected HsqlDeque        pendingQueue;                   
    protected int              rowsQueued;                     
    protected boolean          valid     = true;               
    protected volatile boolean keepGoing = true;

    TriggerDef() {}

    
    public TriggerDef(HsqlNameManager.HsqlName name, int when, int operation,
                      boolean forEach, Table table, Table[] transitions,
                      RangeVariable[] rangeVars, Expression condition,
                      String conditionSQL, int[] updateColumns,
                      String triggerClassName, boolean noWait, int queueSize) {

        this(name, when, operation, forEach, table, transitions, rangeVars,
             condition, conditionSQL, updateColumns);

        this.triggerClassName = triggerClassName;
        this.nowait           = noWait;
        this.maxRowsQueued    = queueSize;
        rowsQueued            = 0;
        pendingQueue          = new HsqlDeque();

        Class cl = null;

        try {
            cl = Class.forName(triggerClassName, true,
                               Thread.currentThread().getContextClassLoader());
        } catch (Throwable t1) {
            try {
                cl = Class.forName(triggerClassName);
            } catch (Throwable t) {}
        }

        if (cl == null) {
            valid   = false;
            trigger = new DefaultTrigger();
        } else {
            try {

                
                trigger = (Trigger) cl.newInstance();
            } catch (Throwable t1) {
                valid   = false;
                trigger = new DefaultTrigger();
            }
        }
    }

    public TriggerDef(HsqlNameManager.HsqlName name, int when, int operation,
                      boolean forEachRow, Table table, Table[] transitions,
                      RangeVariable[] rangeVars, Expression condition,
                      String conditionSQL, int[] updateColumns) {

        this.name          = name;
        this.actionTiming  = when;
        this.operationType = operation;
        this.forEachRow    = forEachRow;
        this.table         = table;
        this.transitions   = transitions;
        this.rangeVars     = rangeVars;
        this.condition     = condition == null ? Expression.EXPR_TRUE
                                               : condition;
        this.updateColumns = updateColumns;
        this.conditionSQL  = conditionSQL;
        hasTransitionRanges = rangeVars[OLD_ROW] != null
                              || rangeVars[NEW_ROW] != null;
        hasTransitionTables = transitions[OLD_TABLE] != null
                              || transitions[NEW_TABLE] != null;

        setUpIndexesAndTypes();
    }

    public boolean isValid() {
        return valid;
    }

    public int getType() {
        return SchemaObject.TRIGGER;
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
        return new OrderedHashSet();
    }

    public OrderedHashSet getComponents() {
        return null;
    }

    public void compile(Session session, SchemaObject parentObject) {}

    
    public String getSQL() {

        StringBuffer sb = getSQLMain();

        if (maxRowsQueued != 0) {
            sb.append(Tokens.T_QUEUE).append(' ');
            sb.append(maxRowsQueued).append(' ');

            if (nowait) {
                sb.append(Tokens.T_NOWAIT).append(' ');
            }
        }

        sb.append(Tokens.T_CALL).append(' ');
        sb.append(StringConverter.toQuotedString(triggerClassName, '"',
                false));

        return sb.toString();
    }

    public long getChangeTimestamp() {
        return changeTimestamp;
    }

    public StringBuffer getSQLMain() {

        StringBuffer sb = new StringBuffer(256);

        sb.append(Tokens.T_CREATE).append(' ');
        sb.append(Tokens.T_TRIGGER).append(' ');
        sb.append(name.getSchemaQualifiedStatementName()).append(' ');
        sb.append(getActionTimingString()).append(' ');
        sb.append(getEventTypeString()).append(' ');

        if (updateColumns != null) {
            sb.append(Tokens.T_OF).append(' ');

            for (int i = 0; i < updateColumns.length; i++) {
                if (i != 0) {
                    sb.append(',');
                }

                HsqlName name = table.getColumn(updateColumns[i]).getName();

                sb.append(name.statementName);
            }

            sb.append(' ');
        }

        sb.append(Tokens.T_ON).append(' ');
        sb.append(table.getName().getSchemaQualifiedStatementName());
        sb.append(' ');

        if (hasTransitionRanges || hasTransitionTables) {
            sb.append(Tokens.T_REFERENCING).append(' ');

            if (rangeVars[OLD_ROW] != null) {
                sb.append(Tokens.T_OLD).append(' ').append(Tokens.T_ROW);
                sb.append(' ').append(Tokens.T_AS).append(' ');
                sb.append(
                    rangeVars[OLD_ROW].getTableAlias().getStatementName());
                sb.append(' ');
            }

            if (rangeVars[NEW_ROW] != null) {
                sb.append(Tokens.T_NEW).append(' ').append(Tokens.T_ROW);
                sb.append(' ').append(Tokens.T_AS).append(' ');
                sb.append(
                    rangeVars[NEW_ROW].getTableAlias().getStatementName());
                sb.append(' ');
            }

            if (transitions[OLD_TABLE] != null) {
                sb.append(Tokens.T_OLD).append(' ').append(Tokens.T_TABLE);
                sb.append(' ').append(Tokens.T_AS).append(' ');
                sb.append(transitions[OLD_TABLE].getName().statementName);
                sb.append(' ');
            }

            if (transitions[NEW_TABLE] != null) {
                sb.append(Tokens.T_OLD).append(' ').append(Tokens.T_TABLE);
                sb.append(' ').append(Tokens.T_AS).append(' ');
                sb.append(transitions[NEW_TABLE].getName().statementName);
                sb.append(' ');
            }
        }

        if (forEachRow) {
            sb.append(Tokens.T_FOR).append(' ');
            sb.append(Tokens.T_EACH).append(' ');
            sb.append(Tokens.T_ROW).append(' ');
        }

        if (condition != Expression.EXPR_TRUE) {
            sb.append(Tokens.T_WHEN).append(' ');
            sb.append(Tokens.T_OPENBRACKET).append(conditionSQL);
            sb.append(Tokens.T_CLOSEBRACKET).append(' ');
        }

        return sb;
    }

    public String getClassName() {
        return trigger.getClass().getName();
    }

    public String getActionTimingString() {

        switch (this.actionTiming) {

            case TriggerDef.BEFORE :
                return Tokens.T_BEFORE;

            case TriggerDef.AFTER :
                return Tokens.T_AFTER;

            case TriggerDef.INSTEAD :
                return Tokens.T_INSTEAD + ' ' + Tokens.T_OF;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "TriggerDef");
        }
    }

    public String getEventTypeString() {

        switch (this.operationType) {

            case StatementTypes.INSERT :
                return Tokens.T_INSERT;

            case StatementTypes.DELETE_WHERE :
                return Tokens.T_DELETE;

            case StatementTypes.UPDATE_WHERE :
                return Tokens.T_UPDATE;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "TriggerDef");
        }
    }

    public boolean isSystem() {
        return isSystem;
    }

    public boolean isForEachRow() {
        return forEachRow;
    }

    public String getConditionSQL() {
        return conditionSQL;
    }

    public String getProcedureSQL() {
        return routine == null ? null
                               : routine.getSQLBodyDefinition();
    }

    public int[] getUpdateColumnIndexes() {
        return updateColumns;
    }

    public boolean hasOldTable() {
        return false;
    }

    public boolean hasNewTable() {
        return false;
    }

    public String getOldTransitionRowName() {

        return rangeVars[OLD_ROW] == null ? null
                                          : rangeVars[OLD_ROW].getTableAlias()
                                              .name;
    }

    public String getNewTransitionRowName() {

        return rangeVars[NEW_ROW] == null ? null
                                          : rangeVars[NEW_ROW].getTableAlias()
                                              .name;
    }

    public String getOldTransitionTableName() {

        return transitions[OLD_TABLE] == null ? null
                                              : transitions[OLD_TABLE]
                                              .getName().name;
    }

    public String getNewTransitionTableName() {

        return transitions[NEW_TABLE] == null ? null
                                              : transitions[NEW_TABLE]
                                              .getName().name;
    }

    
    void setUpIndexesAndTypes() {

        triggerType = 0;

        switch (operationType) {

            case StatementTypes.INSERT :
                triggerType = Trigger.INSERT_AFTER;
                break;

            case StatementTypes.DELETE_WHERE :
                triggerType = Trigger.DELETE_AFTER;
                break;

            case StatementTypes.UPDATE_WHERE :
                triggerType = Trigger.UPDATE_AFTER;
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "TriggerDef");
        }

        if (forEachRow) {
            triggerType += NUM_TRIGGER_OPS;
        }

        if (actionTiming == TriggerDef.BEFORE
                || actionTiming == TriggerDef.INSTEAD) {
            triggerType += NUM_TRIGGER_OPS;
        }
    }

    
    static int getOperationType(int token) {

        switch (token) {

            case Tokens.INSERT :
                return StatementTypes.INSERT;

            case Tokens.DELETE :
                return StatementTypes.DELETE_WHERE;

            case Tokens.UPDATE :
                return StatementTypes.UPDATE_WHERE;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "TriggerDef");
        }
    }

    static int getTiming(int token) {

        switch (token) {

            case Tokens.BEFORE :
                return TriggerDef.BEFORE;

            case Tokens.AFTER :
                return TriggerDef.AFTER;

            case Tokens.INSTEAD :
                return TriggerDef.INSTEAD;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "TriggerDef");
        }
    }

    public int getStatementType() {
        return operationType;
    }

    
    public void run() {

        while (keepGoing) {
            TriggerData triggerData = popPair();

            if (triggerData != null) {
                if (triggerData.username != null) {
                    trigger.fire(this.triggerType, name.name,
                                 table.getName().name, triggerData.oldRow,
                                 triggerData.newRow);
                }
            }
        }

        try {
            thread.setContextClassLoader(null);
        } catch (Throwable t) {}
    }

    
    public synchronized void start() {

        if (maxRowsQueued != 0) {
            thread = new Thread(this);

            thread.start();
        }
    }

    
    public synchronized void terminate() {

        keepGoing = false;

        notify();
    }

    
    synchronized TriggerData popPair() {

        if (rowsQueued == 0) {
            try {
                wait();    
            } catch (InterruptedException e) {

                
            }
        }

        rowsQueued--;

        notify();    

        if (pendingQueue.size() == 0) {
            return null;
        } else {
            return (TriggerData) pendingQueue.removeFirst();
        }
    }

    
    synchronized void pushPair(Session session, Object[] row1, Object[] row2) {

        if (maxRowsQueued == 0) {
            session.getInternalConnection();

            try {
                trigger.fire(triggerType, name.name, table.getName().name,
                             row1, row2);
            } finally {
                session.releaseInternalConnection();
            }

            return;
        }

        if (rowsQueued >= maxRowsQueued) {
            if (nowait) {
                pendingQueue.removeLast();    
            } else {
                try {
                    wait();
                } catch (InterruptedException e) {

                    
                }

                rowsQueued++;
            }
        } else {
            rowsQueued++;
        }

        pendingQueue.add(new TriggerData(session, row1, row2));
        notify();    
    }

    public boolean isBusy() {
        return rowsQueued != 0;
    }

    public Table getTable() {
        return table;
    }

    public String getActionOrientationString() {
        return forEachRow ? Tokens.T_ROW
                          : Tokens.T_STATEMENT;
    }

    
    static class TriggerData {

        public Object[] oldRow;
        public Object[] newRow;
        public String   username;

        public TriggerData(Session session, Object[] oldRow, Object[] newRow) {

            this.oldRow   = oldRow;
            this.newRow   = newRow;
            this.username = session.getUsername();
        }
    }

    static class DefaultTrigger implements org.hsqldb.Trigger {

        public void fire(int i, String name, String table, Object[] row1,
                         Object[] row2) {

            
        }
    }
}
