


package org.hsqldb;

import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.OrderedHashSet;
import org.hsqldb.store.ValuePool;
import org.hsqldb.result.Result;


public class TriggerDefSQL extends TriggerDef {

    OrderedHashSet references;

    public TriggerDefSQL(HsqlNameManager.HsqlName name, int when,
                         int operation, boolean forEachRow, Table table,
                         Table[] transitions, RangeVariable[] rangeVars,
                         Expression condition, String conditionSQL,
                         int[] updateColumns, Routine routine) {

        super(name, when, operation, forEachRow, table, transitions,
              rangeVars, condition, conditionSQL, updateColumns);

        this.routine    = routine;
        this.references = routine.getReferences();
    }

    public OrderedHashSet getReferences() {
        return routine.getReferences();
    }

    public OrderedHashSet getComponents() {
        return null;
    }

    public void compile(Session session, SchemaObject parentObject) {}

    public String getClassName() {
        return null;
    }

    public boolean hasOldTable() {
        return transitions[OLD_TABLE] != null;
    }

    public boolean hasNewTable() {
        return transitions[NEW_TABLE] != null;
    }

    synchronized void pushPair(Session session, Object[] oldData,
                               Object[] newData) {

        Result result = Result.updateZeroResult;

        session.sessionContext.push();

        if (rangeVars[OLD_ROW] != null || rangeVars[NEW_ROW] != null) {
            session.sessionContext.triggerArguments = new Object[][] {
                oldData, newData
            };
        }

        if (condition.testCondition(session)) {
            int variableCount = routine.getVariableCount();

            session.sessionContext.routineVariables =
                new Object[variableCount];
            result = routine.statement.execute(session);
        }

        session.sessionContext.pop();

        if (result.isError()) {
            throw result.getException();
        }
    }

    public String getSQL() {

        StringBuffer sb = getSQLMain();

        sb.append(routine.statement.getSQL());

        return sb.toString();
    }
}
