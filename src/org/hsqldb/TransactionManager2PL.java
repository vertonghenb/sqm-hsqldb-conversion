package org.hsqldb;
import org.hsqldb.persist.CachedObject;
import org.hsqldb.persist.PersistentStore;
public class TransactionManager2PL extends TransactionManagerCommon
implements TransactionManager {
    public TransactionManager2PL(Database db) {
        database   = db;
        lobSession = database.sessionManager.getSysLobSession();
        txModel    = LOCKS;
    }
    public long getGlobalChangeTimestamp() {
        return globalChangeTimestamp.get();
    }
    public boolean isMVRows() {
        return false;
    }
    public boolean isMVCC() {
        return false;
    }
    public int getTransactionControl() {
        return LOCKS;
    }
    public void setTransactionControl(Session session, int mode) {
        super.setTransactionControl(session, mode);
    }
    public void completeActions(Session session) {
        endActionTPL(session);
    }
    public boolean prepareCommitActions(Session session) {
        session.actionTimestamp = nextChangeTimestamp();
        return true;
    }
    public boolean commitTransaction(Session session) {
        if (session.abortTransaction) {
            return false;
        }
        int      limit = session.rowActionList.size();
        Object[] list  = session.rowActionList.getArray();
        writeLock.lock();
        try {
            session.actionTimestamp         = nextChangeTimestamp();
            session.transactionEndTimestamp = session.actionTimestamp;
            endTransaction(session);
            for (int i = 0; i < limit; i++) {
                RowAction action = (RowAction) list[i];
                action.commit(session);
            }
            persistCommit(session, list, limit);
            endTransactionTPL(session);
        } finally {
            writeLock.unlock();
        }
        session.tempSet.clear();
        return true;
    }
    public void rollback(Session session) {
        session.abortTransaction        = false;
        session.actionTimestamp         = nextChangeTimestamp();
        session.transactionEndTimestamp = session.actionTimestamp;
        rollbackPartial(session, 0, session.transactionTimestamp);
        endTransaction(session);
        writeLock.lock();
        try {
            endTransactionTPL(session);
        } finally {
            writeLock.unlock();
        }
    }
    public void rollbackSavepoint(Session session, int index) {
        long timestamp = session.sessionContext.savepointTimestamps.get(index);
        Integer oi = (Integer) session.sessionContext.savepoints.get(index);
        int     start  = oi.intValue();
        while (session.sessionContext.savepoints.size() > index + 1) {
            session.sessionContext.savepoints.remove(
                session.sessionContext.savepoints.size() - 1);
            session.sessionContext.savepointTimestamps.removeLast();
        }
        rollbackPartial(session, start, timestamp);
    }
    public void rollbackAction(Session session) {
        rollbackPartial(session, session.actionIndex, session.actionTimestamp);
        endActionTPL(session);
    }
    void rollbackPartial(Session session, int start, long timestamp) {
        Object[] list  = session.rowActionList.getArray();
        int      limit = session.rowActionList.size();
        if (start == limit) {
            return;
        }
        for (int i = limit - 1; i >= start; i--) {
            RowAction action = (RowAction) list[i];
            if (action == null || action.type == RowActionBase.ACTION_NONE
                    || action.type == RowActionBase.ACTION_DELETE_FINAL) {
                continue;
            }
            Row row = action.memoryRow;
            if (row == null) {
                row = (Row) action.store.get(action.getPos(), false);
            }
            if (row == null) {
                continue;
            }
            action.rollback(session, timestamp);
            int type = action.mergeRollback(session, timestamp, row);
            action.store.rollbackRow(session, row, type, txModel);
        }
        session.rowActionList.setSize(start);
    }
    public RowAction addDeleteAction(Session session, Table table, Row row,
                                     int[] colMap) {
        RowAction action;
        synchronized (row) {
            action = RowAction.addDeleteAction(session, table, row, colMap);
        }
        session.rowActionList.add(action);
        PersistentStore store = table.getRowStore(session);
        store.delete(session, row);
        row.rowAction = null;
        return action;
    }
    public void addInsertAction(Session session, Table table,
                                PersistentStore store, Row row,
                                int[] changedColumns) {
        RowAction action = row.rowAction;
        if (action == null) {
            System.out.println("null insert action " + session + " "
                               + session.actionTimestamp);
        }
        store.indexRow(session, row);
        session.rowActionList.add(action);
        row.rowAction = null;
    }
    public boolean canRead(Session session, Row row, int mode, int[] colMap) {
        return true;
    }
    public boolean canRead(Session session, int id, int mode) {
        return true;
    }
    public void setTransactionInfo(CachedObject object) {}
    public void removeTransactionInfo(CachedObject object) {}
    public void beginTransaction(Session session) {
        if (!session.isTransaction) {
            session.actionTimestamp      = nextChangeTimestamp();
            session.transactionTimestamp = session.actionTimestamp;
            session.isTransaction        = true;
            transactionCount++;
        }
    }
    public void beginAction(Session session, Statement cs) {
        if (session.hasLocks(cs)) {
            return;
        }
        writeLock.lock();
        try {
            if (cs.getCompileTimestamp()
                    < database.schemaManager.getSchemaChangeTimestamp()) {
                cs = session.statementManager.getStatement(session, cs);
                session.sessionContext.currentStatement = cs;
                if (cs == null) {
                    return;
                }
            }
            boolean canProceed = setWaitedSessionsTPL(session, cs);
            if (canProceed) {
                if (session.tempSet.isEmpty()) {
                    lockTablesTPL(session, cs);
                } else {
                    setWaitingSessionTPL(session);
                }
            }
        } finally {
            writeLock.unlock();
        }
    }
    public void beginActionResume(Session session) {
        session.actionTimestamp = nextChangeTimestamp();
        if (!session.isTransaction) {
            session.transactionTimestamp = session.actionTimestamp;
            session.isTransaction        = true;
            transactionCount++;
        }
        return;
    }
    void endTransaction(Session session) {
        if (session.isTransaction) {
            session.isTransaction = false;
            transactionCount--;
        }
    }
}