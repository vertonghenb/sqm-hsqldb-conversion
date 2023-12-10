package org.hsqldb;
import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.lib.HsqlDeque;
import org.hsqldb.lib.IntKeyHashMapConcurrent;
import org.hsqldb.lib.LongDeque;
import org.hsqldb.persist.CachedObject;
import org.hsqldb.persist.PersistentStore;
public class TransactionManagerMV2PL extends TransactionManagerCommon
implements TransactionManager {
    HsqlDeque committedTransactions          = new HsqlDeque();
    LongDeque committedTransactionTimestamps = new LongDeque();
    public TransactionManagerMV2PL(Database db) {
        database        = db;
        lobSession      = database.sessionManager.getSysLobSession();
        rowActionMap    = new IntKeyHashMapConcurrent(10000);
        txModel         = MVLOCKS;
        catalogNameList = new HsqlName[]{ database.getCatalogName() };
    }
    public long getGlobalChangeTimestamp() {
        return globalChangeTimestamp.get();
    }
    public boolean isMVRows() {
        return true;
    }
    public boolean isMVCC() {
        return false;
    }
    public int getTransactionControl() {
        return MVLOCKS;
    }
    public void setTransactionControl(Session session, int mode) {
        super.setTransactionControl(session, mode);
    }
    public void completeActions(Session session) {
        endActionTPL(session);
    }
    public boolean prepareCommitActions(Session session) {
        Object[] list  = session.rowActionList.getArray();
        int      limit = session.rowActionList.size();
        writeLock.lock();
        try {
            session.actionTimestamp = nextChangeTimestamp();
            for (int i = 0; i < limit; i++) {
                RowAction action = (RowAction) list[i];
                action.prepareCommit(session);
            }
            return true;
        } finally {
            writeLock.unlock();
        }
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
            int newLimit = session.rowActionList.size();
            if (newLimit > limit) {
                list = session.rowActionList.getArray();
                mergeTransaction(session, list, limit, newLimit,
                                 session.actionTimestamp);
                finaliseRows(session, list, limit, newLimit, true);
                session.rowActionList.setSize(limit);
            }
            if (getFirstLiveTransactionTimestamp() > session.actionTimestamp
                    || session == lobSession) {
                mergeTransaction(session, list, 0, limit,
                                 session.actionTimestamp);
                finaliseRows(session, list, 0, limit, true);
            } else {
                list = session.rowActionList.toArray();
                addToCommittedQueue(session, list);
            }
            endTransactionTPL(session);
        } finally {
            writeLock.unlock();
        }
        session.tempSet.clear();
        return true;
    }
    public void rollback(Session session) {
        writeLock.lock();
        try {
            session.abortTransaction        = false;
            session.actionTimestamp         = nextChangeTimestamp();
            session.transactionEndTimestamp = session.actionTimestamp;
            rollbackPartial(session, 0, session.transactionTimestamp);
            endTransaction(session);
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
        for (int i = start; i < limit; i++) {
            RowAction action = (RowAction) list[i];
            if (action != null) {
                action.rollback(session, timestamp);
            } else {
                System.out.println("null action in rollback " + start);
            }
        }
        mergeRolledBackTransaction(session, timestamp, list, start, limit);
        finaliseRows(session, list, start, limit, false);
        session.rowActionList.setSize(start);
    }
    public RowAction addDeleteAction(Session session, Table table, Row row,
                                     int[] colMap) {
        RowAction action;
        boolean   newAction;
        synchronized (row) {
            newAction = row.rowAction == null;
            action    = RowAction.addDeleteAction(session, table, row, colMap);
        }
        session.rowActionList.add(action);
        if (newAction && table.tableType == TableBase.CACHED_TABLE) {
            rowActionMap.put(action.getPos(), action);
        }
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
        if (table.tableType == TableBase.CACHED_TABLE) {
            rowActionMap.put(action.getPos(), action);
        }
        store.indexRow(session, row);
        session.rowActionList.add(action);
    }
    public boolean canRead(Session session, Row row, int mode, int[] colMap) {
        RowAction action = row.rowAction;
        if (action == null) {
            return true;
        }
        return action.canRead(session, TransactionManager.ACTION_READ);
    }
    public boolean canRead(Session session, int id, int mode) {
        RowAction action = (RowAction) rowActionMap.get(id);
        return action == null ? true
                              : action.canRead(session,
                                               TransactionManager.ACTION_READ);
    }
    public void setTransactionInfo(CachedObject object) {
        if (object.isMemory()) {
            return;
        }
        Row       row    = (Row) object;
        RowAction rowact = (RowAction) rowActionMap.get(row.position);
        row.rowAction = rowact;
    }
    public void removeTransactionInfo(CachedObject object) {
        if (object.isMemory()) {
            return;
        }
        rowActionMap.remove(object.getPos());
    }
    void addToCommittedQueue(Session session, Object[] list) {
        synchronized (committedTransactionTimestamps) {
            committedTransactions.addLast(list);
            committedTransactionTimestamps.addLast(session.actionTimestamp);
        }
    }
    void mergeExpiredTransactions(Session session) {
        long timestamp = getFirstLiveTransactionTimestamp();
        while (true) {
            long     commitTimestamp = 0;
            Object[] actions         = null;
            synchronized (committedTransactionTimestamps) {
                if (committedTransactionTimestamps.isEmpty()) {
                    break;
                }
                commitTimestamp = committedTransactionTimestamps.getFirst();
                if (commitTimestamp < timestamp) {
                    committedTransactionTimestamps.removeFirst();
                    actions = (Object[]) committedTransactions.removeFirst();
                } else {
                    break;
                }
            }
            mergeTransaction(session, actions, 0, actions.length,
                             commitTimestamp);
            finaliseRows(session, actions, 0, actions.length, true);
        }
    }
    public void beginTransaction(Session session) {
        writeLock.lock();
        try {
            if (!session.isTransaction) {
                session.actionTimestamp      = nextChangeTimestamp();
                session.transactionTimestamp = session.actionTimestamp;
                session.isTransaction        = true;
                transactionCount++;
                liveTransactionTimestamps.addLast(
                    session.transactionTimestamp);
            }
        } finally {
            writeLock.unlock();
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
            } else {
                session.abortTransaction = true;
            }
        } finally {
            writeLock.unlock();
        }
    }
    public void beginActionResume(Session session) {
        writeLock.lock();
        try {
            session.actionTimestamp = nextChangeTimestamp();
            if (!session.isTransaction) {
                session.transactionTimestamp = session.actionTimestamp;
                session.isTransaction        = true;
                liveTransactionTimestamps.addLast(session.actionTimestamp);
                transactionCount++;
            }
        } finally {
            writeLock.unlock();
        }
    }
    void endTransaction(Session session) {
        long timestamp = session.transactionTimestamp;
        session.isTransaction = false;
        int index = liveTransactionTimestamps.indexOf(timestamp);
        if (index >= 0) {
            transactionCount--;
            liveTransactionTimestamps.remove(index);
            mergeExpiredTransactions(session);
        }
    }
}