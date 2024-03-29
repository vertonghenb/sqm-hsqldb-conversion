package org.hsqldb;
import java.util.concurrent.locks.Lock;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.HashSet;
import org.hsqldb.lib.HsqlDeque;
import org.hsqldb.lib.IntKeyHashMapConcurrent;
import org.hsqldb.lib.LongDeque;
import org.hsqldb.persist.CachedObject;
import org.hsqldb.persist.PersistentStore;
public class TransactionManagerMVCC extends TransactionManagerCommon
implements TransactionManager {
    HsqlDeque committedTransactions          = new HsqlDeque();
    LongDeque committedTransactionTimestamps = new LongDeque();
    boolean isLockedMode;
    Session catalogWriteSession;
    long lockTxTs;
    long lockSessionId;
    long unlockTxTs;
    long unlockSessionId;
    int redoCount = 0;
    public TransactionManagerMVCC(Database db) {
        database     = db;
        lobSession   = database.sessionManager.getSysLobSession();
        rowActionMap = new IntKeyHashMapConcurrent(10000);
        txModel      = MVCC;
    }
    public long getGlobalChangeTimestamp() {
        return globalChangeTimestamp.get();
    }
    public boolean isMVRows() {
        return true;
    }
    public boolean isMVCC() {
        return true;
    }
    public int getTransactionControl() {
        return MVCC;
    }
    public void setTransactionControl(Session session, int mode) {
        super.setTransactionControl(session, mode);
    }
    public void completeActions(Session session) {}
    public boolean prepareCommitActions(Session session) {
        Object[] list  = session.rowActionList.getArray();
        int      limit = session.rowActionList.size();
        if (session.abortTransaction) {
            return false;
        }
        writeLock.lock();
        try {
            for (int i = 0; i < limit; i++) {
                RowAction rowact = (RowAction) list[i];
                if (!rowact.canCommit(session, session.tempSet)) {
                    return false;
                }
            }
            session.actionTimestamp = nextChangeTimestamp();
            for (int i = 0; i < limit; i++) {
                RowAction action = (RowAction) list[i];
                action.prepareCommit(session);
            }
            for (int i = 0; i < session.tempSet.size(); i++) {
                Session current =
                    ((RowActionBase) session.tempSet.get(i)).session;
                current.abortTransaction = true;
            }
            return true;
        } finally {
            writeLock.unlock();
            session.tempSet.clear();
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
            for (int i = 0; i < limit; i++) {
                RowAction rowact = (RowAction) list[i];
                if (!rowact.canCommit(session, session.tempSet)) {
                    return false;
                }
            }
            session.actionTimestamp         = nextChangeTimestamp();
            session.transactionEndTimestamp = session.actionTimestamp;
            endTransaction(session);
            for (int i = 0; i < limit; i++) {
                RowAction action = (RowAction) list[i];
                action.commit(session);
            }
            for (int i = 0; i < session.tempSet.size(); i++) {
                Session current =
                    ((RowActionBase) session.tempSet.get(i)).session;
                current.abortTransaction = true;
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
                if (session.rowActionList.size() > 0) {
                    list = session.rowActionList.toArray();
                    addToCommittedQueue(session, list);
                }
            }
            endTransactionTPL(session);
            session.isTransaction = false;
            countDownLatches(session);
        } finally {
            writeLock.unlock();
            session.tempSet.clear();
        }
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
            session.isTransaction = false;
            countDownLatches(session);
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
        writeLock.lock();
        try {
            mergeRolledBackTransaction(session, timestamp, list, start, limit);
            finaliseRows(session, list, start, limit, false);
        } finally {
            writeLock.unlock();
        }
        session.rowActionList.setSize(start);
    }
    public RowAction addDeleteAction(Session session, Table table, Row row,
                                     int[] colMap) {
        RowAction action = addDeleteActionToRow(session, table, row, colMap);
        Session   actionSession = null;
        boolean   redoAction    = true;
        if (action == null) {
            writeLock.lock();
            try {
                rollbackAction(session);
                if (session.isolationLevel == SessionInterface
                        .TX_REPEATABLE_READ || session
                        .isolationLevel == SessionInterface.TX_SERIALIZABLE) {
                    session.tempSet.clear();
                    session.redoAction       = false;
                    session.abortTransaction = session.txConflictRollback;
                    throw Error.error(ErrorCode.X_40501);
                }
                if (row.rowAction != null && row.rowAction.isDeleted()) {
                    session.tempSet.clear();
                    session.redoAction = true;
                    redoCount++;
                    throw Error.error(ErrorCode.X_40501);
                }
                redoAction = !session.tempSet.isEmpty();
                if (redoAction) {
                    actionSession =
                        ((RowActionBase) session.tempSet.get(0)).session;
                    session.tempSet.clear();
                    if (actionSession != null) {
                        redoAction = checkDeadlock(session, actionSession);
                    }
                }
                if (redoAction) {
                    session.redoAction = true;
                    if (actionSession != null) {
                        actionSession.waitingSessions.add(session);
                        session.waitedSessions.add(actionSession);
                        session.latch.countUp();
                    }
                    redoCount++;
                } else {
                    session.redoAction       = false;
                    session.abortTransaction = session.txConflictRollback;
                }
                throw Error.error(ErrorCode.X_40501);
            } finally {
                writeLock.unlock();
            }
        }
        session.rowActionList.add(action);
        return action;
    }
    public void addInsertAction(Session session, Table table,
                                PersistentStore store, Row row,
                                int[] changedColumns) {
        RowAction action        = row.rowAction;
        Session   actionSession = null;
        boolean   redoAction    = false;
        boolean   redoWait      = true;
        HsqlException cause     = null;
        if (action == null) {
            System.out.println("null insert action " + session + " "
                               + session.actionTimestamp);
        }
        if (table.tableType == TableBase.CACHED_TABLE) {
            rowActionMap.put(action.getPos(), action);
        }
        try {
            store.indexRow(session, row);
        } catch (HsqlException e) {
            if (session.tempSet.isEmpty()) {
                throw e;
            }
            redoAction = true;
            cause = e;
        }
        if (!redoAction) {
            session.rowActionList.add(action);
            return;
        }
        writeLock.lock();
        try {
            rollbackAction(session);
            RowActionBase otherAction = (RowActionBase) session.tempSet.get(0);
            actionSession = otherAction.session;
            session.tempSet.clear();
            if (otherAction.commitTimestamp != 0) {
                redoWait = false;
            }
            switch (session.isolationLevel) {
                case SessionInterface.TX_REPEATABLE_READ :
                case SessionInterface.TX_SERIALIZABLE :
                    redoAction = false;
                    break;
                default :
                    redoAction = checkDeadlock(session, actionSession);
            }
            if (redoAction) {
                session.redoAction = true;
                if (redoWait) {
                    actionSession.waitingSessions.add(session);
                    session.waitedSessions.add(actionSession);
                    session.latch.countUp();
                }
                redoCount++;
            } else {
                session.abortTransaction = session.txConflictRollback;
                session.redoAction = false;
            }
            throw Error.error(cause, ErrorCode.X_40501, null);
        } finally {
            writeLock.unlock();
        }
    }
    public boolean canRead(Session session, Row row, int mode, int[] colMap) {
        RowAction action = row.rowAction;
        if (mode == TransactionManager.ACTION_READ) {
            if (action == null) {
                return true;
            }
            return action.canRead(session, TransactionManager.ACTION_READ);
        }
        if (mode == ACTION_REF) {
            boolean result;
            if (action == null) {
                result = true;
            } else {
                result = action.canRead(session,
                                        TransactionManager.ACTION_READ);
            }
            return result;
        }
        if (action == null) {
            return true;
        }
        return action.canRead(session, mode);
    }
    public boolean canRead(Session session, int id, int mode) {
        RowAction action = (RowAction) rowActionMap.get(id);
        if (action == null) {
            return true;
        }
        return action.canRead(session, mode);
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
            long     commitTimestamp;
            Object[] actions;
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
                liveTransactionTimestamps.addLast(
                    session.transactionTimestamp);
                transactionCount++;
            }
        } finally {
            writeLock.unlock();
        }
    }
    public void beginAction(Session session, Statement cs) {
        if (session.isTransaction) {
            return;
        }
        if (cs == null) {
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
            session.isPreTransaction = true;
            if (!isLockedMode && !cs.isCatalogLock()) {
                return;
            }
            beginActionTPL(session, cs);
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
            session.isPreTransaction = false;
        } finally {
            writeLock.unlock();
        }
    }
    RowAction addDeleteActionToRow(Session session, Table table, Row row,
                                   int[] colMap) {
        RowAction action = null;
        synchronized (row) {
            if (table.tableType == TableBase.CACHED_TABLE) {
                Lock mapLock = rowActionMap.getWriteLock();
                mapLock.lock();
                try {
                    action = (RowAction) rowActionMap.get(row.getPos());
                    if (action == null) {
                        action = RowAction.addDeleteAction(session, table,
                                                           row, colMap);
                        if (action != null) {
                            rowActionMap.put(row.getPos(), action);
                        }
                    } else {
                        row.rowAction = action;
                        action = RowAction.addDeleteAction(session, table,
                                                           row, colMap);
                    }
                } finally {
                    mapLock.unlock();
                }
            } else {
                action = RowAction.addDeleteAction(session, table, row,
                                                   colMap);
            }
        }
        return action;
    }
    void endTransaction(Session session) {
        long timestamp = session.transactionTimestamp;
        int  index     = liveTransactionTimestamps.indexOf(timestamp);
        if (index >= 0) {
            transactionCount--;
            liveTransactionTimestamps.remove(index);
            mergeExpiredTransactions(session);
        }
    }
    private void countDownLatches(Session session) {
        for (int i = 0; i < session.waitingSessions.size(); i++) {
            Session current = (Session) session.waitingSessions.get(i);
            current.waitedSessions.remove(session);
            current.latch.countDown();
        }
        session.waitingSessions.clear();
    }
    void getTransactionSessions(HashSet set) {
        Session[] sessions = database.sessionManager.getAllSessions();
        for (int i = 0; i < sessions.length; i++) {
            long timestamp = sessions[i].getTransactionTimestamp();
            if (liveTransactionTimestamps.contains(timestamp)) {
                set.add(sessions[i]);
            } else if (sessions[i].isPreTransaction) {
                set.add(sessions[i]);
            } else if (sessions[i].isTransaction) {
                set.add(sessions[i]);
            }
        }
    }
    void endTransactionTPL(Session session) {
        if (catalogWriteSession != session) {
            return;
        }
        Session nextSession = null;
        session.waitingSessions.size();
        for (int i = 0; i < session.waitingSessions.size(); i++) {
            Session   current = (Session) session.waitingSessions.get(i);
            Statement st      = current.sessionContext.currentStatement;
            if (st != null && st.isCatalogLock()) {
                nextSession = current;
                break;
            }
        }
        if (nextSession == null) {
            catalogWriteSession = null;
            isLockedMode        = false;
        } else {
            for (int i = 0; i < session.waitingSessions.size(); i++) {
                Session current = (Session) session.waitingSessions.get(i);
                if (current != nextSession) {
                    current.waitedSessions.add(nextSession);
                    nextSession.waitingSessions.add(current);
                    current.latch.countUp();
                }
            }
            catalogWriteSession = nextSession;
        }
        unlockTxTs      = session.actionTimestamp;
        unlockSessionId = session.getId();
    }
    boolean beginActionTPL(Session session, Statement cs) {
        if (cs == null) {
            return true;
        }
        if (session.abortTransaction) {
            return false;
        }
        if (session == catalogWriteSession) {
            return true;
        }
        session.tempSet.clear();
        if (cs.isCatalogLock()) {
            if (catalogWriteSession == null) {
                catalogWriteSession = session;
                isLockedMode        = true;
                lockTxTs            = session.actionTimestamp;
                lockSessionId       = session.getId();
                getTransactionSessions(session.tempSet);
                session.tempSet.remove(session);
                if (!session.tempSet.isEmpty()) {
                    setWaitingSessionTPL(session);
                }
                return true;
            }
        }
        if (!isLockedMode) {
            return true;
        }
        if (cs.getTableNamesForWrite().length > 0) {
            if (cs.getTableNamesForWrite()[0].schema
                    == SqlInvariants.LOBS_SCHEMA_HSQLNAME) {
                return true;
            }
        } else if (cs.getTableNamesForRead().length > 0) {
            if (cs.getTableNamesForRead()[0].schema
                    == SqlInvariants.LOBS_SCHEMA_HSQLNAME) {
                return true;
            }
        } else {
            return true;
        }
        if (session.waitingSessions.contains(catalogWriteSession)) {
            return true;
        }
        if (catalogWriteSession.waitingSessions.add(session)) {
            session.waitedSessions.add(catalogWriteSession);
            session.latch.countUp();
        }
        return true;
    }
}