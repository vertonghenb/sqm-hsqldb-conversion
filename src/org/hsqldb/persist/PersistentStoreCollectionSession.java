package org.hsqldb.persist;
import org.hsqldb.HsqlException;
import org.hsqldb.Session;
import org.hsqldb.Table;
import org.hsqldb.TableBase;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.HsqlDeque;
import org.hsqldb.lib.Iterator;
import org.hsqldb.lib.LongKeyHashMap;
import org.hsqldb.store.ValuePool;
public class PersistentStoreCollectionSession
implements PersistentStoreCollection {
    private final Session        session;
    private final LongKeyHashMap rowStoreMapSession     = new LongKeyHashMap();
    private LongKeyHashMap       rowStoreMapTransaction = new LongKeyHashMap();
    private LongKeyHashMap       rowStoreMapStatement   = new LongKeyHashMap();
    private HsqlDeque            rowStoreListStatement;
    public PersistentStoreCollectionSession(Session session) {
        this.session = session;
    }
    public void setStore(Object key, PersistentStore store) {
        TableBase table = (TableBase) key;
        switch (table.persistenceScope) {
            case TableBase.SCOPE_STATEMENT :
                if (store == null) {
                    rowStoreMapStatement.remove(table.getPersistenceId());
                } else {
                    rowStoreMapStatement.put(table.getPersistenceId(), store);
                }
                break;
            case TableBase.SCOPE_FULL :
            case TableBase.SCOPE_TRANSACTION :
                if (store == null) {
                    rowStoreMapTransaction.remove(table.getPersistenceId());
                } else {
                    rowStoreMapTransaction.put(table.getPersistenceId(),
                                               store);
                }
                break;
            case TableBase.SCOPE_SESSION :
                if (store == null) {
                    rowStoreMapSession.remove(table.getPersistenceId());
                } else {
                    rowStoreMapSession.put(table.getPersistenceId(), store);
                }
                break;
            default :
                throw Error.runtimeError(ErrorCode.U_S0500,
                                         "PersistentStoreCollectionSession");
        }
    }
    public PersistentStore getViewStore(long persistenceId) {
        return (PersistentStore) rowStoreMapStatement.get(persistenceId);
    }
    public PersistentStore getStore(Object key) {
        try {
            TableBase       table = (TableBase) key;
            PersistentStore store;
            switch (table.persistenceScope) {
                case TableBase.SCOPE_STATEMENT :
                    store = (PersistentStore) rowStoreMapStatement.get(
                        table.getPersistenceId());
                    if (store == null) {
                        store = session.database.logger.newStore(session,
                                this, table);
                    }
                    return store;
                case TableBase.SCOPE_FULL :
                case TableBase.SCOPE_TRANSACTION :
                    store = (PersistentStore) rowStoreMapTransaction.get(
                        table.getPersistenceId());
                    if (store == null) {
                        store = session.database.logger.newStore(session,
                                this, table);
                    }
                    if (table.getTableType() == TableBase.INFO_SCHEMA_TABLE) {
                        session.database.dbInfo.setStore(session,
                                                         (Table) table, store);
                    }
                    return store;
                case TableBase.SCOPE_SESSION :
                    store = (PersistentStore) rowStoreMapSession.get(
                        table.getPersistenceId());
                    if (store == null) {
                        store = session.database.logger.newStore(session,
                                this, table);
                    }
                    return store;
            }
        } catch (HsqlException e) {}
        throw Error.runtimeError(ErrorCode.U_S0500,
                                 "PersistentStoreCollectionSession");
    }
    public void clearAllTables() {
        clearSessionTables();
        clearTransactionTables();
        clearStatementTables();
        closeResultCache();
    }
    public void clearResultTables(long actionTimestamp) {
        if (rowStoreMapSession.isEmpty()) {
            return;
        }
        Iterator it = rowStoreMapSession.values().iterator();
        while (it.hasNext()) {
            PersistentStore store = (PersistentStore) it.next();
            if (store.getTimestamp() == actionTimestamp) {
                store.release();
                it.remove();
            }
        }
    }
    public void clearSessionTables() {
        if (rowStoreMapSession.isEmpty()) {
            return;
        }
        Iterator it = rowStoreMapSession.values().iterator();
        while (it.hasNext()) {
            PersistentStore store = (PersistentStore) it.next();
            store.release();
        }
        rowStoreMapSession.clear();
    }
    public void clearTransactionTables() {
        if (rowStoreMapTransaction.isEmpty()) {
            return;
        }
        Iterator it = rowStoreMapTransaction.values().iterator();
        while (it.hasNext()) {
            PersistentStore store = (PersistentStore) it.next();
            store.release();
        }
        rowStoreMapTransaction.clear();
    }
    public void clearStatementTables() {
        if (rowStoreMapStatement.isEmpty()) {
            return;
        }
        Iterator it = rowStoreMapStatement.values().iterator();
        while (it.hasNext()) {
            PersistentStore store = (PersistentStore) it.next();
            store.release();
        }
        rowStoreMapStatement.clear();
    }
    public void registerIndex(Table table) {
        PersistentStore store = findStore(table);
        if (store == null) {
            return;
        }
        store.resetAccessorKeys(table.getIndexList());
    }
    public PersistentStore findStore(Table table) {
        PersistentStore store = null;
        switch (table.persistenceScope) {
            case TableBase.SCOPE_STATEMENT :
                store = (PersistentStore) rowStoreMapStatement.get(
                    table.getPersistenceId());
                break;
            case TableBase.SCOPE_FULL :
            case TableBase.SCOPE_TRANSACTION :
                store = (PersistentStore) rowStoreMapTransaction.get(
                    table.getPersistenceId());
                break;
            case TableBase.SCOPE_SESSION :
                store = (PersistentStore) rowStoreMapSession.get(
                    table.getPersistenceId());
                break;
        }
        return store;
    }
    public void moveData(Table oldTable, Table newTable, int colIndex,
                         int adjust) {
        PersistentStore oldStore = findStore(oldTable);
        if (oldStore == null) {
            return;
        }
        PersistentStore newStore = getStore(newTable);
        try {
            newStore.moveData(session, oldStore, colIndex, adjust);
        } catch (HsqlException e) {
            newStore.release();
            setStore(newTable, null);
            throw e;
        }
        setStore(oldTable, null);
    }
    public void push() {
        if (rowStoreListStatement == null) {
            rowStoreListStatement = new HsqlDeque();
        }
        if (rowStoreMapStatement.isEmpty()) {
            rowStoreListStatement.add(ValuePool.emptyObjectArray);
            return;
        }
        Object[] array = rowStoreMapStatement.toArray();
        rowStoreListStatement.add(array);
        rowStoreMapStatement.clear();
    }
    public void pop() {
        Object[] array = (Object[]) rowStoreListStatement.removeLast();
        clearStatementTables();
        for (int i = 0; i < array.length; i++) {
            PersistentStore store = (PersistentStore) array[i];
            rowStoreMapStatement.put(store.getTable().getPersistenceId(),
                                     store);
        }
    }
    DataFileCacheSession resultCache;
    public DataFileCacheSession getResultCache() {
        if (resultCache == null) {
            String path = session.database.logger.getTempDirectoryPath();
            if (path == null) {
                return null;
            }
            try {
                resultCache =
                    new DataFileCacheSession(session.database,
                                             path + "/session_"
                                             + Long.toString(session.getId()));
                resultCache.open(false);
            } catch (Throwable t) {
                return null;
            }
        }
        return resultCache;
    }
    public void closeResultCache() {
        if (resultCache != null) {
            try {
                resultCache.close(false);
                resultCache.deleteFile();
            } catch (HsqlException e) {}
            resultCache = null;
        }
    }
}