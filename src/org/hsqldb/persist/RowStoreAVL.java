package org.hsqldb.persist;
import org.hsqldb.ColumnSchema;
import org.hsqldb.Database;
import org.hsqldb.HsqlException;
import org.hsqldb.Row;
import org.hsqldb.RowAVL;
import org.hsqldb.Session;
import org.hsqldb.Table;
import org.hsqldb.TableBase;
import org.hsqldb.TransactionManager;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.index.Index;
import org.hsqldb.index.IndexAVL;
import org.hsqldb.index.NodeAVL;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.navigator.RowIterator;
import org.hsqldb.rowio.RowInputInterface;
import org.hsqldb.types.Type;
public abstract class RowStoreAVL implements PersistentStore {
    Session                   session;
    Database                  database;
    PersistentStoreCollection manager;
    Index[]                   indexList    = Index.emptyArray;
    CachedObject[]            accessorList = CachedObject.emptyArray;
    TableBase                 table;
    int                       elementCount;
    private long timestamp;
    PersistentStore[] subStores = PersistentStore.emptyArray;
    public TableBase getTable() {
        return table;
    }
    public long getTimestamp() {
        return timestamp;
    }
    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
    public abstract boolean isMemory();
    public void setMemory(boolean mode) {}
    public abstract int getAccessCount();
    public abstract void set(CachedObject object);
    public abstract CachedObject get(int key, boolean keep);
    public abstract CachedObject get(CachedObject object, boolean keep);
    public abstract int getStorageSize(int key);
    public abstract void add(CachedObject object);
    public abstract CachedObject get(RowInputInterface in);
    public abstract CachedObject getNewInstance(int size);
    public abstract CachedObject getNewCachedObject(Session session,
            Object object, boolean tx);
    public abstract void removePersistence(int i);
    public abstract void removeAll();
    public abstract void remove(int i);
    public abstract void release(int i);
    public abstract void commitPersistence(CachedObject object);
    public abstract DataFileCache getCache();
    public abstract void setCache(DataFileCache cache);
    public abstract void release();
    public PersistentStore getAccessorStore(Index index) {
        return null;
    }
    public CachedObject getAccessor(Index key) {
        int position = key.getPosition();
        if (position >= accessorList.length) {
            throw Error.runtimeError(ErrorCode.U_S0500, "RowStoreAVL");
        }
        return accessorList[position];
    }
    public void delete(Session session, Row row) {
        row = (Row) get(row, false);
        for (int i = 0; i < indexList.length; i++) {
            indexList[i].delete(session, this, row);
        }
        for (int i = 0; i < subStores.length; i++) {
            subStores[i].delete(session, row);
        }
        row.delete(this);
        elementCount--;
    }
    public void indexRow(Session session, Row row) {
        int i = 0;
        try {
            for (; i < indexList.length; i++) {
                indexList[i].insert(session, this, row);
            }
            int j = 0;
            try {
                for (j = 0; j < subStores.length; j++) {
                    subStores[j].indexRow(session, row);
                }
            } catch (HsqlException e) {
                int count = j;
                j = 0;
                for (; j < count; j++) {
                    subStores[j].delete(session, row);
                }
                throw e;
            }
            elementCount++;
        } catch (HsqlException e) {
            int count = i;
            i = 0;
            for (; i < count; i++) {
                indexList[i].delete(session, this, row);
            }
            remove(row.getPos());
            throw e;
        }
    }
    public final void indexRows(Session session) {
        for (int i = 1; i < indexList.length; i++) {
            setAccessor(indexList[i], null);
        }
        RowIterator it = rowIterator();
        while (it.hasNext()) {
            Row row = it.getNextRow();
            ((RowAVL) row).clearNonPrimaryNodes();
            for (int i = 1; i < indexList.length; i++) {
                indexList[i].insert(session, this, row);
            }
        }
    }
    public final RowIterator rowIterator() {
        Index index = indexList[0];
        for (int i = 0; i < indexList.length; i++) {
            if (indexList[i].isClustered()) {
                index = indexList[i];
                break;
            }
        }
        return index.firstRow(this);
    }
    public abstract void setAccessor(Index key, CachedObject accessor);
    public abstract void setAccessor(Index key, int accessor);
    public void resetAccessorKeys(Index[] keys) {
        Index[] oldIndexList = indexList;
        if (indexList.length == 0 || accessorList[0] == null) {
            indexList    = keys;
            accessorList = new CachedObject[indexList.length];
            return;
        }
        if (indexList == keys) {
            return;
        }
        CachedObject[] oldAccessors = accessorList;
        int            limit        = indexList.length;
        int            diff         = keys.length - indexList.length;
        int            position     = 0;
        if (diff < -1) {
            throw Error.runtimeError(ErrorCode.U_S0500, "RowStoreAV");
        } else if (diff == -1) {
            limit = keys.length;
        } else if (diff == 0) {
            throw Error.runtimeError(ErrorCode.U_S0500, "RowStoreAV");
        } else if (diff == 1) {
            ;
        } else {
            for (; position < limit; position++) {
                if (indexList[position] != keys[position]) {
                    break;
                }
            }
            Index[] tempKeys = (Index[]) ArrayUtil.toAdjustedArray(indexList,
                null, position, 1);
            tempKeys[position] = keys[position];
            resetAccessorKeys(tempKeys);
            resetAccessorKeys(keys);
            return;
        }
        for (; position < limit; position++) {
            if (indexList[position] != keys[position]) {
                break;
            }
        }
        accessorList = (CachedObject[]) ArrayUtil.toAdjustedArray(accessorList,
                null, position, diff);
        indexList = keys;
        try {
            if (diff > 0) {
                insertIndexNodes(indexList[0], indexList[position]);
            } else {
                dropIndexFromRows(indexList[0], oldIndexList[position]);
            }
        } catch (HsqlException e) {
            accessorList = oldAccessors;
            indexList    = oldIndexList;
            throw e;
        }
    }
    public Index[] getAccessorKeys() {
        return indexList;
    }
    public int elementCount() {
        Index index = this.indexList[0];
        if (elementCount < 0) {
            elementCount = ((IndexAVL) index).getNodeCount(session, this);
        }
        return elementCount;
    }
    public int elementCount(Session session) {
        Index index = this.indexList[0];
        if (elementCount < 0) {
            elementCount = ((IndexAVL) index).getNodeCount(session, this);
        }
        if (session != null) {
            int txControl = session.database.txManager.getTransactionControl();
            if (txControl != TransactionManager.LOCKS) {
                switch (table.getTableType()) {
                    case TableBase.MEMORY_TABLE :
                    case TableBase.CACHED_TABLE :
                    case TableBase.TEXT_TABLE :
                        return ((IndexAVL) index).getNodeCount(session, this);
                    default :
                }
            }
        }
        return elementCount;
    }
    public int elementCountUnique(Index index) {
        return 0;
    }
    public void setElementCount(Index key, int size, int uniqueSize) {
        elementCount = size;
    }
    public final void moveData(Session session, PersistentStore other,
                               int colindex, int adjust) {
        Type   oldtype  = null;
        Type   newtype  = null;
        Object colvalue = null;
        if (adjust >= 0 && colindex != -1) {
            ColumnSchema column = ((Table) table).getColumn(colindex);
            colvalue = column.getDefaultValue(session);
            newtype  = ((Table) table).getColumnTypes()[colindex];
        }
        if (adjust <= 0 && colindex != -1) {
            oldtype = ((Table) other.getTable()).getColumnTypes()[colindex];
        }
        try {
            Table       table = (Table) this.table;
            RowIterator it    = other.rowIterator();
            while (it.hasNext()) {
                Row      row      = it.getNextRow();
                Object[] olddata  = row.getData();
                Object[] data     = table.getEmptyRowData();
                Object   oldvalue = null;
                if (adjust == 0 && colindex != -1) {
                    oldvalue = olddata[colindex];
                    colvalue = newtype.convertToType(session, oldvalue,
                                                     oldtype);
                }
                ArrayUtil.copyAdjustArray(olddata, data, colvalue, colindex,
                                          adjust);
                table.systemSetIdentityColumn(session, data);
                if (table.hasGeneratedColumn()) {
                    ((Table) table).setGeneratedColumns(session, data);
                }
                table.enforceTypeLimits(session, data);
                table.enforceRowConstraints(session, data);
                Row newrow = (Row) getNewCachedObject(session, data, false);
                indexRow(session, newrow);
            }
            if (table.isTemp()) {
                return;
            }
            if (oldtype != null && oldtype.isLobType()) {
                it = other.rowIterator();
                while (it.hasNext()) {
                    Row      row      = it.getNextRow();
                    Object[] olddata  = row.getData();
                    Object   oldvalue = olddata[colindex];
                    if (oldvalue != null) {
                        session.sessionData.adjustLobUsageCount(oldvalue, -1);
                    }
                }
            }
            if (newtype != null && newtype.isLobType()) {
                it = rowIterator();
                while (it.hasNext()) {
                    Row      row   = it.getNextRow();
                    Object[] data  = row.getData();
                    Object   value = data[colindex];
                    if (value != null) {
                        session.sessionData.adjustLobUsageCount(value, +1);
                    }
                }
            }
        } catch (java.lang.OutOfMemoryError e) {
            throw Error.error(ErrorCode.OUT_OF_MEMORY);
        }
    }
    public void reindex(Session session, Index index) {
        setAccessor(index, null);
        RowIterator it = table.rowIterator(this);
        while (it.hasNext()) {
            RowAVL row = (RowAVL) it.getNextRow();
            row.getNode(index.getPosition()).delete();
            index.insert(session, this, row);
        }
    }
    public void writeLock() {}
    public void writeUnlock() {}
    void dropIndexFromRows(Index primaryIndex, Index oldIndex) {
        RowIterator it       = primaryIndex.firstRow(this);
        int         position = oldIndex.getPosition() - 1;
        while (it.hasNext()) {
            Row     row      = it.getNextRow();
            int     i        = position - 1;
            NodeAVL backnode = ((RowAVL) row).getNode(0);
            while (i-- > 0) {
                backnode = backnode.nNext;
            }
            backnode.nNext = backnode.nNext.nNext;
        }
    }
    boolean insertIndexNodes(Index primaryIndex, Index newIndex) {
        int           position = newIndex.getPosition();
        RowIterator   it       = primaryIndex.firstRow(this);
        int           rowCount = 0;
        HsqlException error    = null;
        try {
            while (it.hasNext()) {
                Row row = it.getNextRow();
                ((RowAVL) row).insertNode(position);
                rowCount++;
                newIndex.insert(session, this, row);
            }
            return true;
        } catch (java.lang.OutOfMemoryError e) {
            error = Error.error(ErrorCode.OUT_OF_MEMORY);
        } catch (HsqlException e) {
            error = e;
        }
        it = primaryIndex.firstRow(this);
        for (int i = 0; i < rowCount; i++) {
            Row     row      = it.getNextRow();
            NodeAVL backnode = ((RowAVL) row).getNode(0);
            int     j        = position;
            while (--j > 0) {
                backnode = backnode.nNext;
            }
            backnode.nNext = backnode.nNext.nNext;
        }
        throw error;
    }
}