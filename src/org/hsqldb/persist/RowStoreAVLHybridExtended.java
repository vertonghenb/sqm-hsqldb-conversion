package org.hsqldb.persist;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.hsqldb.Row;
import org.hsqldb.RowAVL;
import org.hsqldb.Session;
import org.hsqldb.TableBase;
import org.hsqldb.index.Index;
import org.hsqldb.index.NodeAVL;
import org.hsqldb.navigator.RowIterator;
public class RowStoreAVLHybridExtended extends RowStoreAVLHybrid {
    public RowStoreAVLHybridExtended(Session session,
                                     PersistentStoreCollection manager,
                                     TableBase table, boolean diskBased) {
        super(session, manager, table, diskBased);
    }
    public CachedObject getNewCachedObject(Session session, Object object,
                                           boolean tx) {
        if (indexList != table.getIndexList()) {
            resetAccessorKeys(table.getIndexList());
        }
        return super.getNewCachedObject(session, object, tx);
    }
    public void indexRow(Session session, Row row) {
        NodeAVL node  = ((RowAVL) row).getNode(0);
        int     count = 0;
        while (node != null) {
            count++;
            node = node.nNext;
        }
        if (count != indexList.length) {
            resetAccessorKeys(table.getIndexList());
            ((RowAVL) row).setNewNodes(this);
        }
        super.indexRow(session, row);
    }
    public CachedObject getAccessor(Index key) {
        int position = key.getPosition();
        if (position >= accessorList.length || indexList[position] != key) {
            resetAccessorKeys(table.getIndexList());
            return getAccessor(key);
        }
        return accessorList[position];
    }
    public synchronized void resetAccessorKeys(Index[] keys) {
        if (indexList.length == 0 || accessorList[0] == null) {
            indexList    = keys;
            accessorList = new CachedObject[indexList.length];
            return;
        }
        if (isCached) {
            resetAccessorKeysForCached();
            return;
        }
        super.resetAccessorKeys(keys);
    }
    private void resetAccessorKeysForCached() {
        RowStoreAVLHybrid tempStore = new RowStoreAVLHybridExtended(session,
            manager, table, true);
        tempStore.changeToDiskTable(session);
        RowIterator iterator = table.rowIterator(this);
        while (iterator.hasNext()) {
            Row row = iterator.getNextRow();
            Row newRow = (Row) tempStore.getNewCachedObject(session,
                row.getData(), false);
            tempStore.indexRow(session, newRow);
        }
        indexList    = tempStore.indexList;
        accessorList = tempStore.accessorList;
    }
}