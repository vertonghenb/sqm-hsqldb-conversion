package org.hsqldb;
import java.io.IOException;
import org.hsqldb.index.NodeAVL;
import org.hsqldb.persist.PersistentStore;
import org.hsqldb.rowio.RowInputInterface;
import org.hsqldb.rowio.RowOutputInterface;
public class RowAVLDiskData extends RowAVL {
    PersistentStore store;
    int             accessCount;
    boolean         hasDataChanged;
    int             storageSize;
    public RowAVLDiskData(PersistentStore store, TableBase t, Object[] o) {
        super(t, o);
        setNewNodes(store);
        hasDataChanged = true;
        this.store     = store;
    }
    public RowAVLDiskData(PersistentStore store, TableBase t,
                          RowInputInterface in) throws IOException {
        super(t, (Object[]) null);
        setNewNodes(store);
        position       = in.getPos();
        storageSize    = in.getSize();
        rowData        = in.readData(table.getColumnTypes());
        hasDataChanged = false;
        this.store     = store;
    }
    public static Object[] getRowData(TableBase t,
                                      RowInputInterface in)
                                      throws IOException {
        return in.readData(t.getColumnTypes());
    }
    public void setData(Object[] data) {
        this.rowData = data;
    }
    public Object[] getData() {
        Object[] data = rowData;
        if (data == null) {
            store.writeLock();
            try {
                store.get(this, false);
                data = rowData;
                if (data == null) {
                    store.get(this, false);
                    data = rowData;
                }
            } finally {
                store.writeUnlock();
            }
        } else {
            accessCount++;
        }
        return data;
    }
    public void setNewNodes(PersistentStore store) {
        int index = store.getAccessorKeys().length;
        nPrimaryNode = new NodeAVL(this);
        NodeAVL n = nPrimaryNode;
        for (int i = 1; i < index; i++) {
            n.nNext = new NodeAVL(this);
            n       = n.nNext;
        }
    }
    public NodeAVL insertNode(int index) {
        NodeAVL backnode = getNode(index - 1);
        NodeAVL newnode  = new NodeAVL(this);
        newnode.nNext  = backnode.nNext;
        backnode.nNext = newnode;
        return newnode;
    }
    void setPrimaryNode(NodeAVL primary) {
        nPrimaryNode = primary;
    }
    public int getRealSize(RowOutputInterface out) {
        return out.getSize(this);
    }
    public void write(RowOutputInterface out) {
        out.writeSize(storageSize);
        out.writeData(this, table.colTypes);
        out.writeEnd();
        hasDataChanged = false;
    }
    public synchronized void setChanged(boolean changed) {
        hasDataChanged = changed;
    }
    public boolean hasChanged() {
        return hasDataChanged;
    }
    public void updateAccessCount(int count) {
        accessCount = count;
    }
    public int getAccessCount() {
        return accessCount;
    }
    public int getStorageSize() {
        return storageSize;
    }
    public void setStorageSize(int size) {
        storageSize = size;
    }
    public void setPos(int pos) {
        position = pos;
    }
    public boolean isMemory() {
        return true;
    }
    public boolean equals(Object obj) {
        return obj == this;
    }
    public boolean isInMemory() {
        return rowData != null;
    }
    public boolean isKeepInMemory() {
        return false;
    }
    public boolean keepInMemory(boolean keep) {
        return true;
    }
    public void setInMemory(boolean in) {
        if (!in) {
            rowData = null;
        }
    }
}