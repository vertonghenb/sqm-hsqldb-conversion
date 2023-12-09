


package org.hsqldb;

import java.io.IOException;

import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.index.NodeAVL;
import org.hsqldb.index.NodeAVLDisk;
import org.hsqldb.lib.IntLookup;
import org.hsqldb.persist.PersistentStore;
import org.hsqldb.rowio.RowInputInterface;
import org.hsqldb.rowio.RowOutputInterface;








public class RowAVLDisk extends RowAVL {

    public static final int NO_POS = -1;

    
    int              storageSize;
    int              keepCount;
    volatile boolean isInMemory;
    int              accessCount;

    
    boolean hasDataChanged;

    
    private boolean hasNodesChanged;

    
    public RowAVLDisk(TableBase t, Object[] o, PersistentStore store) {

        super(t, o);

        setNewNodes(store);

        hasDataChanged = hasNodesChanged = true;
    }

    RowAVLDisk(TableBase t, Object[] o) {

        super(t, o);

        hasDataChanged = hasNodesChanged = true;
    }

    
    public RowAVLDisk(TableBase t, RowInputInterface in) throws IOException {

        super(t, null);

        position    = in.getPos();
        storageSize = in.getSize();

        int indexcount = t.getIndexCount();

        nPrimaryNode = new NodeAVLDisk(this, in, 0);

        NodeAVL n = nPrimaryNode;

        for (int i = 1; i < indexcount; i++) {
            n.nNext = new NodeAVLDisk(this, in, i);
            n       = n.nNext;
        }

        rowData = in.readData(table.getColumnTypes());
    }

    public NodeAVL insertNode(int index) {
        return null;
    }

    private void readRowInfo(RowInputInterface in) throws IOException {

        
    }

    
    public synchronized void setNodesChanged() {
        hasNodesChanged = true;
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

    public boolean isMemory() {
        return false;
    }

    
    public void setPos(int pos) {

        position = pos;

        NodeAVL n = nPrimaryNode;

        while (n != null) {
            ((NodeAVLDisk) n).iData = position;
            n                       = n.nNext;
        }
    }

    
    public synchronized void setChanged(boolean changed) {
        hasDataChanged = changed;
    }

    
    public synchronized boolean hasChanged() {
        return hasNodesChanged || hasDataChanged;
    }

    
    public TableBase getTable() {
        return table;
    }

    public void setStorageSize(int size) {
        storageSize = size;
    }

    
    public synchronized boolean isKeepInMemory() {
        return keepCount > 0;
    }

    
    public void delete(PersistentStore store) {

        RowAVLDisk row = this;

        if (!row.keepInMemory(true)) {
            row = (RowAVLDisk) store.get(row, true);
        }

        super.delete(store);
        row.keepInMemory(false);
    }

    public void destroy() {
        nPrimaryNode = null;
        table        = null;
    }

    public synchronized boolean keepInMemory(boolean keep) {

        if (!isInMemory) {
            return false;
        }

        if (keep) {
            keepCount++;
        } else {
            keepCount--;

            if (keepCount < 0) {
                throw Error.runtimeError(ErrorCode.U_S0500,
                                         "RowAVLDisk - keep count");
            }
        }

        return true;
    }

    public synchronized boolean isInMemory() {
        return isInMemory;
    }

    public synchronized void setInMemory(boolean in) {

        isInMemory = in;

        if (in) {
            return;
        }

        NodeAVL n = nPrimaryNode;

        while (n != null) {
            n.setInMemory(in);

            n = n.nNext;
        }
    }

    public void setNewNodes(PersistentStore store) {

        int indexcount = store.getAccessorKeys().length;

        nPrimaryNode = new NodeAVLDisk(this, 0);

        NodeAVL n = nPrimaryNode;

        for (int i = 1; i < indexcount; i++) {
            n.nNext = new NodeAVLDisk(this, i);
            n       = n.nNext;
        }
    }

    public int getRealSize(RowOutputInterface out) {
        return out.getSize(this);
    }

    
    public void write(RowOutputInterface out) {

        try {
            writeNodes(out);

            if (hasDataChanged) {
                out.writeData(this, table.colTypes);
                out.writeEnd();

                hasDataChanged = false;
            }
        } catch (IOException e) {}
    }

    public void write(RowOutputInterface out, IntLookup lookup) {

        out.writeSize(storageSize);

        NodeAVL rownode = nPrimaryNode;

        while (rownode != null) {
            ((NodeAVLDisk) rownode).write(out, lookup);

            rownode = rownode.nNext;
        }

        out.writeData(this, table.colTypes);
        out.writeEnd();
    }

    
    void writeNodes(RowOutputInterface out) throws IOException {

        out.writeSize(storageSize);

        NodeAVL n = nPrimaryNode;

        while (n != null) {
            n.write(out);

            n = n.nNext;
        }

        hasNodesChanged = false;
    }

    
    public boolean equals(Object obj) {

        if (obj == this) {
            return true;
        }

        if (obj instanceof RowAVLDisk) {
            return ((RowAVLDisk) obj).position == position;
        }

        return false;
    }
}
