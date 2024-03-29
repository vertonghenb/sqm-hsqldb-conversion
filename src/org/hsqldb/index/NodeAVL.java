package org.hsqldb.index;
import org.hsqldb.Row;
import org.hsqldb.RowAVLDisk;
import org.hsqldb.lib.IntLookup;
import org.hsqldb.persist.CachedObject;
import org.hsqldb.persist.PersistentStore;
import org.hsqldb.rowio.RowOutputInterface;
public class NodeAVL implements CachedObject {
    static final int NO_POS = RowAVLDisk.NO_POS;
    public int       iBalance;
    public NodeAVL   nNext;    
    protected NodeAVL nLeft;
    protected NodeAVL nRight;
    protected NodeAVL nParent;
    protected final Row row;
    NodeAVL() {
        row = null;
    }
    public NodeAVL(Row r) {
        row = r;
    }
    public void delete() {
        iBalance = 0;
        nLeft    = nRight = nParent = null;
    }
    NodeAVL getLeft(PersistentStore store) {
        return nLeft;
    }
    NodeAVL setLeft(PersistentStore persistentStore, NodeAVL n) {
        nLeft = n;
        return this;
    }
    public int getBalance(PersistentStore store) {
        return iBalance;
    }
    boolean isLeft(NodeAVL node) {
        return nLeft == node;
    }
    boolean isRight(NodeAVL node) {
        return nRight == node;
    }
    NodeAVL getRight(PersistentStore persistentStore) {
        return nRight;
    }
    NodeAVL setRight(PersistentStore persistentStore, NodeAVL n) {
        nRight = n;
        return this;
    }
    NodeAVL getParent(PersistentStore store) {
        return nParent;
    }
    boolean isRoot(PersistentStore store) {
        return nParent == null;
    }
    NodeAVL setParent(PersistentStore persistentStore, NodeAVL n) {
        nParent = n;
        return this;
    }
    public NodeAVL setBalance(PersistentStore store, int b) {
        iBalance = b;
        return this;
    }
    boolean isFromLeft(PersistentStore store) {
        if (nParent == null) {
            return true;
        }
        return this == nParent.nLeft;
    }
    public NodeAVL child(PersistentStore store, boolean isleft) {
        return isleft ? getLeft(store)
            : getRight(store);
    }
    public NodeAVL set(PersistentStore store, boolean isLeft, NodeAVL n) {
        if (isLeft) {
            nLeft = n;
        } else {
            nRight = n;
        }
        if (n != null) {
            n.nParent = this;
        }
        return this;
    }
    public void replace(PersistentStore store, Index index, NodeAVL n) {
        if (nParent == null) {
            if (n != null) {
                n = n.setParent(store, null);
            }
            store.setAccessor(index, n);
        } else {
            nParent.set(store, isFromLeft(store), n);
        }
    }
    boolean equals(NodeAVL n) {
        return n == this;
    }
    public void setInMemory(boolean in) {}
    public void write(RowOutputInterface out) {}
    public void write(RowOutputInterface out, IntLookup lookup) {}
    public int getPos() {
        return 0;
    }
    protected Row getRow(PersistentStore store) {
        return row;
    }
    protected Object[] getData(PersistentStore store) {
        return row.getData();
    }
    public void updateAccessCount(int count) {}
    public int getAccessCount() {
        return 0;
    }
    public void setStorageSize(int size) {}
    public int getStorageSize() {
        return 0;
    }
    public void setPos(int pos) {}
    public boolean hasChanged() {
        return false;
    }
    public boolean isKeepInMemory() {
        return false;
    }
    ;
    public boolean keepInMemory(boolean keep) {
        return true;
    }
    public boolean isInMemory() {
        return false;
    }
    public void restore() {}
    public void destroy() {}
    public int getRealSize(RowOutputInterface out) {
        return 0;
    }
    public boolean isMemory() {
        return true;
    }
}