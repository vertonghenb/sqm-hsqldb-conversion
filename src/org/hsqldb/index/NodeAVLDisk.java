


package org.hsqldb.index;

import java.io.IOException;

import org.hsqldb.Row;
import org.hsqldb.RowAVLDisk;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.IntLookup;
import org.hsqldb.persist.PersistentStore;
import org.hsqldb.rowio.RowInputInterface;
import org.hsqldb.rowio.RowOutputInterface;






public class NodeAVLDisk extends NodeAVL {

    final RowAVLDisk row;

    
    public int              iData;
    private int             iLeft   = NO_POS;
    private int             iRight  = NO_POS;
    private int             iParent = NO_POS;
    private int             iId;    
    public static final int SIZE_IN_BYTE = 4 * 4;

    public NodeAVLDisk(RowAVLDisk r, RowInputInterface in,
                       int id) throws IOException {

        row      = r;
        iId      = id;
        iData    = r.getPos();
        iBalance = in.readInt();
        iLeft    = in.readInt();

        if (iLeft <= 0) {
            iLeft = NO_POS;
        }

        iRight = in.readInt();

        if (iRight <= 0) {
            iRight = NO_POS;
        }

        iParent = in.readInt();

        if (iParent <= 0) {
            iParent = NO_POS;
        }
    }

    public NodeAVLDisk(RowAVLDisk r, int id) {

        row   = r;
        iId   = id;
        iData = r.getPos();
    }

    public void delete() {

        iLeft    = NO_POS;
        iRight   = NO_POS;
        iParent  = NO_POS;
        nLeft    = null;
        nRight   = null;
        nParent  = null;
        iBalance = 0;

        row.setNodesChanged();
    }

    public boolean isInMemory() {
        return row.isInMemory();
    }

    public boolean isMemory() {
        return false;
    }

    public int getPos() {
        return iData;
    }

    public Row getRow(PersistentStore store) {

        if (!row.isInMemory()) {
            return (RowAVLDisk) store.get(this.row, false);
        } else {
            row.updateAccessCount(store.getAccessCount());
        }

        return row;
    }

    public Object[] getData(PersistentStore store) {
        return row.getData();
    }

    private NodeAVLDisk findNode(PersistentStore store, int pos) {

        NodeAVLDisk ret = null;
        RowAVLDisk  r   = (RowAVLDisk) store.get(pos, false);

        if (r != null) {
            ret = (NodeAVLDisk) r.getNode(iId);
        }

        return ret;
    }

    boolean isLeft(NodeAVL n) {

        if (n == null) {
            return iLeft == NO_POS;
        }

        return iLeft == ((NodeAVLDisk) n).iData;
    }

    boolean isRight(NodeAVL n) {

        if (n == null) {
            return iRight == NO_POS;
        }

        return iRight == ((NodeAVLDisk) n).iData;
    }

    NodeAVL getLeft(PersistentStore store) {

        NodeAVLDisk node = this;
        RowAVLDisk  row  = this.row;

        if (!row.isInMemory()) {
            row  = (RowAVLDisk) store.get(this.row, false);
            node = (NodeAVLDisk) row.getNode(iId);
        }

        if (node.iLeft == NO_POS) {
            return null;
        }

        if (node.nLeft == null || !node.nLeft.isInMemory()) {
            node.nLeft         = findNode(store, node.iLeft);
            node.nLeft.nParent = node;
        }

        return node.nLeft;
    }

    NodeAVL getRight(PersistentStore store) {

        NodeAVLDisk node = this;
        RowAVLDisk  row  = this.row;

        if (!row.isInMemory()) {
            row  = (RowAVLDisk) store.get(this.row, false);
            node = (NodeAVLDisk) row.getNode(iId);
        }

        if (node.iRight == NO_POS) {
            return null;
        }

        if (node.nRight == null || !node.nRight.isInMemory()) {
            node.nRight         = findNode(store, node.iRight);
            node.nRight.nParent = node;
        }

        return node.nRight;
    }

    NodeAVL getParent(PersistentStore store) {

        NodeAVLDisk node = this;
        RowAVLDisk  row  = this.row;

        if (!row.isInMemory()) {
            row  = (RowAVLDisk) store.get(this.row, false);
            node = (NodeAVLDisk) row.getNode(iId);
        }

        if (node.iParent == NO_POS) {
            return null;
        }

        if (node.nParent == null || !node.nParent.isInMemory()) {
            node.nParent = findNode(store, iParent);
        }

        return node.nParent;
    }

    public int getBalance(PersistentStore store) {

        NodeAVLDisk node = this;
        RowAVLDisk  row  = this.row;

        if (!row.isInMemory()) {
            row  = (RowAVLDisk) store.get(this.row, false);
            node = (NodeAVLDisk) row.getNode(iId);
        }

        return node.iBalance;
    }

    boolean isRoot(PersistentStore store) {

        NodeAVLDisk node = this;
        RowAVLDisk  row  = this.row;

        if (!row.isInMemory()) {
            row  = (RowAVLDisk) store.get(this.row, false);
            node = (NodeAVLDisk) row.getNode(iId);
        }

        return node.iParent == NO_POS;
    }

    boolean isFromLeft(PersistentStore store) {

        NodeAVLDisk node = this;
        RowAVLDisk  row  = this.row;

        if (!row.isInMemory()) {
            row  = (RowAVLDisk) store.get(this.row, false);
            node = (NodeAVLDisk) row.getNode(iId);
        }

        if (node.iParent == NO_POS) {
            return true;
        }

        if (node.nParent == null || !node.nParent.isInMemory()) {
            node.nParent = findNode(store, iParent);
        }

        return getPos() == ((NodeAVLDisk) node.nParent).iLeft;
    }

    public NodeAVL child(PersistentStore store, boolean isleft) {
        return isleft ? getLeft(store)
                      : getRight(store);
    }

    NodeAVL setParent(PersistentStore store, NodeAVL n) {

        NodeAVLDisk node = this;
        RowAVLDisk  row  = this.row;

        if (!row.keepInMemory(true)) {
            row  = (RowAVLDisk) store.get(this.row, true);
            node = (NodeAVLDisk) row.getNode(iId);
        }

        if (!row.isInMemory()) {
            row.keepInMemory(false);

            throw Error.runtimeError(ErrorCode.U_S0500, "NodeAVLDisk");
        }

        row.setNodesChanged();

        node.iParent = n == null ? NO_POS
                                 : n.getPos();

        if (n != null && !n.isInMemory()) {
            n = findNode(store, n.getPos());
        }

        node.nParent = (NodeAVLDisk) n;

        row.keepInMemory(false);

        return node;
    }

    public NodeAVL setBalance(PersistentStore store, int b) {

        NodeAVLDisk node = this;
        RowAVLDisk  row  = this.row;

        if (!row.keepInMemory(true)) {
            row  = (RowAVLDisk) store.get(this.row, true);
            node = (NodeAVLDisk) row.getNode(iId);
        }

        if (!row.isInMemory()) {
            throw Error.runtimeError(ErrorCode.U_S0500, "NodeAVLDisk");
        }

        row.setNodesChanged();

        node.iBalance = b;

        row.keepInMemory(false);

        return node;
    }

    NodeAVL setLeft(PersistentStore store, NodeAVL n) {

        NodeAVLDisk node = this;
        RowAVLDisk  row  = this.row;

        if (!row.keepInMemory(true)) {
            row  = (RowAVLDisk) store.get(this.row, true);
            node = (NodeAVLDisk) row.getNode(iId);
        }

        if (!row.isInMemory()) {
            throw Error.runtimeError(ErrorCode.U_S0500, "NodeAVLDisk");
        }

        row.setNodesChanged();

        node.iLeft = n == null ? NO_POS
                               : n.getPos();

        if (n != null && !n.isInMemory()) {
            n = findNode(store, n.getPos());
        }

        node.nLeft = (NodeAVLDisk) n;

        row.keepInMemory(false);

        return node;
    }

    NodeAVL setRight(PersistentStore store, NodeAVL n) {

        NodeAVLDisk node = this;
        RowAVLDisk  row  = this.row;

        if (!row.keepInMemory(true)) {
            row  = (RowAVLDisk) store.get(this.row, true);
            node = (NodeAVLDisk) row.getNode(iId);
        }

        if (!row.isInMemory()) {
            throw Error.runtimeError(ErrorCode.U_S0500, "NodeAVLDisk");
        }

        row.setNodesChanged();

        node.iRight = n == null ? NO_POS
                                : n.getPos();

        if (n != null && !n.isInMemory()) {
            n = findNode(store, n.getPos());
        }

        node.nRight = (NodeAVLDisk) n;

        row.keepInMemory(false);

        return node;
    }

    public NodeAVL set(PersistentStore store, boolean isLeft, NodeAVL n) {

        NodeAVL x;

        if (isLeft) {
            x = setLeft(store, n);
        } else {
            x = setRight(store, n);
        }

        if (n != null) {
            n.setParent(store, this);
        }

        return x;
    }

    public void replace(PersistentStore store, Index index, NodeAVL n) {

        NodeAVLDisk node = this;
        RowAVLDisk  row  = this.row;

        if (!row.keepInMemory(true)) {
            row  = (RowAVLDisk) store.get(this.row, true);
            node = (NodeAVLDisk) row.getNode(iId);
        }

        if (node.iParent == NO_POS) {
            if (n != null) {
                n = n.setParent(store, null);
            }

            store.setAccessor(index, n);
        } else {
            boolean isFromLeft = node.isFromLeft(store);

            node.getParent(store).set(store, isFromLeft, n);
        }

        row.keepInMemory(false);
    }

    boolean equals(NodeAVL n) {

        if (n instanceof NodeAVLDisk) {
            return this == n || (getPos() == ((NodeAVLDisk) n).getPos());
        }

        return false;
    }

    public int getRealSize(RowOutputInterface out) {
        return NodeAVLDisk.SIZE_IN_BYTE;
    }

    public void setInMemory(boolean in) {

        if (!in) {
            if (nLeft != null) {
                nLeft.nParent = null;
            }

            if (nRight != null) {
                nRight.nParent = null;
            }

            if (nParent != null) {
                if (iData == ((NodeAVLDisk) nParent).iLeft) {
                    nParent.nLeft = null;
                } else {
                    nParent.nRight = null;
                }
            }

            nLeft = nRight = nParent = null;
        }
    }

    public void write(RowOutputInterface out) {

        out.writeInt(iBalance);
        out.writeInt((iLeft == NO_POS) ? 0
                                       : iLeft);
        out.writeInt((iRight == NO_POS) ? 0
                                        : iRight);
        out.writeInt((iParent == NO_POS) ? 0
                                         : iParent);
    }

    public void write(RowOutputInterface out, IntLookup lookup) {

        out.writeInt(iBalance);
        writeTranslatePointer(iLeft, out, lookup);
        writeTranslatePointer(iRight, out, lookup);
        writeTranslatePointer(iParent, out, lookup);
    }

    private static void writeTranslatePointer(int pointer,
            RowOutputInterface out, IntLookup lookup) {

        int newPointer = 0;

        if (pointer != NodeAVL.NO_POS) {
            newPointer = lookup.lookup(pointer);
        }

        out.writeInt(newPointer);
    }

    public void restore() {}

    public void destroy() {}

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

    public boolean keepInMemory(boolean keep) {
        return false;
    }
}
