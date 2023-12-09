


package org.hsqldb;

import org.hsqldb.index.NodeAVL;
import org.hsqldb.lib.java.JavaSystem;
import org.hsqldb.persist.PersistentStore;






public class RowAVL extends Row {

    public NodeAVL nPrimaryNode;

    
    protected RowAVL(TableBase table, Object[] data) {
        super(table, data);
    }

    
    public RowAVL(TableBase table, Object[] data, int position,
                  PersistentStore store) {

        super(table, data);

        this.position = position;

        setNewNodes(store);
    }

    public void setNewNodes(PersistentStore store) {

        int indexCount = store.getAccessorKeys().length;

        nPrimaryNode = new NodeAVL(this);

        NodeAVL n = nPrimaryNode;

        for (int i = 1; i < indexCount; i++) {
            n.nNext = new NodeAVL(this);
            n       = n.nNext;
        }
    }

    
    public NodeAVL getNode(int index) {

        NodeAVL n = nPrimaryNode;

        while (index-- > 0) {
            n = n.nNext;
        }

        return n;
    }

    
    NodeAVL getNextNode(NodeAVL n) {

        if (n == null) {
            n = nPrimaryNode;
        } else {
            n = n.nNext;
        }

        return n;
    }

    public NodeAVL insertNode(int index) {

        NodeAVL backnode = getNode(index - 1);
        NodeAVL newnode  = new NodeAVL(this);

        newnode.nNext  = backnode.nNext;
        backnode.nNext = newnode;

        return newnode;
    }

    public void clearNonPrimaryNodes() {

        NodeAVL n = nPrimaryNode.nNext;

        while (n != null) {
            n.delete();

            n.iBalance = 0;
            n          = n.nNext;
        }
    }

    public void delete(PersistentStore store) {

        NodeAVL n = nPrimaryNode;

        while (n != null) {
            n.delete();

            n = n.nNext;
        }
    }

    public void restore() {}

    
    public void destroy() {

        JavaSystem.memoryRecords++;

        clearNonPrimaryNodes();

        NodeAVL n = nPrimaryNode;

        while (n != null) {
            NodeAVL last = n;

            n          = n.nNext;
            last.nNext = null;
        }
    }
}
