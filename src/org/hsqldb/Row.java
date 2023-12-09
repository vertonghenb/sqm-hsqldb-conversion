


package org.hsqldb;

import org.hsqldb.lib.IntLookup;
import org.hsqldb.persist.CachedObject;
import org.hsqldb.persist.PersistentStore;
import org.hsqldb.rowio.RowOutputInterface;


public class Row implements CachedObject {

    int                       position;
    Object[]                  rowData;
    public volatile RowAction rowAction;
    protected TableBase       table;

    public RowAction getAction() {
        return rowAction;
    }

    
    public Row(TableBase table, Object[] data) {
        this.table   = table;
        this.rowData = data;
    }

    
    public Object[] getData() {
        return rowData;
    }

    boolean isDeleted(Session session, PersistentStore store) {

        Row       row    = (Row) store.get(this, false);
        RowAction action = row.rowAction;

        if (action == null) {
            return false;
        }

        return !action.canRead(session, TransactionManager.ACTION_READ);
    }

    public void setChanged(boolean changed) {}

    public void setStorageSize(int size) {}

    public int getStorageSize() {
        return 0;
    }

    public boolean isMemory() {
        return true;
    }

    public void updateAccessCount(int count) {}

    public int getAccessCount() {
        return 0;
    }

    public int getPos() {
        return position;
    }

    public long getId() {
        return ((long) table.getId() << 32) + (long) position;
    }

    public void setPos(int pos) {
        position = pos;
    }

    public boolean hasChanged() {
        return false;
    }

    public boolean isKeepInMemory() {
        return true;
    }

    public boolean keepInMemory(boolean keep) {
        return true;
    }

    public boolean isInMemory() {
        return true;
    }

    public void setInMemory(boolean in) {}

    public void delete(PersistentStore store) {}

    public void restore() {}

    public void destroy() {}

    public int getRealSize(RowOutputInterface out) {
        return 0;
    }

    public TableBase getTable() {
        return table;
    }

    public void write(RowOutputInterface out) {}

    public void write(RowOutputInterface out, IntLookup lookup) {}

    
    public boolean equals(Object obj) {

        if (obj == this) {
            return true;
        }

        if (obj instanceof Row) {
            return ((Row) obj).position == position;
        }

        return false;
    }

    
    public int hashCode() {
        return position;
    }
}
