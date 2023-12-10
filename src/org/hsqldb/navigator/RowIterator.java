package org.hsqldb.navigator;
import org.hsqldb.Row;
public interface RowIterator {
    Row getNextRow();
    Object[] getNext();
    boolean hasNext();
    void remove();
    boolean setRowColumns(boolean[] columns);
    void release();
    long getRowId();
}