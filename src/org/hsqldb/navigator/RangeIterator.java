package org.hsqldb.navigator;
import org.hsqldb.Row;
public interface RangeIterator extends RowIterator {
    boolean isBeforeFirst();
    boolean next();
    Row getCurrentRow();
    Object[] getCurrent();
    Object getCurrent(int i);
    void setCurrent(Object[] data);
    Object getRowidObject();
    void remove();
    void reset();
    int getRangePosition();
}