package org.hsqldb.navigator;
import java.io.IOException;
import org.hsqldb.Row;
import org.hsqldb.SessionInterface;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.result.ResultMetaData;
import org.hsqldb.rowio.RowInputInterface;
import org.hsqldb.rowio.RowOutputInterface;
public abstract class RowSetNavigator implements RangeIterator {
    public RowSetNavigator() {}
    SessionInterface session;
    long             id;
    int              size;
    int              mode;
    boolean          isIterator;
    int              currentPos = -1;
    int              rangePosition;
    public void setId(long id) {
        this.id = id;
    }
    public long getId() {
        return id;
    }
    public abstract Object[] getCurrent();
    public Object getCurrent(int i) {
        Object[] current = getCurrent();
        if (current == null) {
            return null;
        }
        return current[i];
    }
    public void setCurrent(Object[] data) {}
    public long getRowid() {
        return 0;
    }
    public Object getRowidObject() {
        return null;
    }
    public abstract Row getCurrentRow();
    public abstract void add(Object[] data);
    public abstract boolean addRow(Row row);
    public abstract void remove();
    public void reset() {
        currentPos = -1;
    }
    public abstract void clear();
    public abstract void release();
    public void setSession(SessionInterface session) {
        this.session = session;
    }
    public SessionInterface getSession() {
        return session;
    }
    public int getSize() {
        return size;
    }
    public boolean isEmpty() {
        return size == 0;
    }
    public Object[] getNext() {
        return next() ? getCurrent()
                      : null;
    }
    public boolean next() {
        if (hasNext()) {
            currentPos++;
            return true;
        } else if (size != 0) {
            currentPos = size;
        }
        return false;
    }
    public boolean hasNext() {
        return currentPos < size - 1;
    }
    public Row getNextRow() {
        throw Error.runtimeError(ErrorCode.U_S0500, "RowSetNavigator");
    }
    public boolean setRowColumns(boolean[] columns) {
        throw Error.runtimeError(ErrorCode.U_S0500, "RowSetNavigator");
    }
    public long getRowId() {
        throw Error.runtimeError(ErrorCode.U_S0500, "RowSetNavigator");
    }
    public boolean beforeFirst() {
        reset();
        currentPos = -1;
        return true;
    }
    public boolean afterLast() {
        if (size == 0) {
            return false;
        }
        reset();
        currentPos = size;
        return true;
    }
    public boolean first() {
        beforeFirst();
        return next();
    }
    public boolean last() {
        if (size == 0) {
            return false;
        }
        if (isAfterLast()) {
            beforeFirst();
        }
        while (hasNext()) {
            next();
        }
        return true;
    }
    public int getRowNumber() {
        return currentPos;
    }
    public boolean absolute(int position) {
        if (position < 0) {
            position += size;
        }
        if (position < 0) {
            beforeFirst();
            return false;
        }
        if (position >= size) {
            afterLast();
            return false;
        }
        if (size == 0) {
            return false;
        }
        if (position < currentPos) {
            beforeFirst();
        }
        while (position > currentPos) {
            next();
        }
        return true;
    }
    public boolean relative(int rows) {
        int position = currentPos + rows;
        if (position < 0) {
            beforeFirst();
            return false;
        }
        return absolute(position);
    }
    public boolean previous() {
        return relative(-1);
    }
    public boolean isFirst() {
        return size > 0 && currentPos == 0;
    }
    public boolean isLast() {
        return size > 0 && currentPos == size - 1;
    }
    public boolean isBeforeFirst() {
        return size > 0 && currentPos == -1;
    }
    public boolean isAfterLast() {
        return size > 0 && currentPos == size;
    }
    public void writeSimple(RowOutputInterface out,
                            ResultMetaData meta) throws IOException {
        throw Error.runtimeError(ErrorCode.U_S0500, "RowSetNavigator");
    }
    public void readSimple(RowInputInterface in,
                           ResultMetaData meta) throws IOException {
        throw Error.runtimeError(ErrorCode.U_S0500, "RowSetNavigator");
    }
    public abstract void write(RowOutputInterface out,
                               ResultMetaData meta) throws IOException;
    public abstract void read(RowInputInterface in,
                              ResultMetaData meta) throws IOException;
    public boolean isMemory() {
        return true;
    }
    public int getRangePosition() {
        return rangePosition;
    }
}