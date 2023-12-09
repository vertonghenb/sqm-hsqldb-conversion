


package org.hsqldb.navigator;

import java.io.IOException;

import org.hsqldb.HsqlException;
import org.hsqldb.Row;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.result.ResultMetaData;
import org.hsqldb.rowio.RowInputInterface;
import org.hsqldb.rowio.RowOutputInterface;


public class RowSetNavigatorClient extends RowSetNavigator {

    public static final Object[][] emptyTable = new Object[0][];

    
    int currentOffset;
    int baseBlockSize;

    
    Object[][] table;

    
    public RowSetNavigatorClient() {
        table = emptyTable;
    }

    public RowSetNavigatorClient(int blockSize) {
        table = new Object[blockSize][];
    }

    public RowSetNavigatorClient(RowSetNavigator source, int offset,
                                 int blockSize) {

        this.size          = source.size;
        this.baseBlockSize = blockSize;
        this.currentOffset = offset;
        table              = new Object[blockSize][];

        source.absolute(offset);

        for (int count = 0; count < blockSize; count++) {
            table[count] = source.getCurrent();

            source.next();
        }

        source.beforeFirst();
    }

    
    public void setData(Object[][] table) {
        this.table = table;
        this.size  = table.length;
    }

    public void setData(int index, Object[] data) {
        table[index] = data;
    }

    public Object[] getData(int index) {
        return table[index];
    }

    
    public Object[] getCurrent() {

        if (currentPos < 0 || currentPos >= size) {
            return null;
        }

        if (currentPos == currentOffset + table.length) {
            getBlock(currentOffset + table.length);
        }

        return table[currentPos - currentOffset];
    }

    public Row getCurrentRow() {
        throw Error.runtimeError(ErrorCode.U_S0500, "RowSetNavigatorClient");
    }

    public void remove() {
        throw Error.runtimeError(ErrorCode.U_S0500, "RowSetNavigatorClient");
    }

    public void add(Object[] data) {

        ensureCapacity();

        table[size] = data;

        size++;
    }

    public boolean addRow(Row row) {
        throw Error.runtimeError(ErrorCode.U_S0500, "RowSetNavigatorClient");
    }

    public void clear() {
        setData(emptyTable);
        reset();
    }

    public void release() {
        setData(emptyTable);
        reset();
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

        currentPos = position;

        return true;
    }

    public void close() {

        if (session != null) {
            if (currentOffset == 0 && table.length == size) {}
            else {
                session.closeNavigator(id);
            }
        }
    }

    public void readSimple(RowInputInterface in,
                           ResultMetaData meta) throws IOException {

        size = in.readInt();

        if (table.length < size) {
            table = new Object[size][];
        }

        for (int i = 0; i < size; i++) {
            table[i] = in.readData(meta.columnTypes);
        }
    }

    public void writeSimple(RowOutputInterface out,
                            ResultMetaData meta) throws IOException {

        out.writeInt(size);

        for (int i = 0; i < size; i++) {
            Object[] data = table[i];

            out.writeData(meta.getColumnCount(), meta.columnTypes, data, null,
                          null);
        }
    }

    public void read(RowInputInterface in,
                     ResultMetaData meta) throws IOException {

        id            = in.readLong();
        size          = in.readInt();
        currentOffset = in.readInt();
        baseBlockSize = in.readInt();

        if (table.length < baseBlockSize) {
            table = new Object[baseBlockSize][];
        }

        for (int i = 0; i < baseBlockSize; i++) {
            table[i] = in.readData(meta.columnTypes);
        }
    }

    public void write(RowOutputInterface out,
                      ResultMetaData meta) throws HsqlException, IOException {

        int limit = size - currentOffset;

        if (limit > table.length) {
            limit = table.length;
        }

        out.writeLong(id);
        out.writeInt(size);
        out.writeInt(currentOffset);
        out.writeInt(limit);

        for (int i = 0; i < limit; i++) {
            Object[] data = table[i];

            out.writeData(meta.getColumnCount(), meta.columnTypes, data, null,
                          null);
        }
    }

    
    void getBlock(int offset) {

        try {
            RowSetNavigatorClient source = session.getRows(id, offset,
                baseBlockSize);

            table         = source.table;
            currentOffset = source.currentOffset;
        } catch (HsqlException e) {}
    }

    private void ensureCapacity() {

        if (size == table.length) {
            int        newSize  = size == 0 ? 4
                                            : size * 2;
            Object[][] newTable = new Object[newSize][];

            System.arraycopy(table, 0, newTable, 0, size);

            table = newTable;
        }
    }
}
