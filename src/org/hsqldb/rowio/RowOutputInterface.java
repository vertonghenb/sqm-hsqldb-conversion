package org.hsqldb.rowio;
import org.hsqldb.Row;
import org.hsqldb.lib.HashMappedList;
import org.hsqldb.lib.HsqlByteArrayOutputStream;
import org.hsqldb.types.Type;
public interface RowOutputInterface extends Cloneable {
    void writeEnd();
    void writeSize(int size);
    void writeType(int type);
    void writeString(String value);
    void writeByte(int i);
    void writeShort(int i);
    void writeInt(int i);
    void writeIntData(int i, int position);
    void writeLong(long i);
    void writeData(Row row, Type[] types);
    void writeData(int l, Type[] types, Object[] data, HashMappedList cols,
                   int[] primarykeys);
    int getSize(Row row);
    int getStorageSize(int size);
    HsqlByteArrayOutputStream getOutputStream();
    public void setBuffer(byte[] mainBuffer);
    void reset();
    int size();
    public RowOutputInterface duplicate();
}