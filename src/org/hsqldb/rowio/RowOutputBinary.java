package org.hsqldb.rowio;
import java.math.BigDecimal;
import java.math.BigInteger;
import org.hsqldb.Row;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.StringConverter;
import org.hsqldb.lib.java.JavaSystem;
import org.hsqldb.types.BinaryData;
import org.hsqldb.types.BlobData;
import org.hsqldb.types.ClobData;
import org.hsqldb.types.IntervalMonthData;
import org.hsqldb.types.IntervalSecondData;
import org.hsqldb.types.JavaObjectData;
import org.hsqldb.types.TimeData;
import org.hsqldb.types.TimestampData;
import org.hsqldb.types.Type;
import org.hsqldb.types.Types;
public class RowOutputBinary extends RowOutputBase {
    protected static final int INT_STORE_SIZE = 4;
    int                        storageSize;
    final int                  scale;    
    final int                  mask;
    public RowOutputBinary(int initialSize, int scale) {
        super(initialSize);
        this.scale = scale;
        this.mask  = ~(scale - 1);
    }
    public RowOutputBinary(byte[] buffer) {
        super(buffer);
        scale     = 1;
        this.mask = ~(scale - 1);
    }
    public void writeIntData(int i, int position) {
        int temp = count;
        count = position;
        writeInt(i);
        if (count < temp) {
            count = temp;
        }
    }
    public void writeData(Row row, Type[] types) {
        super.writeData(row, types);
    }
    public void writeEnd() {
        if (count > storageSize) {
            Error.runtimeError(ErrorCode.U_S0500, "RowOutputBinary");
        }
        for (; count < storageSize; ) {
            this.write(0);
        }
    }
    public void writeSize(int size) {
        storageSize = size;
        writeInt(size);
    }
    public void writeType(int type) {
        writeShort(type);
    }
    public void writeString(String s) {
        int temp = count;
        writeInt(0);
        if (s != null && s.length() != 0) {
            StringConverter.stringToUTFBytes(s, this);
            writeIntData(count - temp - INT_STORE_SIZE, temp);
        }
    }
    public int getSize(Row row) {
        Object[] data  = row.getData();
        Type[]   types = row.getTable().getColumnTypes();
        int      cols  = row.getTable().getDataColumnCount();
        return INT_STORE_SIZE + getSize(data, cols, types);
    }
    public int getStorageSize(int size) {
        return (size + scale - 1) & mask;
    }
    public void writeFieldType(Type type) {
        write(1);
    }
    public void writeNull(Type type) {
        write(0);
    }
    protected void writeChar(String s, Type t) {
        writeString(s);
    }
    protected void writeSmallint(Number o) {
        writeShort(o.intValue());
    }
    protected void writeInteger(Number o) {
        writeInt(o.intValue());
    }
    protected void writeBigint(Number o) {
        writeLong(o.longValue());
    }
    protected void writeReal(Double o) {
        writeLong(Double.doubleToLongBits((o.doubleValue())));
    }
    protected void writeDecimal(BigDecimal o, Type type) {
        int        scale   = o.scale();
        BigInteger bigint  = JavaSystem.unscaledValue(o);
        byte[]     bytearr = bigint.toByteArray();
        writeByteArray(bytearr);
        writeInt(scale);
    }
    protected void writeBoolean(Boolean o) {
        write(o.booleanValue() ? 1
                               : 0);
    }
    protected void writeDate(TimestampData o, Type type) {
        writeLong(o.getSeconds());
    }
    protected void writeTime(TimeData o, Type type) {
        writeInt(o.getSeconds());
        writeInt(o.getNanos());
        if (type.typeCode == Types.SQL_TIME_WITH_TIME_ZONE) {
            writeInt(o.getZone());
        }
    }
    protected void writeTimestamp(TimestampData o, Type type) {
        writeLong(o.getSeconds());
        writeInt(o.getNanos());
        if (type.typeCode == Types.SQL_TIMESTAMP_WITH_TIME_ZONE) {
            writeInt(o.getZone());
        }
    }
    protected void writeYearMonthInterval(IntervalMonthData o, Type type) {
        writeLong(o.units);
    }
    protected void writeDaySecondInterval(IntervalSecondData o, Type type) {
        writeLong(o.getSeconds());
        writeInt(o.getNanos());
    }
    protected void writeOther(JavaObjectData o) {
        writeByteArray(o.getBytes());
    }
    protected void writeBit(BinaryData o) {
        writeInt((int) o.bitLength(null));
        write(o.getBytes(), 0, o.getBytes().length);
    }
    protected void writeBinary(BinaryData o) {
        writeByteArray(o.getBytes());
    }
    protected void writeClob(ClobData o, Type type) {
        writeLong(o.getId());
    }
    protected void writeBlob(BlobData o, Type type) {
        writeLong(o.getId());
    }
    protected void writeArray(Object[] o, Type type) {
        type = type.collectionBaseType();
        writeInt(o.length);
        for (int i = 0; i < o.length; i++) {
            writeData(type, o[i]);
        }
    }
    public void writeArray(int[] o) {
        writeInt(o.length);
        for (int i = 0; i < o.length; i++) {
            write(1);
            writeInt(o[i]);
        }
    }
    public void writeByteArray(byte[] b) {
        writeInt(b.length);
        write(b, 0, b.length);
    }
    public void writeCharArray(char[] c) {
        writeInt(c.length);
        write(c, 0, c.length);
    }
    public int getSize(int[] array) {
        return 4 + array.length * 5;
    }
    public int getSize(Object[] data, int l, Type[] types) {
        int s = 0;
        for (int i = 0; i < l; i++) {
            Object o = data[i];
            s += getSize(o, types[i]);
        }
        return s;
    }
    private int getSize(Object o, Type type) {
        int s = 1;    
        if (o == null) {
            return s;
        }
        switch (type.typeCode) {
            case Types.SQL_ALL_TYPES :
                break;
            case Types.SQL_CHAR :
            case Types.SQL_VARCHAR :
            case Types.VARCHAR_IGNORECASE :
                s += INT_STORE_SIZE;
                s += StringConverter.getUTFSize((String) o);
                break;
            case Types.TINYINT :
            case Types.SQL_SMALLINT :
                s += 2;
                break;
            case Types.SQL_INTEGER :
                s += 4;
                break;
            case Types.SQL_BIGINT :
            case Types.SQL_REAL :
            case Types.SQL_FLOAT :
            case Types.SQL_DOUBLE :
                s += 8;
                break;
            case Types.SQL_NUMERIC :
            case Types.SQL_DECIMAL :
                s += 8;
                BigDecimal bigdecimal = (BigDecimal) o;
                BigInteger bigint     = JavaSystem.unscaledValue(bigdecimal);
                s += bigint.toByteArray().length;
                break;
            case Types.SQL_BOOLEAN :
                s += 1;
                break;
            case Types.SQL_DATE :
                s += 8;
                break;
            case Types.SQL_TIME :
                s += 8;
                break;
            case Types.SQL_TIME_WITH_TIME_ZONE :
                s += 12;
                break;
            case Types.SQL_TIMESTAMP :
                s += 12;
                break;
            case Types.SQL_TIMESTAMP_WITH_TIME_ZONE :
                s += 16;
                break;
            case Types.SQL_INTERVAL_YEAR :
            case Types.SQL_INTERVAL_YEAR_TO_MONTH :
            case Types.SQL_INTERVAL_MONTH :
                s += 8;
                break;
            case Types.SQL_INTERVAL_DAY :
            case Types.SQL_INTERVAL_DAY_TO_HOUR :
            case Types.SQL_INTERVAL_DAY_TO_MINUTE :
            case Types.SQL_INTERVAL_DAY_TO_SECOND :
            case Types.SQL_INTERVAL_HOUR :
            case Types.SQL_INTERVAL_HOUR_TO_MINUTE :
            case Types.SQL_INTERVAL_HOUR_TO_SECOND :
            case Types.SQL_INTERVAL_MINUTE :
            case Types.SQL_INTERVAL_MINUTE_TO_SECOND :
            case Types.SQL_INTERVAL_SECOND :
                s += 12;
                break;
            case Types.SQL_BINARY :
            case Types.SQL_VARBINARY :
                s += INT_STORE_SIZE;
                s += ((BinaryData) o).length(null);
                break;
            case Types.SQL_BIT :
            case Types.SQL_BIT_VARYING :
                s += INT_STORE_SIZE;
                s += ((BinaryData) o).length(null);
                break;
            case Types.SQL_CLOB :
            case Types.SQL_BLOB :
                s += 8;
                break;
            case Types.SQL_ARRAY : {
                s += 4;
                Object[] array = (Object[]) o;
                type = type.collectionBaseType();
                for (int i = 0; i < array.length; i++) {
                    s += getSize(array[i], type);
                }
                break;
            }
            case Types.OTHER :
                JavaObjectData jo = (JavaObjectData) o;
                s += INT_STORE_SIZE;
                s += jo.getBytesLength();
                break;
            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "RowOutputBinary");
        }
        return s;
    }
    public void ensureRoom(int extra) {
        super.ensureRoom(extra);
    }
    public void reset() {
        super.reset();
        storageSize = 0;
    }
    public void reset(int newSize) {
        super.reset(newSize);
        storageSize = 0;
    }
    public void setBuffer(byte[] buffer) {
        this.buffer = buffer;
        reset();
    }
    public RowOutputInterface duplicate() {
        return new RowOutputBinary(128, this.scale);
    }
}