package org.hsqldb.rowio;
import java.math.BigDecimal;
import org.hsqldb.ColumnSchema;
import org.hsqldb.Row;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.HashMappedList;
import org.hsqldb.lib.HsqlByteArrayOutputStream;
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
abstract class RowOutputBase extends HsqlByteArrayOutputStream
implements RowOutputInterface {
    public static final int CACHED_ROW_160 = 0;
    public static final int CACHED_ROW_170 = 1;
    protected boolean skipSystemId = false;
    public RowOutputBase() {
        super();
    }
    public RowOutputBase(int initialSize) {
        super(initialSize);
    }
    public RowOutputBase(byte[] buffer) {
        super(buffer);
    }
    public abstract void writeEnd();
    public abstract void writeSize(int size);
    public abstract void writeType(int type);
    public abstract void writeIntData(int i, int position);
    public abstract void writeString(String s);
    protected void writeFieldPrefix() {}
    protected abstract void writeFieldType(Type type);
    protected abstract void writeNull(Type type);
    protected abstract void writeChar(String s, Type t);
    protected abstract void writeSmallint(Number o);
    protected abstract void writeInteger(Number o);
    protected abstract void writeBigint(Number o);
    protected abstract void writeReal(Double o);
    protected abstract void writeDecimal(BigDecimal o, Type type);
    protected abstract void writeBoolean(Boolean o);
    protected abstract void writeDate(TimestampData o, Type type);
    protected abstract void writeTime(TimeData o, Type type);
    protected abstract void writeTimestamp(TimestampData o, Type type);
    protected abstract void writeYearMonthInterval(IntervalMonthData o,
            Type type);
    protected abstract void writeDaySecondInterval(IntervalSecondData o,
            Type type);
    protected abstract void writeOther(JavaObjectData o);
    protected abstract void writeBit(BinaryData o);
    protected abstract void writeBinary(BinaryData o);
    protected abstract void writeClob(ClobData o, Type type);
    protected abstract void writeBlob(BlobData o, Type type);
    protected abstract void writeArray(Object[] o, Type type);
    public void writeData(Row row, Type[] types) {
        writeData(types.length, types, row.getData(), null, null);
    }
    public void writeData(int l, Type[] types, Object[] data,
                          HashMappedList cols, int[] primaryKeys) {
        boolean hasPK = primaryKeys != null && primaryKeys.length != 0;
        int     limit = hasPK ? primaryKeys.length
                              : l;
        for (int i = 0; i < limit; i++) {
            int    j = hasPK ? primaryKeys[i]
                             : i;
            Object o = data[j];
            Type   t = types[j];
            if (cols != null) {
                ColumnSchema col = (ColumnSchema) cols.get(j);
                writeFieldPrefix();
                writeString(col.getName().statementName);
            }
            writeData(t, o);
        }
    }
    public void writeData(Type t, Object o) {
        if (o == null) {
            writeNull(t);
            return;
        }
        writeFieldType(t);
        switch (t.typeCode) {
            case Types.SQL_ALL_TYPES :
                break;
            case Types.SQL_CHAR :
            case Types.SQL_VARCHAR :
            case Types.VARCHAR_IGNORECASE :
                writeChar((String) o, t);
                break;
            case Types.TINYINT :
            case Types.SQL_SMALLINT :
                writeSmallint((Number) o);
                break;
            case Types.SQL_INTEGER :
                writeInteger((Number) o);
                break;
            case Types.SQL_BIGINT :
                writeBigint((Number) o);
                break;
            case Types.SQL_REAL :
            case Types.SQL_FLOAT :
            case Types.SQL_DOUBLE :
                writeReal((Double) o);
                break;
            case Types.SQL_NUMERIC :
            case Types.SQL_DECIMAL :
                writeDecimal((BigDecimal) o, t);
                break;
            case Types.SQL_BOOLEAN :
                writeBoolean((Boolean) o);
                break;
            case Types.SQL_DATE :
                writeDate((TimestampData) o, t);
                break;
            case Types.SQL_TIME :
            case Types.SQL_TIME_WITH_TIME_ZONE :
                writeTime((TimeData) o, t);
                break;
            case Types.SQL_TIMESTAMP :
            case Types.SQL_TIMESTAMP_WITH_TIME_ZONE :
                writeTimestamp((TimestampData) o, t);
                break;
            case Types.SQL_INTERVAL_YEAR :
            case Types.SQL_INTERVAL_YEAR_TO_MONTH :
            case Types.SQL_INTERVAL_MONTH :
                writeYearMonthInterval((IntervalMonthData) o, t);
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
                writeDaySecondInterval((IntervalSecondData) o, t);
                break;
            case Types.OTHER :
                writeOther((JavaObjectData) o);
                break;
            case Types.SQL_BLOB :
                writeBlob((BlobData) o, t);
                break;
            case Types.SQL_CLOB :
                writeClob((ClobData) o, t);
                break;
            case Types.SQL_ARRAY :
                writeArray((Object[]) o, t);
                break;
            case Types.SQL_BINARY :
            case Types.SQL_VARBINARY :
                writeBinary((BinaryData) o);
                break;
            case Types.SQL_BIT :
            case Types.SQL_BIT_VARYING :
                writeBit((BinaryData) o);
                break;
            default :
                throw Error.runtimeError(ErrorCode.U_S0500,
                                         "RowOutputBase - "
                                         + t.getNameString());
        }
    }
    public HsqlByteArrayOutputStream getOutputStream() {
        return this;
    }
    public abstract RowOutputInterface duplicate();
}