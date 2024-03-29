package org.hsqldb.rowio;
import java.io.IOException;
import java.math.BigDecimal;
import org.hsqldb.HsqlException;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.HsqlByteArrayInputStream;
import org.hsqldb.types.BinaryData;
import org.hsqldb.types.BlobData;
import org.hsqldb.types.ClobData;
import org.hsqldb.types.IntervalMonthData;
import org.hsqldb.types.IntervalSecondData;
import org.hsqldb.types.TimeData;
import org.hsqldb.types.TimestampData;
import org.hsqldb.types.Type;
import org.hsqldb.types.Types;
abstract class RowInputBase extends HsqlByteArrayInputStream {
    static final int NO_POS = -1;
    protected int filePos = NO_POS;
    protected int size;
    RowInputBase() {
        this(new byte[4]);
    }
    RowInputBase(byte[] buf) {
        super(buf);
        size = buf.length;
    }
    public int getPos() {
        if (filePos == NO_POS) {
        }
        return filePos;
    }
    public int getSize() {
        return size;
    }
    public abstract int readType() throws IOException;
    public abstract String readString() throws IOException;
    protected abstract boolean readNull() throws IOException;
    protected abstract String readChar(Type type) throws IOException;
    protected abstract Integer readSmallint() throws IOException;
    protected abstract Integer readInteger() throws IOException;
    protected abstract Long readBigint() throws IOException;
    protected abstract Double readReal() throws IOException;
    protected abstract BigDecimal readDecimal(Type type) throws IOException;
    protected abstract Boolean readBoole() throws IOException;
    protected abstract TimeData readTime(Type type) throws IOException;
    protected abstract TimestampData readDate(Type type) throws IOException;
    protected abstract TimestampData readTimestamp(Type type)
    throws IOException;
    protected abstract IntervalMonthData readYearMonthInterval(Type type)
    throws IOException;
    protected abstract IntervalSecondData readDaySecondInterval(Type type)
    throws IOException;
    protected abstract Object readOther() throws IOException;
    protected abstract BinaryData readBinary()
    throws IOException, HsqlException;
    protected abstract BinaryData readBit() throws IOException;
    protected abstract ClobData readClob() throws IOException;
    protected abstract BlobData readBlob() throws IOException;
    protected abstract Object[] readArray(Type type) throws IOException;
    public Object[] readData(Type[] colTypes) throws IOException {
        int      l    = colTypes.length;
        Object[] data = new Object[l];
        for (int i = 0; i < l; i++) {
            Type type = colTypes[i];
            data[i] = readData(type);
        }
        return data;
    }
    public Object readData(Type type) throws IOException {
        Object o = null;
        if (readNull()) {
            return null;
        }
        switch (type.typeCode) {
            case Types.SQL_ALL_TYPES :
                break;
            case Types.SQL_CHAR :
            case Types.SQL_VARCHAR :
            case Types.VARCHAR_IGNORECASE :
                o = readChar(type);
                break;
            case Types.TINYINT :
            case Types.SQL_SMALLINT :
                o = readSmallint();
                break;
            case Types.SQL_INTEGER :
                o = readInteger();
                break;
            case Types.SQL_BIGINT :
                o = readBigint();
                break;
            case Types.SQL_REAL :
            case Types.SQL_FLOAT :
            case Types.SQL_DOUBLE :
                o = readReal();
                break;
            case Types.SQL_NUMERIC :
            case Types.SQL_DECIMAL :
                o = readDecimal(type);
                break;
            case Types.SQL_DATE :
                o = readDate(type);
                break;
            case Types.SQL_TIME :
            case Types.SQL_TIME_WITH_TIME_ZONE :
                o = readTime(type);
                break;
            case Types.SQL_TIMESTAMP :
            case Types.SQL_TIMESTAMP_WITH_TIME_ZONE :
                o = readTimestamp(type);
                break;
            case Types.SQL_INTERVAL_YEAR :
            case Types.SQL_INTERVAL_YEAR_TO_MONTH :
            case Types.SQL_INTERVAL_MONTH :
                o = readYearMonthInterval(type);
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
                o = readDaySecondInterval(type);
                break;
            case Types.SQL_BOOLEAN :
                o = readBoole();
                break;
            case Types.OTHER :
                o = readOther();
                break;
            case Types.SQL_CLOB :
                o = readClob();
                break;
            case Types.SQL_BLOB :
                o = readBlob();
                break;
            case Types.SQL_ARRAY :
                o = readArray(type);
                break;
            case Types.SQL_BINARY :
            case Types.SQL_VARBINARY :
                o = readBinary();
                break;
            case Types.SQL_BIT :
            case Types.SQL_BIT_VARYING :
                o = readBit();
                break;
            default :
                throw Error.runtimeError(ErrorCode.U_S0500,
                                         "RowInputBase - "
                                         + type.getNameString());
        }
        return o;
    }
    public void resetRow(int filepos, int rowsize) throws IOException {
        mark = 0;
        reset();
        if (buffer.length < rowsize) {
            buffer = new byte[rowsize];
        }
        filePos   = filepos;
        size      = count = rowsize;
        pos       = 4;
        buffer[0] = (byte) ((rowsize >>> 24) & 0xFF);
        buffer[1] = (byte) ((rowsize >>> 16) & 0xFF);
        buffer[2] = (byte) ((rowsize >>> 8) & 0xFF);
        buffer[3] = (byte) ((rowsize >>> 0) & 0xFF);
    }
    public byte[] getBuffer() {
        return buffer;
    }
    public int skipBytes(int n) throws IOException {
        throw Error.runtimeError(ErrorCode.U_S0500, "RowInputBase");
    }
    public String readLine() throws IOException {
        throw Error.runtimeError(ErrorCode.U_S0500, "RowInputBase");
    }
}