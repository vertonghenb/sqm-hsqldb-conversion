package org.hsqldb.rowio;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import org.hsqldb.Row;
import org.hsqldb.Tokens;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.StringConverter;
import org.hsqldb.types.BinaryData;
import org.hsqldb.types.BlobData;
import org.hsqldb.types.ClobData;
import org.hsqldb.types.IntervalMonthData;
import org.hsqldb.types.IntervalSecondData;
import org.hsqldb.types.JavaObjectData;
import org.hsqldb.types.TimeData;
import org.hsqldb.types.TimestampData;
import org.hsqldb.types.Type;
public class RowOutputTextLog extends RowOutputBase {
    static byte[] BYTES_NULL;
    static byte[] BYTES_TRUE;
    static byte[] BYTES_FALSE;
    static byte[] BYTES_AND;
    static byte[] BYTES_IS;
    static byte[] BYTES_ARRAY;
    static {
        try {
            BYTES_NULL  = Tokens.T_NULL.getBytes("ISO-8859-1");
            BYTES_TRUE  = Tokens.T_TRUE.getBytes("ISO-8859-1");
            BYTES_FALSE = Tokens.T_FALSE.getBytes("ISO-8859-1");
            BYTES_AND   = " AND ".getBytes("ISO-8859-1");
            BYTES_IS    = " IS ".getBytes("ISO-8859-1");
            BYTES_ARRAY = " ARRAY[".getBytes("ISO-8859-1");
        } catch (UnsupportedEncodingException e) {
            Error.runtimeError(ErrorCode.U_S0500, "RowOutputTextLog");
        }
    }
    public static final int MODE_DELETE = 1;
    public static final int MODE_INSERT = 0;
    private boolean         isWritten;
    private int             logMode;
    private boolean         noSeparators;
    public void setMode(int mode) {
        logMode = mode;
    }
    protected void writeFieldPrefix() {
        if (!noSeparators) {
            if (logMode == MODE_DELETE && isWritten) {
                write(BYTES_AND);
            }
        }
    }
    protected void writeChar(String s, Type t) {
        write('\'');
        StringConverter.stringToUnicodeBytes(this, s, true);
        write('\'');
    }
    protected void writeReal(Double o) {
        writeBytes(Type.SQL_DOUBLE.convertToSQLString(o));
    }
    protected void writeSmallint(Number o) {
        this.writeBytes(o.toString());
    }
    public void writeEnd() {}
    protected void writeBit(BinaryData o) {
        ensureRoom((int) (o.length(null) * 8 + 2));
        write('\'');
        String s = StringConverter.byteArrayToBitString(o.getBytes(),
            (int) o.bitLength(null));
        writeBytes(s);
        write('\'');
    }
    protected void writeBinary(BinaryData o) {
        ensureRoom((int) (o.length(null) * 2 + 2));
        write('\'');
        StringConverter.writeHexBytes(getBuffer(), count, o.getBytes());
        count += (o.length(null) * 2);
        write('\'');
    }
    protected void writeClob(ClobData o, Type type) {
        writeBytes(Long.toString(o.getId()));
    }
    protected void writeBlob(BlobData o, Type type) {
        writeBytes(Long.toString(o.getId()));
    }
    protected void writeArray(Object[] o, Type type) {
        type = type.collectionBaseType();
        noSeparators = true;
        write(BYTES_ARRAY);
        for (int i = 0; i < o.length; i++) {
            if (i > 0) {
                write(',');
            }
            writeData(type, o[i]);
        }
        write(']');
        noSeparators = false;
    }
    public void writeType(int type) {}
    public void writeSize(int size) {}
    public int getSize(Row row) {
        return 0;
    }
    public int getStorageSize(int size) {
        return size;
    }
    protected void writeInteger(Number o) {
        this.writeBytes(o.toString());
    }
    protected void writeBigint(Number o) {
        this.writeBytes(o.toString());
    }
    protected void writeNull(Type type) {
        if (!noSeparators) {
            if (logMode == MODE_DELETE) {
                write(BYTES_IS);
            } else if (isWritten) {
                write(',');
            }
            isWritten = true;
        }
        write(BYTES_NULL);
    }
    protected void writeOther(JavaObjectData o) {
        ensureRoom(o.getBytesLength() * 2 + 2);
        write('\'');
        StringConverter.writeHexBytes(getBuffer(), count, o.getBytes());
        count += o.getBytesLength() * 2;
        write('\'');
    }
    public void writeString(String value) {
        StringConverter.stringToUnicodeBytes(this, value, false);
    }
    protected void writeBoolean(Boolean o) {
        write(o.booleanValue() ? BYTES_TRUE
                               : BYTES_FALSE);
    }
    protected void writeDecimal(BigDecimal o, Type type) {
        writeBytes(type.convertToSQLString(o));
    }
    protected void writeFieldType(Type type) {
        if (!noSeparators) {
            if (logMode == MODE_DELETE) {
                write('=');
            } else if (isWritten) {
                write(',');
            }
            isWritten = true;
        }
    }
    public void writeLong(long value) {
        this.writeBytes(Long.toString(value));
    }
    public void writeIntData(int i, int position) {}
    protected void writeTime(TimeData o, Type type) {
        write('\'');
        writeBytes(type.convertToString(o));
        write('\'');
    }
    protected void writeDate(TimestampData o, Type type) {
        write('\'');
        writeBytes(type.convertToString(o));
        write('\'');
    }
    protected void writeTimestamp(TimestampData o, Type type) {
        write('\'');
        writeBytes(type.convertToString(o));
        write('\'');
    }
    protected void writeYearMonthInterval(IntervalMonthData o, Type type) {
        write('\'');
        writeBytes(type.convertToString(o));
        write('\'');
    }
    protected void writeDaySecondInterval(IntervalSecondData o, Type type) {
        write('\'');
        writeBytes(type.convertToString(o));
        write('\'');
    }
    public void writeShort(int i) {
        writeBytes(Integer.toString(i));
    }
    public void writeInt(int i) {
        writeBytes(Integer.toString(i));
    }
    public void reset() {
        super.reset();
        isWritten = false;
    }
    public RowOutputInterface duplicate() {
        throw Error.runtimeError(ErrorCode.U_S0500, "RowOutputText");
    }
}