package org.hsqldb.rowio;
import java.io.IOException;
import java.math.BigDecimal;
import org.hsqldb.Scanner;
import org.hsqldb.Tokens;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.store.ValuePool;
import org.hsqldb.types.BinaryData;
import org.hsqldb.types.BlobData;
import org.hsqldb.types.BlobDataID;
import org.hsqldb.types.ClobData;
import org.hsqldb.types.ClobDataID;
import org.hsqldb.types.IntervalMonthData;
import org.hsqldb.types.IntervalSecondData;
import org.hsqldb.types.IntervalType;
import org.hsqldb.types.JavaObjectData;
import org.hsqldb.types.TimeData;
import org.hsqldb.types.TimestampData;
import org.hsqldb.types.Type;
import org.hsqldb.types.Types;
public class RowInputText extends RowInputBase implements RowInputInterface {
    private String    fieldSep;
    private String    varSep;
    private String    longvarSep;
    private int       fieldSepLen;
    private int       varSepLen;
    private int       longvarSepLen;
    private boolean   fieldSepEnd;
    private boolean   varSepEnd;
    private boolean   longvarSepEnd;
    private int       textLen;
    protected String  text;
    protected int     line;
    protected int     field;
    protected int     next = 0;
    protected boolean allQuoted;
    protected Scanner scanner;
    private int maxPooledStringLength = ValuePool.getMaxStringLength();
    public RowInputText(String fieldSep, String varSep, String longvarSep,
                        boolean allQuoted) {
        super(new byte[0]);
        scanner = new Scanner();
        if (fieldSep.endsWith("\n")) {
            fieldSepEnd = true;
            fieldSep    = fieldSep.substring(0, fieldSep.length() - 1);
        }
        if (varSep.endsWith("\n")) {
            varSepEnd = true;
            varSep    = varSep.substring(0, varSep.length() - 1);
        }
        if (longvarSep.endsWith("\n")) {
            longvarSepEnd = true;
            longvarSep    = longvarSep.substring(0, longvarSep.length() - 1);
        }
        this.allQuoted  = allQuoted;
        this.fieldSep   = fieldSep;
        this.varSep     = varSep;
        this.longvarSep = longvarSep;
        fieldSepLen     = fieldSep.length();
        varSepLen       = varSep.length();
        longvarSepLen   = longvarSep.length();
    }
    public void setSource(String text, int pos, int byteSize) {
        size      = byteSize;
        this.text = text;
        textLen   = text.length();
        filePos   = pos;
        next      = 0;
        line++;
        field = 0;
    }
    protected String getField(String sep, int sepLen,
                              boolean isEnd) throws IOException {
        String s = null;
        try {
            int start = next;
            field++;
            if (isEnd) {
                if ((next >= textLen) && (sepLen > 0)) {
                    throw Error.error(ErrorCode.TEXT_SOURCE_NO_END_SEPARATOR);
                } else if (text.endsWith(sep)) {
                    next = textLen - sepLen;
                } else {
                    throw Error.error(ErrorCode.TEXT_SOURCE_NO_END_SEPARATOR);
                }
            } else {
                next = text.indexOf(sep, start);
                if (next == -1) {
                    next = textLen;
                }
            }
            if (start > next) {
                start = next;
            }
            s    = text.substring(start, next);
            next += sepLen;
            s    = s.trim();
            if (s.length() == 0) {
                s = null;
            }
        } catch (Exception e) {
            Object[] messages = new Object[] {
                new Integer(field), e.toString()
            };
            throw new IOException(
                Error.getMessage(
                    ErrorCode.M_TEXT_SOURCE_FIELD_ERROR, 0, messages));
        }
        return s;
    }
    public String readString() throws IOException {
        return getField(fieldSep, fieldSepLen, fieldSepEnd);
    }
    private String readVarString() throws IOException {
        return getField(varSep, varSepLen, varSepEnd);
    }
    private String readLongVarString() throws IOException {
        return getField(longvarSep, longvarSepLen, longvarSepEnd);
    }
    public short readShort() throws IOException {
        return (short) readInt();
    }
    public int readInt() throws IOException {
        String s = readString();
        if (s == null) {
            return 0;
        }
        s = s.trim();
        if (s.length() == 0) {
            return 0;
        }
        return Integer.parseInt(s);
    }
    public long readLong() throws IOException {
        throw Error.runtimeError(ErrorCode.U_S0500, "RowInputText");
    }
    public int readType() throws IOException {
        return 0;
    }
    protected boolean readNull() {
        return false;
    }
    protected String readChar(Type type) throws IOException {
        String s = null;;
        switch (type.typeCode) {
            case Types.SQL_CHAR :
                s = readString();
                break;
            case Types.SQL_VARCHAR :
            case Types.VARCHAR_IGNORECASE :
                s = readVarString();
                break;
            default :
                s = readLongVarString();
                break;
        }
        if (s == null) {
            return null;
        }
        if (s.length() > this.maxPooledStringLength) {
            return new String(s);
        } else {
            return ValuePool.getString(s);
        }
    }
    protected Integer readSmallint() throws IOException {
        String s = readString();
        if (s == null) {
            return null;
        }
        s = s.trim();
        if (s.length() == 0) {
            return null;
        }
        return ValuePool.getInt(Integer.parseInt(s));
    }
    protected Integer readInteger() throws IOException {
        String s = readString();
        if (s == null) {
            return null;
        }
        s = s.trim();
        if (s.length() == 0) {
            return null;
        }
        return ValuePool.getInt(Integer.parseInt(s));
    }
    protected Long readBigint() throws IOException {
        String s = readString();
        if (s == null) {
            return null;
        }
        s = s.trim();
        if (s.length() == 0) {
            return null;
        }
        return ValuePool.getLong(Long.parseLong(s));
    }
    protected Double readReal() throws IOException {
        String s = readString();
        if (s == null) {
            return null;
        }
        s = s.trim();
        if (s.length() == 0) {
            return null;
        }
        return Double.valueOf(s);
    }
    protected BigDecimal readDecimal(Type type) throws IOException {
        String s = readString();
        if (s == null) {
            return null;
        }
        s = s.trim();
        if (s.length() == 0) {
            return null;
        }
        return new BigDecimal(s);
    }
    protected TimeData readTime(Type type) throws IOException {
        String s = readString();
        if (s == null) {
            return null;
        }
        s = s.trim();
        if (s.length() == 0) {
            return null;
        }
        return scanner.newTime(s);
    }
    protected TimestampData readDate(Type type) throws IOException {
        String s = readString();
        if (s == null) {
            return null;
        }
        s = s.trim();
        if (s.length() == 0) {
            return null;
        }
        return scanner.newDate(s);
    }
    protected TimestampData readTimestamp(Type type) throws IOException {
        String s = readString();
        if (s == null) {
            return null;
        }
        s = s.trim();
        if (s.length() == 0) {
            return null;
        }
        return scanner.newTimestamp(s);
    }
    protected IntervalMonthData readYearMonthInterval(Type type)
    throws IOException {
        String s = readString();
        if (s == null) {
            return null;
        }
        s = s.trim();
        if (s.length() == 0) {
            return null;
        }
        return (IntervalMonthData) scanner.newInterval(s, (IntervalType) type);
    }
    protected IntervalSecondData readDaySecondInterval(Type type)
    throws IOException {
        String s = readString();
        if (s == null) {
            return null;
        }
        s = s.trim();
        if (s.length() == 0) {
            return null;
        }
        return (IntervalSecondData) scanner.newInterval(s,
                (IntervalType) type);
    }
    protected Boolean readBoole() throws IOException {
        String s = readString();
        if (s == null) {
            return null;
        }
        s = s.trim();
        if (s.length() == 0) {
            return null;
        }
        return s.equalsIgnoreCase(Tokens.T_TRUE) ? Boolean.TRUE
                                                 : Boolean.FALSE;
    }
    protected Object readOther() throws IOException {
        String s = readString();
        if (s == null) {
            return null;
        }
        BinaryData data = scanner.convertToBinary(s);
        if (data.length(null) == 0) {
            return null;
        }
        return new JavaObjectData(data.getBytes());
    }
    protected BinaryData readBit() throws IOException {
        String s = readString();
        if (s == null) {
            return null;
        }
        BinaryData data = scanner.convertToBit(s);
        return data;
    }
    protected BinaryData readBinary() throws IOException {
        String s = readString();
        if (s == null) {
            return null;
        }
        BinaryData data = scanner.convertToBinary(s);
        return data;
    }
    protected ClobData readClob() throws IOException {
        String s = readString();
        if (s == null) {
            return null;
        }
        s = s.trim();
        if (s.length() == 0) {
            return null;
        }
        long id = Long.parseLong(s);
        return new ClobDataID(id);
    }
    protected BlobData readBlob() throws IOException {
        String s = readString();
        if (s == null) {
            return null;
        }
        s = s.trim();
        if (s.length() == 0) {
            return null;
        }
        long id = Long.parseLong(s);
        return new BlobDataID(id);
    }
    protected Object[] readArray(Type type) {
        throw Error.runtimeError(ErrorCode.U_S0500, "RowInputText");
    }
    public int getLineNumber() {
        return line;
    }
    public void skippedLine() {
        line++;
    }
    public void reset() {
        text    = "";
        textLen = 0;
        filePos = 0;
        next    = 0;
        field   = 0;
        line    = 0;
    }
}