package org.hsqldb.types;
import java.io.DataInput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.hsqldb.SessionInterface;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.ArrayUtil;
public class BinaryData implements BlobData {
    public static final BinaryData singleBitZero =
        new BinaryData(new byte[]{ 0 }, 1);
    public static final BinaryData singleBitOne =
        new BinaryData(new byte[]{ -0x80 }, 1);
    public static final byte[] zeroLengthBytes = new byte[0];
    public static final BinaryData zeroLengthBinary =
        new BinaryData(zeroLengthBytes, false);
    long             id;
    protected byte[] data;
    private boolean  isBits;
    private long     bitLength;
    private int      hashCode = 0;
    public static BinaryData getBitData(byte[] data, long bitLength) {
        if (bitLength == 1) {
            return data[0] == 0 ? singleBitZero
                                : singleBitOne;
        }
        return new BinaryData(data, bitLength);
    }
    public BinaryData(byte[] data, boolean clone) {
        if (clone) {
            data = (byte[]) ArrayUtil.duplicateArray(data);
        }
        this.data      = data;
        this.bitLength = data.length * 8;
    }
    public BinaryData(SessionInterface session, BlobData b1, BlobData b2) {
        long length = (b1.length(session) + b2.length(session));
        if (length > Integer.MAX_VALUE) {
            throw Error.error(ErrorCode.X_22001);
        }
        data = new byte[(int) length];
        System.arraycopy(b1.getBytes(), 0, data, 0, (int) b1.length(session));
        System.arraycopy(b2.getBytes(), 0, data, (int) b1.length(session),
                         (int) b2.length(session));
        this.bitLength = (b1.length(session) + b2.length(session)) * 8;
    }
    public BinaryData(byte[] data, long bitLength) {
        this.data      = data;
        this.bitLength = bitLength;
        this.isBits    = true;
    }
    public BinaryData(long length, DataInput stream) {
        data      = new byte[(int) length];
        bitLength = data.length * 8;
        try {
            stream.readFully(data);
        } catch (IOException e) {
            throw Error.error(ErrorCode.GENERAL_IO_ERROR, e);
        }
    }
    public byte[] getBytes() {
        return data;
    }
    public long length(SessionInterface session) {
        return data.length;
    }
    public long bitLength(SessionInterface session) {
        return bitLength;
    }
    public boolean isBits() {
        return isBits;
    }
    public byte[] getBytes(SessionInterface session, long pos, int length) {
        if (!isInLimits(data.length, pos, length)) {
            throw new IndexOutOfBoundsException();
        }
        byte[] bytes = new byte[length];
        System.arraycopy(data, (int) pos, bytes, 0, length);
        return bytes;
    }
    public BlobData getBlob(SessionInterface session, long pos, long length) {
        throw Error.runtimeError(ErrorCode.U_S0500, "BinaryData");
    }
    public InputStream getBinaryStream(SessionInterface session) {
        return new BlobInputStream(session, this, 0L, length(session));
    }
    public InputStream getBinaryStream(SessionInterface session, long pos,
                                       long length) {
        if (!isInLimits(data.length, pos, length)) {
            throw new IndexOutOfBoundsException();
        }
        return new BlobInputStream(session, this, pos, length(session));
    }
    public void setBytes(SessionInterface session, long pos, byte[] bytes,
                         int offset, int length) {
        if (!isInLimits(data.length, pos, 0)) {
            throw new IndexOutOfBoundsException();
        }
        if (!isInLimits(data.length, pos, length)) {
            data = (byte[]) ArrayUtil.resizeArray(data, (int) pos + length);
        }
        System.arraycopy(bytes, offset, data, (int) pos, length);
        bitLength = data.length * 8;
    }
    public void setBytes(SessionInterface session, long pos, byte[] bytes) {
        setBytes(session, pos, bytes, 0, bytes.length);
    }
    public void setBinaryStream(SessionInterface session, long pos,
                                InputStream in) {
    }
    public void truncate(SessionInterface session, long len) {
        if (data.length > len) {
            data      = (byte[]) ArrayUtil.resizeArray(data, (int) len);
            bitLength = data.length * 8;
        }
    }
    public BlobData duplicate(SessionInterface session) {
        return new BinaryData(data, true);
    }
    public long position(SessionInterface session, byte[] pattern,
                         long start) {
        if (pattern.length > data.length) {
            return -1;
        }
        if (start >= data.length) {
            return -1;
        }
        return ArrayUtil.find(data, (int) start, data.length, pattern);
    }
    public long position(SessionInterface session, BlobData pattern,
                         long start) {
        if (pattern.length(session) > data.length) {
            return -1;
        }
        byte[] bytes = pattern.getBytes();
        return position(session, bytes, start);
    }
    public long nonZeroLength(SessionInterface session) {
        return data.length;
    }
    public long getId() {
        return id;
    }
    public void setId(long id) {
        this.id = id;
    }
    public int getStreamBlockSize() {
        return 512 * 1024;
    }
    public boolean isClosed() {
        return false;
    }
    public void free() {}
    public void setSession(SessionInterface session) {}
    static boolean isInLimits(long fullLength, long pos, long len) {
        return pos >= 0 && len >= 0 && pos + len <= fullLength;
    }
    public boolean isBinary() {
        return true;
    }
    public boolean equals(Object other) {
        if (other instanceof BinaryData) {
            return Type.SQL_VARBINARY.compare(null, this, other) == 0;
        }
        return false;
    }
    public int hashCode() {
        if (hashCode == 0) {
            int code = 0;
            for (int i = 0; i < data.length && i < 32; i++) {
                code = code * 31 + (0xff & data[i]);
            }
            code     += data.length;
            hashCode = code;
        }
        return hashCode;
    }
}