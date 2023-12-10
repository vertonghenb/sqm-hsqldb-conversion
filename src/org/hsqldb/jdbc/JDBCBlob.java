package org.hsqldb.jdbc;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.SQLException;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.KMPSearchAlgorithm;
import org.hsqldb.lib.java.JavaSystem;
public class JDBCBlob implements Blob {
    public long length() throws SQLException {
        return getData().length;
    }
    public byte[] getBytes(long pos, final int length) throws SQLException {
        final byte[] data = getData();
        final int    dlen = data.length;
        if (pos < MIN_POS || pos > MIN_POS + dlen) {
            throw Util.outOfRangeArgument("pos: " + pos);
        }
        pos--;
        if (length < 0 || length > dlen - pos) {
            throw Util.outOfRangeArgument("length: " + length);
        }
        final byte[] result = new byte[length];
        System.arraycopy(data, (int) pos, result, 0, length);
        return result;
    }
    public InputStream getBinaryStream() throws SQLException {
        return new ByteArrayInputStream(getData());
    }
    public long position(final byte[] pattern,
                         final long start) throws SQLException {
        final byte[] data = getData();
        final int    dlen = data.length;
        if (start < MIN_POS) {
            throw Util.outOfRangeArgument("start: " + start);
        } else if (start > dlen || pattern == null) {
            return -1L;
        }
        final int startIndex = (int) start - 1;
        final int plen       = pattern.length;
        if (plen == 0 || startIndex > dlen - plen) {
            return -1L;
        }
        final int result = KMPSearchAlgorithm.search(data, pattern,
            KMPSearchAlgorithm.computeTable(pattern), startIndex);
        return (result == -1) ? -1
                              : result + 1;
    }
    public long position(final Blob pattern, long start) throws SQLException {
        final byte[] data = getData();
        final int    dlen = data.length;
        if (start < MIN_POS) {
            throw Util.outOfRangeArgument("start: " + start);
        } else if (start > dlen || pattern == null) {
            return -1L;
        }
        final int  startIndex = (int) start - 1;
        final long plen       = pattern.length();
        if (plen == 0 || startIndex > ((long) dlen) - plen) {
            return -1L;
        }
        final int iplen = (int) plen;
        byte[]    bytePattern;
        if (pattern instanceof JDBCBlob) {
            bytePattern = ((JDBCBlob) pattern).data();
        } else {
            bytePattern = pattern.getBytes(1L, iplen);
        }
        final int result = KMPSearchAlgorithm.search(data, bytePattern,
            KMPSearchAlgorithm.computeTable(bytePattern), startIndex);
        return (result == -1) ? -1
                              : result + 1;
    }
    public int setBytes(long pos, byte[] bytes) throws SQLException {
        if (bytes == null) {
            throw Util.nullArgument("bytes");
        }
        return (setBytes(pos, bytes, 0, bytes.length));
    }
    public int setBytes(long pos, byte[] bytes, int offset,
                        int len) throws SQLException {
        if (!m_createdByConnection) {
            throw Util.notSupported();
        }
        if (bytes == null) {
            throw Util.nullArgument("bytes");
        }
        if (offset < 0 || offset > bytes.length) {
            throw Util.outOfRangeArgument("offset: " + offset);
        }
        if (len > bytes.length - offset) {
            throw Util.outOfRangeArgument("len: " + len);
        }
        if (pos < MIN_POS || pos > 1L + (Integer.MAX_VALUE - len)) {
            throw Util.outOfRangeArgument("pos: " + pos);
        }
        pos--;
        byte[]    data = getData();
        final int dlen = data.length;
        if ((pos + len) > dlen) {
            byte[] temp = new byte[(int) pos + len];
            System.arraycopy(data, 0, temp, 0, dlen);
            data = temp;
            temp = null;
        }
        System.arraycopy(bytes, offset, data, (int) pos, len);
        checkClosed();
        setData(data);
        return len;
    }
    public OutputStream setBinaryStream(final long pos) throws SQLException {
        if (!m_createdByConnection) {
            throw Util.notSupported();
        }
        if (pos < MIN_POS || pos > MAX_POS) {
            throw Util.outOfRangeArgument("pos: " + pos);
        }
        checkClosed();
        return new java.io.ByteArrayOutputStream() {
            public synchronized void close() throws java.io.IOException {
                try {
                    JDBCBlob.this.setBytes(pos, toByteArray());
                } catch (SQLException se) {
                    throw JavaSystem.toIOException(se);
                } finally {
                    super.close();
                }
            }
        };
    }
    public void truncate(final long len) throws SQLException {
        final byte[] data = getData();
        if (len < 0 || len > data.length) {
            throw Util.outOfRangeArgument("len: " + len);
        }
        if (len == data.length) {
            return;
        }
        byte[] newData = new byte[(int) len];
        System.arraycopy(data, 0, newData, 0, (int) len);
        checkClosed();    
        setData(newData);
    }
    public synchronized void free() throws SQLException {
        m_closed = true;
        m_data   = null;
    }
    public InputStream getBinaryStream(long pos,
                                       long length) throws SQLException {
        final byte[] data = getData();
        final int    dlen = data.length;
        if (pos < MIN_POS || pos > dlen) {
            throw Util.outOfRangeArgument("pos: " + pos);
        }
        pos--;
        if (length < 0 || length > dlen - pos) {
            throw Util.outOfRangeArgument("length: " + length);
        }
        if (pos == 0 && length == dlen) {
            return new ByteArrayInputStream(data);
        }
        final byte[] result = new byte[(int) length];
        System.arraycopy(data, (int) pos, result, 0, (int) length);
        return new ByteArrayInputStream(result);
    }
    public static final long MIN_POS = 1L;
    public static final long MAX_POS = 1L + (long) Integer.MAX_VALUE;
    private boolean          m_closed;
    private byte[]           m_data;
    private final boolean    m_createdByConnection;
    public JDBCBlob(final byte[] data) throws SQLException {
        if (data == null) {
            throw Util.nullArgument();
        }
        m_data                = data;
        m_createdByConnection = false;
    }
    protected JDBCBlob() {
        m_data                = new byte[0];
        m_createdByConnection = true;
    }
    protected synchronized void checkClosed() throws SQLException {
        if (m_closed) {
            throw Util.sqlException(ErrorCode.X_07501);
        }
    }
    protected byte[] data() throws SQLException {
        return getData();
    }
    private synchronized byte[] getData() throws SQLException {
        checkClosed();
        return m_data;
    }
    private synchronized void setData(byte[] data) throws SQLException {
        checkClosed();
        m_data = data;
    }
}