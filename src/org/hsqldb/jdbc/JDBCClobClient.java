package org.hsqldb.jdbc;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.IllegalCharsetNameException;
import java.sql.Clob;
import java.sql.SQLException;
import org.hsqldb.HsqlException;
import org.hsqldb.SessionInterface;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.types.ClobDataID;
import org.hsqldb.types.ClobInputStream;
public class JDBCClobClient implements Clob {
    public synchronized InputStream getAsciiStream() throws SQLException {
        checkClosed();
        return new InputStream() {
            private final byte[] oneChar = new byte[1];
            private boolean      m_closed;
            private CharBuffer m_charBuffer =
                (CharBuffer) CharBuffer.allocate(64 * 1024).flip();
            private ByteBuffer m_byteBuffer = ByteBuffer.allocate(1024);
            private Charset    m_charset    = charsetForName("US-ASCII");
            private CharsetEncoder m_encoder =
                m_charset.newEncoder().onMalformedInput(
                    CodingErrorAction.REPLACE).onUnmappableCharacter(
                    CodingErrorAction.REPLACE);
            private Reader m_reader = clob.getCharacterStream(session);
            public int read() throws IOException {
                if (isEOF()) {
                    return -1;
                }
                synchronized (oneChar) {
                    int charsRead = read(oneChar, 0, 1);
                    return charsRead == 1 ? oneChar[0]
                            : -1;
                }
            }
            public int read(byte b[], int off, int len) throws IOException {
                checkClosed();
                if (isEOF()) {
                    return -1;
                }
                final CharBuffer cb = m_charBuffer;
                int charsRead;
                int bytesRead;
                if (cb.remaining() == 0) {
                    cb.clear();
                    charsRead = m_reader.read(cb);
                    cb.flip();
                    if (charsRead < 0) {
                        setEOF();
                        return -1;
                    } else if (charsRead == 0) {
                        return 0;
                    }
                }
                final ByteBuffer bb = (m_byteBuffer.capacity() < len)
                                      ? ByteBuffer.allocate(len)
                                      : m_byteBuffer;
                int cbLimit     = cb.limit();
                int cbPosistion = cb.position();
                cb.limit(cbPosistion + len);
                bb.clear();
                int         bbPosition = bb.position();
                CoderResult result     = m_encoder.encode(cb, bb, false);
                if (bbPosition == bb.position() && result.isUnderflow()) {
                    cb.limit(cb.limit() + 1);
                    m_encoder.encode(cb, bb, false);
                }
                cb.limit(cbLimit);
                bb.flip();
                bytesRead = bb.limit();
                if (bytesRead == 0) {
                    setEOF();
                    return -1;
                }
                m_byteBuffer = bb;
                bb.get(b, off, bytesRead);
                return bytesRead;
            }
            public void close() throws IOException {
                boolean isClosed = m_closed;
                if (!isClosed) {
                    m_closed     = true;
                    m_charBuffer = null;
                    m_charset    = null;
                    m_encoder    = null;
                    try {
                        m_reader.close();
                    } catch (Exception ex) {
                    }
                }
            }
            private boolean isEOF() {
                final Reader reader = m_reader;
                return (reader == null);
            }
            private void setEOF() {
                final Reader reader = m_reader;
                if (reader != null) {
                    try {
                        reader.close();
                    } catch (IOException iOException) {
                    }
                }
                m_reader = null;
            }
            private void checkClosed() throws IOException {
                if (JDBCClobClient.this.isClosed()) {
                    try {
                        this.close();
                    } catch (Exception ex) {
                    }
                }
                if (m_closed) {
                    throw new IOException("The stream is closed.");
                }
            }
        };
    }
    public synchronized Reader getCharacterStream() throws SQLException {
        checkClosed();
        return new ClobInputStream(session, clob, 0, length());
    }
    public synchronized String getSubString(long pos,
            int length) throws SQLException {
        checkClosed();
        if (!isInLimits(Long.MAX_VALUE, pos - 1, length)) {
            throw Util.outOfRangeArgument();
        }
        try {
            return clob.getSubString(session, pos - 1, length);
        } catch (HsqlException e) {
            throw Util.sqlException(e);
        }
    }
    public synchronized long length() throws SQLException {
        checkClosed();
        try {
            return clob.length(session);
        } catch (HsqlException e) {
            throw Util.sqlException(e);
        }
    }
    public synchronized long position(String searchstr,
                                      long start) throws SQLException {
        if (!isInLimits(Long.MAX_VALUE, start - 1, 0)) {
            throw Util.outOfRangeArgument();
        }
        checkClosed();
        try {
            return clob.position(session, searchstr, start - 1);
        } catch (HsqlException e) {
            throw Util.sqlException(e);
        }
    }
    public synchronized long position(Clob searchstr,
                                      long start) throws SQLException {
        if (!isInLimits(Long.MAX_VALUE, start - 1, 0)) {
            throw Util.outOfRangeArgument();
        }
        if (searchstr instanceof JDBCClobClient) {
            ClobDataID searchClob = ((JDBCClobClient) searchstr).clob;
            try {
                return clob.position(session, searchClob, start - 1);
            } catch (HsqlException e) {
                throw Util.sqlException(e);
            }
        }
        return position(searchstr.getSubString(1, (int) searchstr.length()),
                        start);
    }
    public synchronized OutputStream setAsciiStream(
            final long pos) throws SQLException {
        checkClosed();
        if (pos < 1) {
            throw Util.outOfRangeArgument("pos: " + pos);
        }
        if (!isWritable) {
            throw Util.notUpdatableColumn();
        }
        startUpdate();
        return new OutputStream() {
            private long    m_position = pos - 1;
            private Charset m_charset  = charsetForName("US-ASCII");
            private CharsetDecoder m_decoder =
                m_charset.newDecoder().onMalformedInput(
                    CodingErrorAction.REPLACE).onUnmappableCharacter(
                    CodingErrorAction.REPLACE);
            private CharBuffer   m_charBuffer = CharBuffer.allocate(64 * 1024);
            private ByteBuffer   m_byteBuffer = ByteBuffer.allocate(1024);
            private final byte[] oneByte      = new byte[1];
            private boolean      m_closed;
            public void write(int b) throws IOException {
                synchronized (oneByte) {
                    oneByte[0] = (byte) b;
                    this.write(oneByte, 0, 1);
                }
            }
            public void write(byte b[], int off, int len) throws IOException {
                checkClosed();
                final ByteBuffer bb = (m_byteBuffer.capacity() < len)
                                      ? ByteBuffer.allocate(len)
                                      : m_byteBuffer;
                if (m_charBuffer.remaining() < len) {
                    flush0();
                }
                final CharBuffer cb = m_charBuffer.capacity() < len
                                      ? CharBuffer.allocate(len)
                                      : m_charBuffer;
                bb.clear();
                bb.put(b, off, len);
                bb.flip();
                m_decoder.decode(bb, cb, false);
                if (cb.remaining() == 0) {
                    flush();
                }
            }
            public void flush() throws IOException {
                checkClosed();
                flush0();
            }
            public void close() throws IOException {
                if (!m_closed) {
                    try {
                        flush0();
                    } finally {
                        m_closed     = true;
                        m_byteBuffer = null;
                        m_charBuffer = null;
                        m_charset    = null;
                        m_decoder    = null;
                    }
                }
            }
            private void checkClosed() throws IOException {
                if (JDBCClobClient.this.isClosed()) {
                    try {
                        close();
                    } catch (Exception ex) {
                    }
                }
                if (m_closed) {
                    throw new IOException("The stream is closed.");
                }
            }
            private void flush0() throws IOException {
                final CharBuffer cb = m_charBuffer;
                cb.flip();
                final char[] chars = new char[cb.length()];
                cb.get(chars);
                cb.clear();
                try {
                    clob.setChars(session, m_position, chars, 0, chars.length);
                } catch (Exception e) {
                    throw new IOException(e.toString());
                }
                m_position += chars.length;
            }
        };
    }
    public synchronized Writer setCharacterStream(
            final long pos) throws SQLException {
        checkClosed();
        if (pos < 1) {
            throw Util.outOfRangeArgument("pos: " + pos);
        }
        if (!isWritable) {
            throw Util.notUpdatableColumn();
        }
        startUpdate();
        return new Writer() {
            private long    m_clobPosition = pos - 1;
            private boolean m_closed;
            public void write(char[] cbuf, int off,
                              int len) throws IOException {
                checkClosed();
                clob.setChars(session, m_clobPosition, cbuf, off, len);
                m_clobPosition += len;
            }
            public void flush() throws IOException {
            }
            @Override
            public void close() throws IOException {
                m_closed = true;
            }
            private void checkClosed() throws IOException {
                if (m_closed || JDBCClobClient.this.isClosed()) {
                    throw new IOException("The stream is closed");
                }
            }
        };
    }
    public synchronized int setString(long pos,
                                      String str) throws SQLException {
        return setString(pos, str, 0, str.length());
    }
    public synchronized int setString(long pos, String str, int offset,
                                      int len) throws SQLException {
        if (!isInLimits(str.length(), offset, len)) {
            throw Util.outOfRangeArgument();
        }
        checkClosed();
        if (pos < 1) {
            throw Util.outOfRangeArgument("pos: " + pos);
        }
        if (!isWritable) {
            throw Util.notUpdatableColumn();
        }
        startUpdate();
        str = str.substring(offset, offset + len);
        try {
            clob.setString(session, pos - 1, str);
            return len;
        } catch (HsqlException e) {
            throw Util.sqlException(e);
        }
    }
    public synchronized void truncate(long len) throws SQLException {
        if (len < 0) {
            throw Util.outOfRangeArgument("len: " + len);
        }
        checkClosed();
        try {
            clob.truncate(session, len);
        } catch (HsqlException e) {
            throw Util.sqlException(e);
        }
    }
    public synchronized void free() throws SQLException {
        isClosed = true;
        clob     = null;
        session  = null;
    }
    public synchronized Reader getCharacterStream(long pos,
            long length) throws SQLException {
        if (!isInLimits(Long.MAX_VALUE, pos - 1, length)) {
            throw Util.outOfRangeArgument();
        }
        checkClosed();
        return new ClobInputStream(session, clob, pos - 1, length);
    }
    char[] getChars(long position, int length) throws SQLException {
        try {
            return clob.getChars(session, position - 1, length);
        } catch (HsqlException e) {
            throw Util.sqlException(e);
        }
    }
    ClobDataID       originalClob;
    ClobDataID       clob;
    SessionInterface session;
    int              colIndex;
    private boolean  isClosed;
    private boolean  isWritable;
    JDBCResultSet    resultSet;
    public JDBCClobClient(SessionInterface session, ClobDataID clob) {
        this.session = session;
        this.clob    = clob;
    }
    public ClobDataID getClob() {
        return clob;
    }
    public synchronized boolean isClosed() {
        return isClosed;
    }
    public synchronized void setWritable(JDBCResultSet result, int index) {
        isWritable = true;
        resultSet  = result;
        colIndex   = index;
    }
    public synchronized void clearUpdates() {
        if (originalClob != null) {
            clob         = originalClob;
            originalClob = null;
        }
    }
    private void startUpdate() throws SQLException {
        if (originalClob != null) {
            return;
        }
        originalClob = clob;
        clob         = (ClobDataID) clob.duplicate(session);
        resultSet.startUpdate(colIndex + 1);
        resultSet.preparedStatement.parameterValues[colIndex] = clob;
        resultSet.preparedStatement.parameterSet[colIndex]    = Boolean.TRUE;
    }
    private void checkClosed() throws SQLException {
        if (isClosed) {
            throw Util.sqlException(ErrorCode.X_07501);
        }
    }
    static boolean isInLimits(long fullLength, long pos, long len) {
        return fullLength >= 0 && pos >= 0 && len >= 0
               && pos <= fullLength - len;
    }
    protected static Charset charsetForName(
            final String charsetName) throws SQLException {
        String csn = charsetName;
        if (csn == null) {
            csn = Charset.defaultCharset().name();
        }
        try {
            if (Charset.isSupported(csn)) {
                return Charset.forName(csn);
            }
        } catch (IllegalCharsetNameException x) {
        }
        throw Util.sqlException(new UnsupportedEncodingException(csn));
    }
}