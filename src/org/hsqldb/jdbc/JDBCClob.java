


package org.hsqldb.jdbc;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.sql.Clob;
import java.sql.SQLException;

import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.KMPSearchAlgorithm;
import org.hsqldb.lib.java.JavaSystem;






















public class JDBCClob implements Clob {

    
    public long length() throws SQLException {
        return getData().length();
    }

    
    public String getSubString(long pos,
                               final int length) throws SQLException {

        final String data = getData();
        final int    dlen = data.length();

        if (pos < MIN_POS || pos > dlen) {
            Util.outOfRangeArgument("pos: " + pos);
        }
        pos--;

        if (length < 0 || length > dlen - pos) {
            throw Util.outOfRangeArgument("length: " + length);
        }

        return (pos == 0 && length == dlen) ? data
                : data.substring((int) pos, (int) pos + length);
    }

    
    public java.io.Reader getCharacterStream() throws SQLException {
        return new StringReader(getData());
    }

    
    public java.io.InputStream getAsciiStream() throws SQLException {

        try {
            return new ByteArrayInputStream(getData().getBytes("US-ASCII"));
        } catch (IOException e) {
            return null;
        }
    }

    
    public long position(final String searchstr,
                         long start) throws SQLException {

        final String data = getData();

        if (start < MIN_POS) {
            throw Util.outOfRangeArgument("start: " + start);
        }

        if (searchstr == null || start > MAX_POS) {
            return -1;
        }

        final int position = KMPSearchAlgorithm.search(data, searchstr, null,
            (int) start);

        return (position == -1) ? -1
                                : position + 1;
    }

    
    public long position(final Clob searchstr,
                         long start) throws SQLException {

        final String data = getData();

        if (start < MIN_POS) {
            throw Util.outOfRangeArgument("start: " + start);
        }

        if (searchstr == null) {
            return -1;
        }

        final long dlen  = data.length();
        final long sslen = searchstr.length();

        start--;





        if (start > dlen - sslen) {
            return -1;
        }

        
        String pattern;

        if (searchstr instanceof JDBCClob) {
            pattern = ((JDBCClob) searchstr).data();
        } else {
            pattern = searchstr.getSubString(1L, (int) sslen);
        }

        final int position = KMPSearchAlgorithm.search(data, pattern, null,
            (int) start);

        return (position == -1) ? -1
                                : position + 1;
    }

    

    
    public int setString(long pos, String str) throws SQLException {

        if (str == null) {
            throw Util.nullArgument("str");
        }

        return setString(pos, str, 0, str.length());
    }

    
    public int setString(long pos, String str, int offset,
                         int len) throws SQLException {

        if (!m_createdByConnection) {

            
            throw Util.notSupported();
        }

        String data = getData();

        if (str == null) {
            throw Util.nullArgument("str");
        }

        final int strlen = str.length();

        if (offset < 0 || offset > strlen) {
            throw Util.outOfRangeArgument("offset: " + offset);
        }

        if (len > strlen - offset) {
            throw Util.outOfRangeArgument("len: " + len);
        }

        if (pos < MIN_POS || pos > 1L + (Integer.MAX_VALUE - len)) {
            throw Util.outOfRangeArgument("pos: " + pos);
        }

        final int    dlen = data.length();
        final int    ipos = (int) (pos - 1);
        StringBuffer sb;

        if (ipos > dlen - len) {
            sb = new StringBuffer(ipos + len);

            sb.append(data.substring(0, ipos));

            data = null;

            sb.append(str.substring(offset, offset + len));

            str = null;
        } else {
            sb   = new StringBuffer(data);
            data = null;

            for (int i = ipos, j = 0; j < len; i++, j++) {
                sb.setCharAt(i, str.charAt(offset + j));
            }
            str = null;
        }
        setData(sb.toString());

        return len;
    }

    
    public java.io.OutputStream setAsciiStream(
            final long pos) throws SQLException {

        if (!m_createdByConnection) {

            
            throw Util.notSupported();
        }
        checkClosed();

        if (pos < MIN_POS || pos > MAX_POS) {
            throw Util.outOfRangeArgument("pos: " + pos);
        }

        return new java.io.ByteArrayOutputStream() {

            public synchronized void close() throws java.io.IOException {

                try {
                    JDBCClob.this.setString(pos,
                            new String(toByteArray(), "US-ASCII"));
                } catch (SQLException se) {
                    throw JavaSystem.toIOException(se);
                } finally {
                    super.close();
                }
            }
        };
    }

    
    public java.io.Writer setCharacterStream(
            final long pos) throws SQLException {

        if (!m_createdByConnection) {

            
            throw Util.notSupported();
        }
        checkClosed();

        if (pos < MIN_POS || pos > MAX_POS) {
            throw Util.outOfRangeArgument("pos: " + pos);
        }

        return new java.io.StringWriter() {

            public synchronized void close() throws java.io.IOException {

                try {
                    JDBCClob.this.setString(pos, toString());
                } catch (SQLException se) {
                    throw JavaSystem.toIOException(se);
                }
            }
        };
    }

    
    public void truncate(final long len) throws SQLException {

        final String data = getData();
        final long   dlen = data.length();

        if (len == dlen) {

            
        } else if (len < 0 || len > dlen) {
            throw Util.outOfRangeArgument("len: " + len);
        } else {

            
            setData(data.substring(0, (int) len));
        }
    }

    

    
    public synchronized void free() throws SQLException {
        m_closed = true;
        m_data   = null;
    }

    
    public Reader getCharacterStream(long pos,
                                     long length) throws SQLException {

        if (length > Integer.MAX_VALUE) {
            throw Util.outOfRangeArgument("length: " + length);
        }

        return new StringReader(getSubString(pos, (int) length));
    }

    
    private static final long MIN_POS = 1L;
    private static final long MAX_POS = 1L + (long) Integer.MAX_VALUE;
    private boolean           m_closed;
    private String            m_data;
    private final boolean     m_createdByConnection;

    
    public JDBCClob(final String data) throws SQLException {

        if (data == null) {
            throw Util.nullArgument();
        }
        m_data                = data;
        m_createdByConnection = false;
    }

    protected JDBCClob() {
        m_data                = "";
        m_createdByConnection = true;
    }

    protected synchronized void checkClosed() throws SQLException {

        if (m_closed) {
            throw Util.sqlException(ErrorCode.X_07501);
        }
    }

    protected String data() throws SQLException {
        return getData();
    }

    private synchronized String getData() throws SQLException {

        checkClosed();

        return m_data;
    }

    private synchronized void setData(String data) throws SQLException {

        checkClosed();

        m_data = data;
    }
}
