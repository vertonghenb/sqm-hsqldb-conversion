


package org.hsqldb.jdbc;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.sql.Blob;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.CountdownInputStream;
import org.hsqldb.lib.FileUtil;
import org.hsqldb.lib.InOutUtil;
import org.hsqldb.lib.KMPSearchAlgorithm;


public class JDBCBlobFile implements java.sql.Blob {

    
    public long length() throws SQLException {

        checkClosed();

        try {
            return m_file.length();
        } catch (Exception e) {
            throw Util.sqlException(e);
        }
    }

    
    public byte[] getBytes(final long pos,
                           final int length) throws SQLException {

        InputStream           is   = null;
        ByteArrayOutputStream baos = null;
        final int initialBufferSize =
            (int) Math.min(InOutUtil.DEFAULT_COPY_BUFFER_SIZE, length);

        try {
            is   = getBinaryStream(pos, length);
            baos = new ByteArrayOutputStream(initialBufferSize);

            InOutUtil.copy(is, baos, length);
        } catch (SQLException ex) {
            throw ex;
        } catch (Exception ex) {
            throw Util.sqlException(ex);
        } finally {
            if (is != null) {
                try {
                    is.close();
                } catch (Exception ex) {}
            }
        }

        return baos.toByteArray();
    }

    
    public InputStream getBinaryStream() throws SQLException {
        return getBinaryStream(1, Long.MAX_VALUE);
    }

    
    public long position(final byte[] pattern,
                         final long start) throws SQLException {

        if (start < 1) {
            throw Util.outOfRangeArgument("start: " + start);
        } else if (pattern == null || pattern.length == 0
                   || start > length()) {
            return -1L;
        }

        InputStream is = null;

        try {
            is = getBinaryStream(start, Long.MAX_VALUE);

            final long matchOffset = KMPSearchAlgorithm.search(is, pattern,
                KMPSearchAlgorithm.computeTable(pattern));

            return (matchOffset == -1) ? -1
                                       : start + matchOffset;
        } catch (SQLException ex) {
            throw ex;
        } catch (Exception ex) {
            throw Util.sqlException(ex);
        } finally {
            if (is != null) {
                try {
                    is.close();
                } catch (Exception ex) {}
            }
        }
    }

    
    public long position(final Blob pattern,
                         final long start) throws SQLException {

        long patternLength;

        if (start < 1) {
            throw Util.outOfRangeArgument("start: " + start);
        } else if ((patternLength = (pattern == null) ? 0
                                                      : pattern.length()) == 0 || start
                                                      > length()) {
            return -1L;
        } else if (patternLength > Integer.MAX_VALUE) {
            throw Util.outOfRangeArgument("pattern.length(): "
                                          + patternLength);
        }

        byte[] bytePattern;

        if (pattern instanceof JDBCBlob) {
            bytePattern = ((JDBCBlob) pattern).data();
        } else {
            bytePattern = pattern.getBytes(1L, (int) patternLength);
        }

        return position(bytePattern, start);
    }

    

    
    public int setBytes(final long pos,
                        final byte[] bytes) throws SQLException {
        return setBytes(pos, bytes, 0, bytes == null ? 0
                                                     : bytes.length);
    }

    
    public int setBytes(final long pos, final byte[] bytes, final int offset,
                        final int len) throws SQLException {

        if (bytes == null) {
            throw Util.nullArgument("bytes");
        }

        final OutputStream os = setBinaryStream(pos);

        try {
            os.write(bytes, offset, len);
        } catch (Exception ex) {
            throw Util.sqlException(ex);
        } finally {
            try {
                os.close();
            } catch (Exception ex) {}
        }

        return len;
    }

    
    public OutputStream setBinaryStream(final long pos) throws SQLException {

        if (pos < 1) {
            throw Util.invalidArgument("pos: " + pos);
        }

        checkClosed();
        createFile();

        OutputStream adapter;

        try {
            adapter = new OutputStreamAdapter(m_file, pos - 1) {

                public void close() throws IOException {

                    try {
                        super.close();
                    } finally {
                        m_streams.remove(this);
                    }
                }
            };
        } catch (Exception ex) {
            throw Util.sqlException(ex);
        }

        m_streams.add(adapter);

        final OutputStream result = new BufferedOutputStream(adapter);

        return result;
    }

    
    public void truncate(long len) throws SQLException {

        if (len < 0) {
            throw Util.invalidArgument("len: " + len);
        }

        checkClosed();

        RandomAccessFile randomAccessFile = null;

        try {
            randomAccessFile = new RandomAccessFile(m_file, "rw");

            randomAccessFile.setLength(len);
        } catch (Exception ex) {
            throw Util.sqlException(ex);
        } finally {
            if (randomAccessFile != null) {
                try {
                    randomAccessFile.close();
                } catch (Exception ex) {}
            }
        }
    }

    
    public synchronized void free() throws SQLException {

        if (m_closed) {
            return;
        }

        m_closed = true;

        final List streams = new ArrayList();

        streams.addAll(m_streams);

        m_streams = null;

        for (Iterator itr = streams.iterator(); itr.hasNext(); ) {
            final Object stream = itr.next();

            if (stream instanceof InputStream) {
                try {
                    ((InputStream) stream).close();
                } catch (Exception ex) {

                    
                }
            } else if (stream instanceof OutputStream) {
                try {
                    ((OutputStream) stream).close();
                } catch (Exception ex) {

                    
                }
            }
        }

        if (m_deleteOnFree) {
            try {
                m_file.delete();
            } catch (Exception e) {}
        }
    }

    
    public InputStream getBinaryStream(final long pos,
                                       final long length) throws SQLException {

        if (pos < 1) {
            throw Util.outOfRangeArgument("pos: " + pos);
        }

        checkClosed();

        InputStream result;

        try {
            result = new InputStreamAdapter(m_file, pos - 1, length) {

                public void close() throws IOException {

                    try {
                        super.close();
                    } finally {
                        m_streams.remove(this);
                    }
                }
            };
        } catch (Exception ex) {
            throw Util.sqlException(ex);
        }

        m_streams.add(result);

        return result;
    }

    
    public File getFile() {
        return m_file;
    }

    
    public boolean isDeleteOnFree() {
        return m_deleteOnFree;
    }

    
    public void setDeleteOnFree(boolean deleteOnFree) {
        m_deleteOnFree = deleteOnFree;
    }

    
    protected void finalize() throws Throwable {

        try {
            super.finalize();
        } finally {
            this.free();
        }
    }

    
    
    
    public static final String TEMP_FILE_PREFIX = "hsql_jdbc_blob_file_";
    public static final String TEMP_FILE_SUFFIX = ".tmp";

    
    private final File m_file;
    private boolean    m_closed;
    private boolean    m_deleteOnFree;
    private List       m_streams = new ArrayList();

    
    public JDBCBlobFile() throws SQLException {
        this(true);
    }

    
    public JDBCBlobFile(boolean deleteOnFree) throws SQLException {

        m_deleteOnFree = deleteOnFree;

        try {
            m_file = File.createTempFile(TEMP_FILE_PREFIX,
                                         TEMP_FILE_SUFFIX).getCanonicalFile();
        } catch (Exception ex) {
            throw Util.sqlException(ex);
        }
    }

    
    public JDBCBlobFile(final File file) throws SQLException {
        this(file, false);
    }

    
    public JDBCBlobFile(final File file,
                        boolean deleteOnFree) throws SQLException {

        m_deleteOnFree = deleteOnFree;

        try {
            m_file = file.getCanonicalFile();
        } catch (Exception ex) {
            throw Util.sqlException(ex);
        }

        checkIsFile( false);
    }

    protected final void checkIsFile(boolean checkExists) throws SQLException {

        boolean exists = false;
        boolean isFile = false;

        try {
            exists = m_file.exists();
        } catch (Exception ex) {
            throw Util.sqlException(ex);
        }

        if (exists) {
            try {
                isFile = m_file.isFile();
            } catch (Exception ex) {
                throw Util.sqlException(ex);
            }
        }

        if (exists) {
            if (!isFile) {
                throw Util.invalidArgument("Is not a file: " + m_file);
            }
        } else if (checkExists) {
            throw Util.invalidArgument("Does not exist: " + m_file);
        }
    }

    private void checkClosed() throws SQLException {

        if (m_closed) {
            throw Util.sqlException(ErrorCode.X_07501);
        }
    }

    private void createFile() throws SQLException {

        try {
            if (!m_file.exists()) {
                FileUtil.getFileUtil().makeParentDirectories(m_file);
                m_file.createNewFile();
            }
        } catch (Exception ex) {
            throw Util.sqlException(ex);
        }

        checkIsFile( true);
    }

    protected static class OutputStreamAdapter extends OutputStream {

        private final RandomAccessFile m_randomAccessFile;

        public OutputStreamAdapter(final File file,
                                   final long pos)
                                   throws FileNotFoundException, IOException {

            if (pos < 0) {
                throw new IllegalArgumentException("pos: " + pos);
            }

            m_randomAccessFile = new RandomAccessFile(file, "rw");

            m_randomAccessFile.seek(pos);
        }

        public void write(int b) throws IOException {
            m_randomAccessFile.write(b);
        }

        public void write(byte b[]) throws IOException {
            m_randomAccessFile.write(b);
        }

        public void write(byte b[], int off, int len) throws IOException {
            m_randomAccessFile.write(b, off, len);
        }

        public void flush() throws IOException {
            m_randomAccessFile.getFD().sync();
        }

        public void close() throws IOException {
            m_randomAccessFile.close();
        }
    }

    static class InputStreamAdapter extends InputStream {

        private final CountdownInputStream m_countdownInputStream;

        InputStreamAdapter(final File file, final long pos,
                           final long length)
                           throws FileNotFoundException, IOException {

            if (file == null) {
                throw new NullPointerException("file");
            }

            if (pos < 0) {
                throw new IllegalArgumentException("pos: " + pos);
            }

            if (length < 0) {
                throw new IllegalArgumentException("length: " + length);
            }

            final FileInputStream fis = new FileInputStream(file);

            if (pos > 0) {
                final long actualPos = fis.skip(pos);
            }

            final BufferedInputStream  bis = new BufferedInputStream(fis);
            final CountdownInputStream cis = new CountdownInputStream(bis);

            cis.setCount(length);

            m_countdownInputStream = cis;
        }

        public int available() throws IOException {
            return m_countdownInputStream.available();
        }

        public int read() throws IOException {
            return m_countdownInputStream.read();
        }

        public int read(byte b[]) throws IOException {
            return m_countdownInputStream.read(b);
        }

        public int read(byte b[], int off, int len) throws IOException {
            return m_countdownInputStream.read(b, off, len);
        }

        public long skip(long n) throws IOException {
            return m_countdownInputStream.skip(n);
        }

        public void close() throws IOException {
            m_countdownInputStream.close();
        }
    }
}
