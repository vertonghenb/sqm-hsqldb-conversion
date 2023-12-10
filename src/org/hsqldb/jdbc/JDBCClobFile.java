package org.hsqldb.jdbc;
import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.CharArrayWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.IllegalCharsetNameException;
import java.sql.Clob;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.FileUtil;
import org.hsqldb.lib.InOutUtil;
import org.hsqldb.lib.KMPSearchAlgorithm;
public class JDBCClobFile implements java.sql.Clob {
    public long length() throws SQLException {
        checkClosed();
        if (m_fixedWidthCharset) {
            return m_file.length() / m_maxCharWidth;
        }
        ReaderAdapter adapter = null;
        try {
            adapter = new ReaderAdapter(m_file, 0, Long.MAX_VALUE);
            final long length = adapter.skip(Long.MAX_VALUE);
            return length;
        } catch (Exception ex) {
            throw Util.sqlException(ex);
        } finally {
            if (adapter != null) {
                try {
                    adapter.close();
                } catch (Exception ex) {}
            }
        }
    }
    public String getSubString(final long pos,
                               final int length) throws SQLException {
        Reader          reader = null;
        CharArrayWriter writer = null;
        try {
            final int initialCapacity =
                Math.min(InOutUtil.DEFAULT_COPY_BUFFER_SIZE, length);
            reader = getCharacterStream(pos, length);
            writer = new CharArrayWriter(initialCapacity);
            InOutUtil.copy(reader, writer, length);
        } catch (SQLException ex) {
            throw ex;
        } catch (Exception ex) {
            throw Util.sqlException(ex);
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (Exception ex) {}
            }
        }
        return writer.toString();
    }
    public Reader getCharacterStream() throws SQLException {
        return getCharacterStream(1, Long.MAX_VALUE);
    }
    public InputStream getAsciiStream() throws SQLException {
        InputStream stream;
        try {
            stream = new JDBCBlobFile.InputStreamAdapter(m_file, 0,
                    Long.MAX_VALUE) {
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
        m_streams.add(stream);
        return stream;
    }
    public long position(final char[] pattern,
                         final long start) throws SQLException {
        if (start < 1) {
            throw Util.outOfRangeArgument("start: " + start);
        } else if (pattern == null || pattern.length == 0
                   || start > length()) {
            return -1L;
        }
        Reader reader = null;
        try {
            reader = getCharacterStream(start, Long.MAX_VALUE);
            final long matchOffset = KMPSearchAlgorithm.search(reader,
                pattern, KMPSearchAlgorithm.computeTable(pattern));
            return matchOffset == -1 ? -1
                                     : start + matchOffset;
        } catch (SQLException ex) {
            throw ex;
        } catch (Exception ex) {
            throw Util.sqlException(ex);
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (Exception ex) {}
            }
        }
    }
    public long position(String searchstr, long start) throws SQLException {
        return position(searchstr == null ? null
                                          : searchstr.toCharArray(), start);
    }
    public long position(final Clob pattern,
                         final long start) throws SQLException {
        long patternLength;
        if (start < 1) {
            throw Util.outOfRangeArgument("start: " + start);
        } else if ((patternLength = pattern == null ? 0
                                                    : pattern.length()) == 0) {
            return -1L;
        } else if (patternLength > Integer.MAX_VALUE) {
            throw Util.outOfRangeArgument("pattern.length(): "
                                          + patternLength);
        }
        char[] charPattern;
        if (pattern instanceof JDBCClob) {
            charPattern = ((JDBCClob) pattern).data().toCharArray();
        } else {
            Reader          reader = null;
            CharArrayWriter writer = new CharArrayWriter();
            try {
                reader = pattern.getCharacterStream();
                InOutUtil.copy(reader, writer, patternLength);
            } catch (IOException ex) {
                throw Util.sqlException(ex);
            } finally {
                if (reader != null) {
                    try {
                        reader.close();
                    } catch (IOException ex) {}
                }
            }
            charPattern = writer.toCharArray();
        }
        return position(charPattern, start);
    }
    public int setString(final long pos,
                         final String str) throws SQLException {
        return setString(pos, str, 0, str == null ? 0
                                                  : str.length());
    }
    public int setString(final long pos, final String str, final int offset,
                         final int len) throws SQLException {
        if (str == null) {
            throw Util.nullArgument("str");
        }
        Writer writer = null;
        try {
            writer = setCharacterStream(pos);
            writer.write(str, offset, len);
        } catch (Exception ex) {
            throw Util.sqlException(ex);
        } finally {
            if (writer != null) {
                try {
                    writer.close();
                } catch (Exception ex) {}
            }
        }
        return len;
    }
    public OutputStream setAsciiStream(long pos) throws SQLException {
        if (pos < 1) {
            throw Util.invalidArgument("pos: " + pos);
        }
        checkClosed();
        createFile();
        OutputStream stream;
        try {
            stream = new JDBCBlobFile.OutputStreamAdapter(m_file, pos - 1) {
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
        m_streams.add(stream);
        return stream;
    }
    public Writer setCharacterStream(final long pos) throws SQLException {
        if (pos < 1) {
            throw Util.invalidArgument("pos: " + pos);
        }
        checkClosed();
        createFile();
        Writer writer;
        try {
            final WriterAdapter adapter = new WriterAdapter(m_file, pos - 1) {
                public void close() throws IOException {
                    try {
                        super.close();
                    } finally {
                        m_streams.remove(this);
                    }
                }
            };
            writer = new BufferedWriter(adapter);
        } catch (Exception ex) {
            throw Util.sqlException(ex);
        }
        m_streams.add(writer);
        return writer;
    }
    public void truncate(long len) throws SQLException {
        if (len < 0) {
            throw Util.invalidArgument("len: " + len);
        }
        checkClosed();
        ReaderAdapter    adapter          = null;
        RandomAccessFile randomAccessFile = null;
        long             filePointer;
        try {
            adapter     = new ReaderAdapter(m_file, len, Long.MAX_VALUE);
            filePointer = adapter.getFilePointer();
            adapter.close();
            randomAccessFile = new RandomAccessFile(m_file, "rw");
            randomAccessFile.setLength(filePointer);
        } catch (Exception ex) {
            throw Util.sqlException(ex);
        } finally {
            if (adapter != null) {
                try {
                    adapter.close();
                } catch (Exception ex) {}
            }
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
    public Reader getCharacterStream(long pos,
                                     long length) throws SQLException {
        if (pos < 1) {
            throw Util.outOfRangeArgument("pos: " + pos);
        }
        pos--;
        if (length < 0) {
            throw Util.outOfRangeArgument("length: " + length);
        }
        Reader reader;
        try {
            reader = new ReaderAdapter(m_file, pos, length) {
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
        m_streams.add(reader);
        return reader;
    }
    public File getFile() {
        return m_file;
    }
    public String getEncoding() {
        return m_encoding;
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
            try {
                this.free();
            } catch (Throwable throwable) {}
        }
    }
    public static final String TEMP_FILE_PREFIX = "hsql_jdbc_clob_file_";
    public static final String TEMP_FILE_SUFFIX = ".tmp";
    private final File m_file;
    private boolean        m_closed;
    private boolean        m_deleteOnFree;
    private String         m_encoding;
    private Charset        m_charset;
    private CharsetEncoder m_encoder;
    private boolean        m_fixedWidthCharset;
    private int            m_maxCharWidth;
    private List           m_streams = new ArrayList();
    public JDBCClobFile() throws SQLException {
        this((String) null);
    }
    public JDBCClobFile(String encoding) throws SQLException {
        try {
            setEncoding(encoding);
            m_file = File.createTempFile(TEMP_FILE_PREFIX, TEMP_FILE_SUFFIX);
            m_deleteOnFree = true;
        } catch (Exception ex) {
            throw Util.sqlException(ex);
        }
    }
    public JDBCClobFile(File file) throws SQLException {
        this(file, null);
    }
    public JDBCClobFile(File file, String encoding) throws SQLException {
        if (file == null) {
            throw Util.nullArgument("file");
        }
        try {
            setEncoding(encoding);
            m_file = file.getCanonicalFile();
            checkIsFile( false);
            m_deleteOnFree = false;
        } catch (Exception ex) {
            throw Util.sqlException(ex);
        }
    }
    protected final void setEncoding(final String encoding)
    throws UnsupportedEncodingException {
        final Charset charSet = charsetForName(encoding);
        final CharsetEncoder encoder = charSet.newEncoder().onMalformedInput(
            CodingErrorAction.REPLACE).onUnmappableCharacter(
            CodingErrorAction.REPLACE);
        final float maxBytesPerChar     = encoder.maxBytesPerChar();
        final float averageBytesPerChar = encoder.averageBytesPerChar();
        final boolean fixedWidthCharset =
            (maxBytesPerChar == Math.round(maxBytesPerChar))
            && (maxBytesPerChar == averageBytesPerChar);
        m_fixedWidthCharset = fixedWidthCharset;
        m_maxCharWidth      = Math.round(maxBytesPerChar);
        m_charset           = charSet;
        m_encoder           = encoder;
        m_encoding          = m_charset.name();
    }
    protected static Charset charsetForName(final String charsetName)
    throws UnsupportedEncodingException {
        String csn = charsetName;
        if (csn == null) {
            csn = Charset.defaultCharset().name();
        }
        try {
            if (Charset.isSupported(csn)) {
                return Charset.forName(csn);
            }
        } catch (IllegalCharsetNameException x) {}
        throw new UnsupportedEncodingException(csn);
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
    protected void checkClosed() throws SQLException {
        if (m_closed) {
            throw Util.sqlException(ErrorCode.X_07501);
        }
    }
    protected void createFile() throws SQLException {
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
    protected class WriterAdapter extends Writer {
        private final RandomAccessFile m_randomAccessFile;
        public WriterAdapter(final File file,
                             final long pos)
                             throws FileNotFoundException, IOException {
            if (file == null) {
                throw new NullPointerException("file");
            }
            if (pos < 0) {
                throw new IllegalArgumentException("pos: " + pos);
            }
            ReaderAdapter reader = null;
            long          filePointer;
            try {
                reader      = new ReaderAdapter(file, pos, Long.MAX_VALUE);
                filePointer = reader.getFilePointer();
            } finally {
                if (reader != null) {
                    try {
                        reader.close();
                    } catch (Exception ex) {}
                }
            }
            m_randomAccessFile = new RandomAccessFile(file, "rw");
            m_randomAccessFile.seek(filePointer);
        }
        public void flush() throws IOException {
            m_randomAccessFile.getFD().sync();
        }
        public void close() throws IOException {
            m_randomAccessFile.close();
        }
        public void write(char[] cbuf, int off, int len) throws IOException {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            OutputStreamWriter writer = m_encoding == null
                                        ? new OutputStreamWriter(baos)
                                        : new OutputStreamWriter(baos,
                                            m_charset);
            writer.write(cbuf, off, len);
            writer.close();
            m_randomAccessFile.write(baos.toByteArray());
        }
    }
    protected class ReaderAdapter extends Reader {
        private static final int CHARBUFFER_CAPACTIY = 128;
        private final Reader m_reader;
        private long         m_remaining = Long.MAX_VALUE;
        private long         m_filePointer;
        private ByteBuffer   m_byteBuffer;
        private CharBuffer   m_charBuffer;
        public ReaderAdapter(final File file, final long pos,
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
            if (!m_fixedWidthCharset) {
                final int charCapacity = CHARBUFFER_CAPACTIY;
                final int byteCapacity = charCapacity * m_maxCharWidth;
                m_charBuffer = CharBuffer.allocate(charCapacity);
                m_byteBuffer = ByteBuffer.allocate(byteCapacity);
            }
            final FileInputStream     fis = new FileInputStream(file);
            final BufferedInputStream bis = new BufferedInputStream(fis);
            final InputStreamReader isr = new InputStreamReader(bis,
                m_charset);
            m_reader = isr;
            for (long i = 0; i < pos; i++) {
                final int ch = read();
                if (ch == -1) {
                    break;
                }
            }
            m_remaining = length;
        }
        public int read(final char[] cbuf, final int off,
                        int len) throws IOException {
            final long l_remaining = m_remaining;
            if (l_remaining <= 0) {
                return -1;
            } else if (l_remaining < len) {
                len = (int) l_remaining;
            }
            int charsRead = m_reader.read(cbuf, off, len);
            if (charsRead == -1) {
                return -1;
            } else if (charsRead > l_remaining) {
                charsRead   = (int) l_remaining;
                m_remaining = 0;
            } else {
                m_remaining -= charsRead;
            }
            int bytesRead;
            if (m_fixedWidthCharset) {
                bytesRead = (m_maxCharWidth * charsRead);
            } else {
                final boolean reallocate = (charsRead
                                            > m_charBuffer.capacity());
                final CharBuffer cb = reallocate
                                      ? CharBuffer.allocate(charsRead)
                                      : m_charBuffer;
                final ByteBuffer bb = reallocate
                                      ? ByteBuffer.allocate(charsRead
                                          * m_maxCharWidth)
                                      : m_byteBuffer;
                cb.clear();
                bb.clear();
                cb.put(cbuf, off, charsRead);
                cb.flip();
                m_encoder.encode(cb, bb,  true);
                bb.flip();
                bytesRead = bb.limit();
                if (reallocate) {
                    m_byteBuffer = bb;
                    m_charBuffer = cb;
                }
            }
            m_filePointer += bytesRead;
            return charsRead;
        }
        public void close() throws IOException {
            m_reader.close();
        }
        public long getFilePointer() {
            return m_filePointer;
        }
    }
}