


package org.hsqldb.jdbc;

import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.SQLException;

import org.hsqldb.HsqlException;
import org.hsqldb.SessionInterface;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.types.BlobDataID;
import org.hsqldb.types.BlobInputStream;


public class JDBCBlobClient implements Blob {

    
    public synchronized long length() throws SQLException {

        try {
            return blob.length(session);
        } catch (HsqlException e) {
            throw Util.sqlException(e);
        }
    }

    
    public synchronized byte[] getBytes(long pos,
                                        int length) throws SQLException {

        if (!isInLimits(Long.MAX_VALUE, pos - 1, length)) {
            throw Util.outOfRangeArgument();
        }

        try {
            return blob.getBytes(session, pos - 1, length);
        } catch (HsqlException e) {
            throw Util.sqlException(e);
        }
    }

    
    public synchronized InputStream getBinaryStream() throws SQLException {
        return new BlobInputStream(session, blob, 0, length());
    }

    
    public synchronized long position(byte[] pattern,
                                      long start) throws SQLException {

        if (!isInLimits(Long.MAX_VALUE, start - 1, 0)) {
            throw Util.outOfRangeArgument();
        }

        try {
            long position = blob.position(session, pattern, start - 1);

            if (position >= 0) {
                position++;
            }

            return position;
        } catch (HsqlException e) {
            throw Util.sqlException(e);
        }
    }

    
    public synchronized long position(Blob pattern,
                                      long start) throws SQLException {

        if (!isInLimits(Long.MAX_VALUE, start - 1, 0)) {
            throw Util.outOfRangeArgument();
        }

        if (pattern instanceof JDBCBlobClient) {
            BlobDataID searchClob = ((JDBCBlobClient) pattern).blob;

            try {
                long position = blob.position(session, searchClob, start - 1);

                if (position >= 0) {
                    position++;
                }

                return position;
            } catch (HsqlException e) {
                throw Util.sqlException(e);
            }
        }

        if (!isInLimits(Integer.MAX_VALUE, 0, pattern.length())) {
            throw Util.outOfRangeArgument();
        }

        byte[] bytePattern = pattern.getBytes(1, (int) pattern.length());

        return position(bytePattern, start);
    }

    
    public synchronized int setBytes(long pos,
                                     byte[] bytes) throws SQLException {
        return setBytes(pos, bytes, 0, bytes.length);
    }

    
    public synchronized int setBytes(long pos, byte[] bytes, int offset,
                                     int len) throws SQLException {

        if (!isInLimits(bytes.length, offset, len)) {
            throw Util.outOfRangeArgument();
        }

        if (!isInLimits(Long.MAX_VALUE, pos - 1, len)) {
            throw Util.outOfRangeArgument();
        }

        if (!isWritable) {
            throw Util.notUpdatableColumn();
        }

        startUpdate();

        try {
            blob.setBytes(session, pos - 1, bytes, offset, len);

            return len;
        } catch (HsqlException e) {
            throw Util.sqlException(e);
        }
    }

    
    public synchronized OutputStream setBinaryStream(long pos)
    throws SQLException {
        throw Util.notSupported();
    }

    
    public synchronized void truncate(long len) throws SQLException {

        try {
            blob.truncate(session, len);
        } catch (HsqlException e) {
            throw Util.sqlException(e);
        }
    }

    
    public synchronized void free() throws SQLException {
        isClosed = true;
    }

    
    public synchronized InputStream getBinaryStream(long pos,
            long length) throws SQLException {

        if (!isInLimits(Long.MAX_VALUE, pos - 1, length)) {
            throw Util.outOfRangeArgument();
        }

        return new BlobInputStream(session, blob, pos - 1, length);
    }

    
    BlobDataID       originalBlob;
    BlobDataID       blob;
    SessionInterface session;
    int              colIndex;
    private boolean  isClosed;
    private boolean  isWritable;
    JDBCResultSet    resultSet;

    public JDBCBlobClient(SessionInterface session, BlobDataID blob) {
        this.session = session;
        this.blob    = blob;
    }

    public boolean isClosed() {
        return isClosed;
    }

    public BlobDataID getBlob() {
        return blob;
    }

    public synchronized void setWritable(JDBCResultSet result, int index) {

        isWritable = true;
        resultSet  = result;
        colIndex   = index;
    }

    public synchronized void clearUpdates() {

        if (originalBlob != null) {
            blob         = originalBlob;
            originalBlob = null;
        }
    }

    private void startUpdate() throws SQLException {

        if (originalBlob != null) {
            return;
        }

        originalBlob = blob;
        blob         = (BlobDataID) blob.duplicate(session);

        resultSet.startUpdate(colIndex + 1);

        resultSet.preparedStatement.parameterValues[colIndex] = blob;
        resultSet.preparedStatement.parameterSet[colIndex]    = Boolean.TRUE;
    }

    private void checkClosed() throws SQLException {

        if (isClosed) {
            throw Util.sqlException(ErrorCode.X_07501);
        }
    }

    static boolean isInLimits(long fullLength, long pos, long len) {
        return pos >= 0 && len >= 0 && pos + len <= fullLength;
    }
}
