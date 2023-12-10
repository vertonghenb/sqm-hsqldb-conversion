package org.hsqldb.types;
import java.io.IOException;
import java.io.Reader;
import org.hsqldb.SessionInterface;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.java.JavaSystem;
public final class ClobInputStream extends Reader {
    final ClobData                clob;
    final long                    availableLength;
    long                          bufferOffset;
    long                          currentPosition;
    char[]                        buffer;
    boolean                       isClosed;
    int                           streamBlockSize;
    public final SessionInterface session;
    public ClobInputStream(SessionInterface session, ClobData clob,
                           long offset, long length) {
        final long clobLength = clob.length(session);
        this.session         = session;
        this.clob            = clob;
        this.availableLength = offset + Math.min(length, clobLength - offset);
        this.currentPosition = offset;
        this.streamBlockSize = session.getStreamBlockSize();
    }
    public int read() throws IOException {
        checkClosed();
        if (currentPosition >= availableLength) {
            return -1;
        }
        if (buffer == null
                || currentPosition >= bufferOffset + buffer.length) {
            try {
                checkClosed();
                readIntoBuffer();
            } catch (Exception e) {
                throw JavaSystem.toIOException(e);
            }
        }
        int val = buffer[(int) (currentPosition - bufferOffset)];
        currentPosition++;
        return val;
    }
    public int read(char[] cbuf, int off, int len) throws IOException {
        checkClosed();
        if (currentPosition == availableLength) {
            return -1;
        }
        if (currentPosition + len > availableLength) {
            len = (int) (availableLength - currentPosition);
        }
        for (int i = off; i < len; i++) {
            cbuf[i] = (char) read();
        }
        return len;
    }
    public long skip(long n) throws IOException {
        checkClosed();
        if (n <= 0) {
            return 0;
        }
        if (currentPosition + n > availableLength) {
            n = availableLength - currentPosition;
        }
        currentPosition += n;
        return n;
    }
    public int available() {
        long avail = availableLength - currentPosition;
        if (avail > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }
        return (int) avail;
    }
    public void close() throws IOException {
        isClosed = true;
    }
    private void checkClosed() throws IOException {
        if (isClosed) {
            throw new IOException(Error.getMessage(ErrorCode.X_0F503));
        }
    }
    private void readIntoBuffer() {
        long readLength = availableLength - currentPosition;
        if (readLength <= 0) {}
        if (readLength > streamBlockSize) {
            readLength = streamBlockSize;
        }
        buffer = clob.getChars(session, currentPosition, (int) readLength);
        bufferOffset = currentPosition;
    }
    static boolean isInLimits(long fullLength, long pos, long len) {
        return pos >= 0 && len >= 0 && pos + len <= fullLength;
    }
}