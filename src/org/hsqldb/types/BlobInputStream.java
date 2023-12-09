


package org.hsqldb.types;

import java.io.IOException;
import java.io.InputStream;

import org.hsqldb.SessionInterface;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.java.JavaSystem;


public class BlobInputStream extends InputStream {

    final BlobData                blob;
    final long                    availableLength;
    long                          bufferOffset;
    long                          currentPosition;
    byte[]                        buffer;
    boolean                       isClosed;
    int                           streamBlockSize;
    public final SessionInterface session;

    public BlobInputStream(SessionInterface session, BlobData blob,
                           long offset, long length) {

        final long blobLength = blob.length(session);

        this.session         = session;
        this.blob            = blob;
        this.availableLength = offset + Math.min(length, blobLength - offset);
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

        int val = buffer[(int) (currentPosition - bufferOffset)] & 0xff;

        currentPosition++;

        return val;
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

        if (isClosed || blob.isClosed()) {
            throw new IOException(Error.getMessage(ErrorCode.X_0F503));
        }
    }

    private void readIntoBuffer() {

        long readLength = availableLength - currentPosition;

        if (readLength <= 0) {}

        if (readLength > streamBlockSize) {
            readLength = streamBlockSize;
        }

        buffer = blob.getBytes(session, currentPosition, (int) readLength);
        bufferOffset = currentPosition;
    }

    static boolean isInLimits(long fullLength, long pos, long len) {
        return pos >= 0 && len >= 0 && pos + len <= fullLength;
    }
}
