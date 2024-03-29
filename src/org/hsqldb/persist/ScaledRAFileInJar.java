package org.hsqldb.persist;
import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import org.hsqldb.Database;
import org.hsqldb.lib.HsqlByteArrayInputStream;
final class ScaledRAFileInJar implements RandomAccessInterface {
    DataInputStream          file;
    final String             fileName;
    long                     fileLength;
    boolean                  bufferDirty = true;
    byte[]                   buffer      = new byte[4096];
    HsqlByteArrayInputStream ba = new HsqlByteArrayInputStream(buffer);
    long                     bufferOffset;
    long seekPosition;
    long realPosition;
    ScaledRAFileInJar(String name) throws FileNotFoundException, IOException {
        fileName   = name;
        fileLength = getLength();
        resetStream();
    }
    public long length() throws IOException {
        return fileLength;
    }
    public void seek(long position) throws IOException {
        seekPosition = position;
    }
    public long getFilePointer() throws IOException {
        return seekPosition;
    }
    private void readIntoBuffer() throws IOException {
        long filePos = seekPosition;
        bufferDirty = false;
        long subOffset  = filePos % buffer.length;
        long readLength = fileLength - (filePos - subOffset);
        if (readLength <= 0) {
            throw new IOException("read beyond end of file");
        }
        if (readLength > buffer.length) {
            readLength = buffer.length;
        }
        fileSeek(filePos - subOffset);
        file.readFully(buffer, 0, (int) readLength);
        bufferOffset = filePos - subOffset;
        realPosition = bufferOffset + readLength;
    }
    public int read() throws IOException {
        if (seekPosition >= fileLength) {
            return -1;
        }
        if (bufferDirty || seekPosition < bufferOffset
                || seekPosition >= bufferOffset + buffer.length) {
            readIntoBuffer();
        }
        ba.reset();
        ba.skip(seekPosition - bufferOffset);
        int val = ba.read();
        seekPosition++;
        return val;
    }
    public long readLong() throws IOException {
        long hi = readInt();
        long lo = readInt();
        return (hi << 32) + (lo & 0xffffffffL);
    }
    public int readInt() throws IOException {
        if (bufferDirty || seekPosition < bufferOffset
                || seekPosition >= bufferOffset + buffer.length) {
            readIntoBuffer();
        }
        ba.reset();
        ba.skip(seekPosition - bufferOffset);
        int val = ba.readInt();
        seekPosition += 4;
        return val;
    }
    public void read(byte[] b, int offset, int length) throws IOException {
        if (bufferDirty || seekPosition < bufferOffset
                || seekPosition >= bufferOffset + buffer.length) {
            readIntoBuffer();
        }
        ba.reset();
        ba.skip(seekPosition - bufferOffset);
        int bytesRead = ba.read(b, offset, length);
        seekPosition += bytesRead;
        if (bytesRead < length) {
            if (seekPosition != realPosition) {
                fileSeek(seekPosition);
            }
            file.readFully(b, offset + bytesRead, length - bytesRead);
            seekPosition += (length - bytesRead);
            realPosition = seekPosition;
        }
    }
    public void write(byte[] b, int off, int len) throws IOException {}
    public void writeInt(int i) throws IOException {}
    public void writeLong(long i) throws IOException {}
    public void close() throws IOException {
        file.close();
    }
    public boolean isReadOnly() {
        return true;
    }
    public boolean wasNio() {
        return false;
    }
    private long getLength() throws IOException {
        int count = 0;
        resetStream();
        while (true) {
            if (file.read() < 0) {
                break;
            }
            count++;
        }
        return count;
    }
    private void resetStream() throws IOException {
        if (file != null) {
            file.close();
        }
        InputStream fis = null;
        try {
            fis = getClass().getResourceAsStream(fileName);
            if (fis == null) {
                ClassLoader cl =
                    Thread.currentThread().getContextClassLoader();
                if (cl != null) {
                    fis = cl.getResourceAsStream(fileName);
                }
            }
        } catch (Throwable t) {
        } finally {
            if (fis == null) {
                throw new FileNotFoundException(fileName);
            }
        }
        file = new DataInputStream(fis);
    }
    private void fileSeek(long position) throws IOException {
        long skipPosition = realPosition;
        if (position < skipPosition) {
            resetStream();
            skipPosition = 0;
        }
        while (position > skipPosition) {
            skipPosition += file.skip(position - skipPosition);
        }
    }
    public boolean ensureLength(long newLong) {
        return true;
    }
    public boolean setLength(long newLength) {
        return false;
    }
    public Database getDatabase() {
        return null;
    }
    public void synch() {}
}