package org.hsqldb.persist;
import java.io.IOException;
public interface RandomAccessInterface {
    long length() throws IOException;
    void seek(long position) throws IOException;
    long getFilePointer() throws IOException;
    int read() throws IOException;
    void read(byte[] b, int offset, int length) throws IOException;
    void write(byte[] b, int offset, int length) throws IOException;
    int readInt() throws IOException;
    void writeInt(int i) throws IOException;
    long readLong() throws IOException;
    void writeLong(long i) throws IOException;
    void close() throws IOException;
    boolean isReadOnly();
    boolean wasNio();
    void synch();
    boolean ensureLength(long newLong);
    boolean setLength(long newLength);
}