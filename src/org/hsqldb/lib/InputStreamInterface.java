package org.hsqldb.lib;
import java.io.IOException;
public interface InputStreamInterface {
    public int read() throws IOException;
    public int read(byte bytes[]) throws IOException;
    public int read(byte bytes[], int offset, int length) throws IOException;
    public long skip(long count) throws IOException;
    public int available() throws IOException;
    public void close() throws IOException;
    public void setSizeLimit(long count);
    public long getSizeLimit();
}