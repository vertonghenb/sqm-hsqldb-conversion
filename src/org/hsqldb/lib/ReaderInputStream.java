package org.hsqldb.lib;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
public class ReaderInputStream extends InputStream {
    protected Reader reader;
    protected long    pos;
    int              lastChar = -1;
    public ReaderInputStream(Reader reader) {
        this.reader = reader;
        this.pos    = 0;
    }
    public int read() throws IOException {
        if (lastChar >= 0) {
            int val = lastChar & 0xff;
            lastChar = -1;
            pos++;
            return val;
        }
        lastChar = reader.read();
        if (lastChar < 0) {
            return lastChar;
        }
        pos++;
        return lastChar >> 8;
    }
}