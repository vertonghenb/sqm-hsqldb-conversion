package org.hsqldb.lib;
import java.io.CharArrayReader;
import java.io.CharArrayWriter;
import java.io.IOException;
import java.io.Writer;
public class ClosableCharArrayWriter extends Writer {
    protected char[] buf;
    protected int count;
    protected boolean closed;
    protected boolean freed;
    public ClosableCharArrayWriter() {
        this(32);
    }
    public ClosableCharArrayWriter(int size) throws IllegalArgumentException {
        if (size < 0) {
            throw new IllegalArgumentException("Negative initial size: "
                                               + size);    
        }
        buf = new char[size];
    }
    public synchronized void write(int c) throws IOException {
        checkClosed();
        int newcount = count + 1;
        if (newcount > buf.length) {
            buf = copyOf(buf, Math.max(buf.length << 1, newcount));
        }
        buf[count] = (char) c;
        count      = newcount;
    }
    public synchronized void write(char c[], int off,
                                   int len) throws IOException {
        checkClosed();
        if ((off < 0) || (off > c.length) || (len < 0)
                || ((off + len) > c.length) || ((off + len) < 0)) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return;
        }
        int newcount = count + len;
        if (newcount > buf.length) {
            buf = copyOf(buf, Math.max(buf.length << 1, newcount));
        }
        System.arraycopy(c, off, buf, count, len);
        count = newcount;
    }
    public synchronized void write(String str, int off,
                                   int len) throws IOException {
        checkClosed();
        int strlen = str.length();
        if ((off < 0) || (off > strlen) || (len < 0) || ((off + len) > strlen)
                || ((off + len) < 0)) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return;
        }
        int newcount = count + len;
        if (newcount > buf.length) {
            buf = copyOf(buf, Math.max(buf.length << 1, newcount));
        }
        str.getChars(off, off + len, buf, count);
        count = newcount;
    }
    public void flush() throws IOException {
        checkClosed();
    }
    public synchronized void writeTo(Writer out) throws IOException {
        checkFreed();
        if (count > 0) {
            out.write(buf, 0, count);
        }
    }
    public synchronized int capacity() throws IOException {
        checkFreed();
        return buf.length;
    }
    public synchronized void reset() throws IOException {
        checkClosed();
        count = 0;
    }
    public synchronized void trimToSize() throws IOException {
        checkFreed();
        if (buf.length > count) {
            buf = copyOf(buf, count);
        }
    }
    public synchronized char[] toCharArray() throws IOException {
        checkFreed();
        return copyOf(buf, count);
    }
    public synchronized int size() throws IOException {
        return count;
    }
    public synchronized void setSize(int newSize) {
        if (newSize < 0) {
            throw new ArrayIndexOutOfBoundsException(newSize);
        } else if (newSize > buf.length) {
            buf = copyOf(buf, Math.max(buf.length << 1, newSize));
        }
        count = newSize;
    }
    public synchronized CharArrayReader toCharArrayReader()
    throws IOException {
        checkFreed();
        CharArrayReader reader = new CharArrayReader(buf, 0, count);
        free();
        return reader;
    }
    public synchronized String toString() {
        try {
            checkFreed();
        } catch (IOException ex) {
            throw new RuntimeException(ex.toString());
        }
        return new String(buf, 0, count);
    }
    public synchronized void close() throws IOException {
        closed = true;
    }
    public synchronized boolean isClosed() {
        return closed;
    }
    public synchronized void free() throws IOException {
        closed = true;
        freed  = true;
        buf    = null;
        count  = 0;
    }
    public synchronized boolean isFreed() {
        return freed;
    }
    protected synchronized void checkClosed() throws IOException {
        if (closed) {
            throw new IOException("writer is closed.");    
        }
    }
    protected synchronized void checkFreed() throws IOException {
        if (freed) {
            throw new IOException("write buffer is freed.");    
        }
    }
    protected char[] copyOf(char[] original, int newLength) {
        char[] copy = new char[newLength];
        System.arraycopy(original, 0, copy, 0,
                         Math.min(original.length, newLength));
        return copy;
    }
}