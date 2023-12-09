


package org.hsqldb.lib;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;






public class ClosableByteArrayOutputStream extends OutputStream {

    
    protected byte[] buf;

    
    protected int count;

    
    protected boolean closed;

    
    protected boolean freed;

    
    public ClosableByteArrayOutputStream() {
        this(32);
    }

    
    public ClosableByteArrayOutputStream(int size)
    throws IllegalArgumentException {

        if (size < 0) {
            throw new IllegalArgumentException("Negative initial size: "
                                               + size);    
        }

        buf = new byte[size];
    }

    
    public synchronized void write(int b) throws IOException {

        checkClosed();

        int newcount = count + 1;

        if (newcount > buf.length) {
            buf = copyOf(buf, Math.max(buf.length << 1, newcount));
        }

        buf[count] = (byte) b;
        count      = newcount;
    }

    
    public synchronized void write(byte b[], int off,
                                   int len) throws IOException {

        checkClosed();

        if ((off < 0) || (off > b.length) || (len < 0)
                || ((off + len) > b.length) || ((off + len) < 0)) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return;
        }

        int newcount = count + len;

        if (newcount > buf.length) {
            buf = copyOf(buf, Math.max(buf.length << 1, newcount));
        }

        System.arraycopy(b, off, buf, count, len);

        count = newcount;
    }

    
    public void flush() throws IOException {
        checkClosed();
    }

    
    public synchronized void writeTo(OutputStream out) throws IOException {
        checkFreed();
        out.write(buf, 0, count);
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

    
    public synchronized byte[] toByteArray() throws IOException {

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

    
    public synchronized ByteArrayInputStream toByteArrayInputStream()
    throws IOException {

        checkFreed();

        ByteArrayInputStream inputStream = new ByteArrayInputStream(buf, 0,
            count);

        free();

        return inputStream;
    }

    
    public synchronized String toString() {

        try {
            checkFreed();
        } catch (IOException ex) {
            throw new RuntimeException(ex.toString());
        }

        return new String(buf, 0, count);
    }

    
    public synchronized String toString(String enc)
    throws IOException, UnsupportedEncodingException {

        checkFreed();

        return new String(buf, 0, count, enc);
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

            throw new IOException("stream is closed.");    
        }
    }

    
    protected synchronized void checkFreed() throws IOException {

        if (freed) {
            throw new IOException("stream buffer is freed.");    
        }
    }

    
    protected byte[] copyOf(byte[] original, int newLength) {

        byte[] copy = new byte[newLength];

        System.arraycopy(original, 0, copy, 0,
                         Math.min(original.length, newLength));

        return copy;
    }
}
