


package org.hsqldb.lib;

import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;


public class HsqlByteArrayInputStream extends InputStream
implements DataInput {

    protected byte[] buffer;
    protected int    pos;
    protected int    mark = 0;
    protected int    count;

    public HsqlByteArrayInputStream(byte[] buf) {

        this.buffer = buf;
        this.pos    = 0;
        this.count  = buf.length;
    }

    public HsqlByteArrayInputStream(byte[] buf, int offset, int length) {

        this.buffer = buf;
        this.pos    = offset;
        this.count  = Math.min(offset + length, buf.length);
        this.mark   = offset;
    }

    
    public final void readFully(byte[] b) throws IOException {
        readFully(b, 0, b.length);
    }

    public final void readFully(byte[] b, int off,
                                int len) throws IOException {

        if (len < 0) {
            throw new IndexOutOfBoundsException();
        }

        int n = 0;

        while (n < len) {
            int count = read(b, off + n, len - n);

            if (count < 0) {
                throw new EOFException();
            }

            n += count;
        }
    }

    public final boolean readBoolean() throws IOException {

        int ch = read();

        if (ch < 0) {
            throw new EOFException();
        }

        return (ch != 0);
    }

    public final byte readByte() throws IOException {

        int ch = read();

        if (ch < 0) {
            throw new EOFException();
        }

        return (byte) ch;
    }

    public final int readUnsignedByte() throws IOException {

        int ch = read();

        if (ch < 0) {
            throw new EOFException();
        }

        return ch;
    }

    public short readShort() throws IOException {

        if (count - pos < 2) {
            pos = count;

            throw new EOFException();
        }

        int ch1 = buffer[pos++] & 0xff;
        int ch2 = buffer[pos++] & 0xff;

        return (short) ((ch1 << 8) + (ch2));
    }

    public final int readUnsignedShort() throws IOException {

        int ch1 = read();
        int ch2 = read();

        if ((ch1 | ch2) < 0) {
            throw new EOFException();
        }

        return (ch1 << 8) + (ch2);
    }

    public final char readChar() throws IOException {

        int ch1 = read();
        int ch2 = read();

        if ((ch1 | ch2) < 0) {
            throw new EOFException();
        }

        return (char) ((ch1 << 8) + (ch2));
    }

    public int readInt() throws IOException {

        if (count - pos < 4) {
            pos = count;

            throw new EOFException();
        }

        int ch1 = buffer[pos++] & 0xff;
        int ch2 = buffer[pos++] & 0xff;
        int ch3 = buffer[pos++] & 0xff;
        int ch4 = buffer[pos++] & 0xff;

        return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4));
    }

    public long readLong() throws IOException {
        return (((long) readInt()) << 32) + (((long) readInt()) & 0xffffffffL);
    }

    public final float readFloat() throws IOException {
        return Float.intBitsToFloat(readInt());
    }

    public final double readDouble() throws IOException {
        return Double.longBitsToDouble(readLong());
    }

    public int skipBytes(int n) throws IOException {
        return (int) skip(n);
    }

    public String readLine() throws IOException {

        
        throw new java.lang.RuntimeException("not implemented.");
    }

    public String readUTF() throws IOException {

        int bytecount = readUnsignedShort();

        if (pos + bytecount >= count) {
            throw new EOFException();
        }

        String result = StringConverter.readUTF(buffer, pos, bytecount);

        pos += bytecount;

        return result;
    }


    public int read() {
        return (pos < count) ? (buffer[pos++] & 0xff)
                             : -1;
    }

    public int read(byte[] b, int off, int len) {

        if (pos >= count) {
            return -1;
        }

        if (pos + len > count) {
            len = count - pos;
        }

        if (len <= 0) {
            return 0;
        }

        System.arraycopy(buffer, pos, b, off, len);

        pos += len;

        return len;
    }

    public long skip(long n) {

        if (pos + n > count) {
            n = count - pos;
        }

        if (n < 0) {
            return 0;
        }

        pos += n;

        return n;
    }

    public int available() {
        return count - pos;
    }

    public boolean markSupported() {
        return true;
    }

    public void mark(int readAheadLimit) {
        mark = pos;
    }

    public void reset() {
        pos = mark;
    }

    public void close() throws IOException {}
}
