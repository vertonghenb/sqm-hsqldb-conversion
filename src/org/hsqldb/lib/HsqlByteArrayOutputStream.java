package org.hsqldb.lib;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.UTFDataFormatException;
import java.io.UnsupportedEncodingException;
public class HsqlByteArrayOutputStream extends java.io.OutputStream
implements DataOutput {
    protected byte[] buffer;
    protected int    count;
    public HsqlByteArrayOutputStream() {
        this(128);
    }
    public HsqlByteArrayOutputStream(int size) {
        if (size < 128) {
            size = 128;
        }
        buffer = new byte[size];
    }
    public HsqlByteArrayOutputStream(byte[] buffer) {
        this.buffer = buffer;
    }
    public HsqlByteArrayOutputStream(InputStream input,
                                     int length) throws IOException {
        buffer = new byte[length];
        int used = write(input, length);
        if (used != length) {
            throw new EOFException();
        }
    }
    public HsqlByteArrayOutputStream(InputStream input) throws IOException {
        buffer = new byte[128];
        for (;;) {
            int read = input.read(buffer, count, buffer.length - count);
            if (read == -1) {
                break;
            }
            count += read;
            if (count == buffer.length) {
                ensureRoom(128);
            }
        }
    }
    public void writeShort(int v) {
        ensureRoom(2);
        buffer[count++] = (byte) (v >>> 8);
        buffer[count++] = (byte) v;
    }
    public void writeInt(int v) {
        if (count + 4 > buffer.length) {
            ensureRoom(4);
        }
        buffer[count++] = (byte) (v >>> 24);
        buffer[count++] = (byte) (v >>> 16);
        buffer[count++] = (byte) (v >>> 8);
        buffer[count++] = (byte) v;
    }
    public void writeLong(long v) {
        writeInt((int) (v >>> 32));
        writeInt((int) v);
    }
    public final void writeBytes(String s) {
        int len = s.length();
        ensureRoom(len);
        for (int i = 0; i < len; i++) {
            buffer[count++] = (byte) s.charAt(i);
        }
    }
    public final void writeFloat(float v) {
        writeInt(Float.floatToIntBits(v));
    }
    public final void writeDouble(double v) {
        writeLong(Double.doubleToLongBits(v));
    }
    public void writeBoolean(boolean v) {
        ensureRoom(1);
        buffer[count++] = (byte) (v ? 1
                                    : 0);
    }
    public void writeByte(int v) {
        ensureRoom(1);
        buffer[count++] = (byte) (v);
    }
    public void writeChar(int v) {
        ensureRoom(2);
        buffer[count++] = (byte) (v >>> 8);
        buffer[count++] = (byte) v;
    }
    public void writeChars(String s) {
        int len = s.length();
        ensureRoom(len * 2);
        for (int i = 0; i < len; i++) {
            int v = s.charAt(i);
            buffer[count++] = (byte) (v >>> 8);
            buffer[count++] = (byte) v;
        }
    }
    public void writeUTF(String str) throws IOException {
        int len = str.length();
        if (len > 0xffff) {
            throw new UTFDataFormatException();
        }
        ensureRoom(len * 3 + 2);
        int initpos = count;
        count += 2;
        StringConverter.stringToUTFBytes(str, this);
        int bytecount = count - initpos - 2;
        if (bytecount > 0xffff) {
            count = initpos;
            throw new UTFDataFormatException();
        }
        buffer[initpos++] = (byte) (bytecount >>> 8);
        buffer[initpos]   = (byte) bytecount;
    }
    public void flush() throws java.io.IOException {}
    public void write(int b) {
        ensureRoom(1);
        buffer[count++] = (byte) b;
    }
    public void write(byte[] b) {
        write(b, 0, b.length);
    }
    public void write(byte[] b, int off, int len) {
        ensureRoom(len);
        System.arraycopy(b, off, buffer, count, len);
        count += len;
    }
    public String toString() {
        return new String(buffer, 0, count);
    }
    public void close() throws IOException {}
    public void writeNoCheck(int b) {
        buffer[count++] = (byte) b;
    }
    public void writeChars(char[] charArray) {
        int len = charArray.length;
        ensureRoom(len * 2);
        for (int i = 0; i < len; i++) {
            int v = charArray[i];
            buffer[count++] = (byte) (v >>> 8);
            buffer[count++] = (byte) v;
        }
    }
    public int write(InputStream input, int countLimit) throws IOException {
        int left = countLimit;
        ensureRoom(countLimit);
        while (left > 0) {
            int read = input.read(buffer, count, left);
            if (read == -1) {
                break;
            }
            left  -= read;
            count += read;
        }
        return countLimit - left;
    }
    public int write(Reader input, int countLimit) throws IOException {
        int left = countLimit;
        ensureRoom(countLimit * 2);
        while (left > 0) {
            int c = input.read();
            if (c == -1) {
                break;
            }
            writeChar(c);
            left--;
        }
        return countLimit - left;
    }
    public void writeTo(OutputStream out) throws IOException {
        out.write(buffer, 0, count);
    }
    public void reset() {
        count = 0;
    }
    public byte[] toByteArray() {
        byte[] newbuf = new byte[count];
        System.arraycopy(buffer, 0, newbuf, 0, count);
        return newbuf;
    }
    public int size() {
        return count;
    }
    public void setPosition(int newPos) {
        if (newPos > buffer.length) {
            throw new ArrayIndexOutOfBoundsException();
        }
        count = newPos;
    }
    public String toString(String enc) throws UnsupportedEncodingException {
        return new String(buffer, 0, count, enc);
    }
    public void write(char[] c, int off, int len) {
        ensureRoom(len * 2);
        for (int i = off; i < len; i++) {
            int v = c[i];
            buffer[count++] = (byte) (v >>> 8);
            buffer[count++] = (byte) v;
        }
    }
    public void fill(int b, int len) {
        ensureRoom(len);
        for (int i = 0; i < len; i++) {
            buffer[count++] = (byte) b;
        }
    }
    public byte[] getBuffer() {
        return this.buffer;
    }
    public void setBuffer(byte[] buffer) {
        count       = 0;
        this.buffer = buffer;
    }
    public void ensureRoom(int extra) {
        int newcount = count + extra;
        int newsize  = buffer.length;
        if (newcount > newsize) {
            while (newcount > newsize) {
                newsize *= 2;
            }
            byte[] newbuf = new byte[newsize];
            System.arraycopy(buffer, 0, newbuf, 0, count);
            buffer = newbuf;
        }
    }
    public void reset(int newSize) {
        count = 0;
        if (newSize > buffer.length) {
            buffer = new byte[newSize];
        }
    }
    public void reset(byte[] buffer) {
        count       = 0;
        this.buffer = buffer;
    }
    public void setSize(int size) {
        if (size > buffer.length) {
            reset(size);
        }
        count = size;
    }
}