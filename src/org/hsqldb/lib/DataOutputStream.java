package org.hsqldb.lib;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
public class DataOutputStream extends java.io.BufferedOutputStream {
    byte[] tempBuffer = new byte[8];
    public DataOutputStream(OutputStream stream) {
        super(stream, 8);
    }
    public final void writeByte(int v) throws IOException {
        write(v);
    }
    public final void writeInt(int v) throws IOException {
        int count = 0;
        tempBuffer[count++] = (byte) (v >>> 24);
        tempBuffer[count++] = (byte) (v >>> 16);
        tempBuffer[count++] = (byte) (v >>> 8);
        tempBuffer[count++] = (byte) v;
        write(tempBuffer, 0, count);
    }
    public final void writeLong(long v) throws IOException {
        writeInt((int) (v >>> 32));
        writeInt((int) v);
    }
    public void writeChar(int v) throws IOException {
        int count = 0;
        tempBuffer[count++] = (byte) (v >>> 8);
        tempBuffer[count++] = (byte) v;
        write(tempBuffer, 0, count);
    }
    public void writeChars(String s) throws IOException {
        int len = s.length();
        for (int i = 0; i < len; i++) {
            int v     = s.charAt(i);
            int count = 0;
            tempBuffer[count++] = (byte) (v >>> 8);
            tempBuffer[count++] = (byte) v;
            write(tempBuffer, 0, count);
        }
    }
    public void writeChars(char[] c) throws IOException {
        writeChars(c, c.length);
    }
    public void writeChars(char[] c, int length) throws IOException {
        for (int i = 0; i < length; i++) {
            int v     = c[i];
            int count = 0;
            tempBuffer[count++] = (byte) (v >>> 8);
            tempBuffer[count++] = (byte) v;
            write(tempBuffer, 0, count);
        }
    }
    public long write(Reader reader, long length) throws IOException {
        InputStream inputStream = new ReaderInputStream(reader);
        return write(inputStream, length * 2) / 2;
    }
    public long write(InputStream inputStream,
                      long length) throws IOException {
        byte[] data = new byte[1024];
        long totalCount = 0;
        while (true) {
            long count = length - totalCount;
            if (count > data.length ) {
                count = data.length;
            }
            count = inputStream.read(data, 0, (int) count);
            if (count < 1) {
                break;
            }
            write(data, 0, (int) count);
            totalCount += count;
        }
        return totalCount;
    }
}