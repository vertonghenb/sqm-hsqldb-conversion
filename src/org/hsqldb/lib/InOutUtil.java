package org.hsqldb.lib;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Serializable;
import java.io.Writer;
public final class InOutUtil {
    public static int readLine(InputStream in,
                               OutputStream out) throws IOException {
        int count = 0;
        for (;;) {
            int b = in.read();
            if (b == -1) {
                break;
            }
            count++;
            out.write(b);
            if (b == '\n') {
                break;
            }
        }
        return count;
    }
    public static byte[] serialize(Serializable s) throws IOException {
        HsqlByteArrayOutputStream bo = new HsqlByteArrayOutputStream();
        ObjectOutputStream        os = new ObjectOutputStream(bo);
        os.writeObject(s);
        return bo.toByteArray();
    }
    public static Serializable deserialize(byte[] ba)
    throws IOException, ClassNotFoundException {
        HsqlByteArrayInputStream bi = new HsqlByteArrayInputStream(ba);
        ObjectInputStream        is = new ObjectInputStream(bi);
        return (Serializable) is.readObject();
    }
    public static final int DEFAULT_COPY_BUFFER_SIZE = 8192;
    public static final long DEFAULT_COPY_AMOUNT = Long.MAX_VALUE;
    public static long copy(
            final InputStream inputStream,
            final OutputStream outputStream) throws IOException {
        return copy(inputStream, outputStream,
                DEFAULT_COPY_AMOUNT,
                DEFAULT_COPY_BUFFER_SIZE);
    }
    public static long copy(
            final InputStream inputStream,
            final OutputStream outputStream,
            final long amount) throws IOException {
        return copy(inputStream, outputStream, amount,
                DEFAULT_COPY_BUFFER_SIZE);
    }
    public static long copy(
            final InputStream inputStream,
            final OutputStream outputStream,
            final long amount,
            final int bufferSize) throws IOException {
        int maxBytesToRead = (int) Math.min((long) bufferSize, amount);
        final byte[] buffer = new byte[maxBytesToRead];
        long bytesCopied = 0;
        int bytesRead;        
        while ((bytesCopied < amount) && -1 != (bytesRead =
                inputStream.read(buffer, 0, maxBytesToRead))) {
            outputStream.write(buffer, 0, bytesRead);
            if (bytesRead > Long.MAX_VALUE - bytesCopied) {
                bytesCopied = Long.MAX_VALUE;
            } else {
                bytesCopied += bytesRead;
            }
            if (bytesCopied >= amount) {
                return bytesCopied;
            }
            maxBytesToRead = (int) Math.min((long) bufferSize,
                    amount - bytesCopied);
        }
        return bytesCopied;
    }
    public static long copy(
            final Reader reader,
            final Writer writer) throws IOException {
        return copy(reader, writer,
                DEFAULT_COPY_AMOUNT,
                DEFAULT_COPY_BUFFER_SIZE);
    }
    public static long copy(
            final Reader reader,
            final Writer writer,
            final long amount) throws IOException {
        return copy(reader, writer, amount,
                DEFAULT_COPY_BUFFER_SIZE);
    }
    public static long copy(
            final Reader reader,
            final Writer writer,
            final long amount,
            final int bufferSize) throws IOException {
        int maxCharsToRead = (int) Math.min((long) bufferSize, amount);
        final char[] buffer = new char[maxCharsToRead];
        long charsCopied = 0;
        int charsRead;        
        while ((charsCopied < amount) && -1 != (charsRead =
                reader.read(buffer, 0, maxCharsToRead))) {
            writer.write(buffer, 0, charsRead);
            if (charsRead > Long.MAX_VALUE - charsCopied) {
                charsCopied = Long.MAX_VALUE;
            } else {
                charsCopied += charsRead;
            }
            if (charsCopied >= amount) {
                return charsCopied;
            }
            maxCharsToRead = (int) Math.min((long) bufferSize,
                    amount - charsCopied);
        }
        return charsCopied;
    }
}