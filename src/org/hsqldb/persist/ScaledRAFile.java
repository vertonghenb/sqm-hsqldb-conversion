package org.hsqldb.persist;
import java.io.EOFException;
import java.io.FileDescriptor;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Constructor;
import org.hsqldb.Database;
import org.hsqldb.lib.HsqlByteArrayInputStream;
import org.hsqldb.lib.HsqlByteArrayOutputStream;
final class ScaledRAFile implements RandomAccessInterface {
    static final int DATA_FILE_RAF    = 0;
    static final int DATA_FILE_NIO    = 1;
    static final int DATA_FILE_JAR    = 2;
    static final int DATA_FILE_STORED = 3;
    static final int DATA_FILE_SINGLE = 4;
    static final int DATA_FILE_TEXT   = 5;
    static final int  bufferScale = 12;
    static final int  bufferSize  = 1 << bufferScale;
    static final long bufferMask  = 0xffffffffffffffffl << bufferScale;
    final Database                  database;
    final RandomAccessFile          file;
    final FileDescriptor            fileDescriptor;
    private final boolean           readOnly;
    final String                    fileName;
    final byte[]                    buffer;
    final HsqlByteArrayInputStream  ba;
    final byte[]                    valueBuffer;
    final HsqlByteArrayOutputStream vbao;
    final HsqlByteArrayInputStream  vbai;
    long                            bufferOffset;
    long                            fileLength;
    final boolean                   extendLength;
    long seekPosition;
    int  cacheHit;
    static RandomAccessInterface newScaledRAFile(Database database,
            String name, boolean readonly,
            int type) throws FileNotFoundException, IOException {
        if (type == DATA_FILE_STORED) {
            try {
                String cname = database.getURLProperties().getProperty(
                    HsqlDatabaseProperties.url_storage_class_name);
                String skey = database.getURLProperties().getProperty(
                    HsqlDatabaseProperties.url_storage_key);
                Class       zclass      = Class.forName(cname);
                Constructor constructor = zclass.getConstructor(new Class[] {
                    String.class, Boolean.class, Object.class
                });
                return (RandomAccessInterface) constructor.newInstance(
                    new Object[] {
                    name, new Boolean(readonly), skey
                });
            } catch (ClassNotFoundException e) {
                throw new IOException();
            } catch (NoSuchMethodException e) {
                throw new IOException();
            } catch (InstantiationException e) {
                throw new IOException();
            } catch (IllegalAccessException e) {
                throw new IOException();
            } catch (java.lang.reflect.InvocationTargetException e) {
                throw new IOException();
            }
        }
        if (type == DATA_FILE_JAR) {
            return new ScaledRAFileInJar(name);
        } else if (type == DATA_FILE_TEXT) {
            ScaledRAFile ra = new ScaledRAFile(database, name, readonly,
                                               false);
            return ra;
        } else if (type == DATA_FILE_RAF) {
            return new ScaledRAFile(database, name, readonly, true);
        } else {
            java.io.File fi     = new java.io.File(name);
            long         length = fi.length();
            if (length > database.logger.propNioMaxSize) {
                return new ScaledRAFile(database, name, readonly, true);
            }
            try {
                Class.forName("java.nio.MappedByteBuffer");
                return new ScaledRAFileHybrid(database, name, readonly);
            } catch (Exception e) {
                return new ScaledRAFile(database, name, readonly, true);
            }
        }
    }
    ScaledRAFile(Database database, String name, boolean readonly,
                 boolean extendLengthToBlock)
                 throws FileNotFoundException, IOException {
        this.database     = database;
        this.fileName     = name;
        this.readOnly     = readonly;
        this.extendLength = extendLengthToBlock;
        String accessMode = readonly ? "r"
                                     : extendLength ? "rw"
                                                    : "rws";
        this.file      = new RandomAccessFile(name, accessMode);
        buffer         = new byte[bufferSize];
        ba             = new HsqlByteArrayInputStream(buffer);
        valueBuffer    = new byte[8];
        vbao           = new HsqlByteArrayOutputStream(valueBuffer);
        vbai           = new HsqlByteArrayInputStream(valueBuffer);
        fileDescriptor = file.getFD();
        fileLength     = length();
        readIntoBuffer();
    }
    public long length() throws IOException {
        return file.length();
    }
    public void seek(long position) throws IOException {
        if (readOnly && fileLength < position) {
            throw new IOException("read beyond end of file");
        }
        seekPosition = position;
    }
    public long getFilePointer() throws IOException {
        return seekPosition;
    }
    private void readIntoBuffer() throws IOException {
        long filePos    = seekPosition & bufferMask;
        long readLength = fileLength - filePos;
        if (readLength > buffer.length) {
            readLength = buffer.length;
        }
        if (readLength < 0) {
            throw new IOException("read beyond end of file");
        }
        try {
            file.seek(filePos);
            file.readFully(buffer, 0, (int) readLength);
            bufferOffset = filePos;
        } catch (IOException e) {
            resetPointer();
            database.logger.logWarningEvent(" " + filePos + " " + readLength,
                                            e);
            throw e;
        }
    }
    public int read() throws IOException {
        try {
            if (seekPosition >= fileLength) {
                return -1;
            }
            if (seekPosition < bufferOffset
                    || seekPosition >= bufferOffset + buffer.length) {
                readIntoBuffer();
            } else {
                cacheHit++;
            }
            int val = buffer[(int) (seekPosition - bufferOffset)] & 0xff;
            seekPosition++;
            return val;
        } catch (IOException e) {
            resetPointer();
            database.logger.logWarningEvent("read failed", e);
            throw e;
        }
    }
    public long readLong() throws IOException {
        vbai.reset();
        read(valueBuffer, 0, 8);
        return vbai.readLong();
    }
    public int readInt() throws IOException {
        vbai.reset();
        read(valueBuffer, 0, 4);
        return vbai.readInt();
    }
    public void read(byte[] b, int offset, int length) throws IOException {
        try {
            if (seekPosition + length > fileLength) {
                throw new EOFException();
            }
            if (length > buffer.length
                    && (seekPosition < bufferOffset
                        || seekPosition >= bufferOffset + buffer.length)) {
                file.seek(seekPosition);
                file.readFully(b, offset, length);
                seekPosition += length;
                return;
            }
            if (seekPosition < bufferOffset
                    || seekPosition >= bufferOffset + buffer.length) {
                readIntoBuffer();
            } else {
                cacheHit++;
            }
            ba.reset();
            if (seekPosition - bufferOffset
                    != ba.skip(seekPosition - bufferOffset)) {
                throw new EOFException();
            }
            int bytesRead = ba.read(b, offset, length);
            seekPosition += bytesRead;
            if (bytesRead < length) {
                file.seek(seekPosition);
                file.readFully(b, offset + bytesRead, length - bytesRead);
                seekPosition += (length - bytesRead);
            }
        } catch (IOException e) {
            resetPointer();
            database.logger.logWarningEvent("failed to read a byte array", e);
            throw e;
        }
    }
    public void write(byte[] b, int off, int length) throws IOException {
        try {
            file.seek(seekPosition);
            if (seekPosition < bufferOffset + buffer.length
                    && seekPosition + length > bufferOffset) {
                writeToBuffer(b, off, length);
            }
            file.write(b, off, length);
            seekPosition += length;
            if (!extendLength && fileLength < seekPosition) {
                fileLength = seekPosition;
            }
        } catch (IOException e) {
            resetPointer();
            database.logger.logWarningEvent("failed to write a byte array", e);
            throw e;
        }
    }
    public void writeInt(int i) throws IOException {
        vbao.reset();
        vbao.writeInt(i);
        write(valueBuffer, 0, 4);
    }
    public void writeLong(long i) throws IOException {
        vbao.reset();
        vbao.writeLong(i);
        write(valueBuffer, 0, 8);
    }
    public void close() throws IOException {
        file.close();
    }
    public boolean isReadOnly() {
        return readOnly;
    }
    public boolean wasNio() {
        return false;
    }
    public boolean ensureLength(long newLength) {
        if (newLength <= fileLength) {
            return true;
        }
        try {
            extendLength(newLength);
        } catch (IOException e) {
            return false;
        }
        return true;
    }
    public boolean setLength(long newLength) {
        try {
            file.setLength(newLength);
            file.seek(0);
            fileLength   = file.length();
            seekPosition = 0;
            readIntoBuffer();
            return true;
        } catch (Throwable t) {
            return false;
        }
    }
    public void synch() {
        try {
            fileDescriptor.sync();
        } catch (IOException e) {
            database.logger.logSevereEvent("RA file sync error ", e);
        }
    }
    private void writeToBuffer(byte[] b, int off, int len) throws IOException {
        int copyLength = len;
        int copyOffset = off;
        int bufferPos  = (int) (seekPosition - bufferOffset);
        if (bufferPos < 0) {
            copyOffset -= bufferPos;
            copyLength += bufferPos;
            bufferPos  = 0;
        }
        int maxLength = (int) (bufferOffset + buffer.length - seekPosition);
        if (maxLength < copyLength) {
            copyLength = maxLength;
        }
        System.arraycopy(b, copyOffset, buffer, bufferPos, copyLength);
    }
    private long getExtendLength(long position) {
        if (!extendLength) {
            return position;
        }
        int scaleUp;
        if (position < 256 * 1024) {
            scaleUp = 2;
        } else if (position < 1024 * 1024) {
            scaleUp = 6;
        } else if (position < 16 * 1024 * 1024) {
            scaleUp = 8;
        } else {
            scaleUp = 10;
        }
        position = getBinaryNormalisedCeiling(position, bufferScale + scaleUp);
        return position;
    }
    private void extendLength(long position) throws IOException {
        long newSize = getExtendLength(position);
        if (newSize > fileLength) {
            try {
                file.seek(newSize - 1);
                file.write(0);
                fileLength = newSize;
            } catch (IOException e) {
                database.logger.logWarningEvent("data file enlarge failed ",
                                                e);
                throw e;
            }
        }
    }
    private void resetPointer() {
        try {
            seekPosition = 0;
            fileLength   = length();
            readIntoBuffer();
        } catch (Throwable e) {}
    }
    static long getBinaryNormalisedCeiling(long value, int scale) {
        long mask    = 0xffffffffffffffffl << scale;
        long newSize = value & mask;
        if (newSize != value) {
            newSize += 1 << scale;
        }
        return newSize;
    }
}