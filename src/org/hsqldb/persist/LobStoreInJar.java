package org.hsqldb.persist;
import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import org.hsqldb.Database;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
public class LobStoreInJar implements LobStore {
    final int       lobBlockSize;
    Database        database;
    DataInputStream file;
    final String    fileName;
    long realPosition;
    public LobStoreInJar(Database database, int lobBlockSize) {
        this.lobBlockSize = lobBlockSize;
        this.database     = database;
        try {
            fileName = database.getPath() + ".lobs";
        } catch (Throwable t) {
            throw Error.error(ErrorCode.DATA_FILE_ERROR, t);
        }
    }
    public byte[] getBlockBytes(int blockAddress, int blockCount) {
        if (file == null) {
            throw Error.error(ErrorCode.FILE_IO_ERROR);
        }
        try {
            long   address   = (long) blockAddress * lobBlockSize;
            int    count     = blockCount * lobBlockSize;
            byte[] dataBytes = new byte[count];
            fileSeek(address);
            file.readFully(dataBytes, 0, count);
            realPosition = address + count;
            return dataBytes;
        } catch (Throwable t) {
            throw Error.error(ErrorCode.DATA_FILE_ERROR, t);
        }
    }
    public void setBlockBytes(byte[] dataBytes, int blockAddress,
                              int blockCount) {}
    public void setBlockBytes(byte[] dataBytes, long position, int offset,
                              int length) {}
    public int getBlockSize() {
        return lobBlockSize;
    }
    public void close() {
        try {
            if (file != null) {
                file.close();
            }
        } catch (Throwable t) {
            throw Error.error(ErrorCode.DATA_FILE_ERROR, t);
        }
    }
    private void resetStream() throws IOException {
        if (file != null) {
            file.close();
        }
        InputStream fis = null;
        try {
            fis = getClass().getResourceAsStream(fileName);
            if (fis == null) {
                ClassLoader cl =
                    Thread.currentThread().getContextClassLoader();
                if (cl != null) {
                    fis = cl.getResourceAsStream(fileName);
                }
            }
        } catch (Throwable t) {
        } finally {
            if (fis == null) {
                throw new FileNotFoundException(fileName);
            }
        }
        file         = new DataInputStream(fis);
        realPosition = 0;
    }
    private void fileSeek(long position) throws IOException {
        if (file == null) {
            resetStream();
        }
        long skipPosition = realPosition;
        if (position < skipPosition) {
            resetStream();
            skipPosition = 0;
        }
        while (position > skipPosition) {
            skipPosition += file.skip(position - skipPosition);
        }
        realPosition = position;
    }
    public void synch() {}
}