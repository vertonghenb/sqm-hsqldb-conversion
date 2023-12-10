package org.hsqldb.persist;
import org.hsqldb.Database;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
public class LobStoreRAFile implements LobStore {
    final int             lobBlockSize;
    RandomAccessInterface file;
    Database              database;
    public LobStoreRAFile(Database database, int lobBlockSize) {
        this.lobBlockSize = lobBlockSize;
        this.database     = database;
        try {
            String name = database.getPath() + ".lobs";
            boolean exists =
                database.logger.getFileAccess().isStreamElement(name);
            if (exists) {
                openFile();
            }
        } catch (Throwable t) {
            throw Error.error(ErrorCode.DATA_FILE_ERROR, t);
        }
    }
    private void openFile() {
        try {
            String  name     = database.getPath() + ".lobs";
            boolean readonly = database.isFilesReadOnly();
            if (database.logger.isStoredFileAccess()) {
                file = ScaledRAFile.newScaledRAFile(
                    database, name, readonly, ScaledRAFile.DATA_FILE_STORED);
            } else {
                file = new ScaledRAFileSimple(database, name, readonly ? "r"
                                                                       : "rws");
            }
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
            file.seek(address);
            file.read(dataBytes, 0, count);
            return dataBytes;
        } catch (Throwable t) {
            throw Error.error(ErrorCode.DATA_FILE_ERROR, t);
        }
    }
    public void setBlockBytes(byte[] dataBytes, int blockAddress,
                              int blockCount) {
        if (file == null) {
            openFile();
        }
        try {
            long address = (long) blockAddress * lobBlockSize;
            int  count   = blockCount * lobBlockSize;
            file.seek(address);
            file.write(dataBytes, 0, count);
        } catch (Throwable t) {
            throw Error.error(ErrorCode.DATA_FILE_ERROR, t);
        }
    }
    public void setBlockBytes(byte[] dataBytes, long position, int offset,
                              int length) {
        if (length == 0) {
            return;
        }
        if (file == null) {
            openFile();
        }
        try {
            file.seek(position);
            file.write(dataBytes, offset, length);
        } catch (Throwable t) {
            throw Error.error(ErrorCode.DATA_FILE_ERROR, t);
        }
    }
    public int getBlockSize() {
        return lobBlockSize;
    }
    public void close() {
        try {
            if (file != null) {
                file.synch();
                file.close();
            }
        } catch (Throwable t) {
            throw Error.error(ErrorCode.DATA_FILE_ERROR, t);
        }
    }
    public void synch() {
        if (file != null) {
            file.synch();
        }
    }
}