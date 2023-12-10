package org.hsqldb.persist;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import org.hsqldb.Database;
import org.hsqldb.lib.HsqlByteArrayOutputStream;
import org.hsqldb.lib.InputStreamInterface;
import org.hsqldb.lib.java.JavaSystem;
import org.hsqldb.store.BitMap;
public class RAShadowFile {
    final Database              database;
    final String                pathName;
    final RandomAccessInterface source;
    RandomAccessInterface       dest;
    final int                   pageSize;
    final long                  maxSize;
    final BitMap                bitMap;
    boolean                     zeroPageSet;
    long                        savedLength;
    HsqlByteArrayOutputStream byteArrayOutputStream =
        new HsqlByteArrayOutputStream(new byte[]{});
    RAShadowFile(Database database, RandomAccessInterface source,
                 String pathName, long maxSize, int pageSize) {
        this.database = database;
        this.pathName = pathName;
        this.source   = source;
        this.pageSize = pageSize;
        this.maxSize  = maxSize;
        int bitSize = (int) (maxSize / pageSize);
        if (maxSize % pageSize != 0) {
            bitSize++;
        }
        bitMap = new BitMap(bitSize);
    }
    void copy(long fileOffset, int size) throws IOException {
        if (!zeroPageSet) {
            copy(0);
            bitMap.set(0);
            zeroPageSet = true;
        }
        if (fileOffset >= maxSize) {
            return;
        }
        long endOffset       = fileOffset + size;
        int  startPageOffset = (int) (fileOffset / pageSize);
        int  endPageOffset   = (int) (endOffset / pageSize);
        if (endOffset % pageSize == 0) {
            endPageOffset--;
        }
        for (; startPageOffset <= endPageOffset; startPageOffset++) {
            copy(startPageOffset);
        }
    }
    private void copy(int pageOffset) throws IOException {
        if (bitMap.set(pageOffset) == 1) {
            return;
        }
        long position = (long) pageOffset * pageSize;
        int  readSize = pageSize;
        if (maxSize - position < pageSize) {
            readSize = (int) (maxSize - position);
        }
        if (dest == null) {
            open();
        }
        long writePos = dest.length();
        try {
            byte[] buffer = new byte[pageSize + 12];
            byteArrayOutputStream.setBuffer(buffer);
            byteArrayOutputStream.writeInt(pageSize);
            byteArrayOutputStream.writeLong(position);
            source.seek(position);
            source.read(buffer, 12, readSize);
            dest.seek(writePos);
            dest.write(buffer, 0, buffer.length);
            savedLength = writePos + buffer.length;
        } catch (Throwable t) {
            bitMap.unset(pageOffset);
            dest.seek(0);
            dest.setLength(writePos);
            close();
            database.logger.logWarningEvent("pos" + position + " " + readSize,
                                            t);
            throw JavaSystem.toIOException(t);
        } finally {}
    }
    private void open() throws IOException {
        if (database.logger.isStoredFileAccess()) {
            dest = ScaledRAFile.newScaledRAFile(database, pathName, false,
                                                ScaledRAFile.DATA_FILE_STORED);
        } else {
            dest = new ScaledRAFileSimple(database, pathName, "rws");
        }
    }
    void close() throws IOException {
        if (dest != null) {
            dest.synch();
            dest.close();
            dest = null;
        }
    }
    public void synch() {
        if (dest != null) {
            dest.synch();
        }
    }
    public long getSavedLength() {
        return savedLength;
    }
    public InputStreamInterface getInputStream() {
        return new InputStreamShadow();
    }
    private static RandomAccessInterface getStorage(Database database,
            String pathName, String openMode) throws IOException {
        if (database.logger.isStoredFileAccess()) {
            return ScaledRAFile.newScaledRAFile(database, pathName,
                                                openMode.equals("r"),
                                                ScaledRAFile.DATA_FILE_STORED);
        } else {
            return new ScaledRAFileSimple(database, pathName, openMode);
        }
    }
    public static void restoreFile(Database database, String sourceName,
                                   String destName) throws IOException {
        RandomAccessInterface source = getStorage(database, sourceName, "r");
        RandomAccessInterface dest   = getStorage(database, destName, "rw");
        while (source.getFilePointer() != source.length()) {
            int    size     = source.readInt();
            long   position = source.readLong();
            byte[] buffer   = new byte[size];
            source.read(buffer, 0, buffer.length);
            dest.seek(position);
            dest.write(buffer, 0, buffer.length);
        }
        source.close();
        dest.synch();
        dest.close();
    }
    class InputStreamShadow implements InputStreamInterface {
        FileInputStream is;
        long            limitSize   = 0;
        long            fetchedSize = 0;
        boolean         initialised = false;
        public int read() throws IOException {
            if (!initialised) {
                initialise();
            }
            if (fetchedSize == limitSize) {
                return -1;
            }
            int byteread = is.read();
            if (byteread >= 0) {
                fetchedSize++;
            }
            return byteread;
        }
        public int read(byte bytes[]) throws IOException {
            return read(bytes, 0, bytes.length);
        }
        public int read(byte bytes[], int offset,
                        int length) throws IOException {
            if (!initialised) {
                initialise();
            }
            if (fetchedSize == limitSize) {
                return -1;
            }
            if (limitSize >= 0 && limitSize - fetchedSize < length) {
                length = (int) (limitSize - fetchedSize);
            }
            int count = is.read(bytes, offset, length);
            if (count >= 0) {
                fetchedSize += count;
            }
            return count;
        }
        public long skip(long count) throws IOException {
            return 0;
        }
        public int available() throws IOException {
            return 0;
        }
        public void close() throws IOException {
            if (is != null) {
                is.close();
            }
        }
        public void setSizeLimit(long count) {
            limitSize = count;
        }
        public long getSizeLimit() {
            if (!initialised) {
                initialise();
            }
            return limitSize;
        }
        private void initialise() {
            if (savedLength > 0) {
                try {
                    is = new FileInputStream(pathName);
                } catch (FileNotFoundException e) {}
            }
            initialised = true;
            limitSize   = savedLength;
        }
    }
}