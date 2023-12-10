package org.hsqldb.persist;
import java.io.IOException;
import org.hsqldb.Database;
public final class ScaledRAFileHybrid implements RandomAccessInterface {
    final Database        database;
    final String          fileName;
    final boolean         isReadOnly;
    boolean               wasNio;
    long                  maxLength = Long.MAX_VALUE;
    RandomAccessInterface store;
    public ScaledRAFileHybrid(Database database, String name,
                              boolean readOnly) throws IOException {
        this.database   = database;
        this.fileName   = name;
        this.isReadOnly = readOnly;
        long         fileLength;
        java.io.File fi = new java.io.File(name);
        fileLength = fi.length();
        newStore(fileLength);
    }
    public long length() throws IOException {
        return store.length();
    }
    public void seek(long position) throws IOException {
        store.seek(position);
    }
    public long getFilePointer() throws IOException {
        return store.getFilePointer();
    }
    public int read() throws IOException {
        return store.read();
    }
    public void read(byte[] b, int offset, int length) throws IOException {
        store.read(b, offset, length);
    }
    public void write(byte[] b, int offset, int length) throws IOException {
        store.write(b, offset, length);
    }
    public int readInt() throws IOException {
        return store.readInt();
    }
    public void writeInt(int i) throws IOException {
        store.writeInt(i);
    }
    public long readLong() throws IOException {
        return store.readLong();
    }
    public void writeLong(long i) throws IOException {
        store.writeLong(i);
    }
    public void close() throws IOException {
        store.close();
    }
    public boolean isReadOnly() {
        return store.isReadOnly();
    }
    public boolean wasNio() {
        return wasNio;
    }
    public boolean ensureLength(long newLength) {
        if (newLength < maxLength && store.ensureLength(newLength)) {
            return true;
        }
        if (wasNio && !store.wasNio()) {
            return false;
        }
        try {
            newStore(newLength);
        } catch (IOException e) {
            return false;
        }
        return store.ensureLength(newLength);
    }
    public boolean setLength(long newLength) {
        return store.setLength(newLength);
    }
    public Database getDatabase() {
        return null;
    }
    public void synch() {
        store.synch();
    }
    void newStore(long requiredPosition) throws IOException {
        long currentPosition = 0;
        if (store != null) {
            currentPosition = store.getFilePointer();
            store.synch();
            store.close();
        }
        if (wasNio) {
            maxLength = Long.MAX_VALUE;
        } else if (requiredPosition <= database.logger.propNioMaxSize) {
            if (requiredPosition >= ScaledRAFileNIO.largeBufferSize / 2) {
                try {
                    store =
                        new ScaledRAFileNIO(database, fileName, isReadOnly,
                                            requiredPosition,
                                            database.logger.propNioMaxSize);
                    store.seek(currentPosition);
                    wasNio = true;
                    return;
                } catch (Throwable e) {
                } finally {
                    maxLength = Long.MAX_VALUE;
                }
            } else {
                maxLength = ScaledRAFileNIO.largeBufferSize / 2;
            }
        }
        store = new ScaledRAFile(database, fileName, isReadOnly, true);
        store.seek(currentPosition);
    }
}