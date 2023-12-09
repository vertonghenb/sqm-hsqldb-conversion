


package org.hsqldb.persist;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.hsqldb.Database;


final class ScaledRAFileSimple implements RandomAccessInterface {

    final RandomAccessFile file;
    final boolean          readOnly;
    final Database         database;

    ScaledRAFileSimple(Database database, String name,
                       String openMode)
                       throws FileNotFoundException, IOException {

        this.file     = new RandomAccessFile(name, openMode);
        this.database = database;
        readOnly      = openMode.equals("r");
    }

    public long length() throws IOException {
        return file.length();
    }

    public void seek(long position) throws IOException {
        file.seek(position);
    }

    public long getFilePointer() throws IOException {
        return file.getFilePointer();
    }

    public int read() throws IOException {
        return file.read();
    }

    public long readLong() throws IOException {
        return file.readLong();
    }

    public int readInt() throws IOException {
        return file.readInt();
    }

    public void read(byte[] b, int offset, int length) throws IOException {
        file.readFully(b, offset, length);
    }

    public void write(byte[] b, int off, int len) throws IOException {
        file.write(b, off, len);
    }

    public void writeInt(int i) throws IOException {
        file.writeInt(i);
    }

    public void writeLong(long i) throws IOException {
        file.writeLong(i);
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

    public boolean ensureLength(long newLong) {
        return true;
    }

    public boolean setLength(long newLength) {

        try {
            file.setLength(newLength);

            return true;
        } catch (Throwable t) {
            return false;
        }
    }

    public Database getDatabase() {
        return null;
    }

    public void synch() {

        try {
            file.getFD().sync();
        } catch (IOException e) {
            database.logger.logSevereEvent("RA file sync error ", e);
        }
    }
}
