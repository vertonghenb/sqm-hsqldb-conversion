


package org.hsqldb.lib;

import java.io.IOException;
import java.io.InputStream;


public class InputStreamWrapper
implements InputStreamInterface {

    InputStream is;
    long        limitSize   = -1;
    long        fetchedSize = 0;

    public InputStreamWrapper(InputStream is) {
        this.is = is;
    }

    public int read() throws IOException {

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

    public int read(byte bytes[], int offset, int length) throws IOException {

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
        return is.skip(count);
    }

    public int available() throws IOException {
        return is.available();
    }

    public void close() throws IOException {
        is.close();
    }

    public void setSizeLimit(long count) {
        limitSize = count;
    }

    public long getSizeLimit() {
        return limitSize;
    }
}
