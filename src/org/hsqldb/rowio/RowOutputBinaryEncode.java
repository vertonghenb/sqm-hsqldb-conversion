


package org.hsqldb.rowio;

import org.hsqldb.Row;
import org.hsqldb.persist.Crypto;
import org.hsqldb.types.Type;


public class RowOutputBinaryEncode extends RowOutputBinary {

    final Crypto crypto;

    public RowOutputBinaryEncode(Crypto crypto, int initialSize, int scale) {

        super(initialSize, scale);

        this.crypto = crypto;
    }

    public void writeData(Row row, Type[] types) {

        if (crypto == null) {
            super.writeData(row, types);
        } else {
            int start = count;

            ensureRoom(row.getStorageSize());
            writeInt(0);
            super.writeData(row, types);

            int origLength = count - start - INT_STORE_SIZE;
            int newLength = crypto.encode(buffer, start + INT_STORE_SIZE,
                                          origLength, buffer,
                                          start + INT_STORE_SIZE);

            writeIntData(newLength, start);

            count = start + INT_STORE_SIZE + newLength;
        }
    }

    
    public int getSize(Row row) {

        int size = super.getSize(row);

        if (crypto != null) {
            size = crypto.getEncodedSize(size - INT_STORE_SIZE)
                   + INT_STORE_SIZE * 2;
        }

        return size;
    }

    public RowOutputInterface duplicate() {
        return new RowOutputBinaryEncode(crypto, 128, this.scale);
    }
}
