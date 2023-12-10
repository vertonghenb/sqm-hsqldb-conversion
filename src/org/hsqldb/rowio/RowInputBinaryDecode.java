package org.hsqldb.rowio;
import java.io.IOException;
import org.hsqldb.persist.Crypto;
import org.hsqldb.types.Type;
public class RowInputBinaryDecode extends RowInputBinary {
    final Crypto crypto;
    public RowInputBinaryDecode(Crypto crypto, byte[] buf) {
        super(buf);
        this.crypto = crypto;
    }
    public Object[] readData(Type[] colTypes) throws IOException {
        if (crypto != null) {
            int start = pos;
            int size  = readInt();
            crypto.decode(buffer, pos, size, buffer, start);
            pos = start;
        }
        return super.readData(colTypes);
    }
}