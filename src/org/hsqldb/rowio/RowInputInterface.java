


package org.hsqldb.rowio;

import java.io.IOException;

import org.hsqldb.types.Type;


public interface RowInputInterface {

    int getPos();

    int getSize();

    int readType() throws IOException;

    String readString() throws IOException;

    byte readByte() throws IOException;

    short readShort() throws IOException;

    int readInt() throws IOException;

    long readLong() throws IOException;

    Object[] readData(Type[] colTypes) throws IOException;

    void resetRow(int filePos, int size) throws IOException;

    byte[] getBuffer();
}
