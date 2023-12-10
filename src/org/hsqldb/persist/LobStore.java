package org.hsqldb.persist;
public interface LobStore {
    byte[] getBlockBytes(int blockAddress, int blockCount);
    void setBlockBytes(byte[] dataBytes, int blockAddress, int blockCount);
    void setBlockBytes(byte[] dataBytes, long position, int offset,
                       int length);
    int getBlockSize();
    void close();
    public void synch();
}