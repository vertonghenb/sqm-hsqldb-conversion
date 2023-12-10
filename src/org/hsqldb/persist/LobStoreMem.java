package org.hsqldb.persist;
import org.hsqldb.lib.HsqlArrayList;
public class LobStoreMem implements LobStore {
    final int     lobBlockSize;
    int           blocksInLargeBlock = 128;
    int           largeBlockSize;
    HsqlArrayList byteStoreList;
    public LobStoreMem(int lobBlockSize) {
        this.lobBlockSize = lobBlockSize;
        largeBlockSize    = lobBlockSize * blocksInLargeBlock;
        byteStoreList     = new HsqlArrayList();
    }
    public byte[] getBlockBytes(int blockAddress, int blockCount) {
        byte[] dataBytes       = new byte[blockCount * lobBlockSize];
        int    dataBlockOffset = 0;
        while (blockCount > 0) {
            int    largeBlockIndex   = blockAddress / blocksInLargeBlock;
            byte[] largeBlock = (byte[]) byteStoreList.get(largeBlockIndex);
            int    blockOffset       = blockAddress % blocksInLargeBlock;
            int    currentBlockCount = blockCount;
            if ((blockOffset + currentBlockCount) > blocksInLargeBlock) {
                currentBlockCount = blocksInLargeBlock - blockOffset;
            }
            System.arraycopy(largeBlock, blockOffset * lobBlockSize,
                             dataBytes, dataBlockOffset * lobBlockSize,
                             currentBlockCount * lobBlockSize);
            blockAddress    += currentBlockCount;
            dataBlockOffset += currentBlockCount;
            blockCount      -= currentBlockCount;
        }
        return dataBytes;
    }
    public void setBlockBytes(byte[] dataBytes, int blockAddress,
                              int blockCount) {
        int dataBlockOffset = 0;
        while (blockCount > 0) {
            int largeBlockIndex = blockAddress / blocksInLargeBlock;
            if (largeBlockIndex >= byteStoreList.size()) {
                byteStoreList.add(new byte[largeBlockSize]);
            }
            byte[] largeBlock = (byte[]) byteStoreList.get(largeBlockIndex);
            int    blockOffset       = blockAddress % blocksInLargeBlock;
            int    currentBlockCount = blockCount;
            if ((blockOffset + currentBlockCount) > blocksInLargeBlock) {
                currentBlockCount = blocksInLargeBlock - blockOffset;
            }
            System.arraycopy(dataBytes, dataBlockOffset * lobBlockSize,
                             largeBlock, blockOffset * lobBlockSize,
                             currentBlockCount * lobBlockSize);
            blockAddress    += currentBlockCount;
            dataBlockOffset += currentBlockCount;
            blockCount      -= currentBlockCount;
        }
    }
    public void setBlockBytes(byte[] dataBytes, long position, int offset,
                              int length) {
        while (length > 0) {
            int largeBlockIndex = (int) (position / largeBlockSize);
            if (largeBlockIndex >= byteStoreList.size()) {
                byteStoreList.add(new byte[largeBlockSize]);
            }
            byte[] largeBlock = (byte[]) byteStoreList.get(largeBlockIndex);
            int    offsetInLargeBlock = (int) (position % largeBlockSize);
            int    currentLength      = length;
            if ((offsetInLargeBlock + currentLength) > largeBlockSize) {
                currentLength = largeBlockSize - offsetInLargeBlock;
            }
            System.arraycopy(dataBytes, offset, largeBlock,
                             offsetInLargeBlock, currentLength);
            position += currentLength;
            offset   += currentLength;
            length   -= currentLength;
        }
    }
    public int getBlockSize() {
        return lobBlockSize;
    }
    public void close() {
        byteStoreList.clear();
    }
    public void synch() {}
}