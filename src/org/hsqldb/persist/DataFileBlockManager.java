package org.hsqldb.persist;
import org.hsqldb.lib.DoubleIntIndex;
public class DataFileBlockManager {
    private DoubleIntIndex lookup;
    private final int      capacity;
    private int            midSize;
    private final int      scale;
    private final int      reuseMin;
    private long           releaseCount;
    private long           requestCount;
    private long           requestSize;
    long    lostFreeBlockSize;
    boolean isModified;
    public DataFileBlockManager(int capacity, int scale, int reuseMin,
                                long lostSize) {
        lookup = new DoubleIntIndex(capacity, true);
        lookup.setValuesSearchTarget();
        this.capacity          = capacity;
        this.scale             = scale;
        this.reuseMin          = reuseMin;
        this.lostFreeBlockSize = lostSize;
        this.midSize           = 128;    
    }
    void add(int pos, int rowSize) {
        isModified = true;
        if (capacity == 0 || rowSize < reuseMin) {
            lostFreeBlockSize += rowSize;
            return;
        }
        releaseCount++;
        if (lookup.size() == capacity) {
            resetList();
        }
        lookup.add(pos, rowSize);
    }
    int get(int rowSize) {
        if (capacity == 0 || rowSize < reuseMin) {
            return -1;
        }
        int index = lookup.findFirstGreaterEqualKeyIndex(rowSize);
        if (index == -1) {
            return -1;
        }
        requestCount++;
        requestSize += rowSize;
        int length     = lookup.getValue(index);
        int difference = length - rowSize;
        int key        = lookup.getKey(index);
        lookup.remove(index);
        if (difference >= midSize) {
            int pos = key + (rowSize / scale);
            lookup.add(pos, difference);
        } else {
            lostFreeBlockSize += difference;
        }
        return key;
    }
    int size() {
        return lookup.size();
    }
    long getLostBlocksSize() {
        return lostFreeBlockSize;
    }
    boolean isModified() {
        return isModified;
    }
    void clear() {
        removeBlocks(lookup.size());
    }
    private void resetList() {
        if (requestCount != 0) {
            midSize = (int) (requestSize / requestCount);
        }
        int first = lookup.findFirstGreaterEqualSlotIndex(midSize);
        if (first < lookup.size() / 4) {
            first = lookup.size() / 4;
        }
        removeBlocks(first);
    }
    private void removeBlocks(int blocks) {
        for (int i = 0; i < blocks; i++) {
            lostFreeBlockSize += lookup.getValue(i);
        }
        lookup.removeRange(0, blocks);
    }
    private void checkIntegrity() throws NullPointerException {}
}