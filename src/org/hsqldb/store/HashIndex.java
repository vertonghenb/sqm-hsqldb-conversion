package org.hsqldb.store;
class HashIndex {
    int[]   hashTable;
    int[]   linkTable;
    int     newNodePointer;
    int     elementCount;
    int     reclaimedNodePointer = -1;
    boolean fixedSize;
    boolean modified;
    HashIndex(int hashTableSize, int capacity, boolean fixedSize) {
        if (capacity < hashTableSize) {
            capacity = hashTableSize;
        }
        reset(hashTableSize, capacity);
        this.fixedSize = fixedSize;
    }
    void reset(int hashTableSize, int capacity) {
        int[] newHT = new int[hashTableSize];
        int[] newLT = new int[capacity];
        hashTable = newHT;
        linkTable = newLT;
        resetTables();
    }
    void resetTables() {
        int   to       = hashTable.length;
        int[] intArray = hashTable;
        while (--to >= 0) {
            intArray[to] = -1;
        }
        newNodePointer       = 0;
        elementCount         = 0;
        reclaimedNodePointer = -1;
        modified             = false;
    }
    void clear() {
        int   to       = linkTable.length;
        int[] intArray = linkTable;
        while (--to >= 0) {
            intArray[to] = 0;
        }
        resetTables();
    }
    int getHashIndex(int hash) {
        return (hash & 0x7fffffff) % hashTable.length;
    }
    int getLookup(int hash) {
        if (elementCount == 0) {
            return -1;
        }
        int index = (hash & 0x7fffffff) % hashTable.length;
        return hashTable[index];
    }
    int getNextLookup(int lookup) {
        return linkTable[lookup];
    }
    int linkNode(int index, int lastLookup) {
        int lookup = reclaimedNodePointer;
        if (lookup == -1) {
            lookup = newNodePointer++;
        } else {
            reclaimedNodePointer = linkTable[lookup];
        }
        if (lastLookup == -1) {
            hashTable[index] = lookup;
        } else {
            linkTable[lastLookup] = lookup;
        }
        linkTable[lookup] = -1;
        elementCount++;
        modified = true;
        return lookup;
    }
    void unlinkNode(int index, int lastLookup, int lookup) {
        if (lastLookup == -1) {
            hashTable[index] = linkTable[lookup];
        } else {
            linkTable[lastLookup] = linkTable[lookup];
        }
        linkTable[lookup]    = reclaimedNodePointer;
        reclaimedNodePointer = lookup;
        elementCount--;
    }
    boolean removeEmptyNode(int lookup) {
        boolean found      = false;
        int     lastLookup = -1;
        for (int i = reclaimedNodePointer; i >= 0;
                lastLookup = i, i = linkTable[i]) {
            if (i == lookup) {
                if (lastLookup == -1) {
                    reclaimedNodePointer = linkTable[lookup];
                } else {
                    linkTable[lastLookup] = linkTable[lookup];
                }
                found = true;
                break;
            }
        }
        if (!found) {
            return false;
        }
        for (int i = 0; i < newNodePointer; i++) {
            if (linkTable[i] > lookup) {
                linkTable[i]--;
            }
        }
        System.arraycopy(linkTable, lookup + 1, linkTable, lookup,
                         newNodePointer - lookup - 1);
        linkTable[newNodePointer - 1] = 0;
        newNodePointer--;
        for (int i = 0; i < hashTable.length; i++) {
            if (hashTable[i] > lookup) {
                hashTable[i]--;
            }
        }
        return true;
    }
}