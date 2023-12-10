package org.hsqldb.lib;
import java.util.NoSuchElementException;
import org.hsqldb.store.BaseHashMap;
public class IntKeyLongValueHashMap extends BaseHashMap {
    public IntKeyLongValueHashMap() {
        this(8);
    }
    public IntKeyLongValueHashMap(int initialCapacity)
    throws IllegalArgumentException {
        super(initialCapacity, BaseHashMap.intKeyOrValue,
              BaseHashMap.longKeyOrValue, false);
    }
    public long get(int key) throws NoSuchElementException {
        int lookup = getLookup(key);
        if (lookup != -1) {
            return longValueTable[lookup];
        }
        throw new NoSuchElementException();
    }
    public long get(int key, int defaultValue) {
        int lookup = getLookup(key);
        if (lookup != -1) {
            return longValueTable[lookup];
        }
        return defaultValue;
    }
    public boolean get(int key, long[] value) {
        int lookup = getLookup(key);
        if (lookup != -1) {
            value[0] = longValueTable[lookup];
            return true;
        }
        return false;
    }
    public boolean put(int key, int value) {
        int oldSize = size();
        super.addOrRemove(key, value, null, null, false);
        return oldSize != size();
    }
    public boolean remove(int key) {
        int oldSize = size();
        super.addOrRemove(key, 0, null, null, true);
        return oldSize != size();
    }
}