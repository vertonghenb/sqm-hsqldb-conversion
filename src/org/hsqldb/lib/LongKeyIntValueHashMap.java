package org.hsqldb.lib;
import java.util.NoSuchElementException;
import org.hsqldb.store.BaseHashMap;
public class LongKeyIntValueHashMap extends BaseHashMap {
    private Set        keySet;
    private Collection values;
    public LongKeyIntValueHashMap() {
        this(8);
    }
    public LongKeyIntValueHashMap(boolean minimize) {
        this(8);
        minimizeOnEmpty = minimize;
    }
    public LongKeyIntValueHashMap(int initialCapacity)
    throws IllegalArgumentException {
        super(initialCapacity, BaseHashMap.longKeyOrValue,
              BaseHashMap.intKeyOrValue, false);
    }
    public int get(long key) throws NoSuchElementException {
        int lookup = getLookup(key);
        if (lookup != -1) {
            return intValueTable[lookup];
        }
        throw new NoSuchElementException();
    }
    public int get(long key, int defaultValue) {
        int lookup = getLookup(key);
        if (lookup != -1) {
            return intValueTable[lookup];
        }
        return defaultValue;
    }
    public boolean get(long key, int[] value) {
        int lookup = getLookup(key);
        if (lookup != -1) {
            value[0] = intValueTable[lookup];
            return true;
        }
        return false;
    }
    public int getLookup(long key) {
        return super.getLookup(key);
    }
    public boolean put(long key, int value) {
        int oldSize = size();
        super.addOrRemove(key, value, null, null, false);
        return oldSize != size();
    }
    public boolean remove(long key) {
        int oldSize = size();
        super.addOrRemove(key, 0, null, null, true);
        return oldSize != size();
    }
    public Set keySet() {
        if (keySet == null) {
            keySet = new KeySet();
        }
        return keySet;
    }
    public Collection values() {
        if (values == null) {
            values = new Values();
        }
        return values;
    }
    class KeySet implements Set {
        public Iterator iterator() {
            return LongKeyIntValueHashMap.this.new BaseHashIterator(true);
        }
        public int size() {
            return LongKeyIntValueHashMap.this.size();
        }
        public boolean contains(Object o) {
            throw new RuntimeException();
        }
        public Object get(Object key) {
            throw new RuntimeException();
        }
        public boolean add(Object value) {
            throw new RuntimeException();
        }
        public boolean addAll(Collection c) {
            throw new RuntimeException();
        }
        public boolean remove(Object o) {
            throw new RuntimeException();
        }
        public boolean isEmpty() {
            return size() == 0;
        }
        public void clear() {
            LongKeyIntValueHashMap.this.clear();
        }
    }
    class Values implements Collection {
        public Iterator iterator() {
            return LongKeyIntValueHashMap.this.new BaseHashIterator(false);
        }
        public int size() {
            return LongKeyIntValueHashMap.this.size();
        }
        public boolean contains(Object o) {
            throw new RuntimeException();
        }
        public boolean add(Object value) {
            throw new RuntimeException();
        }
        public boolean addAll(Collection c) {
            throw new RuntimeException();
        }
        public boolean remove(Object o) {
            throw new RuntimeException();
        }
        public boolean isEmpty() {
            return size() == 0;
        }
        public void clear() {
            LongKeyIntValueHashMap.this.clear();
        }
    }
}