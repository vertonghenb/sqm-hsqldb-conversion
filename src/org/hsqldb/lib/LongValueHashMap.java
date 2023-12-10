package org.hsqldb.lib;
import java.util.NoSuchElementException;
import org.hsqldb.store.BaseHashMap;
public class LongValueHashMap extends BaseHashMap {
    Set keySet;
    public LongValueHashMap() {
        this(8);
    }
    public LongValueHashMap(int initialCapacity)
    throws IllegalArgumentException {
        super(initialCapacity, BaseHashMap.objectKeyOrValue,
              BaseHashMap.longKeyOrValue, false);
    }
    public long get(Object key) throws NoSuchElementException {
        if (key == null) {
            throw new NoSuchElementException();
        }
        int hash   = key.hashCode();
        int lookup = getLookup(key, hash);
        if (lookup != -1) {
            return longValueTable[lookup];
        }
        throw new NoSuchElementException();
    }
    public long get(Object key, int defaultValue) {
        if (key == null) {
            throw new NoSuchElementException();
        }
        int hash   = key.hashCode();
        int lookup = getLookup(key, hash);
        if (lookup != -1) {
            return longValueTable[lookup];
        }
        return defaultValue;
    }
    public boolean get(Object key, long[] value) {
        if (key == null) {
            throw new NoSuchElementException();
        }
        int hash   = key.hashCode();
        int lookup = getLookup(key, hash);
        if (lookup != -1) {
            value[0] = longValueTable[lookup];
            return true;
        }
        return false;
    }
    public Object getKey(long value) {
        BaseHashIterator it = new BaseHashIterator(false);
        while (it.hasNext()) {
            long i = it.nextLong();
            if (i == value) {
                return objectKeyTable[it.getLookup()];
            }
        }
        return null;
    }
    public boolean put(Object key, long value) {
        if (key == null) {
            throw new NoSuchElementException();
        }
        int oldSize = size();
        super.addOrRemove(0, value, key, null, false);
        return oldSize != size();
    }
    public boolean remove(Object key) {
        int oldSize = size();
        super.addOrRemove(0, 0, key, null, true);
        return oldSize != size();
    }
    public boolean containsKey(Object key) {
        return super.containsKey(key);
    }
    public Set keySet() {
        if (keySet == null) {
            keySet = new KeySet();
        }
        return keySet;
    }
    class KeySet implements Set {
        public Iterator iterator() {
            return LongValueHashMap.this.new BaseHashIterator(true);
        }
        public int size() {
            return LongValueHashMap.this.size();
        }
        public boolean contains(Object o) {
            return containsKey(o);
        }
        public Object get(Object key) {
            int lookup = LongValueHashMap.this.getLookup(key, key.hashCode());
            if (lookup < 0) {
                return null;
            } else {
                return LongValueHashMap.this.objectKeyTable[lookup];
            }
        }
        public boolean add(Object value) {
            throw new RuntimeException();
        }
        public boolean addAll(Collection c) {
            throw new RuntimeException();
        }
        public boolean remove(Object o) {
            int oldSize = size();
            LongValueHashMap.this.remove(o);
            return size() != oldSize;
        }
        public boolean isEmpty() {
            return size() == 0;
        }
        public void clear() {
            LongValueHashMap.this.clear();
        }
    }
    public void putAll(LongValueHashMap t) {
        Iterator it = t.keySet().iterator();
        while (it.hasNext()) {
            Object key = it.next();
            put(key, t.get(key));
        }
    }
}