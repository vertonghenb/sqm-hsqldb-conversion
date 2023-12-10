package org.hsqldb.lib;
import java.util.NoSuchElementException;
import org.hsqldb.store.BaseHashMap;
public class IntValueHashMap extends BaseHashMap {
    Set                keySet;
    private Collection values;
    public IntValueHashMap() {
        this(8);
    }
    public IntValueHashMap(int initialCapacity)
    throws IllegalArgumentException {
        super(initialCapacity, BaseHashMap.objectKeyOrValue,
              BaseHashMap.intKeyOrValue, false);
    }
    public int get(Object key) throws NoSuchElementException {
        if (key == null) {
            throw new NoSuchElementException();
        }
        int hash   = key.hashCode();
        int lookup = getLookup(key, hash);
        if (lookup != -1) {
            return intValueTable[lookup];
        }
        throw new NoSuchElementException();
    }
    public int get(Object key, int defaultValue) {
        if (key == null) {
            throw new NoSuchElementException();
        }
        int hash   = key.hashCode();
        int lookup = getLookup(key, hash);
        if (lookup != -1) {
            return intValueTable[lookup];
        }
        return defaultValue;
    }
    public boolean get(Object key, int[] value) {
        if (key == null) {
            throw new NoSuchElementException();
        }
        int hash   = key.hashCode();
        int lookup = getLookup(key, hash);
        if (lookup != -1) {
            value[0] = intValueTable[lookup];
            return true;
        }
        return false;
    }
    public Object getKey(int value) {
        BaseHashIterator it = new BaseHashIterator(false);
        while (it.hasNext()) {
            int i = it.nextInt();
            if (i == value) {
                return objectKeyTable[it.getLookup()];
            }
        }
        return null;
    }
    public boolean put(Object key, int value) {
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
    public boolean containsValue(int value) {
        throw new RuntimeException();
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
            return IntValueHashMap.this.new BaseHashIterator(true);
        }
        public int size() {
            return IntValueHashMap.this.size();
        }
        public boolean contains(Object o) {
            return containsKey(o);
        }
        public Object get(Object key) {
            int lookup = IntValueHashMap.this.getLookup(key, key.hashCode());
            if (lookup < 0) {
                return null;
            } else {
                return IntValueHashMap.this.objectKeyTable[lookup];
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
            IntValueHashMap.this.remove(o);
            return size() != oldSize;
        }
        public boolean isEmpty() {
            return size() == 0;
        }
        public void clear() {
            IntValueHashMap.this.clear();
        }
    }
    class Values implements Collection {
        public Iterator iterator() {
            return IntValueHashMap.this.new BaseHashIterator(false);
        }
        public int size() {
            return IntValueHashMap.this.size();
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
            IntValueHashMap.this.clear();
        }
    }
    public void putAll(IntValueHashMap t) {
        Iterator it = t.keySet().iterator();
        while (it.hasNext()) {
            Object key = it.next();
            put(key, t.get(key));
        }
    }
}