package org.hsqldb.lib;
import org.hsqldb.store.BaseHashMap;
public class IntKeyHashMap extends BaseHashMap {
    Set        keySet;
    Collection values;
    public IntKeyHashMap() {
        this(8);
    }
    public IntKeyHashMap(int initialCapacity) throws IllegalArgumentException {
        super(initialCapacity, BaseHashMap.intKeyOrValue,
              BaseHashMap.objectKeyOrValue, false);
    }
    public Object get(int key) {
        int lookup = getLookup(key);
        if (lookup != -1) {
            return objectValueTable[lookup];
        }
        return null;
    }
    public Object put(int key, Object value) {
        return super.addOrRemove(key, value, null, false);
    }
    public boolean containsValue(Object value) {
        return super.containsValue(value);
    }
    public Object remove(int key) {
        return super.addOrRemove(key, null, null, true);
    }
    public boolean containsKey(int key) {
        return super.containsKey(key);
    }
    public void valuesToArray(Object[] array) {
        Iterator it = values().iterator();
        int      i  = 0;
        while (it.hasNext()) {
            array[i] = it.next();
            i++;
        }
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
            return IntKeyHashMap.this.new BaseHashIterator(true);
        }
        public int size() {
            return IntKeyHashMap.this.size();
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
            IntKeyHashMap.this.clear();
        }
    }
    class Values implements Collection {
        public Iterator iterator() {
            return IntKeyHashMap.this.new BaseHashIterator(false);
        }
        public int size() {
            return IntKeyHashMap.this.size();
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
            IntKeyHashMap.this.clear();
        }
    }
}