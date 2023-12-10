package org.hsqldb.lib;
import org.hsqldb.store.BaseHashMap;
public class HashMap extends BaseHashMap {
    Set        keySet;
    Collection values;
    public HashMap() {
        this(8);
    }
    public HashMap(int initialCapacity) throws IllegalArgumentException {
        super(initialCapacity, BaseHashMap.objectKeyOrValue,
              BaseHashMap.objectKeyOrValue, false);
    }
    public Object get(Object key) {
        int hash   = key.hashCode();
        int lookup = getLookup(key, hash);
        if (lookup != -1) {
            return objectValueTable[lookup];
        }
        return null;
    }
    public Object put(Object key, Object value) {
        return super.addOrRemove(0, 0, key, value, false);
    }
    public Object remove(Object key) {
        return super.removeObject(key, false);
    }
    public boolean containsKey(Object key) {
        return super.containsKey(key);
    }
    public boolean containsValue(Object value) {
        return super.containsValue(value);
    }
    public void putAll(HashMap t) {
        Iterator it = t.keySet().iterator();
        while (it.hasNext()) {
            Object key = it.next();
            put(key, t.get(key));
        }
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
            return HashMap.this.new BaseHashIterator(true);
        }
        public int size() {
            return HashMap.this.size();
        }
        public boolean contains(Object o) {
            return containsKey(o);
        }
        public Object get(Object key) {
            int lookup = HashMap.this.getLookup(key, key.hashCode());
            if (lookup < 0) {
                return null;
            } else {
                return HashMap.this.objectKeyTable[lookup];
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
            HashMap.this.remove(o);
            return size() != oldSize;
        }
        public boolean isEmpty() {
            return size() == 0;
        }
        public void clear() {
            HashMap.this.clear();
        }
    }
    class Values implements Collection {
        public Iterator iterator() {
            return HashMap.this.new BaseHashIterator(false);
        }
        public int size() {
            return HashMap.this.size();
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
            HashMap.this.clear();
        }
    }
}