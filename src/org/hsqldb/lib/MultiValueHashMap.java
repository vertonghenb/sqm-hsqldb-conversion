


package org.hsqldb.lib;

import org.hsqldb.store.BaseHashMap;


public class MultiValueHashMap extends BaseHashMap {

    Set        keySet;
    Collection values;
    Iterator   valueIterator;

    public MultiValueHashMap() {
        this(8);
    }

    public MultiValueHashMap(int initialCapacity)
    throws IllegalArgumentException {

        super(initialCapacity, BaseHashMap.objectKeyOrValue,
              BaseHashMap.objectKeyOrValue, false);

        super.multiValueTable = new boolean[super.objectValueTable.length];
    }

    public Iterator get(Object key) {

        int hash = key.hashCode();

        return super.getValuesIterator(key, hash);
    }

    public Object put(Object key, Object value) {
        return super.addOrRemoveMultiVal(0, 0, key, value, false, false);
    }

    public Object remove(Object key) {
        return super.addOrRemoveMultiVal(0, 0, key, null, true, false);
    }

    public Object remove(Object key, Object value) {
        return super.addOrRemoveMultiVal(0, 0, key, value, false, true);
    }

    public boolean containsKey(Object key) {
        return super.containsKey(key);
    }

    public boolean containsValue(Object value) {
        return super.containsValue(value);
    }

    public void putAll(HashMap t) {

        Iterator it = t.keySet.iterator();

        while (it.hasNext()) {
            Object key = it.next();

            put(key, t.get(key));
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
            return MultiValueHashMap.this.new MultiValueKeyIterator();
        }

        public int size() {
            return MultiValueHashMap.this.size();
        }

        public boolean contains(Object o) {
            return containsKey(o);
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

            int oldSize = size();

            MultiValueHashMap.this.remove(o);

            return size() != oldSize;
        }

        public boolean isEmpty() {
            return size() == 0;
        }

        public void clear() {
            MultiValueHashMap.this.clear();
        }
    }

    class Values implements Collection {

        public Iterator iterator() {
            return MultiValueHashMap.this.new BaseHashIterator(false);
        }

        public int size() {
            return MultiValueHashMap.this.size();
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
            MultiValueHashMap.this.clear();
        }
    }
}
