


package org.hsqldb.lib;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.hsqldb.store.BaseHashMap;


public class LongKeyHashMap extends BaseHashMap {

    Set        keySet;
    Collection values;

    
    ReentrantReadWriteLock           lock = new ReentrantReadWriteLock(true);
    ReentrantReadWriteLock.ReadLock  readLock  = lock.readLock();
    ReentrantReadWriteLock.WriteLock writeLock = lock.writeLock();

    public LongKeyHashMap() {
        this(16);
    }

    public LongKeyHashMap(int initialCapacity)
    throws IllegalArgumentException {
        super(initialCapacity, BaseHashMap.longKeyOrValue,
              BaseHashMap.objectKeyOrValue, false);
    }

    public Lock getWriteLock() {
        return writeLock;
    }

    public Object get(long key) {

        try {
            readLock.lock();

            int lookup = getLookup(key);

            if (lookup != -1) {
                return objectValueTable[lookup];
            }

            return null;
        } finally {
            readLock.unlock();
        }
    }

    public Object put(long key, Object value) {

        try {
            writeLock.lock();

            return super.addOrRemove(key, 0, null, value, false);
        } finally {
            writeLock.unlock();
        }
    }

    public boolean containsValue(Object value) {

        try {
            readLock.lock();

            return super.containsValue(value);
        } finally {
            readLock.unlock();
        }
    }

    public Object remove(long key) {

        try {
            writeLock.lock();

            return super.addOrRemove(key, 0, null, null, true);
        } finally {
            writeLock.unlock();
        }
    }

    public boolean containsKey(long key) {

        try {
            readLock.lock();

            return super.containsKey(key);
        } finally {
            readLock.unlock();
        }
    }

    public Object[] toArray() {

        try {
            readLock.lock();

            Object[] array = new Object[size()];
            int      i     = 0;
            Iterator it    = LongKeyHashMap.this.new BaseHashIterator(false);

            while (it.hasNext()) {
                array[i++] = it.next();
            }

            return array;
        } finally {
            readLock.unlock();
        }
    }

    public int getOrderedMatchCount(int[] array) {

        int i = 0;

        try {
            readLock.lock();

            for (; i < array.length; i++) {
                if (!super.containsKey(array[i])) {
                    break;
                }
            }
        } finally {
            readLock.unlock();
        }

        return i;
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
            return LongKeyHashMap.this.new BaseHashIterator(true);
        }

        public int size() {
            return LongKeyHashMap.this.size();
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
            LongKeyHashMap.this.clear();
        }
    }

    class Values implements Collection {

        public Iterator iterator() {
            return LongKeyHashMap.this.new BaseHashIterator(false);
        }

        public int size() {
            return LongKeyHashMap.this.size();
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
            LongKeyHashMap.this.clear();
        }
    }
}
