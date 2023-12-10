package org.hsqldb.lib;
import org.hsqldb.store.BaseHashMap;
public class HashSet extends BaseHashMap implements Set {
    public HashSet() {
        this(8);
    }
    public HashSet(int initialCapacity) throws IllegalArgumentException {
        super(initialCapacity, BaseHashMap.objectKeyOrValue,
              BaseHashMap.noKeyOrValue, false);
    }
    public boolean contains(Object key) {
        return super.containsKey(key);
    }
    public boolean containsAll(Collection col) {
        Iterator it = col.iterator();
        while (it.hasNext()) {
            if (contains(it.next())) {
                continue;
            }
            return false;
        }
        return true;
    }
    public Object get(Object key) {
        int lookup = getLookup(key, key.hashCode());
        if (lookup < 0) {
            return null;
        } else {
            return objectKeyTable[lookup];
        }
    }
    public boolean add(Object key) {
        int oldSize = size();
        super.addOrRemove(0, 0, key, null, false);
        return oldSize != size();
    }
    public boolean addAll(Collection c) {
        boolean  changed = false;
        Iterator it      = c.iterator();
        while (it.hasNext()) {
            changed |= add(it.next());
        }
        return changed;
    }
    public boolean addAll(Object[] keys) {
        boolean changed = false;
        for (int i = 0; i < keys.length; i++) {
            changed |= add(keys[i]);
        }
        return changed;
    }
    public boolean addAll(Object[] keys, int start, int limit) {
        boolean changed = false;
        for (int i = start; i < keys.length && i < limit; i++) {
            changed |= add(keys[i]);
        }
        return changed;
    }
    public boolean remove(Object key) {
        int oldSize = size();
        return super.removeObject(key, false) != null;
    }
    public boolean removeAll(Collection c) {
        Iterator it     = c.iterator();
        boolean  result = true;
        while (it.hasNext()) {
            result &= remove(it.next());
        }
        return result;
    }
    public boolean removeAll(Object[] keys) {
        boolean result = true;
        for (int i = 0; i < keys.length; i++) {
            result &= remove(keys[i]);
        }
        return result;
    }
    public Object[] toArray(Object[] a) {
        if (a == null || a.length < size()) {
            a = new Object[size()];
        }
        Iterator it = iterator();
        for (int i = 0; it.hasNext(); i++) {
            a[i] = it.next();
        }
        return a;
    }
    public Iterator iterator() {
        return new BaseHashIterator(true);
    }
    public String toString() {
        Iterator     it = iterator();
        StringBuffer sb = new StringBuffer();
        while (it.hasNext()) {
            if (sb.length() > 0) {
                sb.append(", ");
            } else {
                sb.append('[');
            }
            sb.append(it.next());
        }
        return sb.toString() + ']';
    }
}