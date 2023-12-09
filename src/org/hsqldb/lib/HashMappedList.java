


package org.hsqldb.lib;


public class HashMappedList extends HashMap {

    public HashMappedList() {
        this(8);
    }

    public HashMappedList(int initialCapacity)
    throws IllegalArgumentException {
        super(initialCapacity);

        isList = true;
    }

    public Object get(int index) throws IndexOutOfBoundsException {

        checkRange(index);

        return objectValueTable[index];
    }

    public Object remove(Object key) {

        int lookup = getLookup(key, key.hashCode());

        if (lookup < 0) {
            return null;
        }

        Object returnValue = super.remove(key);

        removeRow(lookup);

        return returnValue;
    }

    public Object remove(int index) throws IndexOutOfBoundsException {

        checkRange(index);

        return remove(objectKeyTable[index]);
    }

    public boolean add(Object key, Object value) {

        int lookup = getLookup(key, key.hashCode());

        if (lookup >= 0) {
            return false;
        }

        super.put(key, value);

        return true;
    }

    public Object put(Object key, Object value) {
        return super.put(key, value);
    }

    public Object set(int index,
                      Object value) throws IndexOutOfBoundsException {

        checkRange(index);

        Object returnValue = objectKeyTable[index];

        objectKeyTable[index] = value;

        return returnValue;
    }

    public boolean insert(int index, Object key,
                          Object value) throws IndexOutOfBoundsException {

        if (index < 0 || index > size()) {
            throw new IndexOutOfBoundsException();
        }

        int lookup = getLookup(key, key.hashCode());

        if (lookup >= 0) {
            return false;
        }

        if (index == size()) {
            return add(key, value);
        }

        HashMappedList hm = new HashMappedList(size());

        for (int i = index; i < size(); i++) {
            hm.add(getKey(i), get(i));
        }

        for (int i = size() - 1; i >= index; i--) {
            remove(i);
        }

        for (int i = 0; i < hm.size(); i++) {
            add(hm.getKey(i), hm.get(i));
        }

        return true;
    }

    public boolean set(int index, Object key,
                       Object value) throws IndexOutOfBoundsException {

        checkRange(index);

        if (keySet().contains(key) && getIndex(key) != index) {
            return false;
        }

        super.remove(objectKeyTable[index]);
        super.put(key, value);

        return true;
    }

    public boolean setKey(int index,
                          Object key) throws IndexOutOfBoundsException {

        checkRange(index);

        Object value = objectValueTable[index];

        return set(index, key, value);
    }

    public boolean setValue(int index,
                            Object value) throws IndexOutOfBoundsException {

        boolean result;
        Object  existing = objectValueTable[index];

        if (value == null) {
            result = value != existing;
        } else {
            result = !value.equals(existing);
        }

        objectValueTable[index] = value;

        return result;
    }

    public Object getKey(int index) throws IndexOutOfBoundsException {

        checkRange(index);

        return objectKeyTable[index];
    }

    public int getIndex(Object key) {
        return getLookup(key, key.hashCode());
    }

    public Object[] toValuesArray(Object[] a) {

        int size = size();

        if (a == null || a.length < size) {
            a = new Object[size];
        }

        for (int i = 0; i < size; i++) {
            a[i] = super.objectValueTable[i];
        }

        return a;
    }

    public Object[] toKeysArray(Object[] a) {

        int size = size();

        if (a == null || a.length < size) {
            a = new Object[size];
        }

        for (int i = 0; i < size; i++) {
            a[i] = super.objectKeyTable[i];
        }

        return a;
    }

    private void checkRange(int i) {

        if (i < 0 || i >= size()) {
            throw new IndexOutOfBoundsException();
        }
    }
}
