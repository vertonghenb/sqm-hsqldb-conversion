package org.hsqldb.lib;
public class OrderedHashSet extends HashSet implements HsqlList, Set {
    public OrderedHashSet() {
        super(8);
        isList = true;
    }
    public boolean remove(Object key) {
        int oldSize = size();
        super.removeObject(key, true);
        return oldSize != size();
    }
    public Object remove(int index) throws IndexOutOfBoundsException {
        checkRange(index);
        Object result = objectKeyTable[index];
        remove(result);
        return result;
    }
    public boolean insert(int index,
                          Object key) throws IndexOutOfBoundsException {
        if (index < 0 || index > size()) {
            throw new IndexOutOfBoundsException();
        }
        if (contains(key)) {
            return false;
        }
        if (index == size()) {
            return add(key);
        }
        Object[] set = toArray(new Object[size()]);
        super.clear();
        for (int i = 0; i < index; i++) {
            add(set[i]);
        }
        add(key);
        for (int i = index; i < set.length; i++) {
            add(set[i]);
        }
        return true;
    }
    public Object set(int index, Object key) throws IndexOutOfBoundsException {
        throw new IndexOutOfBoundsException();
    }
    public void add(int index, Object key) throws IndexOutOfBoundsException {
        throw new IndexOutOfBoundsException();
    }
    public Object get(int index) throws IndexOutOfBoundsException {
        checkRange(index);
        return objectKeyTable[index];
    }
    public int getIndex(Object key) {
        return getLookup(key, key.hashCode());
    }
    public int getLargestIndex(OrderedHashSet other) {
        int max = -1;
        for (int i = 0, size = other.size(); i < size; i++) {
            int index = getIndex(other.get(i));
            if (index > max) {
                max = index;
            }
        }
        return max;
    }
    public int getCommonElementCount(Set other) {
        int count = 0;
        for (int i = 0, size = size(); i < size; i++) {
            if (other.contains(objectKeyTable[i])) {
                count++;
            }
        }
        return count;
    }
    public static OrderedHashSet addAll(OrderedHashSet first,
                                          OrderedHashSet second) {
        if (second == null) {
            return first;
        }
        if (first == null) {
            first = new OrderedHashSet();
        }
        first.addAll(second);
        return first;
    }
    public static OrderedHashSet add(OrderedHashSet first,
                                          Object value) {
        if (value == null) {
            return first;
        }
        if (first == null) {
            first = new OrderedHashSet();
        }
        first.add(value);
        return first;
    }
    private void checkRange(int i) {
        if (i < 0 || i >= size()) {
            throw new IndexOutOfBoundsException();
        }
    }
}