package org.hsqldb.lib;
import java.util.NoSuchElementException;
abstract class BaseList {
    protected int elementCount;
    abstract Object get(int index);
    abstract Object remove(int index);
    abstract boolean add(Object o);
    abstract int size();
    public boolean contains(Object o) {
        return indexOf(o) == -1 ? false
                             : true;
    }
    public boolean remove(Object o) {
        int i = indexOf(o);
        if (i == -1) {
            return false;
        }
        remove(i);
        return true;
    }
    public int indexOf(Object o) {
        for (int i = 0, size = size(); i < size; i++) {
            Object current = get(i);
            if (current == null) {
                if (o == null) {
                    return i;
                }
            } else if (current.equals(o)) {
                return i;
            }
        }
        return -1;
    }
    public boolean addAll(Collection other) {
        boolean  result = false;
        Iterator it     = other.iterator();
        while (it.hasNext()) {
            result = true;
            add(it.next());
        }
        return result;
    }
    public boolean addAll(Object[] array) {
        boolean  result = false;
        for (int i = 0; i < array.length; i++) {
            result = true;
            add(array[i]);
        }
        return result;
    }
    public boolean isEmpty() {
        return elementCount == 0;
    }
    public String toString() {
        StringBuffer sb = new StringBuffer(32 + elementCount * 3);
        sb.append("List : size=");
        sb.append(elementCount);
        sb.append(' ');
        sb.append('{');
        Iterator it = iterator();
        while (it.hasNext()) {
            sb.append(it.next());
            if (it.hasNext()) {
                sb.append(',');
                sb.append(' ');
            }
        }
        sb.append('}');
        return sb.toString();
    }
    public Iterator iterator() {
        return new BaseListIterator();
    }
    private class BaseListIterator implements Iterator {
        int     counter = 0;
        boolean removed;
        public boolean hasNext() {
            return counter < elementCount;
        }
        public Object next() {
            if (counter < elementCount) {
                removed = false;
                Object returnValue = get(counter);
                counter++;
                return returnValue;
            }
            throw new NoSuchElementException();
        }
        public int nextInt() {
            throw new NoSuchElementException();
        }
        public long nextLong() {
            throw new NoSuchElementException();
        }
        public void remove() {
            if (removed) {
                throw new NoSuchElementException("Iterator");
            }
            removed = true;
            if (counter != 0) {
                BaseList.this.remove(counter - 1);
                counter--;    
                return;
            }
            throw new NoSuchElementException();
        }
        public void setValue(Object value) {
            throw new NoSuchElementException();
        }
    }
}