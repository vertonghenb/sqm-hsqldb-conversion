


package org.hsqldb.lib;

import java.util.NoSuchElementException;




public class HsqlDeque extends BaseList implements HsqlList {

    private Object[] list;
    private int      firstindex = 0;    
    private int      endindex   = 0;    

    
    
    private static final int DEFAULT_INITIAL_CAPACITY = 10;

    public HsqlDeque() {
        list = new Object[DEFAULT_INITIAL_CAPACITY];
    }

    public int size() {
        return elementCount;
    }

    public boolean isEmpty() {
        return elementCount == 0;
    }

    public Object getFirst() throws NoSuchElementException {

        if (elementCount == 0) {
            throw new NoSuchElementException();
        }

        return list[firstindex];
    }

    public Object getLast() throws NoSuchElementException {

        if (elementCount == 0) {
            throw new NoSuchElementException();
        }

        return list[endindex - 1];
    }

    public Object get(int i) throws IndexOutOfBoundsException {

        int index = getInternalIndex(i);

        return list[index];
    }

    public void add(int i, Object o) throws IndexOutOfBoundsException {

        if (i == elementCount) {
            add(o);

            return;
        }

        resetCapacity();

        int index = getInternalIndex(i);

        if (index < endindex && endindex < list.length) {
            System.arraycopy(list, index, list, index + 1, endindex - index);
            endindex++;
        } else {
            System.arraycopy(list, firstindex, list, firstindex - 1,
                             index - firstindex);
            firstindex--;
        }

        list[index] = o;

        elementCount++;
    }

    public Object set(int i, Object o) throws IndexOutOfBoundsException {

        int    index  = getInternalIndex(i);
        Object result = list[index];

        list[index] = o;

        return result;
    }

    public Object removeFirst() throws NoSuchElementException {

        if (elementCount == 0) {
            throw new NoSuchElementException();
        }

        Object o = list[firstindex];

        list[firstindex] = null;

        firstindex++;
        elementCount--;

        if (elementCount == 0) {
            firstindex = endindex = 0;
        } else if (firstindex == list.length) {
            firstindex = 0;
        }

        return o;
    }

    public Object removeLast() throws NoSuchElementException {

        if (elementCount == 0) {
            throw new NoSuchElementException();
        }

        endindex--;

        Object o = list[endindex];

        list[endindex] = null;

        elementCount--;

        if (elementCount == 0) {
            firstindex = endindex = 0;
        } else if (endindex == 0) {
            endindex = list.length;
        }

        return o;
    }


    public boolean add(Object o) {

        resetCapacity();

        if (endindex == list.length) {
            endindex = 0;
        }

        list[endindex] = o;

        elementCount++;
        endindex++;

        return true;
    }

    public boolean addLast(Object o) {
        return add(o);
    }

    public boolean addFirst(Object o) {

        resetCapacity();

        firstindex--;

        if (firstindex < 0) {
            firstindex = list.length - 1;

            if (endindex == 0) {
                endindex = list.length;
            }
        }

        list[firstindex] = o;

        elementCount++;

        return true;
    }

    public void clear() {

        if (elementCount == 0) {
            return;
        }

        firstindex = endindex = elementCount = 0;

        for (int i = 0; i < list.length; i++) {
            list[i] = null;
        }
    }

    public int indexOf(Object value) {

        for (int i = 0; i < elementCount; i++) {
            int index = firstindex + i;

            if (index >= list.length) {
                index -= list.length;
            }

            if (list[index] == value) {
                return i;
            }

            if (value != null && value.equals(list[index])) {
                return i;
            }
        }

        return -1;
    }

    public Object remove(int index) {

        int    target = getInternalIndex(index);
        Object value  = list[target];

        if (target == firstindex) {
            list[firstindex] = null;

            firstindex++;

            if (firstindex == list.length) {
                firstindex = 0;
            }
        } else if (target > firstindex) {
            System.arraycopy(list, firstindex, list, firstindex + 1,
                             target - firstindex);

            list[firstindex] = null;

            firstindex++;

            if (firstindex == list.length) {
                firstindex = 0;
            }
        } else {
            System.arraycopy(list, target + 1, list, target,
                             endindex - target - 1);

            endindex--;

            list[endindex] = null;

            if (endindex == 0) {
                endindex = list.length;
            }
        }

        elementCount--;

        if (elementCount == 0) {
            firstindex = endindex = 0;
        }

        return value;
    }

    private int getInternalIndex(int i) throws IndexOutOfBoundsException {

        if (i < 0 || i >= elementCount) {
            throw new IndexOutOfBoundsException();
        }

        int index = firstindex + i;

        if (index >= list.length) {
            index -= list.length;
        }

        return index;
    }

    private void resetCapacity() {

        if (elementCount < list.length) {
            return;
        }

        Object[] newList = new Object[list.length * 2];

        System.arraycopy(list, firstindex, newList, firstindex,
                         list.length - firstindex);

        if (endindex <= firstindex) {
            System.arraycopy(list, 0, newList, list.length, endindex);

            endindex = list.length + endindex;
        }

        list = newList;
    }

    public void toArray(Object[] array) {

        int tempCount = list.length - firstindex;

        if (tempCount > elementCount) {
            tempCount = elementCount;
        }

        System.arraycopy(list, firstindex, array, 0, tempCount);

        if (endindex <= firstindex) {
            System.arraycopy(list, 0, array, tempCount, endindex);

            endindex = list.length + endindex;
        }
    }
}
