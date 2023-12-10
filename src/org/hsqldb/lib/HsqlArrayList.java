package org.hsqldb.lib;
import java.lang.reflect.Array;
import java.util.Comparator;
public class HsqlArrayList extends BaseList implements HsqlList {
    private static final int   DEFAULT_INITIAL_CAPACITY = 8;
    private static final float DEFAULT_RESIZE_FACTOR    = 2.0f;
    Object[]                   elementData;
    Object[]                   reserveElementData;
    private boolean            minimizeOnClear;
    public HsqlArrayList(Object[] data, int count) {
        elementData  = data;
        elementCount = count;
    }
    public HsqlArrayList() {
        elementData = new Object[DEFAULT_INITIAL_CAPACITY];
    }
    public HsqlArrayList(int initialCapacity, boolean minimize) {
        if (initialCapacity < DEFAULT_INITIAL_CAPACITY) {
            initialCapacity = DEFAULT_INITIAL_CAPACITY;
        }
        elementData     = new Object[initialCapacity];
        minimizeOnClear = minimize;
    }
    public HsqlArrayList(int initialCapacity) {
        if (initialCapacity < 0) {
            throw new NegativeArraySizeException(
                "Invalid initial capacity given");
        }
        if (initialCapacity < DEFAULT_INITIAL_CAPACITY) {
            initialCapacity = DEFAULT_INITIAL_CAPACITY;
        }
        elementData = new Object[initialCapacity];
    }
    public void add(int index, Object element) {
        if (index > elementCount) {
            throw new IndexOutOfBoundsException("Index out of bounds: "
                                                + index + ">" + elementCount);
        }
        if (index < 0) {
            throw new IndexOutOfBoundsException("Index out of bounds: "
                                                + index + " < 0");
        }
        if (elementCount >= elementData.length) {
            increaseCapacity();
        }
        for (int i = elementCount; i > index; i--) {
            elementData[i] = elementData[i - 1];
        }
        elementData[index] = element;
        elementCount++;
    }
    public boolean add(Object element) {
        if (elementCount >= elementData.length) {
            increaseCapacity();
        }
        elementData[elementCount] = element;
        elementCount++;
        return true;
    }
    public Object get(int index) {
        if (index >= elementCount) {
            throw new IndexOutOfBoundsException("Index out of bounds: "
                                                + index + " >= "
                                                + elementCount);
        }
        if (index < 0) {
            throw new IndexOutOfBoundsException("Index out of bounds: "
                                                + index + " < 0");
        }
        return elementData[index];
    }
    public int indexOf(Object o) {
        if (o == null) {
            for (int i = 0; i < elementCount; i++) {
                if (elementData[i] == null) {
                    return i;
                }
            }
            return -1;
        }
        for (int i = 0; i < elementCount; i++) {
            if (o.equals(elementData[i])) {
                return i;
            }
        }
        return -1;
    }
    public Object remove(int index) {
        if (index >= elementCount) {
            throw new IndexOutOfBoundsException("Index out of bounds: "
                                                + index + " >= "
                                                + elementCount);
        }
        if (index < 0) {
            throw new IndexOutOfBoundsException("Index out of bounds: "
                                                + index + " < 0");
        }
        Object removedObj = elementData[index];
        for (int i = index; i < elementCount - 1; i++) {
            elementData[i] = elementData[i + 1];
        }
        elementCount--;
        if (elementCount == 0) {
            clear();
        } else {
            elementData[elementCount] = null;
        }
        return removedObj;
    }
    public Object set(int index, Object element) {
        if (index >= elementCount) {
            throw new IndexOutOfBoundsException("Index out of bounds: "
                                                + index + " >= "
                                                + elementCount);
        }
        if (index < 0) {
            throw new IndexOutOfBoundsException("Index out of bounds: "
                                                + index + " < 0");
        }
        Object replacedObj = elementData[index];
        elementData[index] = element;
        return replacedObj;
    }
    public final int size() {
        return elementCount;
    }
    private void increaseCapacity() {
        int baseSize = elementData.length == 0 ? 1
                                               : elementData.length;
        baseSize = (int) (baseSize * DEFAULT_RESIZE_FACTOR);
        resize(baseSize);
    }
    private void resize(int baseSize) {
        if (baseSize == elementData.length) {
            return;
        }
        Object[] newArray = (Object[]) Array.newInstance(
            elementData.getClass().getComponentType(), baseSize);
        int count = elementData.length > newArray.length ? newArray.length
                                                         : elementData.length;
        System.arraycopy(elementData, 0, newArray, 0, count);
        if (minimizeOnClear && reserveElementData == null) {
            ArrayUtil.clearArray(ArrayUtil.CLASS_CODE_OBJECT, elementData, 0,
                                 elementData.length);
            reserveElementData = elementData;
        }
        elementData = newArray;
    }
    public void trim() {
        resize(elementCount);
    }
    public void clear() {
        if (minimizeOnClear && reserveElementData != null) {
            elementData        = reserveElementData;
            reserveElementData = null;
            elementCount       = 0;
            return;
        }
        for (int i = 0; i < elementCount; i++) {
            elementData[i] = null;
        }
        elementCount = 0;
    }
    public void setSize(int newSize) {
        if (newSize == 0) {
            clear();
            return;
        }
        if (newSize <= elementCount) {
            for (int i = newSize; i < elementCount; i++) {
                elementData[i] = null;
            }
            elementCount = newSize;
            return;
        }
        for (; newSize > elementData.length; ) {
            increaseCapacity();
        }
        elementCount = newSize;
    }
    public Object[] toArray() {
        Object[] newArray = (Object[]) Array.newInstance(
            elementData.getClass().getComponentType(), elementCount);
        System.arraycopy(elementData, 0, newArray, 0, elementCount);
        return newArray;
    }
    public Object[] toArray(int start, int limit) {
        Object[] newArray = (Object[]) Array.newInstance(
            elementData.getClass().getComponentType(), limit - start);
        System.arraycopy(elementData, start, newArray, 0, limit - start);
        return newArray;
    }
    public Object toArray(Object a) {
        if (Array.getLength(a) < elementCount) {
            a = Array.newInstance(a.getClass().getComponentType(),
                                  elementCount);
        }
        System.arraycopy(elementData, 0, a, 0, elementCount);
        return a;
    }
    public void sort(Comparator c) {
        if (elementCount < 2) {
            return;
        }
        ArraySort.sort(elementData, 0, elementCount, c);
    }
    public Object[] getArray() {
        return elementData;
    }
}