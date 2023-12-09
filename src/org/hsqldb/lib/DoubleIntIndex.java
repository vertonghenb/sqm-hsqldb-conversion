


package org.hsqldb.lib;

import java.util.NoSuchElementException;


public class DoubleIntIndex implements IntLookup {

    private int           count = 0;
    private int           capacity;
    private boolean       sorted       = true;
    private boolean       sortOnValues = true;
    private boolean       hasChanged;
    private final boolean fixedSize;
    private int[]         keys;
    private int[]         values;


    private int targetSearchValue;

    public DoubleIntIndex(int capacity, boolean fixedSize) {

        this.capacity  = capacity;
        keys           = new int[capacity];
        values         = new int[capacity];
        this.fixedSize = fixedSize;
        hasChanged     = true;
    }

    public synchronized int getKey(int i) {

        if (i < 0 || i >= count) {
            throw new IndexOutOfBoundsException();
        }

        return keys[i];
    }

    public synchronized int getValue(int i) {

        if (i < 0 || i >= count) {
            throw new IndexOutOfBoundsException();
        }

        return values[i];
    }

    
    public synchronized void setKey(int i, int key) {

        if (i < 0 || i >= count) {
            throw new IndexOutOfBoundsException();
        }

        if (!sortOnValues) {
            sorted = false;
        }

        keys[i] = key;
    }

    
    public synchronized void setValue(int i, int value) {

        if (i < 0 || i >= count) {
            throw new IndexOutOfBoundsException();
        }

        if (sortOnValues) {
            sorted = false;
        }

        values[i] = value;
    }

    public synchronized int size() {
        return count;
    }

    public synchronized int capacity() {
        return capacity;
    }

    
    public synchronized boolean addUnsorted(int key, int value) {

        if (count == capacity) {
            if (fixedSize) {
                return false;
            } else {
                doubleCapacity();
            }
        }

        if (sorted && count != 0) {
            if (sortOnValues) {
                if (value < values[count - 1]) {
                    sorted = false;
                }
            } else {
                if (key < keys[count - 1]) {
                    sorted = false;
                }
            }
        }

        hasChanged    = true;
        keys[count]   = key;
        values[count] = value;

        count++;

        return true;
    }

    
    public synchronized boolean addSorted(int key, int value) {

        if (count == capacity) {
            if (fixedSize) {
                return false;
            } else {
                doubleCapacity();
            }
        }

        if (count != 0) {
            if (sortOnValues) {
                if (value < values[count - 1]) {
                    return false;
                }
            } else {
                if (key < keys[count - 1]) {
                    return false;
                }
            }
        }

        hasChanged    = true;
        keys[count]   = key;
        values[count] = value;

        count++;

        return true;
    }

    
    public synchronized boolean addUnique(int key, int value) {

        if (count == capacity) {
            if (fixedSize) {
                return false;
            } else {
                doubleCapacity();
            }
        }

        if (!sorted) {
            fastQuickSort();
        }

        targetSearchValue = sortOnValues ? value
                                         : key;

        int i = binaryEmptySlotSearch();

        if (i == -1) {
            return false;
        }

        hasChanged = true;

        if (count != i) {
            moveRows(i, i + 1, count - i);
        }

        keys[i]   = key;
        values[i] = value;

        count++;

        return true;
    }

    
    public synchronized int add(int key, int value) {

        if (count == capacity) {
            if (fixedSize) {
                return -1;
            } else {
                doubleCapacity();
            }
        }

        if (!sorted) {
            fastQuickSort();
        }

        targetSearchValue = sortOnValues ? value
                                         : key;

        int i = binarySlotSearch();

        if (i == -1) {
            return i;
        }

        hasChanged = true;

        if (count != i) {
            moveRows(i, i + 1, count - i);
        }

        keys[i]   = key;
        values[i] = value;

        count++;

        return i;
    }

    public int lookup(int key) throws NoSuchElementException {

        if (sortOnValues) {
            sorted       = false;
            sortOnValues = false;
        }

        int i = findFirstEqualKeyIndex(key);

        if (i == -1) {
            throw new NoSuchElementException();
        }

        return getValue(i);
    }

    public int lookup(int key, int def) {

        if (sortOnValues) {
            sorted       = false;
            sortOnValues = false;
        }

        int i = findFirstEqualKeyIndex(key);

        if (i == -1) {
            return def;
        }

        return getValue(i);
    }

    public int lookupFirstGreaterEqual(int key) throws NoSuchElementException {

        if (sortOnValues) {
            sorted       = false;
            sortOnValues = false;
        }

        int i = findFirstGreaterEqualKeyIndex(key);

        if (i == -1) {
            throw new NoSuchElementException();
        }

        return getValue(i);
    }

    public synchronized void setValuesSearchTarget() {

        if (!sortOnValues) {
            sorted = false;
        }

        sortOnValues = true;
    }

    public synchronized void setKeysSearchTarget() {

        if (sortOnValues) {
            sorted = false;
        }

        sortOnValues = false;
    }

    
    public synchronized int findFirstGreaterEqualKeyIndex(int value) {

        int index = findFirstGreaterEqualSlotIndex(value);

        return index == count ? -1
                              : index;
    }

    
    public synchronized int findFirstEqualKeyIndex(int value) {

        if (!sorted) {
            fastQuickSort();
        }

        targetSearchValue = value;

        return binaryFirstSearch();
    }

    
    public synchronized int findFirstGreaterEqualSlotIndex(int value) {

        if (!sorted) {
            fastQuickSort();
        }

        targetSearchValue = value;

        return binarySlotSearch();
    }

    
    private int binaryFirstSearch() {

        int low     = 0;
        int high    = count;
        int mid     = 0;
        int compare = 0;
        int found   = count;

        while (low < high) {
            mid     = (low + high) / 2;
            compare = compare(mid);

            if (compare < 0) {
                high = mid;
            } else if (compare > 0) {
                low = mid + 1;
            } else {
                high  = mid;
                found = mid;
            }
        }

        return found == count ? -1
                              : found;
    }

    
    private int binaryGreaterSearch() {

        int low     = 0;
        int high    = count;
        int mid     = 0;
        int compare = 0;

        while (low < high) {
            mid     = (low + high) / 2;
            compare = compare(mid);

            if (compare < 0) {
                high = mid;
            } else {
                low = mid + 1;
            }
        }

        return low == count ? -1
                            : low;
    }

    
    private int binarySlotSearch() {

        int low     = 0;
        int high    = count;
        int mid     = 0;
        int compare = 0;

        while (low < high) {
            mid     = (low + high) / 2;
            compare = compare(mid);

            if (compare <= 0) {
                high = mid;
            } else {
                low = mid + 1;
            }
        }

        return low;
    }

    
    private int binaryEmptySlotSearch() {

        int low     = 0;
        int high    = count;
        int mid     = 0;
        int compare = 0;

        while (low < high) {
            mid     = (low + high) / 2;
            compare = compare(mid);

            if (compare < 0) {
                high = mid;
            } else if (compare > 0) {
                low = mid + 1;
            } else {
                return -1;
            }
        }

        return low;
    }

    private synchronized void fastQuickSort() {

        quickSort(0, count - 1);
        insertionSort(0, count - 1);

        sorted = true;
    }

    private void quickSort(int l, int r) {

        int M = 16;
        int i;
        int j;
        int v;

        if ((r - l) > M) {
            i = (r + l) / 2;

            if (lessThan(i, l)) {
                swap(l, i);    
            }

            if (lessThan(r, l)) {
                swap(l, r);
            }

            if (lessThan(r, i)) {
                swap(i, r);
            }

            j = r - 1;

            swap(i, j);

            i = l;
            v = j;

            for (;;) {
                while (lessThan(++i, v)) {}

                while (lessThan(v, --j)) {}

                if (j < i) {
                    break;
                }

                swap(i, j);
            }

            swap(i, r - 1);
            quickSort(l, j);
            quickSort(i + 1, r);
        }
    }

    private void insertionSort(int lo0, int hi0) {

        int i;
        int j;

        for (i = lo0 + 1; i <= hi0; i++) {
            j = i;

            while ((j > lo0) && lessThan(i, j - 1)) {
                j--;
            }

            if (i != j) {
                moveAndInsertRow(i, j);
            }
        }
    }

    protected void moveAndInsertRow(int i, int j) {

        int col1 = keys[i];
        int col2 = values[i];

        moveRows(j, j + 1, i - j);

        keys[j]   = col1;
        values[j] = col2;
    }

    protected void swap(int i1, int i2) {

        int col1 = keys[i1];
        int col2 = values[i1];

        keys[i1]   = keys[i2];
        values[i1] = values[i2];
        keys[i2]   = col1;
        values[i2] = col2;
    }

    
    protected int compare(int i) {

        if (sortOnValues) {
            if (targetSearchValue > values[i]) {
                return 1;
            } else if (targetSearchValue < values[i]) {
                return -1;
            }
        } else {
            if (targetSearchValue > keys[i]) {
                return 1;
            } else if (targetSearchValue < keys[i]) {
                return -1;
            }
        }

        return 0;
    }

    
    protected boolean lessThan(int i, int j) {

        if (sortOnValues) {
            if (values[i] < values[j]) {
                return true;
            }
        } else {
            if (keys[i] < keys[j]) {
                return true;
            }
        }

        return false;
    }

    protected void moveRows(int fromIndex, int toIndex, int rows) {
        System.arraycopy(keys, fromIndex, keys, toIndex, rows);
        System.arraycopy(values, fromIndex, values, toIndex, rows);
    }

    protected void doubleCapacity() {

        keys     = (int[]) ArrayUtil.resizeArray(keys, capacity * 2);
        values   = (int[]) ArrayUtil.resizeArray(values, capacity * 2);
        capacity *= 2;
    }

    public void removeRange(int start, int limit) {

        moveRows(limit, start, count - limit);

        count -= (limit - start);
    }

    public void removeAll() {

        hasChanged = true;

        ArrayUtil.clearArray(ArrayUtil.CLASS_CODE_INT, keys, 0, count);
        ArrayUtil.clearArray(ArrayUtil.CLASS_CODE_INT, values, 0, count);

        count = 0;
    }

    public final synchronized void remove(int position) {

        hasChanged = true;

        moveRows(position + 1, position, count - position - 1);

        count--;

        keys[count]   = 0;
        values[count] = 0;
    }
}
