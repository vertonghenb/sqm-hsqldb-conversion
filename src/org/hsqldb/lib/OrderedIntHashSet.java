


package org.hsqldb.lib;

import org.hsqldb.store.BaseHashMap;


public class OrderedIntHashSet extends BaseHashMap {

    public OrderedIntHashSet() {
        this(8);
    }

    public OrderedIntHashSet(int initialCapacity)
    throws IllegalArgumentException {

        super(initialCapacity, BaseHashMap.intKeyOrValue,
              BaseHashMap.noKeyOrValue, false);

        isList = true;
    }

    public boolean contains(int key) {
        return super.containsKey(key);
    }

    public boolean add(int key) {

        int oldSize = size();

        super.addOrRemove(key, 0, null, null, false);

        return oldSize != size();
    }

    public boolean remove(int key) {

        int oldSize = size();

        super.addOrRemove(key, 0, null, null, true);

        boolean result = oldSize != size();

        if (result) {
            int[] array = toArray();

            super.clear();

            for (int i = 0; i < array.length; i++) {
                add(array[i]);
            }
        }

        return result;
    }

    public int get(int index) {

        checkRange(index);

        return intKeyTable[index];
    }

    public int getIndex(int value) {
        return getLookup(value);
    }

    public int getStartMatchCount(int[] array) {

        int i = 0;

        for (; i < array.length; i++) {
            if (!super.containsKey(array[i])) {
                break;
            }
        }

        return i;
    }

    public int getOrderedStartMatchCount(int[] array) {

        int i = 0;

        for (; i < array.length; i++) {
            if (i >= size() || get(i) != array[i]) {
                break;
            }
        }

        return i;
    }

    public boolean addAll(Collection col) {

        int      oldSize = size();
        Iterator it      = col.iterator();

        while (it.hasNext()) {
            add(it.nextInt());
        }

        return oldSize != size();
    }

    public int[] toArray() {

        int   lookup = -1;
        int[] array  = new int[size()];

        for (int i = 0; i < array.length; i++) {
            lookup = super.nextLookup(lookup);

            int value = intKeyTable[lookup];

            array[i] = value;
        }

        return array;
    }

    private void checkRange(int i) {

        if (i < 0 || i >= size()) {
            throw new IndexOutOfBoundsException();
        }
    }
}
