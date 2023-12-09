


package org.hsqldb.store;

import java.util.NoSuchElementException;

import org.hsqldb.lib.ArrayCounter;
import org.hsqldb.lib.Iterator;


public class BaseHashMap {



    
    boolean           isIntKey;
    boolean           isLongKey;
    boolean           isObjectKey;
    boolean           isNoValue;
    boolean           isIntValue;
    boolean           isLongValue;
    boolean           isObjectValue;
    protected boolean isTwoObjectValue;
    protected boolean isList;

    
    private ValuesIterator valuesIterator;

    
    protected HashIndex hashIndex;

    
    protected int[]    intKeyTable;
    protected Object[] objectKeyTable;
    protected long[]   longKeyTable;

    
    protected int[]    intValueTable;
    protected Object[] objectValueTable;
    protected long[]   longValueTable;

    
    protected int       accessMin;
    protected int       accessCount;
    protected int[]     accessTable;
    protected boolean[] multiValueTable;
    protected Object[]  objectValueTable2;

    
    final float       loadFactor;
    final int         initialCapacity;
    int               threshold;
    protected int     maxCapacity;
    protected int     purgePolicy = NO_PURGE;
    protected boolean minimizeOnEmpty;

    
    boolean hasZeroKey;
    int     zeroKeyIndex = -1;

    
    protected static final int noKeyOrValue     = 0;
    protected static final int intKeyOrValue    = 1;
    protected static final int longKeyOrValue   = 2;
    protected static final int objectKeyOrValue = 3;

    
    protected static final int NO_PURGE      = 0;
    protected static final int PURGE_ALL     = 1;
    protected static final int PURGE_HALF    = 2;
    protected static final int PURGE_QUARTER = 3;

    
    public static final int ACCESS_MAX = Integer.MAX_VALUE - (1 << 20);

    protected BaseHashMap(int initialCapacity, int keyType, int valueType,
                          boolean hasAccessCount)
                          throws IllegalArgumentException {

        if (initialCapacity <= 0) {
            throw new IllegalArgumentException();
        }

        if (initialCapacity < 3) {
            initialCapacity = 3;
        }

        this.loadFactor      = 1;    
        this.initialCapacity = initialCapacity;
        threshold            = initialCapacity;

        int hashtablesize = (int) (initialCapacity * loadFactor);

        if (hashtablesize < 3) {
            hashtablesize = 3;
        }

        hashIndex = new HashIndex(hashtablesize, initialCapacity, true);

        int arraySize = threshold;

        if (keyType == BaseHashMap.intKeyOrValue) {
            isIntKey    = true;
            intKeyTable = new int[arraySize];
        } else if (keyType == BaseHashMap.objectKeyOrValue) {
            isObjectKey    = true;
            objectKeyTable = new Object[arraySize];
        } else {
            isLongKey    = true;
            longKeyTable = new long[arraySize];
        }

        if (valueType == BaseHashMap.intKeyOrValue) {
            isIntValue    = true;
            intValueTable = new int[arraySize];
        } else if (valueType == BaseHashMap.objectKeyOrValue) {
            isObjectValue    = true;
            objectValueTable = new Object[arraySize];
        } else if (valueType == BaseHashMap.longKeyOrValue) {
            isLongValue    = true;
            longValueTable = new long[arraySize];
        } else {
            isNoValue = true;
        }

        if (hasAccessCount) {
            accessTable = new int[arraySize];
        }
    }

    protected int getLookup(Object key, int hash) {

        int    lookup = hashIndex.getLookup(hash);
        Object tempKey;

        for (; lookup >= 0; lookup = hashIndex.getNextLookup(lookup)) {
            tempKey = objectKeyTable[lookup];

            if (key.equals(tempKey)) {
                return lookup;
            }
        }

        return lookup;
    }

    protected int getLookup(int key) {

        int lookup = hashIndex.getLookup(key);
        int tempKey;

        for (; lookup >= 0; lookup = hashIndex.linkTable[lookup]) {
            tempKey = intKeyTable[lookup];

            if (key == tempKey) {
                return lookup;
            }
        }

        return lookup;
    }

    protected int getLookup(long key) {

        int  lookup = hashIndex.getLookup((int) key);
        long tempKey;

        for (; lookup >= 0; lookup = hashIndex.getNextLookup(lookup)) {
            tempKey = longKeyTable[lookup];

            if (key == tempKey) {
                return lookup;
            }
        }

        return lookup;
    }

    protected Iterator getValuesIterator(Object key, int hash) {

        int lookup = getLookup(key, hash);

        if (valuesIterator == null) {
            valuesIterator = new ValuesIterator();
        }

        valuesIterator.reset(key, lookup);

        return valuesIterator;
    }

    
    protected Object addOrRemove(long longKey, long longValue,
                                 Object objectKey, Object objectValue,
                                 boolean remove) {

        int hash = (int) longKey;

        if (isObjectKey) {
            if (objectKey == null) {
                return null;
            }

            hash = objectKey.hashCode();
        }

        int    index       = hashIndex.getHashIndex(hash);
        int    lookup      = hashIndex.hashTable[index];
        int    lastLookup  = -1;
        Object returnValue = null;

        for (; lookup >= 0;
                lastLookup = lookup,
                lookup = hashIndex.getNextLookup(lookup)) {
            if (isObjectKey) {
                if (objectKeyTable[lookup].equals(objectKey)) {
                    break;
                }
            } else if (isIntKey) {
                if (longKey == intKeyTable[lookup]) {
                    break;
                }
            } else if (isLongKey) {
                if (longKey == longKeyTable[lookup]) {
                    break;
                }
            }
        }

        if (lookup >= 0) {
            if (remove) {
                if (isObjectKey) {
                    objectKeyTable[lookup] = null;
                } else {
                    if (longKey == 0) {
                        hasZeroKey   = false;
                        zeroKeyIndex = -1;
                    }

                    if (isIntKey) {
                        intKeyTable[lookup] = 0;
                    } else {
                        longKeyTable[lookup] = 0;
                    }
                }

                if (isObjectValue) {
                    returnValue              = objectValueTable[lookup];
                    objectValueTable[lookup] = null;
                } else if (isIntValue) {
                    intValueTable[lookup] = 0;
                } else if (isLongValue) {
                    longValueTable[lookup] = 0;
                }

                hashIndex.unlinkNode(index, lastLookup, lookup);

                if (accessTable != null) {
                    accessTable[lookup] = 0;
                }

                if (minimizeOnEmpty && hashIndex.elementCount == 0) {
                    rehash(initialCapacity);
                }

                return returnValue;
            }

            if (isObjectValue) {
                returnValue              = objectValueTable[lookup];
                objectValueTable[lookup] = objectValue;
            } else if (isIntValue) {
                intValueTable[lookup] = (int) longValue;
            } else if (isLongValue) {
                longValueTable[lookup] = longValue;
            }

            if (accessTable != null) {
                accessTable[lookup] = ++accessCount;
            }

            return returnValue;
        }

        
        if (remove) {
            return null;
        }

        if (hashIndex.elementCount >= threshold) {

            
            if (reset()) {
                return addOrRemove(longKey, longValue, objectKey, objectValue,
                                   remove);
            } else {
                return null;
            }
        }

        lookup = hashIndex.linkNode(index, lastLookup);

        
        if (isObjectKey) {
            objectKeyTable[lookup] = objectKey;
        } else if (isIntKey) {
            intKeyTable[lookup] = (int) longKey;

            if (longKey == 0) {
                hasZeroKey   = true;
                zeroKeyIndex = lookup;
            }
        } else if (isLongKey) {
            longKeyTable[lookup] = longKey;

            if (longKey == 0) {
                hasZeroKey   = true;
                zeroKeyIndex = lookup;
            }
        }

        if (isObjectValue) {
            objectValueTable[lookup] = objectValue;
        } else if (isIntValue) {
            intValueTable[lookup] = (int) longValue;
        } else if (isLongValue) {
            longValueTable[lookup] = longValue;
        }

        
        if (accessTable != null) {
            accessTable[lookup] = ++accessCount;
        }

        return returnValue;
    }

    
    protected Object addOrRemoveMultiVal(long longKey, long longValue,
                                         Object objectKey, Object objectValue,
                                         boolean removeKey,
                                         boolean removeValue) {

        int hash = (int) longKey;

        if (isObjectKey) {
            if (objectKey == null) {
                return null;
            }

            hash = objectKey.hashCode();
        }

        int     index       = hashIndex.getHashIndex(hash);
        int     lookup      = hashIndex.hashTable[index];
        int     lastLookup  = -1;
        Object  returnValue = null;
        boolean multiValue  = false;

        for (; lookup >= 0;
                lastLookup = lookup,
                lookup = hashIndex.getNextLookup(lookup)) {
            if (isObjectKey) {
                if (objectKeyTable[lookup].equals(objectKey)) {
                    if (removeKey) {
                        while (true) {
                            objectKeyTable[lookup]   = null;
                            returnValue = objectValueTable[lookup];
                            objectValueTable[lookup] = null;

                            hashIndex.unlinkNode(index, lastLookup, lookup);

                            multiValueTable[lookup] = false;
                            lookup = hashIndex.hashTable[index];

                            if (lookup < 0
                                    || !objectKeyTable[lookup].equals(
                                        objectKey)) {
                                return returnValue;
                            }
                        }
                    } else {
                        if (objectValueTable[lookup].equals(objectValue)) {
                            if (removeValue) {
                                objectKeyTable[lookup]   = null;
                                returnValue = objectValueTable[lookup];
                                objectValueTable[lookup] = null;

                                hashIndex.unlinkNode(index, lastLookup,
                                                     lookup);

                                multiValueTable[lookup] = false;
                                lookup                  = lastLookup;

                                return returnValue;
                            } else {
                                return objectValueTable[lookup];
                            }
                        }
                    }

                    multiValue = true;
                }
            } else if (isIntKey) {
                if (longKey == intKeyTable[lookup]) {
                    if (removeKey) {
                        while (true) {
                            if (longKey == 0) {
                                hasZeroKey   = false;
                                zeroKeyIndex = -1;
                            }

                            intKeyTable[lookup]   = 0;
                            intValueTable[lookup] = 0;

                            hashIndex.unlinkNode(index, lastLookup, lookup);

                            multiValueTable[lookup] = false;
                            lookup = hashIndex.hashTable[index];

                            if (lookup < 0 || longKey != intKeyTable[lookup]) {
                                return null;
                            }
                        }
                    } else {
                        if (intValueTable[lookup] == longValue) {
                            return null;
                        }
                    }

                    multiValue = true;
                }
            } else if (isLongKey) {
                if (longKey == longKeyTable[lookup]) {
                    if (removeKey) {
                        while (true) {
                            if (longKey == 0) {
                                hasZeroKey   = false;
                                zeroKeyIndex = -1;
                            }

                            longKeyTable[lookup]   = 0;
                            longValueTable[lookup] = 0;

                            hashIndex.unlinkNode(index, lastLookup, lookup);

                            multiValueTable[lookup] = false;
                            lookup = hashIndex.hashTable[index];

                            if (lookup < 0
                                    || longKey != longKeyTable[lookup]) {
                                return null;
                            }
                        }
                    } else {
                        if (intValueTable[lookup] == longValue) {
                            return null;
                        }
                    }

                    multiValue = true;
                }
            }
        }

        if (removeKey || removeValue) {
            return returnValue;
        }

        if (hashIndex.elementCount >= threshold) {

            
            if (reset()) {
                return addOrRemoveMultiVal(longKey, longValue, objectKey,
                                           objectValue, removeKey,
                                           removeValue);
            } else {
                return null;
            }
        }

        lookup = hashIndex.linkNode(index, lastLookup);

        
        if (isObjectKey) {
            objectKeyTable[lookup] = objectKey;
        } else if (isIntKey) {
            intKeyTable[lookup] = (int) longKey;

            if (longKey == 0) {
                hasZeroKey   = true;
                zeroKeyIndex = lookup;
            }
        } else if (isLongKey) {
            longKeyTable[lookup] = longKey;

            if (longKey == 0) {
                hasZeroKey   = true;
                zeroKeyIndex = lookup;
            }
        }

        if (isObjectValue) {
            objectValueTable[lookup] = objectValue;
        } else if (isIntValue) {
            intValueTable[lookup] = (int) longValue;
        } else if (isLongValue) {
            longValueTable[lookup] = longValue;
        }

        if (multiValue) {
            multiValueTable[lookup] = true;
        }

        
        if (accessTable != null) {
            accessTable[lookup] = ++accessCount;
        }

        return returnValue;
    }

    
    protected Object addOrRemove(long longKey, Object objectValue,
                                 Object objectValueTwo, boolean remove) {

        int    hash        = (int) longKey;
        int    index       = hashIndex.getHashIndex(hash);
        int    lookup      = hashIndex.hashTable[index];
        int    lastLookup  = -1;
        Object returnValue = null;

        for (; lookup >= 0;
                lastLookup = lookup,
                lookup = hashIndex.getNextLookup(lookup)) {
            if (isIntKey) {
                if (longKey == intKeyTable[lookup]) {
                    break;
                }
            } else {
                if (longKey == longKeyTable[lookup]) {
                    break;
                }
            }
        }

        if (lookup >= 0) {
            if (remove) {
                if (longKey == 0) {
                    hasZeroKey   = false;
                    zeroKeyIndex = -1;
                }

                if (isIntKey) {
                    intKeyTable[lookup] = 0;
                } else {
                    longKeyTable[lookup] = 0;
                }

                returnValue              = objectValueTable[lookup];
                objectValueTable[lookup] = null;

                hashIndex.unlinkNode(index, lastLookup, lookup);

                if (isTwoObjectValue) {
                    objectKeyTable[lookup] = null;
                }

                if (accessTable != null) {
                    accessTable[lookup] = 0;
                }

                return returnValue;
            }

            if (isObjectValue) {
                returnValue              = objectValueTable[lookup];
                objectValueTable[lookup] = objectValue;
            }

            if (isTwoObjectValue) {
                objectKeyTable[lookup] = objectValueTwo;
            }

            if (accessTable != null) {
                accessTable[lookup] = ++accessCount;
            }

            return returnValue;
        }

        
        if (remove) {
            return returnValue;
        }

        if (hashIndex.elementCount >= threshold) {
            if (reset()) {
                return addOrRemove(longKey, objectValue, objectValueTwo,
                                   remove);
            } else {
                return null;
            }
        }

        lookup = hashIndex.linkNode(index, lastLookup);

        if (isIntKey) {
            intKeyTable[lookup] = (int) longKey;
        } else {
            longKeyTable[lookup] = longKey;
        }

        if (longKey == 0) {
            hasZeroKey   = true;
            zeroKeyIndex = lookup;
        }

        objectValueTable[lookup] = objectValue;

        if (isTwoObjectValue) {
            objectKeyTable[lookup] = objectValueTwo;
        }

        if (accessTable != null) {
            accessTable[lookup] = ++accessCount;
        }

        return returnValue;
    }

    
    protected Object removeObject(Object objectKey, boolean removeRow) {

        if (objectKey == null) {
            return null;
        }

        int    hash        = objectKey.hashCode();
        int    index       = hashIndex.getHashIndex(hash);
        int    lookup      = hashIndex.hashTable[index];
        int    lastLookup  = -1;
        Object returnValue = null;

        for (; lookup >= 0;
                lastLookup = lookup,
                lookup = hashIndex.getNextLookup(lookup)) {
            if (objectKeyTable[lookup].equals(objectKey)) {
                objectKeyTable[lookup] = null;

                hashIndex.unlinkNode(index, lastLookup, lookup);

                if (isObjectValue) {
                    returnValue              = objectValueTable[lookup];
                    objectValueTable[lookup] = null;
                }

                if (removeRow) {
                    removeRow(lookup);
                }

                return returnValue;
            }
        }

        
        return returnValue;
    }

    protected boolean reset() {

        if (maxCapacity == 0 || maxCapacity > threshold) {
            rehash(hashIndex.linkTable.length * 2);

            return true;
        } else if (purgePolicy == PURGE_ALL) {
            clear();

            return true;
        } else if (purgePolicy == PURGE_QUARTER) {
            clear(threshold / 4, threshold >> 8);

            return true;
        } else if (purgePolicy == PURGE_HALF) {
            clear(threshold / 2, threshold >> 8);

            return true;
        } else if (purgePolicy == NO_PURGE) {
            return false;
        }

        return false;
    }

    
    protected void rehash(int newCapacity) {

        int     limitLookup     = hashIndex.newNodePointer;
        boolean oldZeroKey      = hasZeroKey;
        int     oldZeroKeyIndex = zeroKeyIndex;

        if (newCapacity < hashIndex.elementCount) {
            return;
        }

        hashIndex.reset((int) (newCapacity * loadFactor), newCapacity);

        if (multiValueTable != null) {
            int counter = multiValueTable.length;

            while (--counter >= 0) {
                multiValueTable[counter] = false;
            }
        }

        hasZeroKey   = false;
        zeroKeyIndex = -1;
        threshold    = newCapacity;

        for (int lookup = -1;
                (lookup = nextLookup(lookup, limitLookup, oldZeroKey, oldZeroKeyIndex))
                < limitLookup; ) {
            long   longKey     = 0;
            long   longValue   = 0;
            Object objectKey   = null;
            Object objectValue = null;

            if (isObjectKey) {
                objectKey = objectKeyTable[lookup];
            } else if (isIntKey) {
                longKey = intKeyTable[lookup];
            } else {
                longKey = longKeyTable[lookup];
            }

            if (isObjectValue) {
                objectValue = objectValueTable[lookup];
            } else if (isIntValue) {
                longValue = intValueTable[lookup];
            } else if (isLongValue) {
                longValue = longValueTable[lookup];
            }

            if (multiValueTable == null) {
                addOrRemove(longKey, longValue, objectKey, objectValue, false);
            } else {
                addOrRemoveMultiVal(longKey, longValue, objectKey,
                                    objectValue, false, false);
            }

            if (accessTable != null) {
                accessTable[hashIndex.elementCount - 1] = accessTable[lookup];
            }
        }

        resizeElementArrays(hashIndex.newNodePointer, newCapacity);
    }

    
    private void resizeElementArrays(int dataLength, int newLength) {

        Object temp;
        int    usedLength = newLength > dataLength ? dataLength
                                                   : newLength;

        if (isIntKey) {
            temp        = intKeyTable;
            intKeyTable = new int[newLength];

            System.arraycopy(temp, 0, intKeyTable, 0, usedLength);
        }

        if (isIntValue) {
            temp          = intValueTable;
            intValueTable = new int[newLength];

            System.arraycopy(temp, 0, intValueTable, 0, usedLength);
        }

        if (isLongKey) {
            temp         = longKeyTable;
            longKeyTable = new long[newLength];

            System.arraycopy(temp, 0, longKeyTable, 0, usedLength);
        }

        if (isLongValue) {
            temp           = longValueTable;
            longValueTable = new long[newLength];

            System.arraycopy(temp, 0, longValueTable, 0, usedLength);
        }

        if (objectKeyTable != null) {
            temp           = objectKeyTable;
            objectKeyTable = new Object[newLength];

            System.arraycopy(temp, 0, objectKeyTable, 0, usedLength);
        }

        if (isObjectValue) {
            temp             = objectValueTable;
            objectValueTable = new Object[newLength];

            System.arraycopy(temp, 0, objectValueTable, 0, usedLength);
        }

        if (objectValueTable2 != null) {
            temp              = objectValueTable2;
            objectValueTable2 = new Object[newLength];

            System.arraycopy(temp, 0, objectValueTable2, 0, usedLength);
        }

        if (accessTable != null) {
            temp        = accessTable;
            accessTable = new int[newLength];

            System.arraycopy(temp, 0, accessTable, 0, usedLength);
        }

        if (multiValueTable != null) {
            temp            = multiValueTable;
            multiValueTable = new boolean[newLength];

            System.arraycopy(temp, 0, multiValueTable, 0, usedLength);
        }
    }

    
    private void clearElementArrays(final int from, final int to) {

        if (isIntKey) {
            int counter = to;

            while (--counter >= from) {
                intKeyTable[counter] = 0;
            }
        } else if (isLongKey) {
            int counter = to;

            while (--counter >= from) {
                longKeyTable[counter] = 0;
            }
        } else if (isObjectKey || objectKeyTable != null) {
            int counter = to;

            while (--counter >= from) {
                objectKeyTable[counter] = null;
            }
        }

        if (isIntValue) {
            int counter = to;

            while (--counter >= from) {
                intValueTable[counter] = 0;
            }
        } else if (isLongValue) {
            int counter = to;

            while (--counter >= from) {
                longValueTable[counter] = 0;
            }
        } else if (isObjectValue) {
            int counter = to;

            while (--counter >= from) {
                objectValueTable[counter] = null;
            }
        }

        if (accessTable != null) {
            int counter = to;

            while (--counter >= from) {
                accessTable[counter] = 0;
            }
        }

        if (multiValueTable != null) {
            int counter = to;

            while (--counter >= from) {
                multiValueTable[counter] = false;
            }
        }
    }

    
    void removeFromElementArrays(int lookup) {

        int arrayLength = hashIndex.linkTable.length;

        if (isIntKey) {
            Object array = intKeyTable;

            System.arraycopy(array, lookup + 1, array, lookup,
                             arrayLength - lookup - 1);

            intKeyTable[arrayLength - 1] = 0;
        }

        if (isLongKey) {
            Object array = longKeyTable;

            System.arraycopy(array, lookup + 1, array, lookup,
                             arrayLength - lookup - 1);

            longKeyTable[arrayLength - 1] = 0;
        }

        if (isObjectKey || objectKeyTable != null) {
            Object array = objectKeyTable;

            System.arraycopy(array, lookup + 1, array, lookup,
                             arrayLength - lookup - 1);

            objectKeyTable[arrayLength - 1] = null;
        }

        if (isIntValue) {
            Object array = intValueTable;

            System.arraycopy(array, lookup + 1, array, lookup,
                             arrayLength - lookup - 1);

            intValueTable[arrayLength - 1] = 0;
        }

        if (isLongValue) {
            Object array = longValueTable;

            System.arraycopy(array, lookup + 1, array, lookup,
                             arrayLength - lookup - 1);

            longValueTable[arrayLength - 1] = 0;
        }

        if (isObjectValue) {
            Object array = objectValueTable;

            System.arraycopy(array, lookup + 1, array, lookup,
                             arrayLength - lookup - 1);

            objectValueTable[arrayLength - 1] = null;
        }
    }

    
    int nextLookup(int lookup, int limitLookup, boolean hasZeroKey,
                   int zeroKeyIndex) {

        for (++lookup; lookup < limitLookup; lookup++) {
            if (isObjectKey) {
                if (objectKeyTable[lookup] != null) {
                    return lookup;
                }
            } else if (isIntKey) {
                if (intKeyTable[lookup] != 0) {
                    return lookup;
                } else if (hasZeroKey && lookup == zeroKeyIndex) {
                    return lookup;
                }
            } else {
                if (longKeyTable[lookup] != 0) {
                    return lookup;
                } else if (hasZeroKey && lookup == zeroKeyIndex) {
                    return lookup;
                }
            }
        }

        return lookup;
    }

    
    protected int nextLookup(int lookup) {

        for (++lookup; lookup < hashIndex.newNodePointer; lookup++) {
            if (isObjectKey) {
                if (objectKeyTable[lookup] != null) {
                    return lookup;
                }
            } else if (isIntKey) {
                if (intKeyTable[lookup] != 0) {
                    return lookup;
                } else if (hasZeroKey && lookup == zeroKeyIndex) {
                    return lookup;
                }
            } else {
                if (longKeyTable[lookup] != 0) {
                    return lookup;
                } else if (hasZeroKey && lookup == zeroKeyIndex) {
                    return lookup;
                }
            }
        }

        return -1;
    }

    
    protected void removeRow(int lookup) {
        hashIndex.removeEmptyNode(lookup);
        removeFromElementArrays(lookup);
    }

    
    public void clear() {

        if (hashIndex.modified) {
            accessCount  = 0;
            accessMin    = accessCount;
            hasZeroKey   = false;
            zeroKeyIndex = -1;

            clearElementArrays(0, hashIndex.linkTable.length);
            hashIndex.clear();

            if (minimizeOnEmpty) {
                rehash(initialCapacity);
            }
        }
    }

    
    public int getAccessCountCeiling(int count, int margin) {
        return ArrayCounter.rank(accessTable, hashIndex.newNodePointer, count,
                                 accessMin + 1, accessCount, margin);
    }

    
    public void setAccessCountFloor(int count) {
        accessMin = count;
    }

    public int incrementAccessCount() {
        return ++accessCount;
    }

    
    protected void clear(int count, int margin) {

        if (margin < 64) {
            margin = 64;
        }

        int maxlookup  = hashIndex.newNodePointer;
        int accessBase = getAccessCountCeiling(count, margin);

        for (int lookup = 0; lookup < maxlookup; lookup++) {
            Object o = objectKeyTable[lookup];

            if (o != null && accessTable[lookup] < accessBase) {
                removeObject(o, false);
            }
        }

        accessMin = accessBase;
    }

    protected void resetAccessCount() {

        if (accessCount < ACCESS_MAX) {
            return;
        }

        if (accessMin < Integer.MAX_VALUE - (1 << 24)) {
            accessMin = Integer.MAX_VALUE - (1 << 24);
        }

        int i = accessTable.length;

        while (--i >= 0) {
            if (accessTable[i] <= accessMin) {
                accessTable[i] = 0;
            } else {
                accessTable[i] -= accessMin;
            }
        }

        accessCount -= accessMin;
        accessMin   = 0;
    }

    public int capacity() {
        return hashIndex.linkTable.length;
    }

    public int size() {
        return hashIndex.elementCount;
    }

    public boolean isEmpty() {
        return hashIndex.elementCount == 0;
    }

    protected boolean containsKey(Object key) {

        if (key == null) {
            return false;
        }

        if (hashIndex.elementCount == 0) {
            return false;
        }

        int lookup = getLookup(key, key.hashCode());

        return lookup == -1 ? false
                            : true;
    }

    protected boolean containsKey(int key) {

        if (hashIndex.elementCount == 0) {
            return false;
        }

        int lookup = getLookup(key);

        return lookup == -1 ? false
                            : true;
    }

    protected boolean containsKey(long key) {

        if (hashIndex.elementCount == 0) {
            return false;
        }

        int lookup = getLookup(key);

        return lookup == -1 ? false
                            : true;
    }

    protected boolean containsValue(Object value) {

        int lookup = 0;

        if (hashIndex.elementCount == 0) {
            return false;
        }

        if (value == null) {
            for (; lookup < hashIndex.newNodePointer; lookup++) {
                if (objectValueTable[lookup] == null) {
                    if (isObjectKey) {
                        if (objectKeyTable[lookup] != null) {
                            return true;
                        }
                    } else if (isIntKey) {
                        if (intKeyTable[lookup] != 0) {
                            return true;
                        } else if (hasZeroKey && lookup == zeroKeyIndex) {
                            return true;
                        }
                    } else {
                        if (longKeyTable[lookup] != 0) {
                            return true;
                        } else if (hasZeroKey && lookup == zeroKeyIndex) {
                            return true;
                        }
                    }
                }
            }
        } else {
            for (; lookup < hashIndex.newNodePointer; lookup++) {
                if (value.equals(objectValueTable[lookup])) {
                    return true;
                }
            }
        }

        return false;
    }

    
    protected class ValuesIterator implements org.hsqldb.lib.Iterator {

        int    lookup = -1;
        Object key;

        private void reset(Object key, int lookup) {
            this.key    = key;
            this.lookup = lookup;
        }

        public boolean hasNext() {
            return lookup != -1;
        }

        public Object next() throws NoSuchElementException {

            if (lookup == -1) {
                return null;
            }

            Object value = BaseHashMap.this.objectValueTable[lookup];

            while (true) {
                lookup = BaseHashMap.this.hashIndex.getNextLookup(lookup);

                if (lookup == -1
                        || BaseHashMap.this.objectKeyTable[lookup].equals(
                            key)) {
                    break;
                }
            }

            return value;
        }

        public int nextInt() throws NoSuchElementException {
            throw new NoSuchElementException("Hash Iterator");
        }

        public long nextLong() throws NoSuchElementException {
            throw new NoSuchElementException("Hash Iterator");
        }

        public void remove() throws NoSuchElementException {
            throw new NoSuchElementException("Hash Iterator");
        }

        public void setValue(Object value) {
            throw new NoSuchElementException("Hash Iterator");
        }
    }

    protected class MultiValueKeyIterator implements Iterator {

        boolean keys;
        int     lookup = -1;
        int     counter;
        boolean removed;

        public MultiValueKeyIterator() {
            toNextLookup();
        }

        private void toNextLookup() {

            while (true) {
                lookup = nextLookup(lookup);

                if (lookup == -1 || !multiValueTable[lookup]) {
                    break;
                }
            }
        }

        public boolean hasNext() {
            return lookup != -1;
        }

        public Object next() throws NoSuchElementException {

            Object value = objectKeyTable[lookup];

            toNextLookup();

            return value;
        }

        public int nextInt() throws NoSuchElementException {
            throw new NoSuchElementException("Hash Iterator");
        }

        public long nextLong() throws NoSuchElementException {
            throw new NoSuchElementException("Hash Iterator");
        }

        public void remove() throws NoSuchElementException {
            throw new NoSuchElementException("Hash Iterator");
        }

        public void setValue(Object value) {
            throw new NoSuchElementException("Hash Iterator");
        }
    }

    
    protected class BaseHashIterator implements Iterator {

        boolean keys;
        int     lookup = -1;
        int     counter;
        boolean removed;

        
        public BaseHashIterator() {}

        public BaseHashIterator(boolean keys) {
            this.keys = keys;
        }

        public boolean hasNext() {
            return counter < hashIndex.elementCount;
        }

        public Object next() throws NoSuchElementException {

            if ((keys && !isObjectKey) || (!keys && !isObjectValue)) {
                throw new NoSuchElementException("Hash Iterator");
            }

            removed = false;

            if (hasNext()) {
                counter++;

                lookup = nextLookup(lookup);

                if (keys) {
                    return objectKeyTable[lookup];
                } else {
                    return objectValueTable[lookup];
                }
            }

            throw new NoSuchElementException("Hash Iterator");
        }

        public int nextInt() throws NoSuchElementException {

            if ((keys && !isIntKey) || (!keys && !isIntValue)) {
                throw new NoSuchElementException("Hash Iterator");
            }

            removed = false;

            if (hasNext()) {
                counter++;

                lookup = nextLookup(lookup);

                if (keys) {
                    return intKeyTable[lookup];
                } else {
                    return intValueTable[lookup];
                }
            }

            throw new NoSuchElementException("Hash Iterator");
        }

        public long nextLong() throws NoSuchElementException {

            if ((!isLongKey || !keys)) {
                throw new NoSuchElementException("Hash Iterator");
            }

            removed = false;

            if (hasNext()) {
                counter++;

                lookup = nextLookup(lookup);

                if (keys) {
                    return longKeyTable[lookup];
                } else {
                    return longValueTable[lookup];
                }
            }

            throw new NoSuchElementException("Hash Iterator");
        }

        public void remove() throws NoSuchElementException {

            if (removed) {
                throw new NoSuchElementException("Hash Iterator");
            }

            counter--;

            removed = true;

            if (BaseHashMap.this.isObjectKey) {
                if (multiValueTable == null) {
                    addOrRemove(0, 0, objectKeyTable[lookup], null, true);
                } else {
                    if (keys) {
                        addOrRemoveMultiVal(0, 0, objectKeyTable[lookup],
                                            null, true, false);
                    } else {
                        addOrRemoveMultiVal(0, 0, objectKeyTable[lookup],
                                            objectValueTable[lookup], false,
                                            true);
                    }
                }
            } else if (isIntKey) {
                addOrRemove(intKeyTable[lookup], 0, null, null, true);
            } else {
                addOrRemove(longKeyTable[lookup], 0, null, null, true);
            }

            if (isList) {
                removeRow(lookup);
                lookup--;
            }
        }

        public void setValue(Object value) {

            if (keys) {
                throw new NoSuchElementException();
            }

            objectValueTable[lookup] = value;
        }

        public int getAccessCount() {

            if (removed || accessTable == null) {
                throw new NoSuchElementException();
            }

            return accessTable[lookup];
        }

        public void setAccessCount(int count) {

            if (removed || accessTable == null) {
                throw new NoSuchElementException();
            }

            accessTable[lookup] = count;
        }

        public int getLookup() {
            return lookup;
        }
    }
}
