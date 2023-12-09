


package org.hsqldb.store;

import org.hsqldb.types.TimestampData;




public class ValuePoolHashMap extends BaseHashMap {

    public ValuePoolHashMap(int initialCapacity, int maxCapacity,
                            int purgePolicy) throws IllegalArgumentException {

        super(initialCapacity, BaseHashMap.objectKeyOrValue, BaseHashMap.noKeyOrValue,
              true);

        this.maxCapacity = maxCapacity;
        this.purgePolicy = purgePolicy;
    }

    
    public void resetCapacity(int newCapacity,
                              int newPolicy) throws IllegalArgumentException {

        if (newCapacity != 0 && hashIndex.elementCount > newCapacity) {
            int surplus = hashIndex.elementCount - newCapacity;

            surplus += (surplus >> 5);

            if (surplus > hashIndex.elementCount) {
                surplus = hashIndex.elementCount;
            }

            clear(surplus, (surplus >> 6));
        }

        if (newCapacity != 0 && newCapacity < threshold) {
            rehash(newCapacity);

            if (newCapacity < hashIndex.elementCount) {
                newCapacity = maxCapacity;
            }
        }

        this.maxCapacity = newCapacity;
        this.purgePolicy = newPolicy;
    }

    protected Integer getOrAddInteger(int intKey) {

        Integer testValue;
        int     index      = hashIndex.getHashIndex(intKey);
        int     lookup     = hashIndex.hashTable[index];
        int     lastLookup = -1;

        for (; lookup >= 0;
                lastLookup = lookup,
                lookup = hashIndex.getNextLookup(lookup)) {
            testValue = (Integer) objectKeyTable[lookup];

            if (testValue.intValue() == intKey) {
                if (accessCount > ACCESS_MAX) {
                    resetAccessCount();
                }

                accessTable[lookup] = accessCount++;

                return testValue;
            }
        }

        if (hashIndex.elementCount >= threshold) {
            reset();

            return getOrAddInteger(intKey);
        }

        lookup                 = hashIndex.linkNode(index, lastLookup);
        testValue              = new Integer(intKey);
        objectKeyTable[lookup] = testValue;

        if (accessCount > ACCESS_MAX) {
            resetAccessCount();
        }

        accessTable[lookup] = accessCount++;

        return testValue;
    }

    protected Long getOrAddLong(long longKey) {

        Long testValue;
        int index = hashIndex.getHashIndex((int) (longKey ^ (longKey >>> 32)));
        int  lookup     = hashIndex.hashTable[index];
        int  lastLookup = -1;

        for (; lookup >= 0;
                lastLookup = lookup,
                lookup = hashIndex.getNextLookup(lookup)) {
            testValue = (Long) objectKeyTable[lookup];

            if (testValue.longValue() == longKey) {
                if (accessCount > ACCESS_MAX) {
                    resetAccessCount();
                }

                accessTable[lookup] = accessCount++;

                return testValue;
            }
        }

        if (hashIndex.elementCount >= threshold) {
            reset();

            return getOrAddLong(longKey);
        }

        lookup                 = hashIndex.linkNode(index, lastLookup);
        testValue              = new Long(longKey);
        objectKeyTable[lookup] = testValue;

        if (accessCount > ACCESS_MAX) {
            resetAccessCount();
        }

        accessTable[lookup] = accessCount++;

        return testValue;
    }

    
    protected String getOrAddString(Object key) {

        String testValue;
        int    index      = hashIndex.getHashIndex(key.hashCode());
        int    lookup     = hashIndex.hashTable[index];
        int    lastLookup = -1;

        for (; lookup >= 0;
                lastLookup = lookup,
                lookup = hashIndex.getNextLookup(lookup)) {
            testValue = (String) objectKeyTable[lookup];

            if (key.equals(testValue)) {
                if (accessCount > ACCESS_MAX) {
                    resetAccessCount();
                }

                accessTable[lookup] = accessCount++;

                return testValue;
            }
        }

        if (hashIndex.elementCount >= threshold) {
            reset();

            return getOrAddString(key);
        }

        testValue              = key.toString();
        lookup                 = hashIndex.linkNode(index, lastLookup);
        objectKeyTable[lookup] = testValue;

        if (accessCount > ACCESS_MAX) {
            resetAccessCount();
        }

        accessTable[lookup] = accessCount++;

        return testValue;
    }

    protected String getOrAddSubString(String key, int from, int limit) {

        
        key = key.substring(from, limit);

        String testValue;
        int    index      = hashIndex.getHashIndex(key.hashCode());
        int    lookup     = hashIndex.hashTable[index];
        int    lastLookup = -1;

        for (; lookup >= 0;
                lastLookup = lookup,
                lookup = hashIndex.getNextLookup(lookup)) {
            testValue = (String) objectKeyTable[lookup];

            if (key.equals(testValue)) {
                if (accessCount > ACCESS_MAX) {
                    resetAccessCount();
                }

                accessTable[lookup] = accessCount++;

                return testValue;
            }
        }

        if (hashIndex.elementCount >= threshold) {
            reset();

            return getOrAddString(key);
        }

        testValue              = new String(key.toCharArray());
        lookup                 = hashIndex.linkNode(index, lastLookup);
        objectKeyTable[lookup] = testValue;

        if (accessCount > ACCESS_MAX) {
            resetAccessCount();
        }

        accessTable[lookup] = accessCount++;

        return testValue;
    }

    protected TimestampData getOrAddDate(long longKey) {

        TimestampData testValue;
        int  hash       = (int) longKey ^ (int) (longKey >>> 32);
        int  index      = hashIndex.getHashIndex(hash);
        int  lookup     = hashIndex.hashTable[index];
        int  lastLookup = -1;

        for (; lookup >= 0;
                lastLookup = lookup,
                lookup = hashIndex.getNextLookup(lookup)) {
            testValue = (TimestampData) objectKeyTable[lookup];

            if (testValue.getSeconds() == longKey) {
                if (accessCount > ACCESS_MAX) {
                    resetAccessCount();
                }

                accessTable[lookup] = accessCount++;

                return testValue;
            }
        }

        if (hashIndex.elementCount >= threshold) {
            reset();

            return getOrAddDate(longKey);
        }

        lookup                 = hashIndex.linkNode(index, lastLookup);
        testValue              = new TimestampData(longKey);
        objectKeyTable[lookup] = testValue;

        if (accessCount > ACCESS_MAX) {
            resetAccessCount();
        }

        accessTable[lookup] = accessCount++;

        return testValue;
    }

    protected Double getOrAddDouble(long longKey) {

        Double testValue;
        int index = hashIndex.getHashIndex((int) (longKey ^ (longKey >>> 32)));
        int    lookup     = hashIndex.hashTable[index];
        int    lastLookup = -1;

        for (; lookup >= 0;
                lastLookup = lookup,
                lookup = hashIndex.getNextLookup(lookup)) {
            testValue = (Double) objectKeyTable[lookup];

            if (Double.doubleToLongBits(testValue.doubleValue()) == longKey) {
                if (accessCount > ACCESS_MAX) {
                    resetAccessCount();
                }

                accessTable[lookup] = accessCount++;

                return testValue;
            }
        }

        if (hashIndex.elementCount >= threshold) {
            reset();

            return getOrAddDouble(longKey);
        }

        lookup                 = hashIndex.linkNode(index, lastLookup);
        testValue              = new Double(Double.longBitsToDouble(longKey));
        objectKeyTable[lookup] = testValue;

        if (accessCount > ACCESS_MAX) {
            resetAccessCount();
        }

        accessTable[lookup] = accessCount++;

        return testValue;
    }

    protected Object getOrAddObject(Object key) {

        Object testValue;
        int    index      = hashIndex.getHashIndex(key.hashCode());
        int    lookup     = hashIndex.hashTable[index];
        int    lastLookup = -1;

        for (; lookup >= 0;
                lastLookup = lookup,
                lookup = hashIndex.getNextLookup(lookup)) {
            testValue = objectKeyTable[lookup];

            if (testValue.equals(key)) {
                if (accessCount > ACCESS_MAX) {
                    resetAccessCount();
                }

                accessTable[lookup] = accessCount++;

                return testValue;
            }
        }

        if (hashIndex.elementCount >= threshold) {
            reset();

            return getOrAddObject(key);
        }

        lookup                 = hashIndex.linkNode(index, lastLookup);
        objectKeyTable[lookup] = key;

        if (accessCount > ACCESS_MAX) {
            resetAccessCount();
        }

        accessTable[lookup] = accessCount++;

        return key;
    }
}
