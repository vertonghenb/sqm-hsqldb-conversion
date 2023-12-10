package org.hsqldb.store;
import java.math.BigDecimal;
import org.hsqldb.types.TimestampData;
public class ValuePool {
    static ValuePoolHashMap intPool;
    static ValuePoolHashMap longPool;
    static ValuePoolHashMap doublePool;
    static ValuePoolHashMap bigdecimalPool;
    static ValuePoolHashMap stringPool;
    static ValuePoolHashMap datePool;
    static final int        SPACE_STRING_SIZE       = 50;
    static final int        DEFAULT_VALUE_POOL_SIZE = 8192;
    static final int[]      defaultPoolLookupSize   = new int[] {
        DEFAULT_VALUE_POOL_SIZE, DEFAULT_VALUE_POOL_SIZE,
        DEFAULT_VALUE_POOL_SIZE, DEFAULT_VALUE_POOL_SIZE,
        DEFAULT_VALUE_POOL_SIZE, DEFAULT_VALUE_POOL_SIZE
    };
    static final int POOLS_COUNT            = defaultPoolLookupSize.length;
    static final int defaultSizeFactor      = 2;
    static final int defaultMaxStringLength = 16;
    static ValuePoolHashMap[] poolList;
    static int maxStringLength;
    static {
        initPool();
    }
    public static final Integer INTEGER_0 = ValuePool.getInt(0);
    public static final Integer INTEGER_1 = ValuePool.getInt(1);
    public static final Integer INTEGER_2 = ValuePool.getInt(2);
    public static final Integer INTEGER_MAX =
        ValuePool.getInt(Integer.MAX_VALUE);
    public static final BigDecimal BIG_DECIMAL_0 =
        ValuePool.getBigDecimal(new BigDecimal(0.0));
    public static final BigDecimal BIG_DECIMAL_1 =
        ValuePool.getBigDecimal(new BigDecimal(1.0));
    public static final String[] emptyStringArray = new String[]{};
    public static final Object[] emptyObjectArray = new Object[]{};
    public static final int[]    emptyIntArray    = new int[]{};
    public static String         spaceString;
    private static void initPool() {
        int[] sizeArray  = defaultPoolLookupSize;
        int   sizeFactor = defaultSizeFactor;
        synchronized (ValuePool.class) {
            maxStringLength = defaultMaxStringLength;
            poolList        = new ValuePoolHashMap[POOLS_COUNT];
            for (int i = 0; i < POOLS_COUNT; i++) {
                int size = sizeArray[i];
                poolList[i] = new ValuePoolHashMap(size, size * sizeFactor,
                                                   BaseHashMap.PURGE_HALF);
            }
            intPool        = poolList[0];
            longPool       = poolList[1];
            doublePool     = poolList[2];
            bigdecimalPool = poolList[3];
            stringPool     = poolList[4];
            datePool       = poolList[5];
            char[] c = new char[SPACE_STRING_SIZE];
            for (int i = 0; i < SPACE_STRING_SIZE; i++) {
                c[i] = ' ';
            }
            spaceString = new String(c);
        }
    }
    public static int getMaxStringLength() {
        return maxStringLength;
    }
    public static void resetPool(int[] sizeArray, int sizeFactor) {
        synchronized (ValuePool.class) {
            for (int i = 0; i < POOLS_COUNT; i++) {
                poolList[i].clear();
                poolList[i].resetCapacity(sizeArray[i] * sizeFactor,
                                          BaseHashMap.PURGE_HALF);
            }
        }
    }
    public static void resetPool() {
        synchronized (ValuePool.class) {
            resetPool(defaultPoolLookupSize, defaultSizeFactor);
        }
    }
    public static void clearPool() {
        synchronized (ValuePool.class) {
            for (int i = 0; i < POOLS_COUNT; i++) {
                poolList[i].clear();
            }
        }
    }
    public static Integer getInt(int val) {
        synchronized (intPool) {
            return intPool.getOrAddInteger(val);
        }
    }
    public static Long getLong(long val) {
        synchronized (longPool) {
            return longPool.getOrAddLong(val);
        }
    }
    public static Double getDouble(long val) {
        synchronized (doublePool) {
            return doublePool.getOrAddDouble(val);
        }
    }
    public static String getString(String val) {
        if (val == null || val.length() > maxStringLength) {
            return val;
        }
        synchronized (stringPool) {
            return stringPool.getOrAddString(val);
        }
    }
    public static String getSubString(String val, int start, int limit) {
        synchronized (stringPool) {
            return stringPool.getOrAddString(val.substring(start, limit));
        }
    }
    public static TimestampData getDate(long val) {
        synchronized (datePool) {
            return datePool.getOrAddDate(val);
        }
    }
    public static BigDecimal getBigDecimal(BigDecimal val) {
        if (val == null) {
            return val;
        }
        synchronized (bigdecimalPool) {
            return (BigDecimal) bigdecimalPool.getOrAddObject(val);
        }
    }
    public static Boolean getBoolean(boolean b) {
        return b ? Boolean.TRUE
                 : Boolean.FALSE;
    }
}