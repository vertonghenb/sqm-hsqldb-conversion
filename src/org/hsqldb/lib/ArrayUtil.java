


package org.hsqldb.lib;

import java.lang.reflect.Array;


public class ArrayUtil {

    public static final int        CLASS_CODE_BYTE    = 'B';
    public static final int        CLASS_CODE_CHAR    = 'C';
    public static final int        CLASS_CODE_DOUBLE  = 'D';
    public static final int        CLASS_CODE_FLOAT   = 'F';
    public static final int        CLASS_CODE_INT     = 'I';
    public static final int        CLASS_CODE_LONG    = 'J';
    public static final int        CLASS_CODE_OBJECT  = 'L';
    public static final int        CLASS_CODE_SHORT   = 'S';
    public static final int        CLASS_CODE_BOOLEAN = 'Z';
    private static IntValueHashMap classCodeMap       = new IntValueHashMap();

    static {
        classCodeMap.put(byte.class, ArrayUtil.CLASS_CODE_BYTE);
        classCodeMap.put(char.class, ArrayUtil.CLASS_CODE_SHORT);
        classCodeMap.put(short.class, ArrayUtil.CLASS_CODE_SHORT);
        classCodeMap.put(int.class, ArrayUtil.CLASS_CODE_INT);
        classCodeMap.put(long.class, ArrayUtil.CLASS_CODE_LONG);
        classCodeMap.put(float.class, ArrayUtil.CLASS_CODE_FLOAT);
        classCodeMap.put(double.class, ArrayUtil.CLASS_CODE_DOUBLE);
        classCodeMap.put(boolean.class, ArrayUtil.CLASS_CODE_BOOLEAN);
        classCodeMap.put(Object.class, ArrayUtil.CLASS_CODE_OBJECT);
    }

    
    static int getClassCode(Class cla) {

        if (!cla.isPrimitive()) {
            return ArrayUtil.CLASS_CODE_OBJECT;
        }

        return classCodeMap.get(cla, -1);
    }

    
    public static void clearArray(int type, Object data, int from, int to) {

        switch (type) {

            case ArrayUtil.CLASS_CODE_BYTE : {
                byte[] array = (byte[]) data;

                while (--to >= from) {
                    array[to] = 0;
                }

                return;
            }
            case ArrayUtil.CLASS_CODE_CHAR : {
                byte[] array = (byte[]) data;

                while (--to >= from) {
                    array[to] = 0;
                }

                return;
            }
            case ArrayUtil.CLASS_CODE_SHORT : {
                short[] array = (short[]) data;

                while (--to >= from) {
                    array[to] = 0;
                }

                return;
            }
            case ArrayUtil.CLASS_CODE_INT : {
                int[] array = (int[]) data;

                while (--to >= from) {
                    array[to] = 0;
                }

                return;
            }
            case ArrayUtil.CLASS_CODE_LONG : {
                long[] array = (long[]) data;

                while (--to >= from) {
                    array[to] = 0;
                }

                return;
            }
            case ArrayUtil.CLASS_CODE_FLOAT : {
                float[] array = (float[]) data;

                while (--to >= from) {
                    array[to] = 0;
                }

                return;
            }
            case ArrayUtil.CLASS_CODE_DOUBLE : {
                double[] array = (double[]) data;

                while (--to >= from) {
                    array[to] = 0;
                }

                return;
            }
            case ArrayUtil.CLASS_CODE_BOOLEAN : {
                boolean[] array = (boolean[]) data;

                while (--to >= from) {
                    array[to] = false;
                }

                return;
            }
            default : {
                Object[] array = (Object[]) data;

                while (--to >= from) {
                    array[to] = null;
                }

                return;
            }
        }
    }

    
    public static void adjustArray(int type, Object array, int usedElements,
                                   int index, int count) {

        if (index >= usedElements) {
            return;
        }

        int newCount = usedElements + count;
        int source;
        int target;
        int size;

        if (count >= 0) {
            source = index;
            target = index + count;
            size   = usedElements - index;
        } else {
            source = index - count;
            target = index;
            size   = usedElements - index + count;
        }

        if (size > 0) {
            System.arraycopy(array, source, array, target, size);
        }

        if (count < 0) {
            clearArray(type, array, newCount, usedElements);
        }
    }

    
    public static void sortArray(int[] array) {

        boolean swapped;

        do {
            swapped = false;

            for (int i = 0; i < array.length - 1; i++) {
                if (array[i] > array[i + 1]) {
                    int temp = array[i + 1];

                    array[i + 1] = array[i];
                    array[i]     = temp;
                    swapped      = true;
                }
            }
        } while (swapped);
    }

    
    public static int find(Object[] array, Object object) {

        for (int i = 0; i < array.length; i++) {
            if (array[i] == object) {

                
                return i;
            }

            if (object != null && object.equals(array[i])) {
                return i;
            }
        }

        return -1;
    }

    
    public static int find(int[] array, int value) {

        for (int i = 0; i < array.length; i++) {
            if (array[i] == value) {
                return i;
            }
        }

        return -1;
    }

    public static int find(short[] array, int value) {

        for (int i = 0; i < array.length; i++) {
            if (array[i] == value) {
                return i;
            }
        }

        return -1;
    }

    public static int find(short[] array, int value, int offset, int count) {

        for (int i = offset; i < offset + count; i++) {
            if (array[i] == value) {
                return i;
            }
        }

        return -1;
    }

    
    public static int findNot(int[] array, int value) {

        for (int i = 0; i < array.length; i++) {
            if (array[i] != value) {
                return i;
            }
        }

        return -1;
    }

    
    public static boolean areEqualSets(int[] arra, int[] arrb) {
        return arra.length == arrb.length
               && ArrayUtil.haveEqualSets(arra, arrb, arra.length);
    }

    
    public static boolean areEqual(int[] arra, int[] arrb, int count,
                                   boolean full) {

        if (ArrayUtil.haveEqualArrays(arra, arrb, count)) {
            if (full) {
                return arra.length == arrb.length && count == arra.length;
            }

            return true;
        }

        return false;
    }

    
    public static boolean haveEqualSets(int[] arra, int[] arrb, int count) {

        if (ArrayUtil.haveEqualArrays(arra, arrb, count)) {
            return true;
        }

        if (count > arra.length || count > arrb.length) {
            return false;
        }

        if (count == 1) {
            return arra[0] == arrb[0];
        }

        int[] tempa = (int[]) resizeArray(arra, count);
        int[] tempb = (int[]) resizeArray(arrb, count);

        sortArray(tempa);
        sortArray(tempb);

        for (int j = 0; j < count; j++) {
            if (tempa[j] != tempb[j]) {
                return false;
            }
        }

        return true;
    }

    
    public static boolean haveEqualArrays(int[] arra, int[] arrb, int count) {

        if (count > arra.length || count > arrb.length) {
            return false;
        }

        for (int j = 0; j < count; j++) {
            if (arra[j] != arrb[j]) {
                return false;
            }
        }

        return true;
    }

    
    public static boolean haveEqualArrays(Object[] arra, Object[] arrb,
                                          int count) {

        if (count > arra.length || count > arrb.length) {
            return false;
        }

        for (int j = 0; j < count; j++) {
            if (arra[j] != arrb[j]) {
                if (arra[j] == null || !arra[j].equals(arrb[j])) {
                    return false;
                }
            }
        }

        return true;
    }

    
    public static boolean haveCommonElement(int[] arra, int[] arrb) {

        for (int i = 0; i < arra.length; i++) {
            int c = arra[i];

            for (int j = 0; j < arrb.length; j++) {
                if (c == arrb[j]) {
                    return true;
                }
            }
        }

        return false;
    }

    
    public static int[] commonElements(int[] arra, int[] arrb) {

        int[] c = null;
        int   n = countCommonElements(arra, arrb);

        if (n > 0) {
            c = new int[n];

            int k = 0;

            for (int i = 0; i < arra.length; i++) {
                for (int j = 0; j < arrb.length; j++) {
                    if (arra[i] == arrb[j]) {
                        c[k++] = arra[i];
                    }
                }
            }
        }

        return c;
    }

    
    public static int countCommonElements(int[] arra, int[] arrb) {

        int k = 0;

        for (int i = 0; i < arra.length; i++) {
            for (int j = 0; j < arrb.length; j++) {
                if (arra[i] == arrb[j]) {
                    k++;

                    break;
                }
            }
        }

        return k;
    }

    public static int countCommonElements(Object[] arra, int alen,
                                          Object[] arrb) {

        int k = 0;

        for (int i = 0; i < alen; i++) {
            for (int j = 0; j < arrb.length; j++) {
                if (arra[i] == arrb[j]) {
                    k++;

                    break;
                }
            }
        }

        return k;
    }

    
    public static int countSameElements(byte[] arra, int start, byte[] arrb) {

        int k     = 0;
        int limit = arra.length - start;

        if (limit > arrb.length) {
            limit = arrb.length;
        }

        for (int i = 0; i < limit; i++) {
            if (arra[i + start] == arrb[i]) {
                k++;
            } else {
                break;
            }
        }

        return k;
    }

    
    public static int countSameElements(char[] arra, int start, char[] arrb) {

        int k     = 0;
        int limit = arra.length - start;

        if (limit > arrb.length) {
            limit = arrb.length;
        }

        for (int i = 0; i < limit; i++) {
            if (arra[i + start] == arrb[i]) {
                k++;
            } else {
                break;
            }
        }

        return k;
    }

    
    public static int[] union(int[] arra, int[] arrb) {

        int newSize = arra.length + arrb.length
                      - ArrayUtil.countCommonElements(arra, arrb);

        if (newSize > arra.length && newSize > arrb.length) {
            int[] arrn = (int[]) ArrayUtil.resizeArray(arrb, newSize);
            int   pos  = arrb.length;

            mainloop:
            for (int i = 0; i < arra.length; i++) {
                for (int j = 0; j < arrb.length; j++) {
                    if (arra[i] == arrb[j]) {
                        continue mainloop;
                    }
                }

                arrn[pos++] = arra[i];
            }

            return arrn;
        }

        return arra.length > arrb.length ? arra
                                         : arrb;
    }

    
    public static int find(byte[] arra, int start, int limit, byte[] arrb) {

        int k = start;

        limit = limit - arrb.length + 1;

        int value = arrb[0];

        for (; k < limit; k++) {
            if (arra[k] == value) {
                if (arrb.length == 1) {
                    return k;
                }

                if (containsAt(arra, k, arrb)) {
                    return k;
                }
            }
        }

        return -1;
    }

    
    public static int findNotIn(byte[] arra, int start, int limit,
                                byte[] charset) {

        int k = 0;

        for (; k < limit; k++) {
            for (int i = 0; i < charset.length; i++) {
                if (arra[k] == charset[i]) {
                    continue;
                }
            }

            return k;
        }

        return -1;
    }

    
    public static int findIn(byte[] arra, int start, int limit,
                             byte[] byteSet) {

        int k = 0;

        for (; k < limit; k++) {
            for (int i = 0; i < byteSet.length; i++) {
                if (arra[k] == byteSet[i]) {
                    return k;
                }
            }
        }

        return -1;
    }

    
    public static int find(byte[] arra, int start, int limit, int b, int c) {

        int k = 0;

        for (; k < limit; k++) {
            if (arra[k] == b || arra[k] == c) {
                return k;
            }
        }

        return -1;
    }

    
    public static int[] booleanArrayToIntIndexes(boolean[] arrb) {

        int count = 0;

        for (int i = 0; i < arrb.length; i++) {
            if (arrb[i]) {
                count++;
            }
        }

        int[] intarr = new int[count];

        count = 0;

        for (int i = 0; i < arrb.length; i++) {
            if (arrb[i]) {
                intarr[count++] = i;
            }
        }

        return intarr;
    }

    
    public static void intIndexesToBooleanArray(int[] arra, boolean[] arrb) {

        for (int i = 0; i < arra.length; i++) {
            if (arra[i] < arrb.length) {
                arrb[arra[i]] = true;
            }
        }
    }

    
    public static int countStartIntIndexesInBooleanArray(int[] arra,
            boolean[] arrb) {

        int k = 0;

        for (int i = 0; i < arra.length; i++) {
            if (arrb[arra[i]]) {
                k++;
            } else {
                break;
            }
        }

        return k;
    }

    public static void orBooleanArray(boolean[] source, boolean[] dest) {

        for (int i = 0; i < dest.length; i++) {
            dest[i] |= source[i];
        }
    }

    public static boolean areAllIntIndexesInBooleanArray(int[] arra,
            boolean[] arrb) {

        for (int i = 0; i < arra.length; i++) {
            if (arrb[arra[i]]) {
                continue;
            }

            return false;
        }

        return true;
    }

    public static boolean isAnyIntIndexInBooleanArray(int[] arra,
            boolean[] arrb) {

        for (int i = 0; i < arra.length; i++) {
            if (arrb[arra[i]]) {
                return true;
            }
        }

        return false;
    }

    
    public static boolean containsAllTrueElements(boolean[] arra,
            boolean[] arrb) {

        for (int i = 0; i < arra.length; i++) {
            if (arrb[i] && !arra[i]) {
                return false;
            }
        }

        return true;
    }

    
    public static int countTrueElements(boolean[] arra) {

        int count = 0;

        for (int i = 0; i < arra.length; i++) {
            if (arra[i]) {
                count++;
            }
        }

        return count;
    }

    
    public static boolean hasNull(Object[] array, int[] columnMap) {

        int count = columnMap.length;

        for (int i = 0; i < count; i++) {
            if (array[columnMap[i]] == null) {
                return true;
            }
        }

        return false;
    }

    public static boolean hasAllNull(Object[] array, int[] columnMap) {

        int count = columnMap.length;

        for (int i = 0; i < count; i++) {
            if (array[columnMap[i]] != null) {
                return false;
            }
        }

        return true;
    }

    
    public static boolean containsAt(byte[] arra, int start, byte[] arrb) {
        return countSameElements(arra, start, arrb) == arrb.length;
    }

    
    public static int countStartElementsAt(byte[] arra, int start,
                                           byte[] arrb) {

        int k = 0;

        mainloop:
        for (int i = start; i < arra.length; i++) {
            for (int j = 0; j < arrb.length; j++) {
                if (arra[i] == arrb[j]) {
                    k++;

                    continue mainloop;
                }
            }

            break;
        }

        return k;
    }

    
    public static boolean containsAt(char[] arra, int start, char[] arrb) {
        return countSameElements(arra, start, arrb) == arrb.length;
    }

    
    public static int countNonStartElementsAt(byte[] arra, int start,
            byte[] arrb) {

        int k = 0;

        mainloop:
        for (int i = start; i < arra.length; i++) {
            for (int j = 0; j < arrb.length; j++) {
                if (arra[i] == arrb[j]) {
                    break mainloop;
                }
            }

            k++;
        }

        return k;
    }

    
    public static int copyBytes(long sourceOffset, byte[] source,
                                 int sourceOff, int length, long destOffset,
                                 byte[] dest) {

        if (sourceOffset + sourceOff >= destOffset + dest.length
                || sourceOffset + sourceOff + length <= destOffset) {
            return 0;
        }

        long sourceIndex = destOffset - sourceOffset;
        long destIndex   = 0;
        int  sourceLimit = sourceOff + length;

        if (sourceIndex > 0) {
            if (sourceIndex < sourceOff) {
                sourceIndex = sourceOff;
            }
        } else {
            destIndex   = -sourceIndex + sourceOff;
            sourceIndex = sourceOff;
        }

        length = sourceLimit - (int) sourceIndex;

        if (length > dest.length - destIndex) {
            length = dest.length - (int) destIndex;
        }

        System.arraycopy(source, (int) sourceIndex, dest, (int) destIndex,
                         length);

        return length;
    }

    
    public static void copyArray(Object source, Object dest, int count) {
        System.arraycopy(source, 0, dest, 0, count);
    }

    
    public static int[] arraySlice(int[] source, int start, int count) {

        int[] slice = new int[count];

        System.arraycopy(source, start, slice, 0, count);

        return slice;
    }

    
    public static void fillArray(char[] array, int offset, char value) {

        int to = array.length;

        while (--to >= offset) {
            array[to] = value;
        }
    }

    
    public static void fillArray(byte[] array, int offset, byte value) {

        int to = array.length;

        while (--to >= offset) {
            array[to] = value;
        }
    }

    
    public static void fillArray(Object[] array, Object value) {

        int to = array.length;

        while (--to >= 0) {
            array[to] = value;
        }
    }

    
    public static void fillArray(int[] array, int value) {

        int to = array.length;

        while (--to >= 0) {
            array[to] = value;
        }
    }

    
    public static void fillArray(boolean[] array, boolean value) {

        int to = array.length;

        while (--to >= 0) {
            array[to] = value;
        }
    }

    
    public static Object duplicateArray(Object source) {

        int size = Array.getLength(source);
        Object newarray =
            Array.newInstance(source.getClass().getComponentType(), size);

        System.arraycopy(source, 0, newarray, 0, size);

        return newarray;
    }

    
    public static Object resizeArrayIfDifferent(Object source, int newsize) {

        int oldsize = Array.getLength(source);

        if (oldsize == newsize) {
            return source;
        }

        Object newarray =
            Array.newInstance(source.getClass().getComponentType(), newsize);

        if (oldsize < newsize) {
            newsize = oldsize;
        }

        System.arraycopy(source, 0, newarray, 0, newsize);

        return newarray;
    }

    
    public static Object resizeArray(Object source, int newsize) {

        Object newarray =
            Array.newInstance(source.getClass().getComponentType(), newsize);
        int oldsize = Array.getLength(source);

        if (oldsize < newsize) {
            newsize = oldsize;
        }

        System.arraycopy(source, 0, newarray, 0, newsize);

        return newarray;
    }

    
    public static Object toAdjustedArray(Object source, Object addition,
                                         int colindex, int adjust) {

        int newsize = Array.getLength(source) + adjust;
        Object newarray =
            Array.newInstance(source.getClass().getComponentType(), newsize);

        copyAdjustArray(source, newarray, addition, colindex, adjust);

        return newarray;
    }

    
    public static void copyAdjustArray(Object source, Object dest,
                                       Object addition, int colindex,
                                       int adjust) {

        int length = Array.getLength(source);

        if (colindex < 0) {
            System.arraycopy(source, 0, dest, 0, length);

            return;
        }

        System.arraycopy(source, 0, dest, 0, colindex);

        if (adjust == 0) {
            int endcount = length - colindex - 1;

            Array.set(dest, colindex, addition);

            if (endcount > 0) {
                System.arraycopy(source, colindex + 1, dest, colindex + 1,
                                 endcount);
            }
        } else if (adjust < 0) {
            int endcount = length - colindex - 1;

            if (endcount > 0) {
                System.arraycopy(source, colindex + 1, dest, colindex,
                                 endcount);
            }
        } else {
            int endcount = length - colindex;

            Array.set(dest, colindex, addition);

            if (endcount > 0) {
                System.arraycopy(source, colindex, dest, colindex + 1,
                                 endcount);
            }
        }
    }

    
    public static int[] toAdjustedColumnArray(int[] colarr, int colindex,
            int adjust) {

        if (colarr == null) {
            return null;
        }

        if (colindex < 0) {
            return colarr;
        }

        int[] intarr = new int[colarr.length];
        int   j      = 0;

        for (int i = 0; i < colarr.length; i++) {
            if (colarr[i] > colindex) {
                intarr[j] = colarr[i] + adjust;

                j++;
            } else if (colarr[i] == colindex) {
                if (adjust < 0) {

                    
                } else {
                    intarr[j] = colarr[i] + adjust;

                    j++;
                }
            } else {
                intarr[j] = colarr[i];

                j++;
            }
        }

        if (colarr.length != j) {
            int[] newarr = new int[j];

            copyArray(intarr, newarr, j);

            return newarr;
        }

        return intarr;
    }

    
    public static void projectRow(Object[] row, int[] columnMap,
                                  Object[] newRow) {

        for (int i = 0; i < columnMap.length; i++) {
            newRow[i] = row[columnMap[i]];
        }
    }

    public static void projectRow(int[] row, int[] columnMap, int[] newRow) {

        for (int i = 0; i < columnMap.length; i++) {
            newRow[i] = row[columnMap[i]];
        }
    }

    
    public static void projectRowReverse(Object[] row, int[] columnMap,
                                         Object[] newRow) {

        for (int i = 0; i < columnMap.length; i++) {
            row[columnMap[i]] = newRow[i];
        }
    }


    public static void projectMap(int[] mainMap, int[] subMap,
                                  int[] newSubMap) {

        for (int i = 0; i < subMap.length; i++) {
            for (int j = 0; j < mainMap.length; j++) {
                if (subMap[i] == mainMap[j]) {
                    newSubMap[i] = j;

                    break;
                }
            }
        }
    }

    public static void reorderMaps(int[] mainMap, int[] firstMap,
                                   int[] secondMap) {

        for (int i = 0; i < mainMap.length; i++) {
            for (int j = i; j < firstMap.length; j++) {
                if (mainMap[i] == firstMap[j]) {
                    int temp = firstMap[i];

                    firstMap[i]  = firstMap[j];
                    firstMap[j]  = temp;
                    temp         = secondMap[i];
                    secondMap[i] = secondMap[j];
                    secondMap[j] = temp;

                    break;
                }
            }
        }
    }

    public static void fillSequence(int[] colindex) {

        for (int i = 0; i < colindex.length; i++) {
            colindex[i] = i;
        }
    }

    public static char[] byteArrayToChars(byte[] bytes) {

        char[] chars = new char[bytes.length / 2];

        for (int i = 0, j = 0; j < chars.length; i += 2, j++) {
            chars[j] = (char) ((bytes[i] << 8) + (bytes[i + 1] & 0xff));
        }

        return chars;
    }

    public static byte[] charArrayToBytes(char[] chars) {

        byte[] bytes = new byte[chars.length * 2];

        for (int i = 0, j = 0; j < chars.length; i += 2, j++) {
            int c = chars[j];

            bytes[i]     = (byte) (c >> 8);
            bytes[i + 1] = (byte) c;
        }

        return bytes;
    }

    
    public static boolean isInSortedArray(char ch, char[] array) {

        if (array.length == 0 || ch < array[0]
                || ch > array[array.length - 1]) {
            return false;
        }

        int low  = 0;
        int high = array.length;
        int mid  = 0;

        while (low < high) {
            mid = (low + high) / 2;

            if (ch < array[mid]) {
                high = mid;
            } else if (ch > array[mid]) {
                low = mid + 1;
            } else {
                return true;
            }
        }

        return false;
    }

    
    public static boolean containsAll(Object[] arra, Object[] arrb) {

        mainLoop:
        for (int i = 0; i < arrb.length; i++) {
            for (int j = 0; j < arra.length; j++) {
                if (arrb[i] == arra[j] || arrb[i].equals(arra[j])) {
                    continue mainLoop;
                }
            }

            return false;
        }

        return true;
    }

    
    public static boolean containsAny(Object[] arra, Object[] arrb) {

        mainLoop:
        for (int i = 0; i < arrb.length; i++) {
            for (int j = 0; j < arra.length; j++) {
                if (arrb[i] == arra[j] || arrb[i].equals(arra[j])) {
                    return true;
                }
            }
        }

        return false;
    }

    
    public static boolean containsAll(int[] arra, int[] arrb) {

        mainLoop:
        for (int i = 0; i < arrb.length; i++) {
            for (int j = 0; j < arra.length; j++) {
                if (arrb[i] == arra[j]) {
                    continue mainLoop;
                }
            }

            return false;
        }

        return true;
    }

    
    public static boolean containsAllAtStart(int[] arra, int[] arrb) {

        if (arrb.length > arra.length) {
            return false;
        }

        mainLoop:
        for (int i = 0; i < arra.length; i++) {
            if (i == arrb.length) {
                return true;
            }

            for (int j = 0; j < arrb.length; j++) {
                if (arra[i] == arrb[j]) {
                    continue mainLoop;
                }
            }

            return false;
        }

        return true;
    }

    
    public static byte[] toByteArray(long hi, long lo) {

        byte[] bytes = new byte[16];
        int    count = 0;
        int    v;

        while (count < 16) {
            if (count == 0) {
                v = (int) hi >>> 32;
            } else if (count == 4) {
                v = (int) hi;
            } else if (count == 8) {
                v = (int) lo >>> 32;
            } else {
                v = (int) lo;
            }

            bytes[count++] = (byte) (v >>> 24);
            bytes[count++] = (byte) (v >>> 16);
            bytes[count++] = (byte) (v >>> 8);
            bytes[count++] = (byte) v;
        }

        return bytes;
    }

    
    public static int compare(byte[] a, byte[] b) {

        int length = a.length;

        if (length > b.length) {
            length = b.length;
        }

        for (int i = 0; i < length; i++) {
            if (a[i] == b[i]) {
                continue;
            }

            return a[i] < b[i] ? -1
                               : 1;
        }

        if (a.length == b.length) {
            return 0;
        }

        return a.length < b.length ? -1
                                   : 1;
    }

    
    public static long getBinaryNormalisedCeiling(long value, int scale) {

        long mask    = 0xffffffffffffffffl << scale;
        long newSize = value & mask;

        if (newSize != value) {
            newSize += 1 << scale;
        }

        return newSize;
    }

    
    public static boolean isTwoPower(int n, int max) {

        for (int i = 0; i <= max; i++) {
            if ((n & 1) != 0) {
                return n == 1;
            }

            n >>= 1;
        }

        return false;
    }

    
    public static int getTwoPowerFloor(int n) {

        int shift = 0;

        if (n == 0) {
            return 0;
        }

        for (int i = 0; i < 32; i++) {
            if ((n & 1) != 0) {
                shift = i;
            }

            n >>= 1;
        }

        return 1 << shift;
    }
}
