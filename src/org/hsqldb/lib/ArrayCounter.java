package org.hsqldb.lib;
public class ArrayCounter {
    public static int[] countSegments(int[] array, int elements,
                                      int segments, int start, int limit) {
        int[] counts   = new int[segments];
        long  interval = calcInterval(segments, start, limit);
        int   index    = 0;
        int   element  = 0;
        if (interval <= 0) {
            return counts;
        }
        for (int i = 0; i < elements; i++) {
            element = array[i];
            if (element < start || element >= limit) {
                continue;
            }
            index = (int) ((element - start) / interval);
            counts[index]++;
        }
        return counts;
    }
    public static int rank(int[] array, int elements, int target, int start,
                           int limit, int margin) {
        final int segments     = 256;
        int       elementCount = 0;
        int       currentLimit = limit;
        for (;;) {
            long interval = calcInterval(segments, start, currentLimit);
            int[] counts = countSegments(array, elements, segments, start,
                                         currentLimit);
            for (int i = 0; i < counts.length; i++) {
                if (elementCount + counts[i] < target) {
                    elementCount += counts[i];
                    start        += interval;
                } else {
                    break;
                }
            }
            if (elementCount + margin >= target) {
                return start;
            }
            if (interval <= 1) {
                return start;
            }
            currentLimit = start + interval < limit ? (int) (start + interval)
                                                    : limit;
        }
    }
    static long calcInterval(int segments, int start, int limit) {
        long range = limit - start;
        if (range < 0) {
            return 0;
        }
        int partSegment = (range % segments) == 0 ? 0
                                                  : 1;
        return (range / segments) + partSegment;
    }
}