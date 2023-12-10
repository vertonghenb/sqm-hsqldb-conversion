package org.hsqldb.lib;
import java.lang.reflect.Array;
public class StringUtil {
    public static String toZeroPaddedString(long value, int precision,
            int maxSize) {
        StringBuffer sb = new StringBuffer();
        if (value < 0) {
            value = -value;
        }
        String s = Long.toString(value);
        if (s.length() > precision) {
            s = s.substring(precision);
        }
        for (int i = s.length(); i < precision; i++) {
            sb.append('0');
        }
        sb.append(s);
        if (maxSize < precision) {
            sb.setLength(maxSize);
        }
        return sb.toString();
    }
    public static String toPaddedString(String source, int length, char pad,
                                        boolean trailing) {
        int len = source.length();
        if (len >= length) {
            return source;
        }
        StringBuffer sb = new StringBuffer(length);
        if (trailing) {
            sb.append(source);
        }
        for (int i = len; i < length; i++) {
            sb.append(pad);
        }
        if (!trailing) {
            sb.append(source);
        }
        return sb.toString();
    }
    public static String toPaddedString(String source, int length, String pad,
                                        boolean trailing) {
        int len = source.length();
        if (len == length) {
            return source;
        }
        if (len > length) {
            if (trailing) {
                return source.substring(0, length);
            } else {
                return source.substring(len - length, len);
            }
        }
        StringBuffer sb         = new StringBuffer(length);
        int          padLength  = source.length();
        int          partLength = (length - padLength) % pad.length();
        if (trailing) {
            sb.append(source);
            sb.append(pad.substring(pad.length() - partLength, pad.length()));
        }
        for (; padLength + pad.length() <= length; padLength += pad.length()) {
            sb.append(pad);
        }
        if (!trailing) {
            sb.append(pad.substring(0, partLength));
            sb.append(source);
        }
        return sb.toString();
    }
    public static String toLowerSubset(String source, char substitute) {
        int          len = source.length();
        StringBuffer sb  = new StringBuffer(len);
        char         ch;
        for (int i = 0; i < len; i++) {
            ch = source.charAt(i);
            if (!Character.isLetterOrDigit(ch)) {
                sb.append(substitute);
            } else if ((i == 0) && Character.isDigit(ch)) {
                sb.append(substitute);
            } else {
                sb.append(Character.toLowerCase(ch));
            }
        }
        return sb.toString();
    }
    public static String arrayToString(Object array) {
        int          len  = Array.getLength(array);
        int          last = len - 1;
        StringBuffer sb   = new StringBuffer(2 * (len + 1));
        sb.append('{');
        for (int i = 0; i < len; i++) {
            sb.append(Array.get(array, i));
            if (i != last) {
                sb.append(',');
            }
        }
        sb.append('}');
        return sb.toString();
    }
    public static String getList(String[] s, String separator, String quote) {
        int          len = s.length;
        StringBuffer sb  = new StringBuffer(len * 16);
        for (int i = 0; i < len; i++) {
            sb.append(quote);
            sb.append(s[i]);
            sb.append(quote);
            if (i + 1 < len) {
                sb.append(separator);
            }
        }
        return sb.toString();
    }
    public static String getList(int[] s, String separator, String quote) {
        int          len = s.length;
        StringBuffer sb  = new StringBuffer(len * 8);
        for (int i = 0; i < len; i++) {
            sb.append(quote);
            sb.append(s[i]);
            sb.append(quote);
            if (i + 1 < len) {
                sb.append(separator);
            }
        }
        return sb.toString();
    }
    public static String getList(long[] s, String separator, String quote) {
        int          len = s.length;
        StringBuffer sb  = new StringBuffer(len * 8);
        for (int i = 0; i < len; i++) {
            sb.append(quote);
            sb.append(s[i]);
            sb.append(quote);
            if (i + 1 < len) {
                sb.append(separator);
            }
        }
        return sb.toString();
    }
    public static String getList(String[][] s, String separator,
                                 String quote) {
        int          len = s.length;
        StringBuffer sb  = new StringBuffer(len * 16);
        for (int i = 0; i < len; i++) {
            sb.append(quote);
            sb.append(s[i][0]);
            sb.append(quote);
            if (i + 1 < len) {
                sb.append(separator);
            }
        }
        return sb.toString();
    }
    public static void appendPair(StringBuffer b, String s1, String s2,
                                  String separator, String terminator) {
        b.append(s1);
        b.append(separator);
        b.append(s2);
        b.append(terminator);
    }
    public static boolean isEmpty(String s) {
        int i = s == null ? 0
                          : s.length();
        while (i > 0) {
            if (s.charAt(--i) > ' ') {
                return false;
            }
        }
        return true;
    }
    public static int rightTrimSize(String s) {
        int i = s.length();
        while (i > 0) {
            i--;
            if (s.charAt(i) != ' ') {
                return i + 1;
            }
        }
        return 0;
    }
    public static int skipSpaces(String s, int start) {
        int limit = s.length();
        int i     = start;
        for (; i < limit; i++) {
            if (s.charAt(i) != ' ') {
                break;
            }
        }
        return i;
    }
    public static String[] split(String s, String separator) {
        HsqlArrayList list      = new HsqlArrayList();
        int           currindex = 0;
        for (boolean more = true; more; ) {
            int nextindex = s.indexOf(separator, currindex);
            if (nextindex == -1) {
                nextindex = s.length();
                more      = false;
            }
            list.add(s.substring(currindex, nextindex));
            currindex = nextindex + separator.length();
        }
        return (String[]) list.toArray(new String[list.size()]);
    }
}