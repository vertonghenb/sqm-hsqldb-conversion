package org.hsqldb;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.TimeZone;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.lib.StringUtil;
import org.hsqldb.types.DTIType;
import org.hsqldb.types.Types;
public class HsqlDateTime {
    private static Locale        defaultLocale = Locale.UK;
    private static long          currentDateMillis;
    public static final Calendar tempCalDefault = new GregorianCalendar();
    public static final Calendar tempCalGMT =
        new GregorianCalendar(TimeZone.getTimeZone("GMT"), defaultLocale);
    private static final Date   tempDate        = new Date(0);
    private static final String sdfdPattern     = "yyyy-MM-dd";
    static SimpleDateFormat     sdfd = new SimpleDateFormat(sdfdPattern);
    private static final String sdftPattern     = "HH:mm:ss";
    static SimpleDateFormat     sdft = new SimpleDateFormat(sdftPattern);
    private static final String sdftsPattern    = "yyyy-MM-dd HH:mm:ss";
    static SimpleDateFormat     sdfts = new SimpleDateFormat(sdftsPattern);
    private static final String sdftsSysPattern = "yyyy-MM-dd HH:mm:ss.SSS";
    static SimpleDateFormat sdftsSys = new SimpleDateFormat(sdftsSysPattern);
    static {
        tempCalGMT.setLenient(false);
        sdfd.setCalendar(new GregorianCalendar(TimeZone.getTimeZone("GMT"),
                                               defaultLocale));
        sdfd.setLenient(false);
        sdft.setCalendar(new GregorianCalendar(TimeZone.getTimeZone("GMT"),
                                               defaultLocale));
        sdft.setLenient(false);
        sdfts.setCalendar(new GregorianCalendar(TimeZone.getTimeZone("GMT"),
                defaultLocale));
        sdfts.setLenient(false);
    }
    static {
        currentDateMillis = getNormalisedDate(System.currentTimeMillis());
    }
    public static long getDateSeconds(String s) {
        try {
            synchronized (sdfd) {
                java.util.Date d = sdfd.parse(s);
                return d.getTime() / 1000;
            }
        } catch (Exception e) {
            throw Error.error(ErrorCode.X_22007);
        }
    }
    public static String getDateString(long seconds) {
        synchronized (sdfd) {
            sysDate.setTime(seconds * 1000);
            return sdfd.format(sysDate);
        }
    }
    public static long getTimestampSeconds(String s) {
        try {
            synchronized (sdfts) {
                java.util.Date d = sdfts.parse(s);
                return d.getTime() / 1000;
            }
        } catch (Exception e) {
            throw Error.error(ErrorCode.X_22007);
        }
    }
    public static void getTimestampString(StringBuffer sb, long seconds,
                                          int nanos, int scale) {
        synchronized (sdfts) {
            tempDate.setTime(seconds * 1000);
            sb.append(sdfts.format(tempDate));
            if (scale > 0) {
                sb.append('.');
                sb.append(StringUtil.toZeroPaddedString(nanos, 9, scale));
            }
        }
    }
    public static String getTimestampString(long millis) {
        synchronized (sdfts) {
            sysDate.setTime(millis);
            return sdfts.format(sysDate);
        }
    }
    public static synchronized long getCurrentDateMillis(long millis) {
        if (millis - currentDateMillis >= 24 * 3600 * 1000) {
            currentDateMillis = getNormalisedDate(millis);
        }
        return currentDateMillis;
    }
    private static java.util.Date sysDate = new java.util.Date();
    public static String getSystemTimeString() {
        synchronized (sdftsSys) {
            sysDate.setTime(System.currentTimeMillis());
            return sdftsSys.format(sysDate);
        }
    }
    public static void resetToDate(Calendar cal) {
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
    }
    public static void resetToTime(Calendar cal) {
        cal.set(Calendar.YEAR, 1970);
        cal.set(Calendar.MONTH, 0);
        cal.set(Calendar.DATE, 1);
        cal.set(Calendar.MILLISECOND, 0);
    }
    public static long convertMillisToCalendar(Calendar calendar,
            long millis) {
        calendar.clear();
        synchronized (tempCalGMT) {
            tempCalGMT.setTimeInMillis(millis);
            calendar.set(tempCalGMT.get(Calendar.YEAR),
                         tempCalGMT.get(Calendar.MONTH),
                         tempCalGMT.get(Calendar.DAY_OF_MONTH),
                         tempCalGMT.get(Calendar.HOUR_OF_DAY),
                         tempCalGMT.get(Calendar.MINUTE),
                         tempCalGMT.get(Calendar.SECOND));
            return calendar.getTimeInMillis();
        }
    }
    public static long convertMillisFromCalendar(Calendar calendar,
            long millis) {
        synchronized (tempCalGMT) {
            tempCalGMT.clear();
            calendar.setTimeInMillis(millis);
            tempCalGMT.set(calendar.get(Calendar.YEAR),
                           calendar.get(Calendar.MONTH),
                           calendar.get(Calendar.DAY_OF_MONTH),
                           calendar.get(Calendar.HOUR_OF_DAY),
                           calendar.get(Calendar.MINUTE),
                           calendar.get(Calendar.SECOND));
            return tempCalGMT.getTimeInMillis();
        }
    }
    public static void setTimeInMillis(Calendar cal, long millis) {
        cal.setTimeInMillis(millis);
    }
    public static long getTimeInMillis(Calendar cal) {
        return cal.getTimeInMillis();
    }
    public static long convertToNormalisedTime(long t) {
        return convertToNormalisedTime(t, tempCalGMT);
    }
    public static long convertToNormalisedTime(long t, Calendar cal) {
        synchronized (cal) {
            setTimeInMillis(cal, t);
            resetToDate(cal);
            long t1 = getTimeInMillis(cal);
            return t - t1;
        }
    }
    public static long convertToNormalisedDate(long t, Calendar cal) {
        synchronized (cal) {
            setTimeInMillis(cal, t);
            resetToDate(cal);
            return getTimeInMillis(cal);
        }
    }
    public static long getNormalisedTime(long t) {
        Calendar cal = tempCalGMT;
        synchronized (cal) {
            setTimeInMillis(cal, t);
            resetToTime(cal);
            return getTimeInMillis(cal);
        }
    }
    public static long getNormalisedTime(Calendar cal, long t) {
        synchronized (cal) {
            setTimeInMillis(cal, t);
            resetToTime(cal);
            return getTimeInMillis(cal);
        }
    }
    public static long getNormalisedDate(long d) {
        synchronized (tempCalGMT) {
            setTimeInMillis(tempCalGMT, d);
            resetToDate(tempCalGMT);
            return getTimeInMillis(tempCalGMT);
        }
    }
    public static long getNormalisedDate(Calendar cal, long d) {
        synchronized (cal) {
            setTimeInMillis(cal, d);
            resetToDate(cal);
            return getTimeInMillis(cal);
        }
    }
    public static int getZoneSeconds(Calendar cal) {
        return (cal.get(Calendar.ZONE_OFFSET) + cal.get(Calendar.DST_OFFSET))
               / 1000;
    }
    public static int getZoneMillis(Calendar cal, long millis) {
        return cal.getTimeZone().getOffset(millis);
    }
    public static int getDateTimePart(long m, int part) {
        synchronized (tempCalGMT) {
            tempCalGMT.setTimeInMillis(m);
            return tempCalGMT.get(part);
        }
    }
    public static long getTruncatedPart(long m, int part) {
        synchronized (tempCalGMT) {
            tempCalGMT.setTimeInMillis(m);
            switch (part) {
                case DTIType.WEEK_OF_YEAR : {
                    int year = tempCalGMT.get(Calendar.YEAR);
                    int week = tempCalGMT.get(Calendar.WEEK_OF_YEAR);
                    tempCalGMT.clear();
                    tempCalGMT.set(Calendar.YEAR, year);
                    tempCalGMT.set(Calendar.WEEK_OF_YEAR, week);
                    break;
                }
                default : {
                    zeroFromPart(tempCalGMT, part);
                    break;
                }
            }
            return tempCalGMT.getTimeInMillis();
        }
    }
    public static long getRoundedPart(long m, int part) {
        synchronized (tempCalGMT) {
            tempCalGMT.setTimeInMillis(m);
            switch (part) {
                case Types.SQL_INTERVAL_YEAR :
                    if (tempCalGMT.get(Calendar.MONTH) > 6) {
                        tempCalGMT.add(Calendar.YEAR, 1);
                    }
                    break;
                case Types.SQL_INTERVAL_MONTH :
                    if (tempCalGMT.get(Calendar.DAY_OF_MONTH) > 15) {
                        tempCalGMT.add(Calendar.MONTH, 1);
                    }
                    break;
                case Types.SQL_INTERVAL_DAY :
                    if (tempCalGMT.get(Calendar.HOUR_OF_DAY) > 11) {
                        tempCalGMT.add(Calendar.DAY_OF_MONTH, 1);
                    }
                    break;
                case Types.SQL_INTERVAL_HOUR :
                    if (tempCalGMT.get(Calendar.MINUTE) > 29) {
                        tempCalGMT.add(Calendar.HOUR_OF_DAY, 1);
                    }
                    break;
                case Types.SQL_INTERVAL_MINUTE :
                    if (tempCalGMT.get(Calendar.SECOND) > 29) {
                        tempCalGMT.add(Calendar.MINUTE, 1);
                    }
                    break;
                case Types.SQL_INTERVAL_SECOND :
                    if (tempCalGMT.get(Calendar.MILLISECOND) > 499) {
                        tempCalGMT.add(Calendar.SECOND, 1);
                    }
                    break;
                case DTIType.WEEK_OF_YEAR : {
                    int year = tempCalGMT.get(Calendar.YEAR);
                    int week = tempCalGMT.get(Calendar.WEEK_OF_YEAR);
                    int day  = tempCalGMT.get(Calendar.DAY_OF_WEEK);
                    if (day > 3) {
                        week++;
                    }
                    tempCalGMT.clear();
                    tempCalGMT.set(Calendar.YEAR, year);
                    tempCalGMT.set(Calendar.WEEK_OF_YEAR, week);
                    return tempCalGMT.getTimeInMillis();
                }
            }
            zeroFromPart(tempCalGMT, part);
            return tempCalGMT.getTimeInMillis();
        }
    }
    static void zeroFromPart(Calendar cal, int part) {
        switch (part) {
            case Types.SQL_INTERVAL_YEAR :
                cal.set(Calendar.MONTH, 0);
            case Types.SQL_INTERVAL_MONTH :
                cal.set(Calendar.DAY_OF_MONTH, 1);
            case Types.SQL_INTERVAL_DAY :
                cal.set(Calendar.HOUR_OF_DAY, 0);
            case Types.SQL_INTERVAL_HOUR :
                cal.set(Calendar.MINUTE, 0);
            case Types.SQL_INTERVAL_MINUTE :
                cal.set(Calendar.SECOND, 0);
            case Types.SQL_INTERVAL_SECOND :
                cal.set(Calendar.MILLISECOND, 0);
        }
    }
    private static final char[][] dateTokens     = {
        { 'R', 'R', 'R', 'R' }, { 'I', 'Y', 'Y', 'Y' }, { 'Y', 'Y', 'Y', 'Y' },
        { 'I', 'Y' }, { 'Y', 'Y' },
        { 'B', 'C' }, { 'B', '.', 'C', '.' }, { 'A', 'D' }, { 'A', '.', 'D', '.' },
        { 'M', 'O', 'N' }, { 'M', 'O', 'N', 'T', 'H' },
        { 'M', 'M' },
        { 'D', 'A', 'Y' }, { 'D', 'Y' },
        { 'W', 'W' }, { 'I', 'W' }, { 'D', 'D' }, { 'D', 'D', 'D' },
        { 'W' },
        { 'H', 'H', '2', '4' }, { 'H', 'H', '1', '2' }, { 'H', 'H' },
        { 'M', 'I' },
        { 'S', 'S' },
        { 'A', 'M' }, { 'P', 'M' }, { 'A', '.', 'M', '.' }, { 'P', '.', 'M', '.' },
        { 'F', 'F' }
    };
    private static final String[] javaDateTokens = {
        "yyyy", "'*IYYY'", "yyyy",
        "'*IY'", "yy",
        "G", "G", "G", "G",
        "MMM", "MMMMM",
        "MM",
        "EEEE", "EE",
        "'*WW'", "w", "dd", "D",
        "'*W'",
        "HH", "KK", "KK",
        "mm", "ss",
        "aaa", "aaa", "aaa", "aaa",
        "S"
    };
    private static final int[] sqlIntervalCodes = {
        -1, -1, Types.SQL_INTERVAL_YEAR,
        -1, Types.SQL_INTERVAL_YEAR,
        -1, -1, -1, -1,
        Types.SQL_INTERVAL_MONTH, Types.SQL_INTERVAL_MONTH,
        Types.SQL_INTERVAL_MONTH,
        -1, -1,
        DTIType.WEEK_OF_YEAR, -1, Types.SQL_INTERVAL_DAY, Types.SQL_INTERVAL_DAY,
        Types.SQL_INTERVAL_HOUR, -1, Types.SQL_INTERVAL_HOUR,
        Types.SQL_INTERVAL_MINUTE,
        Types.SQL_INTERVAL_SECOND,
        -1,-1,-1,-1,
        -1
    };
    private static final char e = 0xffff;
    public static Date toDate(String string, String pattern,
                              SimpleDateFormat format) {
        Date   date;
        String javaPattern = HsqlDateTime.toJavaDatePattern(pattern);
        int    matchIndex  = javaPattern.indexOf("*IY");
        if (matchIndex >= 0) {
            throw Error.error(ErrorCode.X_22511);
        }
        matchIndex = javaPattern.indexOf("*WW");
        if (matchIndex >= 0) {
            throw Error.error(ErrorCode.X_22511);
        }
        matchIndex = javaPattern.indexOf("*W");
        if (matchIndex >= 0) {
            throw Error.error(ErrorCode.X_22511);
        }
        try {
            format.applyPattern(javaPattern);
            date = format.parse(string);
        } catch (Exception e) {
            throw Error.error(ErrorCode.X_22007, e.toString());
        }
        return date;
    }
    public static String toFormattedDate(Date date, String pattern,
                                         SimpleDateFormat format) {
        String javaPattern = HsqlDateTime.toJavaDatePattern(pattern);
        try {
            format.applyPattern(javaPattern);
        } catch (Exception e) {
            throw Error.error(ErrorCode.X_22511);
        }
        String result     = format.format(date);
        int    matchIndex = result.indexOf("*IY");
        if (matchIndex >= 0) {
            Calendar cal         = format.getCalendar();
            int      matchLength = 3;
            int      temp        = result.indexOf("*IYYY");
            if (temp >= 0) {
                matchLength = 5;
                matchIndex  = temp;
            }
            int year       = cal.get(Calendar.YEAR);
            int weekOfYear = cal.get(Calendar.WEEK_OF_YEAR);
            if (weekOfYear == 1 && cal.get(Calendar.DAY_OF_YEAR) > 360) {
                year++;
            }
            String yearString = String.valueOf(year);
            if (matchLength == 3) {
                yearString = yearString.substring(yearString.length() - 2);
            }
            StringBuilder sb = new StringBuilder(result);
            sb.replace(matchIndex, matchIndex + matchLength, yearString);
            result = sb.toString();
        }
        matchIndex = result.indexOf("*WW");
        if (matchIndex >= 0) {
            Calendar      cal         = format.getCalendar();
            int           matchLength = 3;
            int           dayOfYear   = cal.get(Calendar.DAY_OF_YEAR);
            int           weekOfYear  = ((dayOfYear - 1) / 7) + 1;
            StringBuilder sb          = new StringBuilder(result);
            sb.replace(matchIndex, matchIndex + matchLength,
                       String.valueOf(weekOfYear));
            result = sb.toString();
        }
        matchIndex = result.indexOf("*W");
        if (matchIndex >= 0) {
            Calendar      cal         = format.getCalendar();
            int           matchLength = 2;
            int           dayOfMonth   = cal.get(Calendar.DAY_OF_MONTH);
            int           weekOfMonth  = ((dayOfMonth - 1) / 7) + 1;
            StringBuilder sb          = new StringBuilder(result);
            sb.replace(matchIndex, matchIndex + matchLength,
                       String.valueOf(weekOfMonth));
            result = sb.toString();
        }
        return result;
    }
    public static String toJavaDatePattern(String format) {
        int          len = format.length();
        char         ch;
        StringBuffer sb        = new StringBuffer(len);
        Tokenizer    tokenizer = new Tokenizer();
        for (int i = 0; i <= len; i++) {
            ch = (i == len) ? e
                            : format.charAt(i);
            if (tokenizer.isInQuotes()) {
                if (tokenizer.isQuoteChar(ch)) {
                    ch = '\'';
                } else if (ch == '\'') {
                    sb.append(ch);
                }
                sb.append(ch);
                continue;
            }
            if (!tokenizer.next(ch, i)) {
                if (tokenizer.consumed) {
                    int index = tokenizer.getLastMatch();
                    sb.append(javaDateTokens[index]);
                    i = tokenizer.matchOffset;
                } else {
                    if (tokenizer.isQuoteChar(ch)) {
                        ch = '\'';
                        sb.append(ch);
                    } else if (tokenizer.isLiteral(ch)) {
                        sb.append(ch);
                    } else if (ch == e) {
                    } else {
                        throw Error.error(ErrorCode.X_22007,
                                          format.substring(i));
                    }
                }
                tokenizer.reset();
            }
        }
        if (tokenizer.isInQuotes()) {
            throw Error.error(ErrorCode.X_22007);
        }
        String javaPattern = sb.toString();
        return javaPattern;
    }
    public static int toStandardIntervalPart(String format) {
        int       len = format.length();
        char      ch;
        Tokenizer tokenizer = new Tokenizer();
        for (int i = 0; i <= len; i++) {
            ch = (i == len) ? e
                            : format.charAt(i);
            if (!tokenizer.next(ch, i)) {
                int index = tokenizer.getLastMatch();
                if (index >= 0) {
                    return sqlIntervalCodes[index];
                }
                return -1;
            }
        }
        return -1;
    }
    static class Tokenizer {
        private int     lastMatched;
        private int     matchOffset;
        private int     offset;
        private long    state;
        private boolean consumed;
        private boolean isInQuotes;
        private boolean matched;
        private final char    quoteChar;
        private final char[]  literalChars;
        private static char[] defaultLiterals = new char[] {
            ' ', ',', '-', '.', '/', ':', ';'
        };
        char[][]              tokens;
        public Tokenizer() {
            this.quoteChar    = '\"';
            this.literalChars = defaultLiterals;
            tokens            = dateTokens;
            reset();
        }
        public void reset() {
            lastMatched = -1;
            offset      = -1;
            state       = 0;
            consumed    = false;
            matched     = false;
        }
        public int length() {
            return offset;
        }
        public int getLastMatch() {
            return lastMatched;
        }
        public boolean isConsumed() {
            return consumed;
        }
        public boolean wasMatched() {
            return matched;
        }
        public boolean isInQuotes() {
            return isInQuotes;
        }
        public boolean isQuoteChar(char ch) {
            if (quoteChar == ch) {
                isInQuotes = !isInQuotes;
                return true;
            }
            return false;
        }
        public boolean isLiteral(char ch) {
            return ArrayUtil.isInSortedArray(ch, literalChars);
        }
        private boolean isZeroBit(int bit) {
            return (state & (1L << bit)) == 0;
        }
        private void setBit(int bit) {
            state |= (1L << bit);
        }
        public boolean next(char ch, int position) {
            int index = ++offset;
            int len   = offset + 1;
            int left  = 0;
            matched = false;
            for (int i = tokens.length; --i >= 0; ) {
                if (isZeroBit(i)) {
                    if (tokens[i][index] == ch) {
                        if (tokens[i].length == len) {
                            setBit(i);
                            lastMatched = i;
                            consumed    = true;
                            matched     = true;
                            matchOffset = position;
                        } else {
                            ++left;
                        }
                    } else {
                        setBit(i);
                    }
                }
            }
            return left > 0;
        }
    }
}