package org.hsqldb.lib;
import java.io.LineNumberReader;
import org.hsqldb.store.ValuePool;
public class LineGroupReader {
    private static final String[] defaultContinuations = new String[] {
        " ", "*"
    };
    private static final String[] defaultIgnoredStarts = new String[]{ "--" };
    static final String LS = System.getProperty("line.separator", "\n");
    LineNumberReader reader;
    String           nextStartLine       = null;
    int              startLineNumber     = 0;
    int              nextStartLineNumber = 0;
    final String[] sectionContinuations;
    final String[] sectionStarts;
    final String[] ignoredStarts;
    public LineGroupReader(LineNumberReader reader) {
        this.sectionContinuations = defaultContinuations;
        this.sectionStarts        = ValuePool.emptyStringArray;
        this.ignoredStarts        = defaultIgnoredStarts;
        this.reader               = reader;
        try {
            getSection();
        } catch (Exception e) {}
    }
    public LineGroupReader(LineNumberReader reader, String[] sectionStarts) {
        this.sectionStarts        = sectionStarts;
        this.sectionContinuations = ValuePool.emptyStringArray;
        this.ignoredStarts        = ValuePool.emptyStringArray;
        this.reader               = reader;
        try {
            getSection();
        } catch (Exception e) {}
    }
    public HsqlArrayList getSection() {
        String        line;
        HsqlArrayList list = new HsqlArrayList();
        if (nextStartLine != null) {
            list.add(nextStartLine);
            startLineNumber = nextStartLineNumber;
        }
        while (true) {
            boolean newSection = false;
            line = null;
            try {
                line = reader.readLine();
            } catch (Exception e) {}
            if (line == null) {
                nextStartLine = null;
                return list;
            }
            line = line.substring(
                0, org.hsqldb.lib.StringUtil.rightTrimSize(line));
            if (line.length() == 0 || isIgnoredLine(line)) {
                continue;
            }
            if (isNewSectionLine(line)) {
                newSection = true;
            }
            if (newSection) {
                nextStartLine       = line;
                nextStartLineNumber = reader.getLineNumber();
                return list;
            }
            list.add(line);
        }
    }
    public HashMappedList getAsMap() {
        HashMappedList map = new HashMappedList();
        while (true) {
            HsqlArrayList list = getSection();
            if (list.size() < 1) {
                break;
            }
            String key   = (String) list.get(0);
            String value = LineGroupReader.convertToString(list, 1);
            map.put(key, value);
        }
        return map;
    }
    private boolean isNewSectionLine(String line) {
        if (sectionStarts.length == 0) {
            for (int i = 0; i < sectionContinuations.length; i++) {
                if (line.startsWith(sectionContinuations[i])) {
                    return false;
                }
            }
            return true;
        } else {
            for (int i = 0; i < sectionStarts.length; i++) {
                if (line.startsWith(sectionStarts[i])) {
                    return true;
                }
            }
            return false;
        }
    }
    private boolean isIgnoredLine(String line) {
        for (int i = 0; i < ignoredStarts.length; i++) {
            if (line.startsWith(ignoredStarts[i])) {
                return true;
            }
        }
        return false;
    }
    public int getStartLineNumber() {
        return startLineNumber;
    }
    public void close() {
        try {
            reader.close();
        } catch (Exception e) {}
    }
    public static String convertToString(HsqlArrayList list, int offset) {
        StringBuffer sb = new StringBuffer();
        for (int i = offset; i < list.size(); i++) {
            sb.append(list.get(i)).append(LS);
        }
        return sb.toString();
    }
}