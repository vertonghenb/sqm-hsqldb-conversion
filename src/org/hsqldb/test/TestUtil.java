package org.hsqldb.test;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import org.hsqldb.lib.ArraySort;
import org.hsqldb.lib.FileUtil;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.LineGroupReader;
import org.hsqldb.lib.StopWatch;
import org.hsqldb.lib.StringComparator;
import org.hsqldb.lib.StringUtil;
public class TestUtil {
    static private final SimpleDateFormat sdfYMDHMS =
        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    static private boolean      abortOnErr        = false;
    static final private String TIMESTAMP_VAR_STR = "${timestamp}";
    static final String LS = System.getProperty("line.separator", "\n");
    public static void main(String[] argv) {
        StopWatch sw = new StopWatch(true);
        TestUtil.testScripts("testrun/hsqldb");
        System.out.println(sw.currentElapsedTimeToMessage("Total time :"));
    }
    public static void deleteDatabase(String path) {
        FileUtil.deleteOrRenameDatabaseFiles(path);
    }
    static boolean delete(String file) {
        return new File(file).delete();
    }
    public static void checkDatabaseFilesDeleted(String path) {
        File[] list = FileUtil.getDatabaseFileList(path);
        if (list.length != 0) {
            System.out.println("database files not deleted");
        }
    }
    static protected void expandStamps(StringBuffer sb) {
        int i = sb.indexOf(TIMESTAMP_VAR_STR);
        if (i < 1) {
            return;
        }
        String timestamp;
        synchronized (sdfYMDHMS) {
            timestamp = sdfYMDHMS.format(new java.util.Date());
        }
        while (i > -1) {
            sb.replace(i, i + TIMESTAMP_VAR_STR.length(), timestamp);
            i = sb.indexOf(TIMESTAMP_VAR_STR);
        }
    }
    static void testScripts(String directory) {
        TestUtil.deleteDatabase("test1");
        try {
            Class.forName("org.hsqldb.jdbc.JDBCDriver");
            String     url = "jdbc:hsqldb:test1;sql.enforce_strict_size=true";
            String     user        = "sa";
            String     password    = "";
            Connection cConnection = null;
            String[]   filelist;
            String     absolute = new File(directory).getAbsolutePath();
            filelist = new File(absolute).list();
            ArraySort.sort((Object[]) filelist, 0, filelist.length,
                           new StringComparator());
            for (int i = 0; i < filelist.length; i++) {
                String fname = filelist[i];
                if (fname.startsWith("TestSelf") && fname.endsWith(".txt")) {
                    print("Openning DB");
                    cConnection = DriverManager.getConnection(url, user,
                            password);
                    testScript(cConnection, absolute + File.separator + fname);
                    cConnection.close();
                }
            }
            cConnection = DriverManager.getConnection(url, user, password);
        } catch (Exception e) {
            e.printStackTrace();
            print("TestUtil init error: " + e.toString());
        }
    }
    static void testScript(Connection aConnection, String aPath) {
        File file = new File(aPath);
        try {
            TestUtil.testScript(aConnection, file.getAbsolutePath(),
                                new FileReader(file));
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("test script file error: " + e.toString());
        }
    }
    public static void testScript(Connection aConnection, String sourceName,
                                  Reader inReader)
                                  throws SQLException, IOException {
        Statement        statement = aConnection.createStatement();
        LineNumberReader reader    = new LineNumberReader(inReader);
        LineGroupReader  sqlReader = new LineGroupReader(reader);
        int              startLine = 0;
        System.out.println("Opened test script file: " + sourceName);
        try {
            while (true) {
                HsqlArrayList section = sqlReader.getSection();
                startLine = sqlReader.getStartLineNumber();
                if (section.size() == 0) {
                    break;
                }
                testSection(statement, section, sourceName, startLine);
            }
            statement.close();
        } catch (SQLException se) {
            System.out.println("Error encountered at command beginning at "
                               + sourceName + ':' + startLine);
            throw se;
        } catch (RuntimeException re) {
            System.out.println("Error encountered at command beginning at "
                               + sourceName + ':' + startLine);
            throw re;
        }
        System.out.println("Processed " + reader.getLineNumber()
                           + " lines from " + sourceName);
    }
    static void test(Statement stat, String s, int line) {
        TestUtil.test(stat, s, null, line);
    }
    static void test(Statement stat, String s, String sourceName, int line) {
        HsqlArrayList section = new HsqlArrayList();
        section.add(s);
        testSection(stat, section, sourceName, line);
    }
    static void print(String s) {
        System.out.println(s);
    }
    private static void testSection(Statement stat, HsqlArrayList section,
                                    String scriptName, int line) {
        ParsedSection pSection = parsedSectionFactory(section);
        if (pSection == null) {    
            System.out.println(
                "The section starting at " + scriptName + ':' + line
                + " could not be parsed, and so was not processed." + LS);
            return;
        }
        if (pSection instanceof IgnoreParsedSection) {
            System.out.println("At " + scriptName + ':' + line + ": "
                               + pSection.getResultString());
            return;
        }
        if (pSection instanceof DisplaySection
                || pSection instanceof WaitSection
                || pSection instanceof ProceedSection) {
            String s = pSection.getResultString();
            if (s != null) {
                System.out.println(pSection.getResultString());
            }
        }
        if (pSection instanceof DisplaySection) {
            return;    
        }
        if (!pSection.test(stat)) {
            System.out.println("Section starting at " + scriptName + ':'
                               + line + " returned an unexpected result: "
                               + pSection);
            if (TestUtil.abortOnErr) {
                throw new TestRuntimeException(scriptName + ": " + line
                                               + "pSection");
            }
        }
    }
    private static ParsedSection parsedSectionFactory(HsqlArrayList aSection) {
        char type = ' ';
        String[] rows = null;
        String topLine = (String) aSection.get(0);
        if (topLine.startsWith("/*")) {
            type = topLine.charAt(2);
            if ((Character.isUpperCase(type))
                    && (Boolean.getBoolean("IgnoreCodeCase"))) {
                type = Character.toLowerCase(type);
            }
            if (!ParsedSection.isValidCode(type)) {
                return null;
            }
            topLine = topLine.substring(3);
        }
        int offset = 0;
        if (topLine.trim().length() > 0) {
            rows    = new String[aSection.size()];
            rows[0] = topLine;
        } else {
            rows   = new String[aSection.size() - 1];
            offset = 1;
        }
        for (int i = (1 - offset); i < rows.length; i++) {
            rows[i] = (String) aSection.get(i + offset);
        }
        switch (type) {
            case 'u' :
                return new UpdateParsedSection(rows);
            case 's' :
                return new SilentParsedSection(rows);
            case 'w' :
                return new WaitSection(rows);
            case 'p' :
                return new ProceedSection(rows);
            case 'r' :
                return new ResultSetParsedSection(rows);
            case 'o' :
                return new ResultSetOutputParsedSection(rows);
            case 'c' :
                return new CountParsedSection(rows);
            case 'd' :
                return new DisplaySection(rows);
            case 'e' :
                return new ExceptionParsedSection(rows);
            case ' ' :
                return new BlankParsedSection(rows);
            default :
                return new IgnoreParsedSection(rows, type);
        }
    }
    public static void setAbortOnErr(boolean aoe) {
        abortOnErr = aoe;
    }
    static class TestRuntimeException extends RuntimeException {
        public TestRuntimeException(String s) {
            super(s);
        }
        public TestRuntimeException(Throwable t) {
            super(t);
        }
        public TestRuntimeException(String s, Throwable t) {
            super(s, t);
        }
    }
}
abstract class ParsedSection {
    static final String LS = System.getProperty("line.separator", "\n");
    protected char type = ' ';
    String message = null;
    protected String[] lines = null;
    protected int resEndRow = 0;
    protected String sqlString = null;
    protected ParsedSection() {}
    protected ParsedSection(String[] aLines) {
        lines = aLines;
        StringBuffer sqlBuff  = new StringBuffer();
        int          endIndex = 0;
        int          k        = lines.length - 1;
        do {
            if ((endIndex = lines[k].indexOf("*/")) != -1) {
                sqlBuff.insert(0, lines[k].substring(endIndex + 2));
                lines[k] = lines[k].substring(0, endIndex);
                if (lines[k].length() == 0) {
                    resEndRow = k - 1;
                } else {
                    resEndRow = k;
                }
                break;
            } else {
                sqlBuff.insert(0, lines[k]);
            }
            k--;
        } while (k >= 0);
        sqlString = sqlBuff.toString();
    }
    public String toString() {
        StringBuffer b = new StringBuffer();
        b.append(LS + "******" + LS);
        b.append("contents of lines array:" + LS);
        for (int i = 0; i < lines.length; i++) {
            if (lines[i].trim().length() > 0) {
                b.append("line ").append(i).append(": ").append(
                    lines[i]).append(LS);
            }
        }
        b.append("Type: ");
        b.append(getType()).append(LS);
        b.append("SQL: ").append(getSql()).append(LS);
        b.append("results:").append(LS);
        b.append(getResultString());
        if (getMessage() != null) {
            b.append("\nmessage:").append(LS);
            b.append(getMessage());
        }
        b.append(LS + "******" + LS);
        return b.toString();
    }
    protected abstract String getResultString();
    protected String getMessage() {
        return message;
    }
    protected char getType() {
        return type;
    }
    protected String getSql() {
        return sqlString;
    }
    protected boolean test(Statement aStatement) {
        try {
            aStatement.execute(getSql());
        } catch (Exception x) {
            message = x.toString();
            return false;
        }
        return true;
    }
    protected static boolean isValidCode(char aCode) {
        switch (aCode) {
            case ' ' :
            case 'r' :
            case 'o' :
            case 'e' :
            case 'c' :
            case 'u' :
            case 's' :
            case 'd' :
            case 'w' :
            case 'p' :
                return true;
        }
        return false;
    }
}
class ResultSetParsedSection extends ParsedSection {
    private String   delim = System.getProperty("TestUtilFieldDelimiter", ",");
    private String[] expectedRows = null;
    protected ResultSetParsedSection(String[] lines) {
        super(lines);
        type = 'r';
        expectedRows = new String[(resEndRow + 1)];
        for (int i = 0; i <= resEndRow; i++) {
            int skip = StringUtil.skipSpaces(lines[i], 0);
            expectedRows[i] = lines[i].substring(skip);
        }
    }
    protected String getResultString() {
        StringBuffer printVal = new StringBuffer();
        for (int i = 0; i < getExpectedRows().length; i++) {
            printVal.append(getExpectedRows()[i]).append(LS);
        }
        return printVal.toString();
    }
    protected boolean test(Statement aStatement) {
        try {
            try {
                aStatement.execute(getSql());
            } catch (SQLException s) {
                throw new Exception("Expected a ResultSet, but got the error: "
                                    + s.getMessage());
            }
            if (aStatement.getUpdateCount() != -1) {
                throw new Exception(
                    "Expected a ResultSet, but got an update count of "
                    + aStatement.getUpdateCount());
            }
            ResultSet results = aStatement.getResultSet();
            int       count   = 0;
            while (results.next()) {
                if (count < getExpectedRows().length) {
                    String[] expectedFields =
                        StringUtil.split(getExpectedRows()[count], delim);
                    if (results.getMetaData().getColumnCount()
                            == expectedFields.length) {
                        int j = 0;
                        for (int i = 0; i < expectedFields.length; i++) {
                            j = i + 1;
                            String actual = results.getString(j);
                            if (actual == null) {    
                                if (!expectedFields[i].equalsIgnoreCase(
                                        "NULL")) {
                                    throw new Exception(
                                        "Expected row " + count
                                        + " of the ResultSet to contain:" + LS
                                        + getExpectedRows()[count] + LS
                                        + "but field " + j
                                        + " contained NULL");
                                }
                            } else if (!actual.equals(expectedFields[i])) {
                                throw new Exception(
                                    "Expected row " + (count + 1)
                                    + " of the ResultSet to contain:" + LS
                                    + getExpectedRows()[count] + LS
                                    + "but field " + j + " contained "
                                    + results.getString(j));
                            }
                        }
                    } else {
                        throw new Exception(
                            "Expected the ResultSet to contain "
                            + expectedFields.length
                            + " fields, but it contained "
                            + results.getMetaData().getColumnCount()
                            + " fields.");
                    }
                }
                count++;
            }
            if (count != getExpectedRows().length) {
                throw new Exception("Expected the ResultSet to contain "
                                    + getExpectedRows().length
                                    + " rows, but it contained " + count
                                    + " rows.");
            }
        } catch (Exception x) {
            message = x.toString();
            return false;
        }
        return true;
    }
    private String[] getExpectedRows() {
        return expectedRows;
    }
}
class ResultSetOutputParsedSection extends ParsedSection {
    private String   delim = System.getProperty("TestUtilFieldDelimiter", ",");
    private String[] expectedRows = null;
    protected ResultSetOutputParsedSection(String[] lines) {
        super(lines);
        type = 'o';
    }
    protected String getResultString() {
        return "";
    }
    protected boolean test(Statement aStatement) {
        try {
            try {
                aStatement.execute(getSql());
            } catch (SQLException s) {
                throw new Exception("Expected a ResultSet, but got the error: "
                                    + s.getMessage());
            }
            if (aStatement.getUpdateCount() != -1) {
                throw new Exception(
                    "Expected a ResultSet, but got an update count of "
                    + aStatement.getUpdateCount());
            }
            ResultSet    results  = aStatement.getResultSet();
            StringBuffer printVal = new StringBuffer();
            while (results.next()) {
                for (int j = 0; j < results.getMetaData().getColumnCount();
                        j++) {
                    if (j != 0) {
                        printVal.append(',');
                    }
                    printVal.append(results.getString(j + 1));
                }
                printVal.append(LS);
            }
            throw new Exception(printVal.toString());
        } catch (Exception x) {
            message = x.toString();
            return false;
        }
    }
    private String[] getExpectedRows() {
        return expectedRows;
    }
}
class UpdateParsedSection extends ParsedSection {
    int countWeWant;
    protected UpdateParsedSection(String[] lines) {
        super(lines);
        type        = 'u';
        countWeWant = Integer.parseInt(lines[0]);
    }
    protected String getResultString() {
        return Integer.toString(getCountWeWant());
    }
    private int getCountWeWant() {
        return countWeWant;
    }
    protected boolean test(Statement aStatement) {
        try {
            try {
                aStatement.execute(getSql());
            } catch (SQLException s) {
                throw new Exception("Expected an update count of "
                                    + getCountWeWant()
                                    + ", but got the error: "
                                    + s.getMessage());
            }
            if (aStatement.getUpdateCount() != getCountWeWant()) {
                throw new Exception("Expected an update count of "
                                    + getCountWeWant()
                                    + ", but got an update count of "
                                    + aStatement.getUpdateCount() + ".");
            }
        } catch (Exception x) {
            message = x.toString();
            return false;
        }
        return true;
    }
}
class WaitSection extends ParsedSection {
    static private String W_SYNTAX_MSG =
        "Syntax of Wait commands:" + LS
        + "    /*w 123*/     To Wait 123 milliseconds" + LS
        + "    /*w false x*/ Wait until /*p*/ command in another script has executed"
        + LS
        + "    /*w true x*/  Same, but the /*p*/ must not have executed yet";
    long    sleepTime       = -1;
    Waiter  waiter          = null;
    boolean enforceSequence = false;
    protected WaitSection(String[] inLines) {
        lines = inLines;
        int    closeCmd = lines[0].indexOf("*/");
        String cmd      = lines[0].substring(0, closeCmd);
        lines[0] = lines[0].substring(closeCmd + 2).trim();
        String trimmed = cmd.trim();
        if (trimmed.indexOf('e') < 0 && trimmed.indexOf('E') < 0) {
            sleepTime = Long.parseLong(trimmed);
        } else {
            try {
                int index = trimmed.indexOf(' ');
                if (index < 0) {
                    throw new IllegalArgumentException();
                }
                enforceSequence = Boolean.valueOf(trimmed.substring(0,
                        index)).booleanValue();
                waiter = Waiter.getWaiter(trimmed.substring(index).trim());
            } catch (IllegalArgumentException ie) {
                throw new IllegalArgumentException(W_SYNTAX_MSG);
            }
        }
        type = 'w';
    }
    protected String getResultString() {
        StringBuffer sb = new StringBuffer();
        if (lines.length == 1 && lines[0].trim().length() < 1) {
            return null;
        }
        for (int i = 0; i < lines.length; i++) {
            if (i > 0) {
                sb.append(LS);
            }
            sb.append("+ " + lines[i]);
        }
        TestUtil.expandStamps(sb);
        return sb.toString().trim();
    }
    protected boolean test(Statement aStatement) {
        if (waiter == null) {
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException ie) {
                throw new RuntimeException("Test sleep interrupted", ie);
            }
        } else {
            waiter.waitFor(enforceSequence);
        }
        return true;
    }
}
class ProceedSection extends ParsedSection {
    static private String P_SYNTAX_MSG =
        "Syntax of Proceed commands:" + LS
        + "    /*p false x*/ /*p*/ command in another script may Proceed" + LS
        + "    /*p true x*/  Same, but the /*w*/ must be waiting when we execute /*p*/"
    ;
    Waiter  waiter          = null;
    boolean enforceSequence = false;
    protected ProceedSection(String[] inLines) {
        lines = inLines;
        int    closeCmd = lines[0].indexOf("*/");
        String cmd      = lines[0].substring(0, closeCmd);
        lines[0] = lines[0].substring(closeCmd + 2).trim();
        String trimmed = cmd.trim();
        try {
            int index = trimmed.indexOf(' ');
            if (index < 0) {
                throw new IllegalArgumentException();
            }
            enforceSequence = Boolean.valueOf(trimmed.substring(0,
                    index)).booleanValue();
            waiter = Waiter.getWaiter(trimmed.substring(index).trim());
        } catch (IllegalArgumentException ie) {
            throw new IllegalArgumentException(P_SYNTAX_MSG);
        }
        type = 'p';
    }
    protected String getResultString() {
        StringBuffer sb = new StringBuffer();
        if (lines.length == 1 && lines[0].trim().length() < 1) {
            return null;
        }
        for (int i = 0; i < lines.length; i++) {
            if (i > 0) {
                sb.append(LS);
            }
            sb.append("+ " + lines[i]);
        }
        TestUtil.expandStamps(sb);
        return sb.toString().trim();
    }
    protected boolean test(Statement aStatement) {
        waiter.resume(enforceSequence);
        return true;
    }
}
class SilentParsedSection extends ParsedSection {
    protected SilentParsedSection(String[] lines) {
        super(lines);
        type = 's';
    }
    protected String getResultString() {
        return null;
    }
    protected boolean test(Statement aStatement) {
        try {
            aStatement.execute(getSql());
        } catch (Exception x) {}
        return true;
    }
}
class CountParsedSection extends ParsedSection {
    private int countWeWant;
    protected CountParsedSection(String[] lines) {
        super(lines);
        type        = 'c';
        countWeWant = Integer.parseInt(lines[0]);
    }
    protected String getResultString() {
        return Integer.toString(getCountWeWant());
    }
    private int getCountWeWant() {
        return countWeWant;
    }
    protected boolean test(Statement aStatement) {
        try {
            try {
                aStatement.execute(getSql());
            } catch (SQLException s) {
                throw new Exception("Expected a ResultSet containing "
                                    + getCountWeWant()
                                    + " rows, but got the error: "
                                    + s.getMessage());
            }
            if (aStatement.getUpdateCount() != -1) {
                throw new Exception(
                    "Expected a ResultSet, but got an update count of "
                    + aStatement.getUpdateCount());
            }
            ResultSet results = aStatement.getResultSet();
            int       count   = 0;
            while (results.next()) {
                count++;
            }
            if (count != getCountWeWant()) {
                throw new Exception("Expected the ResultSet to contain "
                                    + getCountWeWant()
                                    + " rows, but it contained " + count
                                    + " rows.");
            }
        } catch (Exception x) {
            message = x.toString();
            return false;
        }
        return true;
    }
}
class ExceptionParsedSection extends ParsedSection {
    private String    expectedState = null;
    private Throwable caught        = null;
    protected ExceptionParsedSection(String[] lines) {
        super(lines);
        expectedState = lines[0].trim();
        if (expectedState.length() < 1) {
            expectedState = null;
        }
        type = 'e';
    }
    protected String getResultString() {
        return (caught == null) ? "Nothing thrown"
                                : caught.toString();
    }
    protected boolean test(Statement aStatement) {
        try {
            aStatement.execute(getSql());
        } catch (SQLException sqlX) {
            caught = sqlX;
            if (expectedState == null
                    || expectedState.equalsIgnoreCase(sqlX.getSQLState())) {
                return true;
            }
            message = "SQLState '" + sqlX.getSQLState() + "' : "
                      + sqlX.toString() + " instead of '" + expectedState
                      + "'";
        } catch (Exception x) {
            caught  = x;
            message = x.toString();
        }
        return false;
    }
}
class BlankParsedSection extends ParsedSection {
    protected BlankParsedSection(String[] lines) {
        super(lines);
        type = ' ';
    }
    protected String getResultString() {
        return message;
    }
}
class IgnoreParsedSection extends ParsedSection {
    protected IgnoreParsedSection(String[] inLines, char aType) {
        super(inLines);
        type = aType;
    }
    protected String getResultString() {
        return "This section, of type '" + getType() + "' was ignored";
    }
}
class DisplaySection extends ParsedSection {
    protected DisplaySection(String[] inLines) {
        lines = inLines;
        int firstSlash = lines[0].indexOf('/');
        lines[0] = lines[0].substring(firstSlash + 1).trim();
    }
    protected String getResultString() {
        StringBuffer sb = new StringBuffer();
        if (lines.length == 1 && lines[0].trim().length() < 1) {
            return null;
        }
        for (int i = 0; i < lines.length; i++) {
            if (i > 0) {
                sb.append(LS);
            }
            sb.append("+ " + lines[i]);
        }
        TestUtil.expandStamps(sb);
        return sb.toString().trim();
    }
}