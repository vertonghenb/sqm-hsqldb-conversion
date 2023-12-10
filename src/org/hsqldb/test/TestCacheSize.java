package org.hsqldb.test;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;
import org.hsqldb.lib.FileUtil;
import org.hsqldb.lib.StopWatch;
import org.hsqldb.persist.HsqlProperties;
public class TestCacheSize {
    protected boolean filedb = true;
    protected boolean shutdown = true;
    protected String url = "jdbc:hsqldb:";
    protected String filepath = "/hsql/testcache/test";
    boolean reportProgress = true;
    String  tableType      = "CACHED";
    int     cacheScale     = 14;
    int     cacheSizeScale = 10;
    boolean nioMode        = true;
    int     writeDelay     = 60;
    boolean indexZip       = false;
    boolean indexLastName  = false;
    boolean addForeignKey  = false;
    boolean refIntegrity   = true;
    boolean createTempTable = false;
    boolean deleteWhileInsert         = false;
    int     deleteWhileInsertInterval = 10000;
    int bigrows = 4*256000;
    int bigops    = 4*256000;
    int smallops  = 32000;
    int smallrows = 0xfff;
    boolean multikeytable = false;
    String     user;
    String     password;
    Statement  sStatement;
    Connection cConnection;
    FileWriter writer;
    String filler = "ABCDEFGHIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVWXYZ"
                    + "ABCDEFGHIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVWXYZ";
    private void checkSelects() {
        countTestID();
        selectID();
    }
    private void checkUpdates() {
        updateTestString();
        countTestID();
        deleteTest();
        countTestID();
        countZip();
    }
    protected void setUp() {
        try {
            writer = new FileWriter("speedtests.html", true);
            writer.write("<table>\n");
            storeResult(new java.util.Date().toString(), 0, 0, 0);
            storeResult(filepath + " " + tableType + " " + nioMode,
                        cacheScale, 0, 0);
        } catch (Exception e) {}
        user     = "sa";
        password = "";
        try {
            sStatement  = null;
            cConnection = null;
            Class.forName("org.hsqldb.jdbc.JDBCDriver");
            if (filedb) {
                deleteDatabase(filepath);
                cConnection = DriverManager.getConnection(url + filepath,
                        user, password);
                sStatement = cConnection.createStatement();
                sStatement.execute("SET FILES DEFRAG " + 0);
                sStatement.execute("SET FILES LOG SIZE " + 0);
                sStatement.execute("SET DATABASE EVENT LOG LEVEL 1");
                int cacheRows = (1 << cacheScale) * 3;
                int cacheSize = (1 << cacheSizeScale) * cacheRows / 1024;
                sStatement.execute("SET FILES CACHE ROWS " + cacheRows);
                sStatement.execute("SET FILES CACHE SIZE " + cacheSize);
                sStatement.execute("SET FILES NIO " + nioMode);
                sStatement.execute("SET FILES BACKUP INCREMENT " + true);
                sStatement.execute("SHUTDOWN");
                cConnection.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("TestSql.setUp() error: " + e.getMessage());
        }
    }
    public void testFillUp() {
        StopWatch sw    = new StopWatch();
        String    ddl1  = "DROP TABLE test IF EXISTS";
        String    ddl11 = "DROP TABLE zip IF EXISTS";
        String    ddl2  = "CREATE TABLE zip( zip INT IDENTITY )";
        String ddl3 = "CREATE " + tableType + " TABLE test( id INT IDENTITY,"
                      + " firstname VARCHAR(20), " + " lastname VARCHAR(20), "
                      + " zip INTEGER, " + " filler VARCHAR(300))";
        String ddl31 = "SET TABLE test SOURCE \"test.csv;cache_scale="
                       + cacheScale + "\"";
        String ddl4 = "CREATE INDEX idx1 ON TEST (lastname)";
        String ddl5 = "CREATE INDEX idx2 ON TEST (zip)";
        String ddl6 =
            "ALTER TABLE test add constraint c1 FOREIGN KEY (zip) REFERENCES zip(zip) ON DELETE CASCADE;";
        String ddl7 = "CREATE TEMP TABLE temptest( id INT,"
                      + " firstname VARCHAR, " + " lastname VARCHAR, "
                      + " zip INTEGER, " + " filler VARCHAR)";
        String mddl1 = "DROP TABLE test2 IF EXISTS";
        String mddl2 = "CREATE " + tableType
                       + " TABLE test2( id1 INT, id2 INT,"
                       + " firstname VARCHAR, " + " lastname VARCHAR, "
                       + " zip INTEGER, " + " filler VARCHAR, "
                       + " PRIMARY KEY (id1,id2) )";
        String mdd13 = "SET TABLE test2 SOURCE \"test2.csv;cache_scale="
                       + cacheScale + "\"";
        try {
            sw.zero();
            cConnection = null;
            sStatement  = null;
            cConnection = DriverManager.getConnection(url + filepath, user,
                    password);
            System.out.println("connection time -- " + sw.elapsedTime());
            sw.zero();
            sStatement = cConnection.createStatement();
            java.util.Random randomgen = new java.util.Random();
            sStatement.execute(ddl1);
            sStatement.execute(ddl2);
            sStatement.execute(ddl3);
            if (tableType.equals("TEXT")) {
                sStatement.execute(ddl31);
            }
            if (indexLastName) {
                sStatement.execute(ddl4);
                System.out.println("created index on lastname");
            }
            if (indexZip) {
                sStatement.execute(ddl5);
                System.out.println("created index on zip");
            }
            if (addForeignKey) {
                sStatement.execute(ddl6);
                System.out.println("added foreign key");
            }
            if (createTempTable) {
                sStatement.execute(ddl7);
                System.out.println("created temp table");
            }
            if (multikeytable) {
                sStatement.execute(mddl1);
                sStatement.execute(mddl2);
                if (tableType.equals("TEXT")) {
                    sStatement.execute(mdd13);
                }
                System.out.println("created multi key table");
            }
            System.out.println("complete setup time -- " + sw.elapsedTime()
                               + " ms");
            fillUpBigTable(filler, randomgen);
            if (multikeytable) {
                fillUpMultiTable(filler, randomgen);
            }
            sw.zero();
            if (shutdown) {
                sStatement.execute("SHUTDOWN");
                long time = sw.elapsedTime();
                storeResult("shutdown", 0, time, 0);
                System.out.println("shutdown time  -- " + time + " ms");
            }
            cConnection.close();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }
    private void fillUpBigTable(String filler,
                                Random randomgen) throws SQLException {
        StopWatch sw = new StopWatch();
        int       i;
        PreparedStatement ps =
            cConnection.prepareStatement("INSERT INTO zip VALUES(?)");
        for (i = 0; i <= smallrows; i++) {
            ps.setInt(1, i);
            ps.execute();
        }
        ps.close();
        sStatement.execute("SET DATABASE REFERENTIAL INTEGRITY "
                           + this.refIntegrity);
        ps = cConnection.prepareStatement(
            "INSERT INTO test (firstname,lastname,zip,filler) VALUES (?,?,?,?)");
        ps.setString(1, "Julia");
        ps.setString(2, "Clancy");
        for (i = 0; i < bigrows; i++) {
            ps.setInt(3, nextIntRandom(randomgen, smallrows));
            {
                long nextrandom   = randomgen.nextLong();
                int  randomlength = (int) nextrandom & 0x7f;
                if (randomlength > filler.length()) {
                    randomlength = filler.length();
                }
                String varfiller = filler.substring(0, randomlength);
                ps.setString(4, nextrandom + varfiller);
            }
            ps.execute();
            if (reportProgress && (i + 1) % 10000 == 0) {
                System.out.println("insert " + (i + 1) + " : "
                                   + sw.elapsedTime());
            }
            if (deleteWhileInsert && i != 0
                    && i % deleteWhileInsertInterval == 0) {
                sStatement.execute("CALL IDENTITY();");
                ResultSet rs = sStatement.getResultSet();
                rs.next();
                int lastId = rs.getInt(1);
                sStatement.execute(
                    "SELECT * INTO TEMP tempt FROM test WHERE id > "
                    + (lastId - 4000));
                sStatement.execute("DELETE FROM test WHERE id > "
                                   + (lastId - 4000));
                sStatement.execute("INSERT INTO test SELECT * FROM tempt");
                sStatement.execute("DROP TABLE tempt");
            }
        }
        ps.close();
        long time = sw.elapsedTime();
        long rate = ((long) i * 1000) / (time + 1);
        storeResult("insert", i, time, rate);
        System.out.println("insert time for " + i + " rows -- " + time
                           + " ms -- " + rate + " tps");
    }
    private void fillUpMultiTable(String filler,
                                  Random randomgen) throws SQLException {
        StopWatch sw = new StopWatch();
        int       i;
        PreparedStatement ps = cConnection.prepareStatement(
            "INSERT INTO test2 (id1, id2, firstname,lastname,zip,filler) VALUES (?,?,?,?,?,?)");
        ps.setString(3, "Julia");
        ps.setString(4, "Clancy");
        int id1 = 0;
        for (i = 0; i < bigrows; i++) {
            int id2 = nextIntRandom(randomgen, Integer.MAX_VALUE);
            if (i % 1000 == 0) {
                id1 = nextIntRandom(randomgen, Integer.MAX_VALUE);
            }
            ps.setInt(1, id1);
            ps.setInt(2, id2);
            ps.setInt(5, nextIntRandom(randomgen, smallrows));
            long nextrandom   = randomgen.nextLong();
            int  randomlength = (int) nextrandom & 0x7f;
            if (randomlength > filler.length()) {
                randomlength = filler.length();
            }
            String varfiller = filler.substring(0, randomlength);
            ps.setString(6, nextrandom + varfiller);
            try {
                ps.execute();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            if (reportProgress && (i + 1) % 10000 == 0) {
                System.out.println("insert " + (i + 1) + " : "
                                   + sw.elapsedTime());
            }
        }
        ps.close();
        System.out.println("total multi key rows inserted: " + i);
        System.out.println("insert time: " + sw.elapsedTime() + " rps: "
                           + (i * 1000 / (sw.elapsedTime() + 1)));
    }
    protected void tearDown() {
        try {
            writer.write("\n</table>\n");
            writer.close();
        } catch (Exception e) {}
    }
    protected void checkResults() {
        try {
            StopWatch sw = new StopWatch();
            ResultSet rs;
            cConnection = DriverManager.getConnection(url + filepath, user,
                    password);
            long time = sw.elapsedTime();
            storeResult("reopen", 0, time, 0);
            System.out.println("database reopen time -- " + time + " ms");
            sw.zero();
            sStatement = cConnection.createStatement();
            checkSelects();
            checkUpdates();
            sw.zero();
            if (shutdown) {
                sStatement.execute("SHUTDOWN");
                time = sw.elapsedTime();
                storeResult("shutdown", 0, time, 0);
                System.out.println("shutdown time  -- " + time + " ms");
            }
            cConnection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    void selectZip() {
        StopWatch        sw        = new StopWatch();
        java.util.Random randomgen = new java.util.Random();
        int              i         = 0;
        boolean          slow      = false;
        try {
            PreparedStatement ps = cConnection.prepareStatement(
                "SELECT TOP 1 firstname,lastname,zip,filler FROM test WHERE zip = ?");
            for (; i < bigops; i++) {
                ps.setInt(1, nextIntRandom(randomgen, smallrows));
                ps.execute();
                if ((i + 1) == 100 && sw.elapsedTime() > 50000) {
                    slow = true;
                }
                if (reportProgress && (i + 1) % 10000 == 0
                        || (slow && (i + 1) % 100 == 0)) {
                    System.out.println("Select " + (i + 1) + " : "
                                       + sw.elapsedTime() + " rps: "
                                       + (i * 1000 / (sw.elapsedTime() + 1)));
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        long time = sw.elapsedTime();
        long rate = ((long) i * 1000) / (time + 1);
        storeResult("select random zip", i, time, rate);
        System.out.println("select time for random zip " + i + " rows  -- "
                           + time + " ms -- " + rate + " tps");
    }
    void selectID() {
        StopWatch        sw        = new StopWatch();
        java.util.Random randomgen = new java.util.Random();
        int              i         = 0;
        boolean          slow      = false;
        try {
            PreparedStatement ps = cConnection.prepareStatement(
                "SELECT firstname,lastname,zip,filler FROM test WHERE id = ?");
            for (i = 0; i < smallops; i++) {
                ps.setInt(1, nextIntRandom(randomgen, bigrows - 1));
                ps.execute();
                if (reportProgress && (i + 1) % 10000 == 0
                        || (slow && (i + 1) % 100 == 0)) {
                    System.out.println("Select " + (i + 1) + " : "
                                       + (sw.elapsedTime() + 1));
                }
            }
            ps.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        long time = sw.elapsedTime();
        long rate = ((long) i * 1000) / (time + 1);
        storeResult("select random id", i, time, rate);
        System.out.println("select time for random id " + i + " rows  -- "
                           + time + " ms -- " + rate + " tps");
    }
    void selectZipTable() {
        StopWatch        sw        = new StopWatch();
        java.util.Random randomgen = new java.util.Random();
        int              i         = 0;
        boolean          slow      = false;
        try {
            PreparedStatement ps = cConnection.prepareStatement(
                "SELECT zip FROM zip WHERE zip = ?");
            for (i = 0; i < bigops; i++) {
                ps.setInt(1, nextIntRandom(randomgen, smallrows - 1));
                ps.execute();
                if (reportProgress && (i + 1) % 10000 == 0
                        || (slow && (i + 1) % 100 == 0)) {
                    System.out.println("Select " + (i + 1) + " : "
                                       + (sw.elapsedTime() + 1));
                }
            }
            ps.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        long time = sw.elapsedTime();
        long rate = ((long) i * 1000) / (time + 1);
        storeResult("select random zip (zip table)", i, time, rate);
        System.out.println("select time for random zip from zip table " + i
                           + " rows  -- " + time + " ms -- " + rate + " tps");
    }
    private void countTestID() {
        try {
            StopWatch sw = new StopWatch();
            sStatement.execute("SELECT count(*) from TEST where id > -1");
            ResultSet rs = sStatement.getResultSet();
            rs.next();
            long time = sw.elapsedTime();
            long rate = ((long) bigrows * 1000) / (time + 1);
            storeResult("count (index on id)", rs.getInt(1), time, rate);
            System.out.println("count time (index on id) " + rs.getInt(1)
                               + " rows  -- " + time + " ms -- " + rate
                               + " tps");
            sw.zero();
            sStatement.execute("SELECT count(*) from TEST");
            rs = sStatement.getResultSet();
            rs.next();
            time = sw.elapsedTime();
            rate = (1000L) / (time + 1);
            storeResult("count (index on id)", rs.getInt(1), time, rate);
            System.out.println("count time (full count) " + rs.getInt(1)
                               + " rows  -- " + time + " ms -- " + rate
                               + " tps");
        } catch (SQLException e) {}
    }
    private void countTestZip() {
        try {
            StopWatch sw = new StopWatch();
            sStatement.execute("SELECT count(*) from TEST where zip > -1");
            ResultSet rs = sStatement.getResultSet();
            rs.next();
            long time = (long) sw.elapsedTime();
            long rate = ((long) bigrows * 1000) / (time + 1);
            storeResult("count (index on zip)", rs.getInt(1), time, rate);
            System.out.println("count time (index on zip) " + rs.getInt(1)
                               + " rows  -- " + time + " ms -- " + rate
                               + " tps");
        } catch (SQLException e) {}
    }
    private void countZip() {
        try {
            StopWatch sw = new StopWatch();
            sStatement.execute("SELECT count(*) from zip where zip > -1");
            ResultSet rs = sStatement.getResultSet();
            rs.next();
            System.out.println("count time (zip table) " + rs.getInt(1)
                               + " rows  -- " + sw.elapsedTime() + " ms");
        } catch (SQLException e) {}
    }
    private void updateZip() {
        StopWatch        sw        = new StopWatch();
        java.util.Random randomgen = new java.util.Random();
        int              i         = 0;
        boolean          slow      = false;
        int              count     = 0;
        int              random    = 0;
        try {
            PreparedStatement ps = cConnection.prepareStatement(
                "UPDATE test SET filler = filler || zip WHERE zip = ?");
            for (; i < smallrows; i++) {
                random = nextIntRandom(randomgen, smallrows - 1);
                ps.setInt(1, random);
                count += ps.executeUpdate();
                if (reportProgress && count % 10000 < 20) {
                    System.out.println("Update " + count + " : "
                                       + (sw.elapsedTime() + 1));
                }
            }
            ps.close();
        } catch (SQLException e) {
            System.out.println("error : " + random);
            e.printStackTrace();
        }
        long time = sw.elapsedTime();
        long rate = (i * 1000) / (time + 1);
        storeResult("update with random zip", i, time, rate);
        System.out.println("update time with random zip " + i + " rows  -- "
                           + time + " ms -- " + rate + " tps");
    }
    void updateID() {
        StopWatch        sw        = new StopWatch();
        java.util.Random randomgen = new java.util.Random();
        int              i         = 0;
        boolean          slow      = false;
        int              count     = 0;
        int              random    = 0;
        try {
            PreparedStatement ps = cConnection.prepareStatement(
                "UPDATE test SET zip = zip + 1 WHERE id = ? and zip <> "
                + smallrows);
            for (i = 0; i < smallops; i++) {
                random = nextIntRandom(randomgen, bigrows - 1);
                ps.setInt(1, random);
                ps.execute();
                if (reportProgress && (i + 1) % 10000 == 0
                        || (slow && (i + 1) % 100 == 0)) {
                    System.out.println("Update " + (i + 1) + " : "
                                       + sw.elapsedTime() + " rps: "
                                       + (i * 1000 / (sw.elapsedTime() + 1)));
                }
            }
            ps.close();
        } catch (SQLException e) {
            System.out.println("error : " + random);
            e.printStackTrace();
        }
        long time = sw.elapsedTime();
        long rate = (i * 1000) / (time + 1);
        storeResult("update with random id", i, time, rate);
        System.out.println("update time with random id " + i + " rows  -- "
                           + time + " ms -- " + rate + " tps");
    }
    void updateTestString() {
        StopWatch        sw        = new StopWatch();
        java.util.Random randomgen = new java.util.Random();
        int              i         = 0;
        boolean          slow      = false;
        int              count     = 0;
        int              random    = 0;
        try {
            PreparedStatement ps = cConnection.prepareStatement(
                "UPDATE test SET filler = ? WHERE id = ? and zip <> "
                + smallrows);
            for (i = 0; i < smallops * 2; i++) {
                random = nextIntRandom(randomgen, bigrows - 1);
                int randomLength = nextIntRandom(randomgen, filler.length());
                String newFiller = filler.substring(randomLength);
                ps.setString(1, newFiller);
                ps.setInt(2, random);
                ps.execute();
                if (reportProgress && (i + 1) % 10000 == 0
                        || (slow && (i + 1) % 100 == 0)) {
                    System.out.println("Update " + (i + 1) + " : "
                                       + sw.elapsedTime() + " rps: "
                                       + (i * 1000 / (sw.elapsedTime() + 1)));
                }
            }
            ps.close();
        } catch (SQLException e) {
            System.out.println("error : " + random);
            e.printStackTrace();
        }
        long time = sw.elapsedTime();
        long rate = (i * 1000) / (time + 1);
        storeResult("update with random id", i, time, rate);
        System.out.println("update time with random id " + i + " rows  -- "
                           + time + " ms -- " + rate + " tps");
    }
    void updateIDLinear() {
        StopWatch        sw        = new StopWatch();
        java.util.Random randomgen = new java.util.Random();
        int              i         = 0;
        boolean          slow      = false;
        int              count     = 0;
        int              random    = 0;
        try {
            PreparedStatement ps = cConnection.prepareStatement(
                "UPDATE test SET zip = zip + 1 WHERE id = ? and zip <> "
                + smallrows);
            for (i = 0; i < bigops; i++) {
                random = i;
                ps.setInt(1, random);
                ps.execute();
                if (reportProgress && (i + 1) % 10000 == 0
                        || (slow && (i + 1) % 100 == 0)) {
                    System.out.println("Update " + (i + 1) + " : "
                                       + sw.elapsedTime() + " rps: "
                                       + (i * 1000 / (sw.elapsedTime() + 1)));
                }
            }
            ps.close();
        } catch (SQLException e) {
            System.out.println("error : " + random);
            e.printStackTrace();
        }
        long time = sw.elapsedTime();
        long rate = (i * 1000) / (time + 1);
        storeResult("update with sequential id", i, time, rate);
        System.out.println("update time with sequential id " + i
                           + " rows  -- " + time + " ms -- " + rate + " tps");
    }
    void deleteTest() {
        StopWatch        sw        = new StopWatch();
        java.util.Random randomgen = new java.util.Random();
        int              i         = 0;
        boolean          slow      = false;
        int              count     = 0;
        int              random    = 0;
        try {
            PreparedStatement ps =
                cConnection.prepareStatement("DELETE FROM test WHERE id = ?");
            for (i = 0; count < smallops; i++) {
                random = nextIntRandom(randomgen, bigrows);
                ps.setInt(1, random);
                count += ps.executeUpdate();
                if (reportProgress && (i + 1) % 10000 == 0
                        || (slow && (i + 1) % 100 == 0)) {
                    System.out.println("delete " + (i + 1) + " : "
                                       + sw.elapsedTime() + " rps: "
                                       + (i * 1000 / (sw.elapsedTime() + 1)));
                }
            }
            ps.close();
        } catch (SQLException e) {
            System.out.println("error : " + random);
            e.printStackTrace();
        }
        long time = sw.elapsedTime();
        long rate = (count * 1000) / (time + 1);
        storeResult("delete with random id", count, time, rate);
        System.out.println("delete time for random id " + count + " rows  -- "
                           + time + " ms -- " + rate + " tps");
    }
    void deleteZipTable() {
        StopWatch        sw        = new StopWatch();
        java.util.Random randomgen = new java.util.Random();
        int              i         = 0;
        boolean          slow      = false;
        int              count     = 0;
        int              random    = 0;
        try {
            PreparedStatement ps =
                cConnection.prepareStatement("DELETE FROM zip WHERE zip = ?");
            for (i = 0; i <= smallrows; i++) {
                random = i;
                ps.setInt(1, random);
                count += ps.executeUpdate();
                if (reportProgress && (i + 1) % 10000 == 0
                        || (slow && (i + 1) % 100 == 0)) {
                    System.out.println("delete " + (i + 1) + " : "
                                       + sw.elapsedTime() + " rps: "
                                       + (i * 1000 / (sw.elapsedTime() + 1)));
                }
            }
            ps.close();
        } catch (SQLException e) {
            System.out.println("error : " + random);
            e.printStackTrace();
        }
        long time = sw.elapsedTime();
        long rate = ((long) count * 1000) / (time + 1);
        storeResult("delete with random zip", count, time, rate);
        System.out.println("delete time for random zip " + count
                           + " rows  -- " + time + " ms -- " + rate + " tps");
    }
    void storeResult(String description, int count, long time, long rate) {
        try {
            writer.write("<tr><td>" + description + "</td><td>" + count
                         + "</td><td>" + time + "</td><td>" + rate
                         + "</td></tr>\n");
        } catch (Exception e) {}
    }
    static void deleteDatabase(String path) {
        FileUtil fileUtil = FileUtil.getFileUtil();
        fileUtil.delete(path + ".backup");
        fileUtil.delete(path + ".properties");
        fileUtil.delete(path + ".script");
        fileUtil.delete(path + ".data");
        fileUtil.delete(path + ".log");
        fileUtil.delete(path + ".lck");
        fileUtil.delete(path + ".csv");
    }
    int nextIntRandom(Random r, int range) {
        int b = r.nextInt();
        if (b == Integer.MIN_VALUE) {
            b = Integer.MAX_VALUE;
        }
        b = Math.abs(b);
        return b % range;
    }
    public static void main(String[] argv) {
        TestCacheSize  test  = new TestCacheSize();
        HsqlProperties props = HsqlProperties.argArrayToProps(argv, "test");
        test.bigops   = props.getIntegerProperty("test.bigops", test.bigops);
        test.bigrows  = test.bigops;
        test.smallops = test.bigops / 8;
        test.cacheScale = props.getIntegerProperty("test.scale",
                test.cacheScale);
        test.tableType = props.getProperty("test.tabletype", test.tableType);
        test.nioMode   = props.isPropertyTrue("test.nio", test.nioMode);
        if (props.getProperty("test.dbtype", "").equals("mem")) {
            test.filepath = "mem:test";
            test.filedb   = false;
            test.shutdown = false;
        }
        test.setUp();
        StopWatch sw = new StopWatch();
        test.testFillUp();
        test.checkResults();
        long time = sw.elapsedTime();
        test.storeResult("total test time", 0, (int) time, 0);
        System.out.println("total test time -- " + sw.elapsedTime() + " ms");
        test.tearDown();
    }
}