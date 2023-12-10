package org.hsqldb.test;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Enumeration;
import junit.framework.TestCase;
import junit.framework.TestResult;
public class TestMerge extends TestBase {
    Statement         stmnt;
    PreparedStatement pstmnt;
    Connection        connection;
    public TestMerge(String name) {
        super(name);
    }
    protected void setUp() {
        super.setUp();
        try {
            connection = super.newConnection();
            stmnt      = connection.createStatement();
        } catch (Exception e) {}
    }
    private void printTable(String table, String cols,
                            int expected) throws SQLException {
        int               rows = 0;
        ResultSet rs = stmnt.executeQuery("SELECT " + cols + " FROM " + table);
        ResultSetMetaData rsmd = rs.getMetaData();
        String result = "Table " + table + ", expecting " + expected
                        + " rows total:\n";
        while (rs.next()) {
            for (int i = 0; i < rsmd.getColumnCount(); i++) {
                result += rsmd.getColumnLabel(i + 1) + ":"
                          + rs.getString(i + 1) + ":";
            }
            result += "\n";
            rows++;
        }
        rs.close();
        System.out.println(result);
        assertEquals(expected, rows);
    }
    private void executeMerge(String merge) throws SQLException {
        stmnt.execute("DROP SCHEMA SA IF EXISTS CASCADE;");
        stmnt.execute("CREATE SCHEMA SA AUTHORIZATION SA");
        stmnt.execute("DROP TABLE SA.T IF EXISTS;");
        stmnt.execute(
            "CREATE TABLE SA.T (I IDENTITY, A CHAR(10), B CHAR(10));");
        stmnt.execute("INSERT INTO SA.T VALUES ((0, 'A', 'a'),"
                      + "(1, 'B', 'b'), (4, 'C', 'c'));");
        stmnt.execute("DROP TABLE SA.S IF EXISTS;");
        stmnt.execute(
            "CREATE TABLE SA.S (I IDENTITY, A CHAR(10), B CHAR(10), C CHAR(10));");
        stmnt.execute(
            "INSERT INTO SA.S VALUES ((0, 'D', 'd', 'Dd'),"
            + "(2, 'E', 'e', 'Ee'), (3, 'F', 'f', 'Ff'), (4, 'G', 'g', 'Gg'));");
        printTable("SA.T", "*", 3);
        printTable("SA.S", "*", 4);
        stmnt.execute(merge);
    }
    public void testMerge1() {
        try {
            executeMerge(
                "MERGE INTO SA.T X " +
                "USING SA.S AS Y " +
                "ON X.I = Y.I " +
                "WHEN MATCHED THEN " +
                    "UPDATE SET X.A = Y.A, X.B = 'UPDATED' " +
                "WHEN NOT MATCHED THEN " +
                    "INSERT (I, A, B) VALUES (Y.I, Y.A, 'INSERTED');"
            );
            printTable("SA.T", "*", 5);
        } catch (SQLException e) {
            fail(e.getMessage());
        }
        System.out.println("testMerge1 complete\n");
    }
    public void testMerge2() {
        try {
            executeMerge(
                "MERGE INTO SA.T " +
                "USING SA.S " +
                "ON T.I = S.I " +
                "WHEN MATCHED THEN " +
                    "UPDATE SET T.A = S.A, T.B = 'UPDATED';"
            );
            printTable("SA.T", "*", 3);
        } catch (SQLException e) {
            fail(e.getMessage());
        }
        System.out.println("testMerge2 complete\n");
    }
    public void testMerge3() {
        try {
            executeMerge(
                "MERGE INTO SA.T " +
                "USING SA.S " +
                "ON T.I = S.I " +
                "WHEN NOT MATCHED THEN " +
                    "INSERT VALUES (S.I, S.A, 'INSERTED');"
            );
            printTable("SA.T", "*", 5);
        } catch (SQLException e) {
            fail(e.getMessage());
        }
        System.out.println("testMerge3 complete\n");
    }
    public void testMerge4() {
        try {
            executeMerge(
                "MERGE INTO SA.T " +
                "USING SA.S " +
                "ON T.I = S.I " +
                "WHEN MATCHED THEN " +
                    "UPDATE SET T.A = S.A, T.B = 'UPDATED' " +
                "WHEN NOT MATCHED THEN " +
                    "INSERT VALUES (S.I, S.A, 'INSERTED');"
            );
            printTable("SA.T", "*", 5);
        } catch (SQLException e) {
            fail(e.getMessage());
        }
        System.out.println("testMerge4 complete\n");
    }
    public void testMerge5() {
        try {
            executeMerge(
                "MERGE INTO SA.T " +
                "USING (SELECT * FROM SA.S) AS X " +
                "ON T.I = X.I " +
                "WHEN MATCHED THEN " +
                    "UPDATE SET T.A = X.A, T.B = 'UPDATED' " +
                "WHEN NOT MATCHED THEN " +
                    "INSERT VALUES (X.I, X.A, 'INSERTED');"
            );
            printTable("SA.T", "*", 5);
        } catch (SQLException e) {
            fail(e.getMessage());
        }
        System.out.println("testMerge5 complete\n");
    }
    public void testMerge6() {
        try {
            executeMerge(
                "MERGE INTO SA.T " +
                "USING (SELECT I, A, C FROM SA.S) AS X " +
                "ON T.I = X.I " +
                "WHEN MATCHED THEN " +
                    "UPDATE SET T.A = X.A, T.B = 'UPDATED' " +
                "WHEN NOT MATCHED THEN " +
                    "INSERT VALUES (X.I, X.A, 'INSERTED');"
            );
            printTable("SA.T", "*", 5);
        } catch (SQLException e) {
            fail(e.getMessage());
        }
        System.out.println("testMerge6 complete\n");
    }
    public void testMerge7() {
        try {
            executeMerge(
                "MERGE INTO SA.T " +
                "USING (SELECT * FROM SA.S WHERE I = 4) AS X " +
                "ON T.I = X.I " +
                "WHEN MATCHED THEN " +
                    "UPDATE SET T.A = X.A, T.B = 'UPDATED' " +
                "WHEN NOT MATCHED THEN " +
                    "INSERT VALUES (X.I, X.A, 'INSERTED');"
            );
            printTable("SA.T", "*", 3);
        } catch (SQLException e) {
            fail(e.getMessage());
        }
        System.out.println("testMerge7 complete\n");
    }
    public void testMerge8() {
        try {
            executeMerge(
                "MERGE INTO SA.T " +
                "USING (SELECT * FROM SA.S WHERE I = 3) AS X " +
                "ON T.I = X.I " +
                "WHEN MATCHED THEN " +
                    "UPDATE SET T.A = X.A, T.B = 'UPDATED' " +
                "WHEN NOT MATCHED THEN " +
                    "INSERT VALUES (X.I, X.A, 'INSERTED');"
            );
            printTable("SA.T", "*", 4);
        } catch (SQLException e) {
            fail(e.getMessage());
        }
        System.out.println("testMerge8 complete\n");
    }
    public void testMerge9() {
        try {
            executeMerge(
                "MERGE INTO SA.T " +
                "USING (SELECT * FROM SA.S WHERE I > 2) AS X " +
                "ON T.I = X.I " +
                "WHEN MATCHED THEN " +
                    "UPDATE SET T.A = X.A, T.B = 'UPDATED' " +
                "WHEN NOT MATCHED THEN " +
                    "INSERT VALUES (X.I, X.A, 'INSERTED');"
            );
            printTable("SA.T", "*", 4);
        } catch (SQLException e) {
            fail(e.getMessage());
        }
        System.out.println("testMerge9 complete\n");
    }
    public void testMerge10() {
        try {
            executeMerge("MERGE INTO SA.T "
                         + "USING (VALUES(10, 'testA', 'testB')) AS X (I, A, B) "
                         + "ON T.I = X.I " + "WHEN MATCHED THEN "
                         + "UPDATE SET T.A = X.A, T.B = 'UPDATED' "
                         + "WHEN NOT MATCHED THEN "
                         + "INSERT VALUES (X.I, X.A, 'INSERTED');");
            printTable("SA.T", "*", 4);
        } catch (SQLException e) {
            fail(e.getMessage());
        }
        System.out.println("testMerge10 complete\n");
    }
    public void testMerge11() {
        try {
            executeMerge("SET SCHEMA PUBLIC");
            PreparedStatement ps = connection.prepareStatement(
        "MERGE INTO SA.T "
                         + "USING (VALUES(CAST(? AS INT), 'testA', 'testB')) AS X (I, A, B) "
                         + "ON T.I = X.I " + "WHEN MATCHED THEN "
                         + "UPDATE SET T.A = X.A, T.B = 'UPDATED' "
                         + "WHEN NOT MATCHED THEN "
                         + "INSERT VALUES (X.I, X.A, 'INSERTED');");
            ps.setInt(1, 10);
            ps.executeUpdate();
            printTable("SA.T", "*", 4);
        } catch (SQLException e) {
            fail(e.getMessage());
        }
        System.out.println("testMerge10 complete\n");
    }
    protected void tearDown() {
        try {
            stmnt.execute("DROP SCHEMA SA IF EXISTS CASCADE;");
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("TestSql.tearDown() error: " + e.getMessage());
        }
        super.tearDown();
    }
    public static void main(String[] argv) {
        TestResult result = new TestResult();
        TestCase   testA  = new TestMerge("testMerge1");
        TestCase   testB  = new TestMerge("testMerge2");
        TestCase   testC  = new TestMerge("testMerge3");
        TestCase   testD  = new TestMerge("testMerge4");
        TestCase   testE  = new TestMerge("testMerge5");
        TestCase   testF  = new TestMerge("testMerge6");
        TestCase   testG  = new TestMerge("testMerge7");
        TestCase   testH  = new TestMerge("testMerge8");
        TestCase   testI  = new TestMerge("testMerge9");
        testA.run(result);
        testB.run(result);
        testC.run(result);
        testD.run(result);
        testE.run(result);
        testF.run(result);
        testG.run(result);
        testH.run(result);
        testI.run(result);
        System.out.println("TestMerge error count: " + result.failureCount());
        Enumeration e = result.failures();
        while (e.hasMoreElements()) {
            System.out.println(e.nextElement());
        }
    }
}