package org.hsqldb.test;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Savepoint;
import java.sql.Statement;
import org.hsqldb.server.WebServer;
import junit.framework.TestCase;
import junit.framework.TestResult;
import org.hsqldb.server.Server;
public class TestJDBCSavepoints extends TestCase {
    String serverProps =
        "database.0=mem:test;dbname.0=;silent=true;trace=false";
    String url = "jdbc:hsqldb:hsql://localhost";
    String     user;
    String     password;
    Statement  stmt;
    Connection conn1;
    Connection conn2;
    Server server;
    public TestJDBCSavepoints(String name) {
        super(name);
    }
    protected void setUp() {
        user     = "sa";
        password = "";
        stmt     = null;
        conn1    = null;
        conn2    = null;
        server   = new Server();
        server.putPropertiesFromString(serverProps);
        server.start();
        try {
            Class.forName("org.hsqldb.jdbc.JDBCDriver");
            conn1 = DriverManager.getConnection(url, user, password);
            conn2 = DriverManager.getConnection(url, user, password);
            stmt  = conn1.createStatement();
        } catch (Exception e) {
            System.out.println(this + ".setUp() error: " + e.getMessage());
        }
    }
    protected void tearDown() {
        try {
            conn1.close();
        } catch (Exception e) {
            System.out.println(this + ".tearDown() error: " + e.getMessage());
        }
        try {
            conn2.close();
        } catch (Exception e) {
            System.out.println(this + ".tearDown() error: " + e.getMessage());
        }
        server.stop();
    }
    public void testJDBCSavepoints() throws Exception {
        String            sql;
        String            msg;
        int               i;
        PreparedStatement ps;
        ResultSet         rs;
        Savepoint         sp1;
        Savepoint         sp2;
        Savepoint         sp3;
        Savepoint         sp4;
        Savepoint         sp5;
        Savepoint         sp6;
        Savepoint         sp7;
        int               rowcount = 0;
        sql = "drop table t if exists";
        stmt.executeUpdate(sql);
        sql = "create table t(id int, fn varchar(40), ln varchar(40), zip int)";
        stmt.executeUpdate(sql);
        conn1.setAutoCommit(true);
        conn1.setAutoCommit(false);
        sql = "insert into t values(?,?,?,?)";
        ps  = conn1.prepareStatement(sql);
        ps.setString(2, "Mary");
        ps.setString(3, "Peterson-Clancy");
        i = 0;
        for (; i < 10; i++) {
            ps.setInt(1, i);
            ps.setInt(4, i);
            ps.executeUpdate();
        }
        sp1 = conn1.setSavepoint("savepoint1");
        for (; i < 20; i++) {
            ps.setInt(1, i);
            ps.setInt(4, i);
            ps.executeUpdate();
        }
        sp2 = conn1.setSavepoint("savepoint2");
        for (; i < 30; i++) {
            ps.setInt(1, i);
            ps.setInt(4, i);
            ps.executeUpdate();
        }
        sp3 = conn1.setSavepoint("savepoint3");
        for (; i < 40; i++) {
            ps.setInt(1, i);
            ps.setInt(4, i);
            ps.executeUpdate();
        }
        sp4 = conn1.setSavepoint("savepoint4");
        for (; i < 50; i++) {
            ps.setInt(1, i);
            ps.setInt(4, i);
            ps.executeUpdate();
        }
        sp5 = conn1.setSavepoint("savepoint5");
        sp6 = conn1.setSavepoint("savepoint6");
        sp7 = conn1.setSavepoint("savepoint7");
        rs  = stmt.executeQuery("select count(*) from t");
        rs.next();
        rowcount = rs.getInt(1);
        rs.close();
        msg = "select count(*) from t value";
        try {
            assertEquals(msg, 50, rowcount);
        } catch (Exception e) {}
        conn2.setAutoCommit(false);
        conn2.setSavepoint("savepoint1");
        conn2.setSavepoint("savepoint2");
        msg = "savepoint released succesfully on non-originating connection";
        try {
            conn2.releaseSavepoint(sp2);
            assertTrue(msg, false);
        } catch (Exception e) {}
        try {
            conn2.rollback(sp1);
            msg = "succesful rollback to savepoint on "
                  + "non-originating connection";
            assertTrue(msg, false);
        } catch (Exception e) {}
        msg = "direct execution of <release savepoint> statement failed to "
              + "release JDBC-created SQL-savepoint with identical savepoint name";
        try {
            conn2.createStatement().executeUpdate(
                "release savepoint \"savepoint2\"");
        } catch (Exception e) {
            try {
                assertTrue(msg, false);
            } catch (Exception e2) {}
        }
        msg = "direct execution of <rollback to savepoint> statement failed to "
              + "roll back to existing JDBC-created SQL-savepoint with identical "
              + "savepoint name";
        try {
            conn2.createStatement().executeUpdate(
                "rollback to savepoint \"savepoint1\"");
        } catch (Exception e) {
            e.printStackTrace();
            try {
                assertTrue(msg, false);
            } catch (Exception e2) {}
        }
        conn1.releaseSavepoint(sp6);
        msg = "savepoint released succesfully > 1 times";
        try {
            conn1.releaseSavepoint(sp6);
            assertTrue(msg, false);
        } catch (Exception e) {}
        msg = "savepoint released successfully after preceding savepoint released";
        try {
            conn1.releaseSavepoint(sp7);
            assertTrue(msg, false);
        } catch (Exception e) {}
        msg = "preceding same-point savepoint destroyed by following savepoint release";
        try {
            conn1.releaseSavepoint(sp5);
        } catch (Exception e) {
            try {
                assertTrue(msg, false);
            } catch (Exception e2) {}
        }
        conn1.rollback(sp4);
        rs = stmt.executeQuery("select count(*) from t");
        rs.next();
        rowcount = rs.getInt(1);
        rs.close();
        msg = "select * rowcount after 50 inserts - 10 rolled back:";
        try {
            assertEquals(msg, 40, rowcount);
        } catch (Exception e) {}
        msg = "savepoint rolled back succesfully > 1 times";
        try {
            conn1.rollback(sp4);
            assertTrue(msg, false);
        } catch (Exception e) {}
        conn1.rollback(sp3);
        rs = stmt.executeQuery("select count(*) from t");
        rs.next();
        rowcount = rs.getInt(1);
        rs.close();
        msg = "select count(*) after 50 inserts - 20 rolled back:";
        try {
            assertEquals(msg, 30, rowcount);
        } catch (Exception e) {}
        msg = "savepoint released succesfully after use in rollback";
        try {
            conn1.releaseSavepoint(sp3);
            assertTrue(msg, false);
        } catch (Exception e) {}
        conn1.rollback(sp1);
        msg = "savepoint rolled back without raising an exception after "
              + "rollback to a preceeding savepoint";
        try {
            conn1.rollback(sp2);
            assertTrue(msg, false);
        } catch (Exception e) {}
        conn1.rollback();
        msg = "savepoint released succesfully when it should have been "
              + "destroyed by a full rollback";
        try {
            conn1.releaseSavepoint(sp1);
            assertTrue(msg, false);
        } catch (Exception e) {}
        conn1.setAutoCommit(false);
        sp1 = conn1.setSavepoint("savepoint1");
        conn1.rollback();
        conn1.setAutoCommit(false);
        conn1.createStatement().executeUpdate("savepoint \"savepoint1\"");
        conn1.setAutoCommit(false);
        sp1 = conn1.setSavepoint("savepoint1");
        conn1.createStatement().executeUpdate("savepoint \"savepoint1\"");
        conn1.setAutoCommit(false);
        sp1 = conn1.setSavepoint("savepoint1");
        conn1.createStatement().executeUpdate("savepoint \"savepoint1\"");
    }
    public void testJDBCAutoSavepoints() throws Exception {
        String            sql;
        int               i;
        PreparedStatement ps;
        ResultSet         rs;
        Savepoint         sp1;
        int               rowcount = 0;
        sql = "drop table t if exists";
        stmt.executeUpdate(sql);
        sql = "create table t(id int, fn varchar(40), ln varchar(40), zip int)";
        stmt.executeUpdate(sql);
        conn1.setAutoCommit(false);
        sql = "insert into t values(?,?,?,?)";
        ps  = conn1.prepareStatement(sql);
        ps.setString(2, "Mary");
        ps.setString(3, "Peterson-Clancy");
        i = 0;
        for (; i < 10; i++) {
            ps.setInt(1, i);
            ps.setInt(4, i);
            ps.executeUpdate();
        }
        sp1 = conn1.setSavepoint();
        for (; i < 12; i++) {
            ps.setInt(1, i);
            ps.setInt(4, i);
            ps.executeUpdate();
        }
        conn1.rollback(sp1);
        rs = stmt.executeQuery("select count(*) from t");
        rs.next();
        rowcount = rs.getInt(1);
        assertEquals(10, rowcount);
        rs.close();
    }
    public static void main(String[] args) throws Exception {
        TestResult            result;
        TestCase              test;
        java.util.Enumeration failures;
        int                   count;
        result = new TestResult();
        test   = new TestJDBCSavepoints("testJDBCSavepoints");
        test.run(result);
        count = result.failureCount();
        System.out.println("TestJDBCSavepoints failure count: " + count);
        failures = result.failures();
        while (failures.hasMoreElements()) {
            System.out.println(failures.nextElement());
        }
    }
}