package org.hsqldb.test;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import junit.framework.TestCase;
import junit.framework.TestResult;
public class TestBug778213 extends TestBase {
    public TestBug778213(String name) {
        super(name);
    }
    public void test() throws Exception {
        Connection        conn = newConnection();
        PreparedStatement pstmt;
        int               updateCount;
        try {
            pstmt = conn.prepareStatement("drop table test if exists");
            pstmt.executeUpdate();
            pstmt       = conn.prepareStatement("create table test(id int)");
            updateCount = pstmt.executeUpdate();
            assertTrue("expected update count of zero", updateCount == 0);
            pstmt       = conn.prepareStatement("drop table test");
            updateCount = pstmt.executeUpdate();
            assertTrue("expected update count of zero", updateCount == 0);
        } catch (Exception e) {
            assertTrue("unable to prepare or execute DDL", false);
        } finally {
            conn.close();
        }
        conn = newConnection();
        try {
            pstmt = conn.prepareStatement("create table test(id int)");
            assertTrue("got data expecting update count", !pstmt.execute());
        } catch (Exception e) {
            assertTrue("unable to prepare or execute DDL", false);
        } finally {
            conn.close();
        }
        conn = newConnection();
        boolean exception = true;
        try {
            pstmt = conn.prepareStatement("drop table test");
            pstmt.executeQuery();
        } catch (SQLException e) {
            exception = false;
        } finally {
            conn.close();
        }
        if (exception) {
            assertTrue("no exception thrown for executeQuery(DDL)", false);
        }
        conn = newConnection();
        try {
            pstmt = conn.prepareStatement("call identity()");
            pstmt.execute();
        } catch (Exception e) {
            assertTrue("unable to prepare or execute call", false);
        } finally {
            conn.close();
        }
        exception = false;
        conn      = newConnection();
        try {
            pstmt = conn.prepareStatement("create table test(id int)");
            pstmt.addBatch();
        } catch (SQLException e) {
            exception = true;
        } finally {
            conn.close();
        }
        if (exception) {
            assertTrue("not expected exception batching prepared DDL", false);
        }
        conn = newConnection();
        try {
            pstmt = conn.prepareStatement("create table test(id int)");
            assertTrue("expected null ResultSetMetadata for prepared DDL",
                       null == pstmt.getMetaData());
        } finally {
            conn.close();
        }
        conn = newConnection();
        try {
            pstmt = conn.prepareStatement("create table test(id int)");
            assertTrue("expected zero parameter for prepared DDL",
                       0 == pstmt.getParameterMetaData().getParameterCount());
        } finally {
            conn.close();
        }
    }
    public static void main(String[] args) throws Exception {
        TestResult            result;
        TestCase              test;
        java.util.Enumeration failures;
        int                   count;
        result = new TestResult();
        test   = new TestBug778213("test");
        test.run(result);
        count = result.failureCount();
        System.out.println("TestBug778213 failure count: " + count);
        failures = result.failures();
        while (failures.hasMoreElements()) {
            System.out.println(failures.nextElement());
        }
    }
}