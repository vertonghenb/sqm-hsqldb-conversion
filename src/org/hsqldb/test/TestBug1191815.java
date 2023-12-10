package org.hsqldb.test;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;
import junit.framework.TestCase;
import junit.framework.TestResult;
public class TestBug1191815 extends TestBase {
    public TestBug1191815(String name) {
        super(name);
    }
    public void test() throws Exception {
        try {
            Connection conn = newConnection();
            Statement  stmt = conn.createStatement();
            stmt.executeUpdate("drop table testA if exists;");
            stmt.executeUpdate("create table testA(data timestamp);");
            TimeZone pst = TimeZone.getTimeZone("PST");
            Calendar cal = new GregorianCalendar(pst);
            cal.clear();
            cal.set(2005, 0, 1, 0, 0, 0);
            Timestamp ts = new Timestamp(cal.getTimeInMillis());
            ts.setNanos(1000);
            PreparedStatement ps =
                conn.prepareStatement("insert into testA values(?)");
            ps.setTimestamp(1, ts, cal);
            ps.execute();
            ps.setTimestamp(1, ts, null);
            ps.execute();
            String sql = "select * from testA";
            stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            rs.next();
            Timestamp returned = rs.getTimestamp(1, cal);
            rs.next();
            Timestamp def = rs.getTimestamp(1, null);
            assertEquals(ts, returned);
            assertEquals(ts, def);
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }
    public static void main(String[] args) throws Exception {
        TestResult            result;
        TestCase              test;
        java.util.Enumeration exceptions;
        java.util.Enumeration failures;
        int                   count;
        result = new TestResult();
        test   = new TestBug1191815("test");
        test.run(result);
        count = result.failureCount();
        System.out.println("TestBug1192000 failure count: " + count);
        failures = result.failures();
        while (failures.hasMoreElements()) {
            System.out.println(failures.nextElement());
        }
    }
}