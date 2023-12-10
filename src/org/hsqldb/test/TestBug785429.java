package org.hsqldb.test;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import junit.framework.TestCase;
import junit.framework.TestResult;
public class TestBug785429 extends TestBase {
    Statement  stmt;
    Connection conn;
    public TestBug785429(String name) {
        super(name);
    }
    public void test() throws Exception {
        Connection conn = newConnection();
        conn.setAutoCommit(false);
        Statement         stmt = conn.createStatement();
        String            sql;
        String            msg;
        PreparedStatement ps;
        ResultSet         rs;
        int               rowcount = 0;
        stmt.executeUpdate("drop table testA if exists;");
        stmt.executeUpdate("drop table testB if exists;");
        stmt.executeUpdate(
            "create table testA(oid binary(2), data integer);");
        stmt.executeUpdate(
            "create table testB(oid binary(2), data integer);");
        stmt.executeUpdate("insert into testA values(X'0001',1);");
        stmt.executeUpdate("insert into testB values(X'0001',1);");
        sql = "select * from testA as ttt,(select oid,data from testB) as tst "
              + "where (tst.oid=ttt.oid) and (tst.oid=X'0001');";
        rs       = stmt.executeQuery(sql);
        rowcount = 0;
        while (rs.next()) {
            rowcount++;
        }
        msg = sql + ": row count:";
        assertEquals(msg, 1, rowcount);
        stmt.execute("drop table testA if exists");
        stmt.execute("drop table testB if exists");
        stmt.execute("create table testA(oid binary(2), data integer)");
        stmt.execute("create table testB(oid binary(2), data integer)");
        byte[] oid = new byte[] {
            0, 1
        };
        ps = conn.prepareStatement("insert into testA values(?,1)");
        ps.setBytes(1, oid);
        ps.execute();
        ps = conn.prepareStatement("insert into testB values (?,1)");
        ps.setBytes(1, oid);
        ps.execute();
        sql = "select * from testA as ttt,(select oid,data from testB) as tst "
              + "where (tst.oid=ttt.oid) and (tst.oid=?);";
        try {
            ps = conn.prepareStatement(sql);
        } catch (Exception e) {
            e.printStackTrace();
        }
        ps.setBytes(1, oid);
        rs       = ps.executeQuery();
        rowcount = 0;
        int colCount = rs.getMetaData().getColumnCount();
        while (rs.next()) {
            rowcount++;
        }
        msg = sql + ": row count:";
        assertEquals(msg, 1, rowcount);
    }
    public static void main(String[] args) throws Exception {
        TestResult            result;
        TestCase              test;
        java.util.Enumeration exceptions;
        java.util.Enumeration failures;
        int                   count;
        result = new TestResult();
        test   = new TestBug785429("test");
        test.run(result);
        count = result.failureCount();
        System.out.println("TestBug785429 failure count: " + count);
        failures = result.failures();
        while (failures.hasMoreElements()) {
            System.out.println(failures.nextElement());
        }
    }
}