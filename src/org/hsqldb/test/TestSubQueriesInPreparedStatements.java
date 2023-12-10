package org.hsqldb.test;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
public class TestSubQueriesInPreparedStatements {
    public static void main(String[] args) throws Exception {
        test();
    }
    public static void test() throws Exception {
        Connection        conn;
        Statement         stmnt;
        PreparedStatement pstmnt;
        Driver            driver;
        driver =
            (Driver) Class.forName("org.hsqldb.jdbc.JDBCDriver").newInstance();
        DriverManager.registerDriver(driver);
        conn = DriverManager.getConnection("jdbc:hsqldb:mem:test", "sa", "");
        stmnt  = conn.createStatement();
        pstmnt = conn.prepareStatement("drop table t if exists");
        boolean result = pstmnt.execute();
        pstmnt = conn.prepareStatement("create table t(i decimal)");
        int updatecount = pstmnt.executeUpdate();
        pstmnt = conn.prepareStatement("insert into t values(?)");
        for (int i = 0; i < 100; i++) {
            pstmnt.setInt(1, i);
            pstmnt.executeUpdate();
        }
        pstmnt = conn.prepareStatement(
            "select * from (select * from t where i < ?)");
        System.out.println("Expecting: 0..3");
        pstmnt.setInt(1, 4);
        ResultSet rs = pstmnt.executeQuery();
        while (rs.next()) {
            System.out.println(rs.getInt(1));
        }
        System.out.println("Expecting: 0..4");
        pstmnt.setInt(1, 5);
        rs = pstmnt.executeQuery();
        while (rs.next()) {
            System.out.println(rs.getInt(1));
        }
        pstmnt = conn.prepareStatement(
            "select sum(i) from (select i from t where i between ? and ?)");
        System.out.println("Expecting: 9");
        pstmnt.setInt(1, 4);
        pstmnt.setInt(2, 5);
        rs = pstmnt.executeQuery();
        while (rs.next()) {
            System.out.println(rs.getInt(1));
        }
        System.out.println("Expecting: 15");
        pstmnt.setInt(2, 6);
        rs = pstmnt.executeQuery();
        while (rs.next()) {
            System.out.println(rs.getInt(1));
        }
        pstmnt = conn.prepareStatement(
            "select * from (select i as c1 from t where i < ?) a, (select i as c2 from t where i < ?) b");
        System.out.println("Expecting: (0,0)");
        pstmnt.setInt(1, 1);
        pstmnt.setInt(2, 1);
        rs = pstmnt.executeQuery();
        while (rs.next()) {
            System.out.println("(" + rs.getInt(1) + "," + rs.getInt(2) + ")");
        }
        System.out.println("Expecting: ((0,0), (0,1), (1,0), (1,1)");
        pstmnt.setInt(1, 2);
        pstmnt.setInt(2, 2);
        rs = pstmnt.executeQuery();
        while (rs.next()) {
            System.out.println("(" + rs.getInt(1) + "," + rs.getInt(2) + ")");
        }
        System.out.println("Expecting: ((0,0) .. (3,3)");
        pstmnt.setInt(1, 4);
        pstmnt.setInt(2, 4);
        rs = pstmnt.executeQuery();
        while (rs.next()) {
            System.out.println("(" + rs.getInt(1) + "," + rs.getInt(2) + ")");
        }
    }
}