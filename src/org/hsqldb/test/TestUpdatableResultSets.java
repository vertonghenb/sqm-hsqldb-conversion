package org.hsqldb.test;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
public class TestUpdatableResultSets extends TestBase {
    Connection connection;
    Statement  statement;
    public TestUpdatableResultSets(String name) {
        super(name);
    }
    protected void setUp() {
        super.setUp();
        try {
            connection = super.newConnection();
            statement = connection.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                                                   ResultSet.CONCUR_UPDATABLE);
        } catch (Exception e) {
            System.out.println(e);
        }
    }
    public void testUpdatable() {
        try {
            statement.execute("drop table t1 if exists");
            statement.execute(
                "create table t1 (i int primary key, c varchar(10), t varbinary(3))");
            String            insert = "insert into t1 values(?,?,?)";
            String            select = "select i, c, t from t1";
            PreparedStatement ps     = connection.prepareStatement(insert);
            for (int i = 0; i < 10; i++) {
                ps.setInt(1, i);
                ps.setString(2, String.valueOf(i) + " s");
                ps.setBytes(3, new byte[] {
                    (byte) i, ' ', (byte) i
                });
                ps.execute();
            }
            connection.setAutoCommit(false);
            ResultSet rs = statement.executeQuery(select);
            while (rs.next()) {
                String s = rs.getString(2);
                rs.updateString(2, s + s);
                rs.updateRow();
            }
            rs.close();
            rs = statement.executeQuery(select);
            while (rs.next()) {
                String s = rs.getString(2);
                System.out.println(s);
            }
            connection.rollback();
            rs = statement.executeQuery(select);
            while (rs.next()) {
                String s = rs.getString(2);
                System.out.println(s);
            }
            connection.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    public void testScrollable() {
        try {
            statement.execute("drop table t1 if exists");
            statement.execute(
                "create table t1 (i int primary key, c varchar(10), t varbinary(3))");
            statement.close();
            String            insert = "insert into t1 values(?,?,?)";
            String            select = "select i, c, t from t1";
            PreparedStatement ps     = connection.prepareStatement(insert);
            for (int i = 0; i < 10; i++) {
                ps.setInt(1, i);
                ps.setString(2, String.valueOf(i) + " s");
                ps.setBytes(3, new byte[] {
                    (byte) i, ' ', (byte) i
                });
                ps.execute();
            }
            connection.setAutoCommit(false);
            statement =
                connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                                           ResultSet.CONCUR_READ_ONLY);
            ResultSet srs = statement.executeQuery("select * from t1 limit 2");
            srs.afterLast();
            while (srs.previous()) {
                String name = srs.getString(2);
                float  id   = srs.getFloat(1);
                System.out.println(name + "   " + id);
            }
            srs.close();
            srs = statement.executeQuery("select * from t1 limit 2");
            srs.absolute(3);
            while (srs.previous()) {
                String name = srs.getString(2);
                float  id   = srs.getFloat(1);
                System.out.println(name + "   " + id);
            }
            srs.absolute(2);
            while (srs.previous()) {
                String name = srs.getString(2);
                float  id   = srs.getFloat(1);
                System.out.println(name + "   " + id);
            }
            srs.absolute(-1);
            while (srs.previous()) {
                String name = srs.getString(2);
                float  id   = srs.getFloat(1);
                System.out.println(name + "   " + id);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}