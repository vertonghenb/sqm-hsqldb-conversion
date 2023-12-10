package org.hsqldb.test;
import java.io.File;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
public class HsqldbTestCase {
    public static boolean deleteDir(File dir) {
        if (dir.isDirectory()) {
            String[] children = dir.list();
            for (int i = 0; i < children.length; i++) {
                boolean success = deleteDir(new File(dir, children[i]));
                if (!success) {
                    return false;
                }
            }
        }
        return dir.delete();
    }
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("user", "sa");
        props.put("password", "");
        props.put("hsqldb.default_table_type", "cached");
        props.put("hsqldb.cache_scale", "8");
        props.put("hsqldb.applog", "0");
        props.put("hsqldb.log_size", "200");
        props.put("hsqldb.result_memory_rows", "10");
        props.put("shutdown", "true");
        String url = "jdbc:hsqldb:";
        url += "/hsql/statBase/test";
        HsqldbTestCase.deleteDir(new File("/hsql/statBase/"));
        try {
            Class  clsDriver = Class.forName("org.hsqldb.jdbc.JDBCDriver");
            Driver driver    = (Driver) clsDriver.newInstance();
            DriverManager.registerDriver(driver);
            Connection con = DriverManager.getConnection(url, props);
            String createQuery =
                "drop table test1 if exists;create table test1 (rowNum identity, col1 varchar(50), col2 int, col3 varchar(50))";
            Statement st = con.createStatement();
            st.execute(createQuery);
            st.close();
            String insertQuery =
                "insert into test1 (col1,col2,col3) values (?,?,?)";
            PreparedStatement pst = con.prepareStatement(insertQuery);
            for (int i = 0; i < 1001; i++) {
                pst.setString(1, "string_" + i);
                pst.setInt(2, i);
                pst.setString(3, "string2_" + i);
                pst.addBatch();
                if ((i > 0) && (i % 100 == 0)) {
                    pst.executeBatch();
                }
            }
            pst.close();
            String selectQuery = "select * from test1";
            st = con.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                                     ResultSet.CONCUR_READ_ONLY);
            ResultSet scrollableSet = st.executeQuery(selectQuery);
            scrollableSet.setFetchSize(100);
            scrollableSet.next();
            int tmpIndex = scrollableSet.getInt(3);
            if (tmpIndex != 0) {
                System.out.println("index at 0 is !=0");
            }
            for (int i = 0; i <= 1000; i += 100) {
                scrollableSet.absolute(i + 1);
                tmpIndex = scrollableSet.getInt(3);
                System.out.println(tmpIndex);
            }
            for (int i = 1000; i > 0; i -= 100) {
                scrollableSet.relative(-100);
                tmpIndex = scrollableSet.getInt(3);
                System.out.println(tmpIndex);
            }
        } catch (Exception ex) {
            ex.printStackTrace(System.out);
        }
    }
}