package org.hsqldb.test;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import org.hsqldb.lib.StopWatch;
public class TestAnother {
    protected String url = "jdbc:hsqldb:";
    protected String filepath = "/hsql/testtime/test";
    public TestAnother() {}
    public void setUp() {
        String user     = "sa";
        String password = "";
        try {
            Connection conn = null;
            Class.forName("org.hsqldb.jdbc.JDBCDriver");
            conn = DriverManager.getConnection(url + filepath, user,
                                               password);
            Statement stmnt = conn.createStatement();
            Statement st    = conn.createStatement();
            st.executeUpdate("CREATE TABLE TT(D DATE)");
            st.executeUpdate("INSERT INTO TT VALUES ('2004-01-02')");
            st.executeUpdate("INSERT INTO TT VALUES ('2004-02-02')");
            ResultSet rs = st.executeQuery("SELECT * FROM TT");
            while (rs.next()) {
                System.out.println(rs.getDate(1));
            }
            st.executeUpdate("DROP TABLE TT");
            rs.close();
            Statement stm = conn.createStatement();
            stm.executeUpdate(
                "create table test (id int,atime timestamp default current_timestamp)");
            stm = conn.createStatement();
            int count = stm.executeUpdate("insert into test (id) values (1)");
            System.out.println(count);
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("TestSql.setUp() error: " + e.getMessage());
        }
    }
    public static void main(String[] argv) {
        StopWatch   sw   = new StopWatch();
        TestAnother test = new TestAnother();
        test.setUp();
        System.out.println("Total Test Time: " + sw.elapsedTime());
    }
}