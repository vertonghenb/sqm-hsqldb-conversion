package org.hsqldb.test;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Calendar;
import java.util.GregorianCalendar;
public class TestKarl {
    static final String SHUTDOWN             = "SHUTDOWN";
    static final String SHUTDOWN_IMMEDIATELY = "SHUTDOWN IMMEDIATELY";
    static final String SHUTDOWN_COMPACT     = "SHUTDOWN COMPACT";
    public static void main(String[] args) {
        Connection c = null;
        try {
            File f = new File("db");
            if (!f.exists()) {
                f.mkdir();
            } else {
                File[] list = f.listFiles();
                for (int i = 0; i < list.length; i++) {
                    list[i].delete();
                }
            }
            copy("save/brwahl.properties", "db/brwahl.properties");
            copy("save/brwahl.script", "db/brwahl.script");
            copy("save/waehler.csv", "db/waehler.csv");
            String  shutdown   = SHUTDOWN;
            boolean autocommit = false;
            Class.forName("org.hsqldb.jdbc.JDBCDriver");
            c = DriverManager.getConnection("jdbc:hsqldb:file:db/brwahl",
                                            "sa", "");
            c.setAutoCommit(autocommit);
            printMeta(c);
            printTable(c, "WAEHLER");
            doUpdateInsertDeleteWaehler(c);
            printTable(c, "WAEHLER");
            doCreateTableTest(c);
            printTable(c, "TEST");
            Statement st = c.createStatement();
            st.execute(shutdown);
            st.close();
            c.close();
            c = null;
            System.out.println("\nDB OK? ...");
            c = DriverManager.getConnection("jdbc:hsqldb:file:db/brwahl",
                                            "sa", "");
            c.setAutoCommit(false);
            printTable(c, "WAEHLER");
            printTable(c, "TEST");
            doUpdateInsertDeleteWaehler(c);
            c.rollback();
            if (!autocommit) {
                doCreateTableTest(c);
                c.rollback();
            }
            c.close();
            c = null;
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (c != null) {
                try {
                    c.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    private static void doCreateTableTest(Connection p_connection)
    throws SQLException {
        System.out.println("CREATE TESTTABLE START ...");
        Statement st = p_connection.createStatement();
        st.executeUpdate("DROP TABLE TEST IF EXISTS");
        st.executeUpdate("CREATE TABLE TEST (TEST INTEGER)");
        st.close();
        System.out.println("END CREATE TESTTABLE");
        System.out.println("INSERT INTO TESTTABLE START ...");
        PreparedStatement p = p_connection.prepareStatement(
            "INSERT INTO TEST (TEST) values (?)");
        p.setInt(1, 123);
        p.execute();
        p.close();
        System.out.println("END INSERT INTO TESTTABLE");
    }
    private static void doUpdateInsertDeleteWaehler(Connection p_connection)
    throws SQLException {
        System.out.println("UPDATE WAEHLER START ...");
        PreparedStatement p = p_connection.prepareStatement(
            "UPDATE WAEHLER SET AUSTRITTSDATUM=? WHERE NAME=?");
        p.setDate(1, null);
        p.setString(2, "Muster1");
        p.execute();
        p.close();
        System.out.println("END UPDATE WAEHLER");
        System.out.println("INSERT INTO WAEHLER START ...");
        p = p_connection.prepareStatement(
            "INSERT INTO WAEHLER (NAME, AUSTRITTSDATUM) VALUES (?,?)");
        Calendar cal = GregorianCalendar.getInstance();
        p.setString(1, "Muster3");
        p.setDate(2, new Date(cal.getTimeInMillis()), cal);
        p.execute();
        p.close();
        System.out.println("END INSERT INTO WAEHLER");
        System.out.println("DELETE FROM WAEHLER START ...");
        p = p_connection.prepareStatement(
            "DELETE FROM WAEHLER WHERE NAME = ?");
        p.setString(1, "Muster2");
        p.execute();
        p.close();
        System.out.println("END DELETE FROM WAEHLER");
    }
    private static void printTable(Connection p_connection,
                                   String p_table) throws SQLException {
        System.out.println("GET TABLE " + p_table + " START ...");
        Statement st        = p_connection.createStatement();
        ResultSet rs        = st.executeQuery("SELECT * FROM " + p_table);
        int       col_count = rs.getMetaData().getColumnCount();
        for (int i = 1; i <= col_count; i++) {
            System.out.print(rs.getMetaData().getColumnLabel(i) + "\t");
        }
        System.out.println("");
        while (rs.next()) {
            for (int i = 1; i <= col_count; i++) {
                System.out.print(rs.getObject(i));
                System.out.print("\t");
            }
            System.out.println("");
        }
        st.close();
        System.out.println("... END GET TABLE " + p_table);
    }
    private static void printMeta(Connection p_connection)
    throws SQLException {
        System.out.println("GET METADATA START ...");
        ResultSet rs = p_connection.getMetaData().getTables(null, null, null,
            null);
        System.out.println(rs.toString());
        int col_count = rs.getMetaData().getColumnCount();
        for (int i = 1; i <= col_count; i++) {
            System.out.print(rs.getMetaData().getColumnLabel(i) + "\t");
        }
        System.out.println("");
        while (rs.next()) {
            for (int i = 1; i <= col_count; i++) {
                System.out.print(rs.getObject(i));
                System.out.print("\t");
            }
            System.out.println("");
        }
        System.out.println("END GET METADATA");
    }
    private static void copy(String src, String dest) throws IOException {
        File inputFile  = new File(src);
        File outputFile = new File(dest);
        if (!inputFile.exists()) {
            return;
        }
        FileInputStream  in  = new FileInputStream(inputFile);
        FileOutputStream out = new FileOutputStream(outputFile);
        int              c;
        while ((c = in.read()) != -1) {
            out.write(c);
        }
        in.close();
        out.close();
    }
}