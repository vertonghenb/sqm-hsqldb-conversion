package org.hsqldb.test;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.hsqldb.lib.StopWatch;
public class TestBatchExecution extends TestBase {
    static final String drop_table_sql = "drop table test if exists";
    static final String create_cached  = "create cached ";
    static final String create_memory  = "create memory ";
    static final String create_temp    = "create temp ";
    static final String table_sql = "table test(id int identity primary key,"
                                    + "fname varchar(20), lname "
                                    + "varchar(20), zip int)";
    static final String insert_sql = "insert into test values(?,?,?,?)";
    static final String update_sql =
        "update test set fname = 'Hans' where id = ?";
    static final String select_sql   = "select * from test where id = ?";
    static final String delete_sql   = "delete from test where id = ?";
    static final String call_sql     = "call identity()";
    static final String shutdown_sql = "shutdown compact";
    static final String def_db_path  = "batchtest";
    static final int    def_runs     = 5;
    static final int    rows         = 10000;
    static Connection   conn;
    static Statement    stmnt;
    static String       url;
    public TestBatchExecution(String name) {
        super(name);
    }
    public void test() throws Exception {
        conn  = newConnection();
        stmnt = conn.createStatement();
        url   = super.url;
        nonPreparedTest();
        preparedTestOne(5);
    }
    static void print(String s) {
        System.out.print(s);
    }
    static void println(String s) {
        System.out.println(s);
    }
    static void printCommandStats(StopWatch sw, String cmd) {
        long et = sw.elapsedTime();
        print(sw.elapsedTimeToMessage(rows + " " + cmd));
        println(" " + ((1000 * rows) / et) + " ops/s.");
    }
    public static void main(String[] args) throws Exception {
        int    runs;
        String db_path;
        Driver driver;
        runs    = def_runs;
        db_path = def_db_path;
        try {
            runs = Integer.parseInt(args[0]);
        } catch (Exception e) {}
        db_path = "batchtest";
        try {
            db_path = args[1];
        } catch (Exception e) {}
        driver =
            (Driver) Class.forName("org.hsqldb.jdbc.JDBCDriver").newInstance();
        DriverManager.registerDriver(driver);
        url = "jdbc:hsqldb:file:" + db_path
              + ";crypt_key=604a6105889da65326bf35790a923932;crypt_type=blowfish;hsqldb.default_table_type=cached;hsqldb.cache_rows=100"
        ;
        conn  = DriverManager.getConnection(url, "SA", "");
        stmnt = conn.createStatement();
        runTests(runs);
    }
    static void runTests(int runs) throws Exception {
        println("");
        println("***************************************");
        println("featuring cached (persistent) table");
        println("***************************************");
        println(drop_table_sql);
        stmnt.execute(drop_table_sql);
        println(create_cached + table_sql);
        stmnt.execute(create_cached + table_sql);
        preparedTestOne(runs);
        println(drop_table_sql);
        stmnt.execute(drop_table_sql);
        println("---------------------------------------");
        println("shutting down database");
        stmnt.execute(shutdown_sql);
        println("---------------------------------------");
        conn  = DriverManager.getConnection(url, "SA", "");
        stmnt = conn.createStatement();
        println("");
        println("***************************************");
        println("featuring memory (persistent) table");
        println("***************************************");
        println(drop_table_sql);
        stmnt.execute(drop_table_sql);
        println(create_memory + table_sql);
        stmnt.execute(create_memory + table_sql);
        preparedTestOne(runs);
        println(drop_table_sql);
        stmnt.execute(drop_table_sql);
        println("---------------------------------------");
        println("shutting down database");
        stmnt.execute(shutdown_sql);
        println("---------------------------------------");
        conn  = DriverManager.getConnection(url, "SA", "");
        stmnt = conn.createStatement();
        println("");
        println("***************************************");
        println("featuring temp (transient) table");
        println("***************************************");
        println(drop_table_sql);
        stmnt.execute(drop_table_sql);
        println(create_temp + table_sql);
        stmnt.execute(create_temp + table_sql);
        preparedTestOne(runs);
        println(drop_table_sql);
        stmnt.execute(drop_table_sql);
        println("---------------------------------------");
        println("shutting down database");
        stmnt.execute(shutdown_sql);
        println("---------------------------------------");
        preparedTestTwo();
    }
    public static void nonPreparedTest() throws Exception {
        stmnt.addBatch(drop_table_sql);
        stmnt.addBatch(create_memory + table_sql);
        stmnt.executeBatch();
    }
    public static void preparedTestOne(int runs) throws Exception {
        PreparedStatement insertStmnt;
        PreparedStatement updateStmnt;
        PreparedStatement selectStmnt;
        PreparedStatement deleteStmnt;
        PreparedStatement callStmnt;
        StopWatch         sw;
        println("---------------------------------------");
        println("Preparing Statements:");
        println("---------------------------------------");
        println(insert_sql);
        println(update_sql);
        println(select_sql);
        println(delete_sql);
        println(call_sql);
        sw = new StopWatch();
        insertStmnt = conn.prepareStatement(insert_sql,
                                            Statement.RETURN_GENERATED_KEYS);
        updateStmnt = conn.prepareStatement(update_sql);
        selectStmnt = conn.prepareStatement(select_sql);
        deleteStmnt = conn.prepareStatement(delete_sql);
        callStmnt   = conn.prepareCall(call_sql);
        println("---------------------------------------");
        println(sw.elapsedTimeToMessage("statements prepared"));
        println("---------------------------------------");
        sw.zero();
        for (int i = 0; i < rows; i++) {
            insertStmnt.setInt(1, i);
            insertStmnt.setString(2, "Julia");
            insertStmnt.setString(3, "Peterson-Clancy");
            insertStmnt.setInt(4, i);
            updateStmnt.setInt(1, i);
            selectStmnt.setInt(1, i);
            deleteStmnt.setInt(1, i);
            insertStmnt.addBatch();
            updateStmnt.addBatch();
            selectStmnt.addBatch();
            deleteStmnt.addBatch();
            callStmnt.addBatch();
        }
        println("---------------------------------------");
        println(sw.elapsedTimeToMessage("" + 5 * rows
                                        + " batch entries created"));
        sw.zero();
        for (int i = 0; i < 1; i++) {
            println("---------------------------------------");
            sw.zero();
            insertStmnt.executeBatch();
            printCommandStats(sw, "inserts");
            ResultSet    generated = insertStmnt.getGeneratedKeys();
            StringBuffer sb        = new StringBuffer();
            while (generated.next()) {
                int gen = generated.getInt(1);
                if (gen % 1000 == 0) {
                    sb.append(gen).append(" - ");
                }
            }
            System.out.println(sb.toString());
            printCommandStats(sw, "generated reads");
            sw.zero();
            updateStmnt.executeBatch();
            printCommandStats(sw, "updates");
            sw.zero();
            sw.zero();
            deleteStmnt.executeBatch();
            printCommandStats(sw, "deletes");
            sw.zero();
        }
    }
    public static void preparedTestTwo() {
        try {
            Class.forName("org.hsqldb.jdbc.JDBCDriver");
            Connection con = DriverManager.getConnection("jdbc:hsqldb:mem:.",
                "sa", "");
            System.out.println("con=" + con);
            Statement stmt = con.createStatement();
            try {
                stmt.executeUpdate("drop table ttt");
            } catch (Exception e) {}
            stmt.executeUpdate("create table ttt (id integer)");
            PreparedStatement prep =
                con.prepareStatement("INSERT INTO ttt (id) VALUES (?)");
            con.setAutoCommit(false);
            for (int i = 1; i <= 4; i++) {    
                prep.setInt(1, i);
                prep.addBatch();
                System.out.println("executeBatch() for " + i);
                prep.executeBatch();
                con.commit();
            }
            prep.close();
            ResultSet rs = stmt.executeQuery("select * from ttt");
            while (rs.next()) {
                System.out.println("id = " + rs.getInt(1));
            }
            System.out.println("bye.");
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}