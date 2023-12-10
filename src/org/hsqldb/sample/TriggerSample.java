package org.hsqldb.sample;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.Trigger;
import org.hsqldb.lib.StringUtil;
/**
 * <P>Sample code for use of triggers in hsqldb.
 *
 * This class org.hsqldb.sample package, but a typical implementation is in
 * users's class hierarchy.
 *
 * SQL to invoke is:<p>
 * CREATE TRIGGER triggerSample BEFORE|AFTER INSERT|UPDATE|DELETE
 * ON myTable [FOR EACH ROW] [QUEUE n] [NOWAIT] CALL "myPackage.trigClass"<br>
 *
 * This will create a thread that will wait for its firing event to occur;
 * when this happens, the trigger's thread runs the 'trigClass.fire'
 * Note that this is still in the same Java Virtual Machine as the
 * database, so make sure the fired method does not hang.<p>
 *
 * There is a queue of events waiting to be run by each trigger thread.
 * This is particularly useful for 'FOR EACH ROW' triggers, when a large
 * number of trigger events occur in rapid succession, without the trigger
 * thread getting a chance to run. If the queue becomes full, subsequent
 * additions to it cause the database engine to suspend awaiting space
 * in the queue. Take great care to avoid this situation if the trigger
 * action involves accessing the database, as deadlock will occur.
 * This can be avoided either by ensuring the QUEUE parameter makes a large
 * enough queue, or by using the NOWAIT parameter, which causes a new
 * trigger event to overwrite the most recent event in the queue.
 * The default queue size is 1024.<p>
 *
 * Ensure that "myPackage.trigClass" is present in the classpath which
 * you use to start hsql.<p>
 *
 * If the method wants to access the database, it must establish
 * a JDBC connection.<p>
 *
 * When the 'fire' method is called, it is passed the following arguments: <p>
 *
 * fire (int type, String trigName, String tabName, Object oldRow[],
 *       Object[] newRow) <p>
 *
 * where 'type' is one of the values enumerated in the Trigger interface and
 * the 'oldRow'/'newRow' pair represents the rows acted on. The first
 * length - 1 array slots contain column values and the final slot contains
 * either null or the value of the internally assigned row identity, if
 * the concerned table has no primary key. The final slot must _never_ be
 * modified. <p>
 *
 * The mapping of row classes to database types is specified in
 * /doc/hsqlSyntax.html#Datatypes. <p>
 *
 * To be done:<p>
 *
 * <ol>
 * <li> Implement the "jdbc:default:connection: URL to provide transparent
 *      and portable access to internal connections for use in triggers and
 *      stored procedures. <p>
 *
 * <li> Implement declaritive column to trigger method argument
 *      mapping, conditional execution (WHEN clause), etc. <p>
 *
 * <li> Investigate and refine synchronous and asynchronous trigger models. <p>
 *
 *      Because certain combinations of trigger create parameters cause the
 *      individual triggered actions of a multirow update to run in different
 *      threads, it is possible for an 'after' trigger to run before its
 *      corresponding 'before' trigger; the acceptability and implications
 *      of this needs to be investigated, documented and the behaviour of
 *      the engine fully specified.
 *
 * <li> Investigate and implement the SQL 200n specified execution stack under
 *      arbitrary triggered action and SQL-invoked routine call graphs.
 * </ol>
 *
 * @author Peter Hudson
 * @author boucherb@users
 * @version 1.7.2
 * @since 1.7.0
 */
public class TriggerSample implements Trigger {
    static final PrintWriter out  = new PrintWriter(System.out);
    static final String      drv  = "org.hsqldb.jdbc.JDBCDriver";
    static final String      url  = "jdbc:hsqldb:mem:trigger-sample";
    static final String      usr  = "SA";
    static final String      pwd  = "";
    static final String      impl = TriggerSample.class.getName();
    static final String      tn   = "trig_test";
    static final String drop_test_table_stmt = "DROP TABLE " + tn
        + " IF EXISTS";
    static final String create_test_table_stmt = "CREATE TABLE " + tn
        + "(id INTEGER PRIMARY KEY, value VARCHAR(20))";
    static final String drop_audit_table_stmt = "DROP TABLE audit IF EXISTS";
    static final String create_audit_table_stmt = "CREATE TABLE audit("
        + "id  INTEGER GENERATED BY DEFAULT AS IDENTITY(START WITH 1), "
        + "op  VARCHAR(6), " + "tn  VARCHAR(20), " + "ors LONGVARCHAR, "
        + "nrs LONGVARCHAR, " + "ts  TIMESTAMP DEFAULT CURRENT_TIMESTAMP)";
    static final String audit_insert_stmt =
        "INSERT INTO audit(op, tn, ors, nrs) VALUES(?, ?, ?, ?)";
    /**
     * A sample HSQLDB Trigger interface implementation. <p>
     *
     * This sample prints information about the firing trigger and records
     * actions in an audit table. <p>
     *
     * The techniques used here are simplified dramatically for demonstration
     * purposes and are in no way recommended as a model upon which to build
     * actual installations involving triggered actions.
     *
     * @param typ trigger type
     * @param trn trigger name
     * @param tn  table name
     * @param or  old row
     * @param nr  new row
     */
    public void fire(int typ, String trn, String tn, Object[] or,
                     Object[] nr) {
        synchronized (TriggerSample.class) {
            String ors = or == null ? "null"
                                    : StringUtil.arrayToString(or);
            String nrs = nr == null ? "null"
                                    : StringUtil.arrayToString(nr);
            out.println("----------------------------------------");
            out.println(getTriggerDescriptor(trn, typ, tn));
            out.println("old row : " + ors);
            out.println("new row : " + nrs);
            out.flush();
            if ("TRIG_TEST".equals(tn)) {
                switch (typ) {
                    case INSERT_BEFORE_ROW : {
                        final int ID = ((Number) nr[0]).intValue();
                        doAssert(ID < 11, "ID < 11");
                        break;
                    }
                    case UPDATE_BEFORE_ROW : {
                        if ("unchangable".equals(or[1])) {
                            nr[1] = or[1];    
                        }
                        break;
                    }
                }
            }
            doAuditStep(typ, tn, ors, nrs);
        }
    }
    private static void doAssert(boolean b, String msg) {
        if (b) {
        } else {
            throw org.hsqldb.error.Error.error(ErrorCode.GENERAL_ERROR,
                                               msg);
        }
    }
    private static void doAuditStep(int typ, String tn, String ors,
                                    String nrs) {
        Connection        conn;
        PreparedStatement stmt;
        switch (typ) {
            case INSERT_AFTER_ROW :
            case UPDATE_AFTER_ROW :
            case DELETE_AFTER_ROW : {
                try {
                    conn = getConnection();
                    stmt = conn.prepareStatement(audit_insert_stmt);
                    stmt.setString(1, getOperationSpec(typ));
                    stmt.setString(2, tn);
                    stmt.setString(3, ors);
                    stmt.setString(4, nrs);
                    stmt.executeUpdate();
                    conn.close();
                } catch (SQLException se) {
                    se.printStackTrace();
                }
            }
        }
    }
    public static String getWhenSpec(int type) {
        switch (type) {
            case INSERT_BEFORE_ROW :
            case UPDATE_BEFORE_ROW :
            case DELETE_BEFORE_ROW : {
                return "BEFORE";
            }
            case INSERT_AFTER :
            case INSERT_AFTER_ROW :
            case UPDATE_AFTER :
            case UPDATE_AFTER_ROW :
            case DELETE_AFTER :
            case DELETE_AFTER_ROW : {
                return "AFTER";
            }
            default : {
                return "";
            }
        }
    }
    public static String getOperationSpec(int type) {
        switch (type) {
            case INSERT_AFTER :
            case INSERT_AFTER_ROW :
            case INSERT_BEFORE_ROW : {
                return "INSERT";
            }
            case UPDATE_AFTER :
            case UPDATE_AFTER_ROW :
            case UPDATE_BEFORE_ROW : {
                return "UPDATE";
            }
            case DELETE_AFTER :
            case DELETE_AFTER_ROW :
            case DELETE_BEFORE_ROW : {
                return "DELETE";
            }
            default : {
                return "";
            }
        }
    }
    public static String getQueueSpec(int qs) {
        return (qs < 0) ? ""
                        : ("QUEUE " + qs);
    }
    public static String getForEachSpec(int type) {
        switch (type) {
            case INSERT_BEFORE_ROW :
            case INSERT_AFTER_ROW :
            case UPDATE_BEFORE_ROW :
            case UPDATE_AFTER_ROW :
            case DELETE_AFTER_ROW :
            case DELETE_BEFORE_ROW : {
                return "FOR EACH ROW";
            }
            default : {
                return "FOR EACH STATEMENT";
            }
        }
    }
    public static String getTriggerDDL(String trn, int typ, String tab,
                                       int qs,
                                       String impl) throws SQLException {
        StringBuffer sb = new StringBuffer();
        sb.append("CREATE TRIGGER ");
        sb.append(trn);
        sb.append(' ');
        sb.append(getWhenSpec(typ));
        sb.append(' ');
        sb.append(getOperationSpec(typ));
        sb.append(" ON ");
        sb.append(tab);
        sb.append(' ');
        sb.append(getForEachSpec(typ));
        sb.append(' ');
        sb.append(getQueueSpec(qs));
        sb.append(" CALL \"");
        sb.append(impl);
        sb.append("\"");
        return sb.toString();
    }
    public static String getTriggerDescriptor(String trn, int typ,
            String tab) {
        StringBuffer sb = new StringBuffer();
        sb.append("TRIGGER : ");
        sb.append(trn);
        sb.append(' ');
        sb.append(getWhenSpec(typ));
        sb.append(' ');
        sb.append(getOperationSpec(typ));
        sb.append(" ON ");
        sb.append(tab);
        sb.append(' ');
        sb.append(getForEachSpec(typ));
        return sb.toString();
    }
    private static Connection getConnection() throws SQLException {
        try {
            Class.forName(drv).newInstance();
            return DriverManager.getConnection(url, usr, pwd);
        } catch (SQLException se) {
            throw se;
        } catch (Exception e) {
            throw new SQLException(e.toString());
        }
    }
    private static void createTrigger(Statement stmt, String trn,
                                      int typ) throws SQLException {
        stmt.execute(getTriggerDDL(trn, typ, tn, 0, impl));
    }
    private static void setup() throws SQLException {
        Connection conn = getConnection();
        Statement  stmt = conn.createStatement();
        stmt.execute(drop_test_table_stmt);
        stmt.execute(create_test_table_stmt);
        stmt.execute(drop_audit_table_stmt);
        stmt.execute(create_audit_table_stmt);
        createTrigger(stmt, "tibr_" + tn, INSERT_BEFORE_ROW);
        createTrigger(stmt, "tia_" + tn, INSERT_AFTER);
        createTrigger(stmt, "tiar_" + tn, INSERT_AFTER_ROW);
        createTrigger(stmt, "tubr_" + tn, UPDATE_BEFORE_ROW);
        createTrigger(stmt, "tua_" + tn, UPDATE_AFTER);
        createTrigger(stmt, "tuar_" + tn, UPDATE_AFTER_ROW);
        createTrigger(stmt, "tdbr_" + tn, DELETE_BEFORE_ROW);
        createTrigger(stmt, "tda_" + tn, DELETE_AFTER);
        createTrigger(stmt, "tdar_" + tn, DELETE_AFTER_ROW);
        stmt.close();
        conn.close();
    }
    private static void doSomeWork() throws SQLException {
        Connection conn = getConnection();
        Statement  stmt = conn.createStatement();
        conn.setAutoCommit(false);
        stmt.execute("INSERT INTO trig_test VALUES (1, 'hello')");
        stmt.execute("INSERT INTO trig_test VALUES (2, 'now what?')");
        stmt.execute("INSERT INTO trig_test VALUES (3, 'unchangable')");
        stmt.execute("INSERT INTO trig_test VALUES (4, 'goodbye')");
        conn.commit();
        dumpTable("trig_test");
        stmt.execute("UPDATE trig_test SET value = 'all done'");
        conn.commit();
        dumpTable("trig_test");
        stmt.execute("DELETE FROM trig_test");
        conn.rollback();
        dumpTable("trig_test");
        try {
            stmt.execute("INSERT INTO trig_test VALUES(11, 'whatever')");
        } catch (SQLException se) {
            se.printStackTrace();
        }
        stmt.execute("INSERT INTO trig_test VALUES(10, 'whatever')");
        conn.commit();
        dumpTable("trig_test");
        stmt.close();
        conn.close();
    }
    private static void dumpTable(String tn) throws SQLException {
        Connection        conn  = getConnection();
        Statement         stmt  = conn.createStatement();
        ResultSet         rs    = stmt.executeQuery("select * from " + tn);
        ResultSetMetaData rsmd  = rs.getMetaData();
        int               count = rsmd.getColumnCount();
        out.println();
        out.println("****************************************");
        out.println("DUMP FOR TABLE: " + tn);
        out.println("****************************************");
        out.flush();
        while (rs.next()) {
            out.print("[");
            for (int i = 1; i <= count; i++) {
                out.print(rs.getString(i));
                if (i < count) {
                    out.print(" : ");
                }
            }
            out.println("]");
        }
        out.println();
        out.flush();
        rs.close();
        stmt.close();
        conn.close();
    }
    private static void runSample() throws SQLException {
        setup();
        doSomeWork();
        dumpTable("audit");
    }
    public static void main(String[] args) throws SQLException {
        runSample();
    }
}