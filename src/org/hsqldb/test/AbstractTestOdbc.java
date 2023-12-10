package org.hsqldb.test;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import org.hsqldb.persist.HsqlProperties;
import org.hsqldb.server.Server;
import org.hsqldb.server.ServerConstants;
public abstract class AbstractTestOdbc extends junit.framework.TestCase {
    protected Connection netConn = null;
    protected Server server = null;
    static protected String portString = null;
    static protected String dsnName = null;
    public AbstractTestOdbc() {}
    public AbstractTestOdbc(String s) {
        super(s);
    }
    static {
        try {
            Class.forName("org.hsqldb.jdbc.JDBCDriver");
        } catch (ClassNotFoundException cnfe) {
            throw new RuntimeException(
                "<clinit> failed.  JDBC Driver class not in CLASSPATH");
        }
        portString = System.getProperty("test.hsqlodbc.port");
        dsnName = System.getProperty("test.hsqlodbc.dsnname");
        if (portString == null) {
            portString = "9797";
        }
        if (dsnName == null) {
            dsnName = "HSQLDB_UTEST";
        }
    }
    protected void tearDown() throws SQLException {
        if (netConn != null) {
            netConn.rollback();
            netConn.createStatement().executeUpdate("SHUTDOWN");
            netConn.close();
            netConn = null;
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ie) {
            }
        }
        if (server != null
            && server.getState() != ServerConstants.SERVER_STATE_SHUTDOWN) {
            throw new RuntimeException("Server failed to shut down");
        }
    }
    protected void setUp() {
        try {
            Connection setupConn = DriverManager.getConnection(
                "jdbc:hsqldb:mem:test", "SA", "");
            setupConn.setAutoCommit(false);
            Statement st = setupConn.createStatement();
            st.executeUpdate("SET PASSWORD 'sapwd'");
            populate(st);
            st.close();
            setupConn.commit();
            setupConn.close();
        } catch (SQLException se) {
            throw new RuntimeException(
                "Failed to set up in-memory database", se);
        }
        try {
            server = new Server();
            HsqlProperties properties = new HsqlProperties();
            if (System.getProperty("VERBOSE") == null) {
                server.setLogWriter(null);
                server.setErrWriter(null);
            } else {
                properties.setProperty("server.silent", "false");
                properties.setProperty("server.trace", "true");
            }
            properties.setProperty("server.database.0", "mem:test");
            properties.setProperty("server.dbname.0", "");
            properties.setProperty("server.port", AbstractTestOdbc.portString);
            server.setProperties(properties);
            server.start();
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ie) {
            }
        } catch (Exception e) {
            throw new RuntimeException(
                "Failed to set up in-memory database", e);
        }
        if (server.getState() != ServerConstants.SERVER_STATE_ONLINE) {
            throw new RuntimeException("Server failed to start up");
        }
        try {
            netConn = DriverManager.getConnection(
                "jdbc:odbc:" + dsnName, "SA", "sapwd");
        } catch (SQLException se) {
            if (se.getMessage().indexOf("No suitable driver") > -1) {
                throw new RuntimeException(
                    "You must install the native library for Sun's jdbc:odbc "
                    + "JDBC driver");
            }
            if (se.getMessage().indexOf("Data source name not found") > -1) {
                throw new RuntimeException(
                    "You must configure ODBC DSN '" + dsnName
                    + "' (you may change the name and/or port by setting Java "
                    + "system properties 'test.hsqlodbc.port' or "
                    + "'test.hsqlodbc.dsnname'");
            }
            throw new RuntimeException(
                "Failed to set up JDBC/ODBC network connection", se);
        }
    }
    protected void enableAutoCommit() {
        try {
            netConn.setAutoCommit(false);
        } catch (SQLException se) {
            junit.framework.AssertionFailedError ase
                = new junit.framework.AssertionFailedError(se.getMessage());
            ase.initCause(se);
            throw ase;
        }
    }
    public static void staticRunner(Class c, String[] sa) {
            junit.textui.TestRunner runner = new junit.textui.TestRunner();
            junit.framework.TestResult result =
                runner.run(runner.getTest(c.getName()));
            System.exit(result.wasSuccessful() ? 0 : 1);
    }
    abstract protected void populate(Statement st) throws SQLException;
}