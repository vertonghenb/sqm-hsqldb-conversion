


package org.hsqldb.test;

import java.lang.reflect.Constructor;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.hsqldb.server.Server;
import org.hsqldb.server.WebServer;
import junit.framework.TestCase;
import junit.framework.TestResult;


public abstract class TestBase extends TestCase {

    
    String  serverProps;
    String  url;
    String  user     = "sa";
    String  password = "";
    Server  server;
    boolean isNetwork = true;
    boolean isHTTP    = false;

    public TestBase(String name) {
        super(name);
    }

    public TestBase(String name, String url, boolean isNetwork,
                    boolean isHTTP) {

        super(name);

        this.isNetwork = isNetwork;
        this.url       = url;
        this.isHTTP    = isHTTP;
    }

    protected void setUp() {

        if (isNetwork) {
            if (url == null) {
                url = isHTTP ? "jdbc:hsqldb:http://localhost/test"
                             : "jdbc:hsqldb:hsql://localhost/test";
            }

            server = isHTTP ? new WebServer()
                            : new Server();

            server.setDatabaseName(0, "test");
            server.setDatabasePath(0, "mem:test;sql.enforce_strict_size=true");
            server.setLogWriter(null);
            server.setErrWriter(null);
            server.start();
        } else {
            if (url == null) {
                url = "jdbc:hsqldb:file:test;sql.enforce_strict_size=true";
            }
        }

        try {
            Class.forName("org.hsqldb.jdbc.JDBCDriver");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(this + ".setUp() error: " + e.getMessage());
        }
    }

    protected void tearDown() {

        if (isNetwork) {
            server.stop();

            server = null;
        }
    }

    Connection newConnection() throws SQLException {
        return DriverManager.getConnection(url, user, password);
    }

    public static void runWithResult(Class testCaseClass, String testName) {

        try {
            Constructor ctor = testCaseClass.getConstructor(new Class[]{
                String.class });
            TestBase theTest = (TestBase) ctor.newInstance(new Object[]{
                testName });

            theTest.runWithResult();
        } catch (Exception ex) {
            System.err.println("couldn't execute test:");
            ex.printStackTrace(System.err);
        }
    }

    public void runWithResult() {

        TestResult result   = run();
        String     testName = this.getClass().getName();

        if (testName.startsWith("org.hsqldb.test.")) {
            testName = testName.substring(16);
        }

        testName += "." + getName();

        int failureCount = result.failureCount();

        System.out.println(testName + " failure count: " + failureCount);

        java.util.Enumeration failures = result.failures();

        while (failures.hasMoreElements()) {
            System.err.println(failures.nextElement());
        }
    }
}
