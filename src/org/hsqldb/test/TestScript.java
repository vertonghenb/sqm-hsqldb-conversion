


package org.hsqldb.test;

import java.sql.Connection;

public class TestScript extends TestBase {

      String path = "TestSelf01TempTables.txt";














































    public TestScript(String name) {
        super(name, null, false, false);
    }

    public void test() throws java.lang.Exception {

        TestUtil.deleteDatabase("test");

        Connection conn = newConnection();
        String fullPath = "testrun/hsqldb/" + path;
        TestUtil.testScript(conn, fullPath);
        conn.createStatement().execute("SHUTDOWN IMMEDIATELY");
    }

    public static void main(String[] Args) throws Exception {

        TestScript ts = new TestScript("test");

        ts.test();
    }
}
