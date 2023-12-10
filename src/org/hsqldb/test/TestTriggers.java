package org.hsqldb.test;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import org.hsqldb.Trigger;
import org.hsqldb.lib.ArrayUtil;
public class TestTriggers extends TestBase {
    Connection conn;
    public TestTriggers(String testName) {
        super(testName, "jdbc:hsqldb:file:trigs", false, false);
    }
    public void setUp() {
        super.setUp();
        try {
            openConnection();
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }
    public void tearDown() {
        try {
            conn.close();
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
        super.tearDown();
    }
    public void testTriggerAction() {
        runScript();
        try {
            runStatements();
        } catch (SQLException e) {
            e.printStackTrace();
            assertTrue(false);
        }
        try {
            shutdownDatabase();
        } catch (SQLException e) {
            e.printStackTrace();
            assertTrue(false);
        }
        try {
            openConnection();
        } catch (SQLException e) {
            e.printStackTrace();
            assertTrue(false);
        }
        try {
            runStatements();
        } catch (SQLException e) {
            e.printStackTrace();
            assertTrue(false);
        }
    }
    private void openConnection() throws SQLException {
        conn = newConnection();
    }
    private void runScript() {
        TestUtil.testScript(conn, "TestTriggers.txt");
    }
    private void shutdownDatabase() throws SQLException {
        Statement st = conn.createStatement();
        st.execute("shutdown");
        st.close();
    }
    private void runStatements() throws SQLException {
        Statement st = conn.createStatement();
        st.execute("delete from testtrig");
        st.execute("alter table testtrig alter column c1 restart with 0");
        clearCalls();
        st.execute(
            "insert into testtrig values (default, 'inserted val 1', 100)");
        checkCallCount(3);
        checkCalls(Trigger.INSERT_AFTER, 1);
        checkCalls(Trigger.INSERT_BEFORE_ROW, 1);
        checkCalls(Trigger.INSERT_AFTER_ROW, 1);
        clearCalls();
        st.execute(
            "insert into testtrig (c2, c3) select c2, c3 from testtrig where c1 < 0");
        checkCallCount(1);
        checkCalls(Trigger.INSERT_AFTER, 1);
        checkCalls(Trigger.INSERT_BEFORE_ROW, 0);
        checkCalls(Trigger.INSERT_AFTER_ROW, 0);
        clearCalls();
        st.execute("update testtrig set c2 = c2 || ' updated' where c1 = 0");
        checkCallCount(3);
        checkCalls(Trigger.UPDATE_AFTER, 1);
        checkCalls(Trigger.UPDATE_BEFORE_ROW, 1);
        checkCalls(Trigger.UPDATE_AFTER_ROW, 1);
        clearCalls();
        st.execute("update testtrig set c2 = c2 || ' updated' where c1 < 0");
        checkCallCount(1);
        checkCalls(Trigger.UPDATE_AFTER, 1);
        checkCalls(Trigger.UPDATE_BEFORE_ROW, 0);
        checkCalls(Trigger.UPDATE_AFTER_ROW, 0);
        st.close();
    }
    void checkCalls(int trigType, int callCount) {
        assertEquals("call count mismatch", TriggerClass.callCounts[trigType],
                     callCount);
    }
    void clearCalls() {
        TriggerClass.callCount = 0;
        ArrayUtil.fillArray(TriggerClass.callCounts, 0);
    }
    void checkCallCount(int count) {
        assertEquals("trigger call mismatch", count, TriggerClass.callCount);
    }
}