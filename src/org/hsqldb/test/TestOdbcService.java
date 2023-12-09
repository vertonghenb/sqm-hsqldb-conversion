


package org.hsqldb.test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.Statement;


public class TestOdbcService extends AbstractTestOdbc {

    public TestOdbcService() {}

    
    public TestOdbcService(String s) {
        super(s);
    }

    public void testSanity() {
        try {
            ResultSet rs = netConn.createStatement().executeQuery(
                "SELECT count(*) FROM nullmix");
            if (!rs.next()) {
                throw new RuntimeException("The most basic query failed.  "
                    + "No row count from 'nullmix'.");
            }
            assertEquals("Sanity check failed.  Rowcount of 'nullmix'", 6,
                rs.getInt(1));
            rs.close();
        } catch (SQLException se) {
            junit.framework.AssertionFailedError ase
                = new junit.framework.AssertionFailedError(
                "The most basic query failed");
            ase.initCause(se);
            throw ase;
        }
    }

    
    public void testFullyPreparedQuery() {
        try {
            ResultSet rs;
            PreparedStatement ps = netConn.prepareStatement(
                "SELECT i, 3, vc, 'str' FROM nullmix WHERE i < ? OR i > ? "
                + "ORDER BY i");
            ps.setInt(1, 10);
            ps.setInt(2, 30);
            rs = ps.executeQuery();

            assertTrue("No rows fetched", rs.next());
            assertEquals("str", rs.getString(4));
            assertEquals(5, rs.getInt(1));
            assertEquals(3, rs.getInt(2));
            assertEquals("five", rs.getString(3));

            assertTrue("Not enough rows fetched", rs.next());
            assertEquals(3, rs.getInt(2));
            assertEquals(40, rs.getInt(1));
            assertEquals("forty", rs.getString(3));
            assertEquals("str", rs.getString(4));

            assertFalse("Too many rows fetched", rs.next());
            rs.close();

            ps.setInt(1, 16);
            ps.setInt(2, 100);
            rs = ps.executeQuery();

            assertTrue("No rows fetched", rs.next());
            assertEquals("str", rs.getString(4));
            assertEquals(5, rs.getInt(1));
            assertEquals(3, rs.getInt(2));
            assertEquals("five", rs.getString(3));

            assertTrue("No rows fetched", rs.next());
            assertEquals("str", rs.getString(4));
            assertEquals(10, rs.getInt(1));
            assertEquals(3, rs.getInt(2));
            assertEquals("ten", rs.getString(3));

            assertTrue("No rows fetched", rs.next());
            assertEquals("str", rs.getString(4));
            assertEquals(15, rs.getInt(1));
            assertEquals(3, rs.getInt(2));
            assertEquals("fifteen", rs.getString(3));

            assertFalse("Too many rows fetched", rs.next());
            rs.close();

            verifySimpleQueryOutput(); 
        } catch (SQLException se) {
            junit.framework.AssertionFailedError ase
                = new junit.framework.AssertionFailedError(se.getMessage());
            ase.initCause(se);
            throw ase;
        }
    }

    public void testDetailedSimpleQueryOutput() {
        try {
            verifySimpleQueryOutput();
        } catch (SQLException se) {
            junit.framework.AssertionFailedError ase
                = new junit.framework.AssertionFailedError(se.getMessage());
            ase.initCause(se);
            throw ase;
        }
    }

    
    public void verifySimpleQueryOutput() throws SQLException {
        ResultSet rs = netConn.createStatement().executeQuery(
            "SELECT i, 3, vc, 'str' FROM nullmix WHERE i > 20 ORDER BY i");
        assertTrue("No rows fetched", rs.next());
        assertEquals("str", rs.getString(4));
        assertEquals(21, rs.getInt(1));
        assertEquals(3, rs.getInt(2));
        assertEquals("twenty one", rs.getString(3));

        assertTrue("Not enough rows fetched", rs.next());
        assertEquals(3, rs.getInt(2));
        assertEquals(25, rs.getInt(1));
        assertNull(rs.getString(3));
        assertEquals("str", rs.getString(4));

        assertTrue("Not enough rows fetched", rs.next());
        assertEquals("str", rs.getString(4));
        assertEquals(3, rs.getInt(2));
        assertEquals(40, rs.getInt(1));
        assertEquals("forty", rs.getString(3));

        assertFalse("Too many rows fetched", rs.next());
        rs.close();
    }

    public void testPreparedNonRowStatement() {
        try {
            PreparedStatement ps = netConn.prepareStatement(
                    "UPDATE nullmix set xtra = ? WHERE i < ?");
            ps.setString(1, "first");
            ps.setInt(2, 25);
            assertEquals("First update failed", 4, ps.executeUpdate());

            ps.setString(1, "second");
            ps.setInt(2, 15);
            assertEquals("Second update failed", 2, ps.executeUpdate());
            ps.close();


            ResultSet rs = netConn.createStatement().executeQuery(
                "SELECT i, 3, vc, xtra FROM nullmix ORDER BY i");

            assertTrue("No rows fetched", rs.next());
            assertEquals("second", rs.getString(4));
            assertEquals(5, rs.getInt(1));
            assertEquals(3, rs.getInt(2));
            assertEquals("five", rs.getString(3));

            assertTrue("No rows fetched", rs.next());
            assertEquals("second", rs.getString(4));
            assertEquals(10, rs.getInt(1));
            assertEquals(3, rs.getInt(2));
            assertEquals("ten", rs.getString(3));

            assertTrue("No rows fetched", rs.next());
            assertEquals("first", rs.getString(4));
            assertEquals(15, rs.getInt(1));
            assertEquals(3, rs.getInt(2));
            assertEquals("fifteen", rs.getString(3));

            assertTrue("Not enough rows fetched", rs.next());
            assertEquals(3, rs.getInt(2));
            assertEquals(21, rs.getInt(1));
            assertEquals("twenty one", rs.getString(3));
            assertEquals("first", rs.getString(4));

            assertTrue("Not enough rows fetched", rs.next());
            assertEquals(3, rs.getInt(2));
            assertEquals(25, rs.getInt(1));
            assertNull(rs.getString(3));
            assertNull(rs.getString(4));

            assertTrue("Not enough rows fetched", rs.next());
            assertEquals(3, rs.getInt(2));
            assertEquals(40, rs.getInt(1));
            assertEquals("forty", rs.getString(3));
            assertNull(rs.getString(4));

            assertFalse("Too many rows fetched", rs.next());
            rs.close();
        } catch (SQLException se) {
            junit.framework.AssertionFailedError ase
                = new junit.framework.AssertionFailedError(se.getMessage());
            ase.initCause(se);
            throw ase;
        }
    }

    public void testParamlessPreparedQuery() {
        try {
            ResultSet rs;
            PreparedStatement ps = netConn.prepareStatement(
                "SELECT i, 3, vc, 'str' FROM nullmix WHERE i != 21 "
                + "ORDER BY i");
            rs = ps.executeQuery();

            assertTrue("No rows fetched", rs.next());
            assertEquals("str", rs.getString(4));
            assertEquals(5, rs.getInt(1));
            assertEquals(3, rs.getInt(2));
            assertEquals("five", rs.getString(3));

            assertTrue("No rows fetched", rs.next());
            assertEquals("str", rs.getString(4));
            assertEquals(10, rs.getInt(1));
            assertEquals(3, rs.getInt(2));
            assertEquals("ten", rs.getString(3));

            assertTrue("No rows fetched", rs.next());
            assertEquals("str", rs.getString(4));
            assertEquals(15, rs.getInt(1));
            assertEquals(3, rs.getInt(2));
            assertEquals("fifteen", rs.getString(3));

            assertTrue("Not enough rows fetched", rs.next());
            assertEquals(3, rs.getInt(2));
            assertEquals(25, rs.getInt(1));
            assertNull(rs.getString(3));
            assertEquals("str", rs.getString(4));

            assertTrue("Not enough rows fetched", rs.next());
            assertEquals(3, rs.getInt(2));
            assertEquals(40, rs.getInt(1));
            assertEquals("forty", rs.getString(3));
            assertEquals("str", rs.getString(4));

            assertFalse("Too many rows fetched", rs.next());
            rs.close();

            rs = ps.executeQuery();

            assertTrue("No rows fetched", rs.next());
            assertEquals("str", rs.getString(4));
            assertEquals(5, rs.getInt(1));
            assertEquals(3, rs.getInt(2));
            assertEquals("five", rs.getString(3));

            assertTrue("No rows fetched", rs.next());
            assertEquals("str", rs.getString(4));
            assertEquals(10, rs.getInt(1));
            assertEquals(3, rs.getInt(2));
            assertEquals("ten", rs.getString(3));

            assertTrue("No rows fetched", rs.next());
            assertEquals("str", rs.getString(4));
            assertEquals(15, rs.getInt(1));
            assertEquals(3, rs.getInt(2));
            assertEquals("fifteen", rs.getString(3));

            assertTrue("Not enough rows fetched", rs.next());
            assertEquals(3, rs.getInt(2));
            assertEquals(25, rs.getInt(1));
            assertNull(rs.getString(3));
            assertEquals("str", rs.getString(4));

            assertTrue("Not enough rows fetched", rs.next());
            assertEquals(3, rs.getInt(2));
            assertEquals(40, rs.getInt(1));
            assertEquals("forty", rs.getString(3));
            assertEquals("str", rs.getString(4));

            assertFalse("Too many rows fetched", rs.next());
            rs.close();

            verifySimpleQueryOutput(); 
        } catch (SQLException se) {
            junit.framework.AssertionFailedError ase
                = new junit.framework.AssertionFailedError(se.getMessage());
            ase.initCause(se);
            throw ase;
        }
    }

    public void testSimpleUpdate() {
        try {
            Statement st = netConn.createStatement();
            assertEquals(2, st.executeUpdate(
                    "UPDATE nullmix SET xtra = 'updated' WHERE i < 12"));
            ResultSet rs = netConn.createStatement().executeQuery(
                "SELECT * FROM nullmix WHERE xtra = 'updated'");
            assertTrue("No rows updated", rs.next());
            assertTrue("Only one row updated", rs.next());
            assertFalse("Too many rows updated", rs.next());
            rs.close();
        } catch (SQLException se) {
            junit.framework.AssertionFailedError ase
                = new junit.framework.AssertionFailedError(se.getMessage());
            ase.initCause(se);
            throw ase;
        }
    }

    public void testTranSanity() {
        enableAutoCommit();
        testSanity();
    }
    public void testTranFullyPreparedQuery() {
        enableAutoCommit();
        testFullyPreparedQuery();
    }
    public void testTranDetailedSimpleQueryOutput() {
        enableAutoCommit();
        testDetailedSimpleQueryOutput();
    }
    public void testTranPreparedNonRowStatement() {
        enableAutoCommit();
        testPreparedNonRowStatement();
    }
    public void testTranParamlessPreparedQuery() {
        enableAutoCommit();
        testParamlessPreparedQuery();
    }
    public void testTranSimpleUpdate() {
        enableAutoCommit();
        testSimpleUpdate();
    }

    protected void populate(Statement st) throws SQLException {
        st.executeUpdate("DROP TABLE nullmix IF EXISTS");
        st.executeUpdate("CREATE TABLE nullmix "
                + "(i INT NOT NULL, vc VARCHAR(20), xtra VARCHAR(20))");

        
        
        
        st.executeUpdate("INSERT INTO nullmix (i, vc) values(10, 'ten')");
        st.executeUpdate("INSERT INTO nullmix (i, vc) values(5, 'five')");
        st.executeUpdate("INSERT INTO nullmix (i, vc) values(15, 'fifteen')");
        st.executeUpdate(
                "INSERT INTO nullmix (i, vc) values(21, 'twenty one')");
        st.executeUpdate("INSERT INTO nullmix (i, vc) values(40, 'forty')");
        st.executeUpdate("INSERT INTO nullmix (i) values(25)");
    }

    public static void main(String[] sa) {
        staticRunner(TestOdbcService.class, sa);
    }
}
