package org.hsqldb.test;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
public class TestTextTables extends TestBase {
    static String url =
        "jdbc:hsqldb:file:/hsql/testtext/test;sql.enforce_strict_size=true";
    public TestTextTables(String name) {
        super(name, url, false, false);
    }
    protected void setUp() {
        super.setUp();
    }
    public void testSectionOne() throws Exception {
        TestUtil.deleteDatabase("/hsql/testtext/test");
        TestUtil.delete("/hsql/testtext/t.txt");
        TestUtil.delete("/hsql/testtext/tt.txt");
        TestUtil.delete("/hsql/testtext/tident.txt");
        TestUtil.delete("/hsql/testtext/tsingle.txt");
        initDatabase();
        partA();
        partD();
    }
    public void testSectionTwo() throws Exception {
        TestUtil.deleteDatabase("/hsql/testtext/test");
        TestUtil.delete("/hsql/testtext/t.txt");
        TestUtil.delete("/hsql/testtext/tt.txt");
        TestUtil.delete("/hsql/testtext/tident.txt");
        TestUtil.delete("/hsql/testtext/tsingle.txt");
        initDatabase();
        partB();
        partD();
    }
    public void testSectionThree() throws Exception {
        Connection conn = newConnection();
        Statement  st   = conn.createStatement();
        st.execute("SHUTDOWN SCRIPT");
    }
    public void testSectionFour() throws Exception {
        partD();
        Connection conn = newConnection();
        Statement  st   = conn.createStatement();
        st.execute("SHUTDOWN");
    }
    public void testSectionFive() throws Exception {
        Connection conn = newConnection();
        PreparedStatement ps =
            conn.prepareStatement("insert into tident (c2) values ?");
        for (int i = 0; i < 20; i++) {
            ps.setString(1, String.valueOf(i));
            ps.executeUpdate();
        }
        ps.close();
        ps = conn.prepareStatement("insert into tsingle (c1) values ?");
        for (int i = 0; i < 20; i++) {
            ps.setInt(1, i + 7);
            ps.executeUpdate();
        }
        ps.close();
        Statement st = conn.createStatement();
        st.execute("SHUTDOWN IMMEDIATELY");
        partD();
        conn = newConnection();
        st   = conn.createStatement();
        st.execute("insert into tident values default, 'dont know'");
        int count = st.executeUpdate("update tident set c2 = c2 || ' '");
        assertEquals("identity table count mismatch", 21, count);
        ResultSet rs = st.executeQuery("select count(*) from tsingle");
        assertTrue(rs.next());
        assertEquals(20, rs.getInt(1));
        st.execute("set table tsingle read only");
        st.execute("SHUTDOWN SCRIPT");
        conn = newConnection();
        st   = conn.createStatement();
        st.execute("SHUTDOWN SCRIPT");
    }
    public void testSectionSix() throws Exception {
        Connection conn = newConnection();
        Statement st = conn.createStatement();
        st.execute("set table tsingle read write");
        st.execute("SHUTDOWN SCRIPT");
        conn = newConnection();
        st = conn.createStatement();
        st.execute("create memory table tmsingle (c1 int primary key)");
        st.execute("truncate table tident restart identity");
        st.execute("truncate table tsingle restart identity");
        ResultSet rs = st.executeQuery("select count(*) from tident");
        assertTrue(rs.next());
        assertEquals(0, rs.getInt(1));
        st.execute("set table tident source off");
        st.execute("set table tsingle source off");
        st.execute("alter table tsingle add unique(c1)");
        st.execute("alter table tident add foreign key (c1) references tmsingle(c1)");
        st.execute("set table tident source on");
        st.execute("set table tsingle source on");
        rs = st.executeQuery("select count(*) from tmsingle");
        assertTrue(rs.next());
        assertEquals(0, rs.getInt(1));
        rs = st.executeQuery("select count(*) from tident");
        assertTrue(rs.next());
        assertEquals(0, rs.getInt(1));
        PreparedStatement ps =
            conn.prepareStatement("insert into tmsingle(c1) values ?");
        for (int i = 0; i < 20; i++) {
            ps.setInt(1, i);
            ps.executeUpdate();
        }
        ps.close();
        ps = conn.prepareStatement("insert into tident (c1, c2) values ?, ?");
        for (int i = 0; i < 20; i++) {
            ps.setInt(1, i);
            ps.setString(2, String.valueOf(i));
            ps.executeUpdate();
        }
        ps.close();
        st   = conn.createStatement();
        rs = st.executeQuery("select count(*) from tmsingle");
        assertTrue(rs.next());
        assertEquals(20, rs.getInt(1));
        rs = st.executeQuery("select count(*) from tident");
        assertTrue(rs.next());
        assertEquals(20, rs.getInt(1));
        st.execute("SHUTDOWN SCRIPT");
        conn = newConnection();
        st   = conn.createStatement();
        rs = st.executeQuery("select count(*) from tmsingle");
        assertTrue(rs.next());
        assertEquals(20, rs.getInt(1));
        rs = st.executeQuery("select count(*) from tident");
        assertTrue(rs.next());
        assertEquals(20, rs.getInt(1));
        conn = newConnection();
        st   = conn.createStatement();
        st.execute("SHUTDOWN");
    }
    void initDatabase() throws Exception {
        Connection conn = newConnection();
        Statement  st   = conn.createStatement();
        st.execute("set files write delay 0");
        st.execute("set database transaction control locks");
    }
    void partA() throws Exception {
        Connection conn = newConnection();
        TestUtil.testScript(conn, "testrun/hsqldb/TestText01.txt");
        Statement st = conn.createStatement();
        st.execute("SHUTDOWN");
    }
    void partB() throws Exception {
        Connection conn = newConnection();
        TestUtil.testScript(conn, "testrun/hsqldb/TestText01.txt");
        Statement st = conn.createStatement();
        st.execute("SHUTDOWN IMMEDIATELY");
    }
    void partD() throws Exception {
        Connection conn = newConnection();
        TestUtil.testScript(conn, "testrun/hsqldb/TestText02.txt");
        Statement st = conn.createStatement();
        st.execute("SHUTDOWN");
    }
}