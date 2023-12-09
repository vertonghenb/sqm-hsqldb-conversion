


package org.hsqldb.test;


import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.TestCase;


public class TestCascade extends TestCase {

    Connection con;

    public TestCascade(String name) {
        super(name);
    }

    protected void setUp() {

        try {
            Class.forName("org.hsqldb.jdbc.JDBCDriver");
            createDatabase();

            con = DriverManager.getConnection("jdbc:hsqldb:testdb", "sa", "");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(this + ".setUp() error: " + e.getMessage());
        }
    }

    protected void tearDown() {

        try {
            con.createStatement().execute("SHUTDOWN");
            con.close();
        } catch (SQLException e) {}
    }

    public void testDelete() {

        try {
            insertData(con);

            Statement stmt = con.createStatement();
            ResultSet rs =
                stmt.executeQuery("SELECT COUNT(EIACODXA) FROM CA");

            rs.next();

            int origCount = rs.getInt(1);

            rs.close();
            deleteXBRecord(con);

            rs = stmt.executeQuery("SELECT COUNT(EIACODXA) FROM CA");

            rs.next();

            int newCount = rs.getInt(1);

            rs.close();
            stmt.close();
            assertEquals(9, newCount);
        } catch (SQLException e) {
            this.assertTrue("SQLException thrown", false);
        }
    }

    private static void createDatabase() throws SQLException {

        new File("testdb.backup").delete();
        new File("testdb.data").delete();
        new File("testdb.properties").delete();
        new File("testdb.script").delete();

        Connection con = DriverManager.getConnection("jdbc:hsqldb:testdb",
            "sa", "");
        String[] saDDL = {
            "CREATE CACHED TABLE XB (EIACODXA VARCHAR(10) NOT NULL, LSACONXB VARCHAR(18) NOT NULL, ALTLCNXB VARCHAR(2) NOT NULL, LCNTYPXB VARCHAR(1) NOT NULL, LCNINDXB VARCHAR(1), LCNAMEXB VARCHAR(19), UPDT_BY VARCHAR(32), LST_UPDT TIMESTAMP, CONSTRAINT XPKXB PRIMARY KEY (EIACODXA, LSACONXB, ALTLCNXB, LCNTYPXB));",


            "CREATE CACHED TABLE CA ( EIACODXA VARCHAR(10) NOT NULL, LSACONXB VARCHAR(18) NOT NULL, ALTLCNXB VARCHAR(2) NOT NULL, LCNTYPXB VARCHAR(1) NOT NULL, TASKCDCA VARCHAR(7) NOT NULL, TSKFRQCA NUMERIC(7,4), UPDT_BY VARCHAR(32), LST_UPDT TIMESTAMP, CONSTRAINT XPKCA PRIMARY KEY (EIACODXA, LSACONXB, ALTLCNXB, LCNTYPXB, TASKCDCA),        CONSTRAINT R_XB_CA FOREIGN KEY (EIACODXA, LSACONXB, ALTLCNXB, LCNTYPXB) REFERENCES XB ON DELETE CASCADE);",


        };
        Statement stmt = con.createStatement();

        for (int index = 0; index < saDDL.length; index++) {
            stmt.executeUpdate(saDDL[index]);
        }

        stmt.execute("SHUTDOWN");
        con.close();
    }    

    
    private static void deleteXBRecord(Connection con) throws SQLException {

        Statement stmt = con.createStatement();

        stmt.executeUpdate(
            "DELETE FROM XB WHERE LSACONXB = 'LEAA' AND EIACODXA = 'T850' AND LCNTYPXB = 'P' AND ALTLCNXB = '00'");
        stmt.close();
    }    

    private static void insertData(Connection con) throws SQLException {

        String[] saData = {
            "INSERT INTO XB VALUES('T850','LEAA','00','P',NULL,'LCN NAME','sa',NOW)",
            "INSERT INTO XB VALUES('T850','LEAA01','00','P',NULL,'LCN NAME','sa',NOW)",
            "INSERT INTO XB VALUES('T850','LEAA02','00','P',NULL,'LCN NAME','sa',NOW)",
            "INSERT INTO XB VALUES('T850','LEAA03','00','P',NULL,'LCN NAME','sa',NOW)",
            "INSERT INTO CA VALUES('T850','LEAA','00','P','ABCDEFG',3.14,'sa',NOW)",
            "INSERT INTO CA VALUES('T850','LEAA','00','P','QRSTUJV',3.14,'sa',NOW)",
            "INSERT INTO CA VALUES('T850','LEAA','00','P','ZZZZZZZ',3.14,'sa',NOW)",
            "INSERT INTO CA VALUES('T850','LEAA01','00','P','ABCDEFG',3.14,'sa',NOW)",
            "INSERT INTO CA VALUES('T850','LEAA01','00','P','QRSTUJV',3.14,'sa',NOW)",
            "INSERT INTO CA VALUES('T850','LEAA01','00','P','ZZZZZZZ',3.14,'sa',NOW)",
            "INSERT INTO CA VALUES('T850','LEAA02','00','P','ABCDEFG',3.14,'sa',NOW)",
            "INSERT INTO CA VALUES('T850','LEAA02','00','P','QRSTUJV',3.14,'sa',NOW)",
            "INSERT INTO CA VALUES('T850','LEAA02','00','P','ZZZZZZZ',3.14,'sa',NOW)",
            "INSERT INTO CA VALUES('T850','LEAA03','00','P','ABCDEFG',3.14,'sa',NOW)",
            "INSERT INTO CA VALUES('T850','LEAA03','00','P','QRSTUJV',3.14,'sa',NOW)",
            "INSERT INTO CA VALUES('T850','LEAA03','00','P','ZZZZZZZ',3.14,'sa',NOW)"
        };
        Statement stmt = con.createStatement();

        for (int index = 0; index < saData.length; index++) {
            stmt.executeUpdate(saData[index]);
        }
    }    
}
