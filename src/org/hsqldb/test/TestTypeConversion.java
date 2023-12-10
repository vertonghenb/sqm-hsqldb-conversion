package org.hsqldb.test;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
public class TestTypeConversion extends TestBase {
    Connection connection;
    Statement  statement;
    public TestTypeConversion(String name) {
        super(name);
    }
    protected void setUp() {
        super.setUp();
        try {
            connection = super.newConnection();
            statement  = connection.createStatement();
        } catch (Exception e) {
            System.out.println(e);
        }
    }
    public void testStreams() {
        try {
            String ddl0 = "DROP TABLE BSTREAM IF EXISTS";
            String ddl1 =
                "CREATE TABLE BSTREAM(A INT IDENTITY PRIMARY KEY, B VARBINARY(20))";
            statement.execute(ddl0);
            statement.execute(ddl1);
        } catch (SQLException e) {
            e.printStackTrace();
            fail("ddl failure");
        }
        try {
            String            dml0 = "insert into bstream values(default, ?)";
            String            dql0 = "select * from bstream where a = ?";
            PreparedStatement ps1   = connection.prepareStatement(dml0);
            PreparedStatement ps2   = connection.prepareStatement(dql0);
            byte[]            data = new byte[] {
                0x10, 0x11, 0x12, 0x13, 0x14, 0x15
            };
            InputStream is = new ByteArrayInputStream(data);
            ps1.setBinaryStream(1,  is);
            ps1.execute();
            ps1.setObject(1, data);
            ps1.execute();
            ps2.setInt(1, 1);
            ResultSet rs = ps2.executeQuery();
            rs.next();
            InputStream isr = rs.getBinaryStream(2);
            for(int i =0; i < data.length ; i++) {
                int val = isr.read();
                assertTrue(val == data[i]);
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail("dml failure");
        }
    }
    public void testBitA() {
        try {
            String ddl0 = "DROP TABLE BITTEST IF EXISTS";
            String ddl1 =
                "CREATE TABLE BITTEST(BITA BIT(1), BITB BIT(2), "
                + "BITVA BIT VARYING(1), BITVB BIT VARYING(2), ID IDENTITY)";
            statement.execute(ddl0);
            statement.execute(ddl1);
        } catch (SQLException e) {
            e.printStackTrace();
            fail("ddl failure");
        }
        try {
            String dml0 = "insert into bittest values(?, ?, ?, ?, default)";
            String            dql0 = "select * from bittest;";
            PreparedStatement ps   = connection.prepareStatement(dml0);
            byte[]            data = new byte[]{ -0x80 };
            ps.setBoolean(1, true);
            ps.setBytes(2, data);
            ps.setBytes(3, data);
            ps.setBytes(4, data);
            ps.executeUpdate();
            data = new byte[]{ 0 };
            ps.setBoolean(1, false);
            ps.setBytes(2, data);
            ps.setBytes(3, data);
            ps.setBytes(4, data);
            ps.executeUpdate();
            ps.close();
            ps = connection.prepareStatement(dql0);
            ResultSet rs = ps.executeQuery();
            rs.next();
            boolean boole = rs.getBoolean(1);
            assertTrue(boole);
            boole = rs.getBoolean(1);
            assertTrue(boole);
            rs.next();
            boole = rs.getBoolean(1);
            assertFalse(boole);
            boole = rs.getBoolean(1);
            assertFalse(boole);
        } catch (SQLException e) {
            e.printStackTrace();
            fail("dml failure");
        }
    }
    public void testArrayA() {
        try {
            String ddl0 = "DROP TABLE ARRAYTEST IF EXISTS";
            String ddl1 = "CREATE TABLE ARRAYTEST(A INTEGER ARRAY)";
            String dml1 = "INSERT INTO ARRAYTEST VALUES(ARRAY[0,0])";
            String dml2 = "INSERT INTO ARRAYTEST VALUES ?";
            statement.execute(ddl0);
            statement.execute(ddl1);
            statement.execute(dml1);
            PreparedStatement ps = connection.prepareStatement(dml2);
            Object[] objects = new Object [] { "1", 3, 9};
            Array array = connection.createArrayOf("INTEGER", objects);
            ps.setArray(1, array);
            ps.execute();
        } catch (SQLException e) {
            e.printStackTrace();
            fail("array failure");
        }
    }
}