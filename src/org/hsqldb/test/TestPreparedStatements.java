


package org.hsqldb.test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;

import junit.framework.TestCase;

import java.sql.ResultSet;


public class TestPreparedStatements extends TestCase {

    private Connection con = null;

    private class sqlStmt {

        boolean  prepare;
        boolean  update;
        String   command;
        Object[] args;
        Object   ret;

        sqlStmt(String c) {

            command = c;
            prepare = false;
            update  = false;
        }

        sqlStmt(String c, boolean p, boolean u, Object[] a) {

            command = c;
            prepare = p;
            update  = u;
            args    = a;
        }

        sqlStmt(String c, boolean p, boolean u, Object[] a, Object r) {

            command = c;
            prepare = p;
            update  = u;
            args    = a;
            ret     = r;
        }
    }

    private sqlStmt[] stmtArray = {
        new sqlStmt("drop table public.ivtest if exists cascade"),
        new sqlStmt(
            "create cached table ivtest(interval1 INTERVAL YEAR TO MONTH,"
            + " interval2 INTERVAL DAY TO SECOND(3))"),
        new sqlStmt("drop table public.dttest if exists cascade"),
        new sqlStmt("create cached table dttest(adate date not null, "
                    + "atime time not null,bg int, primary key(adate,atime))"),
        new sqlStmt(
            "insert into dttest values(current_date - 10 day, current_time + 1 hour, 1)",
            false, true, null),
        new sqlStmt(
            "insert into dttest values(current_date - 8 day, current_time - 5 hour, 2)",
            false, true, null),
        new sqlStmt(
            "insert into dttest values(current_date - 7 day, current_time - 4 hour, 3)",
            false, true, null),
        new sqlStmt("insert into dttest values(current_date, '12:44:31', 4)",
                    false, true, null),
        new sqlStmt(
            "insert into dttest values(current_date + 3 day, current_time - 12 hour, 5)",
            false, true, null),
        new sqlStmt("insert into dttest values(current_date + 1 day, current_time - 1 hour, 6)", false, true, null),
        new sqlStmt("select atime adate from dttest where atime =  ? and adate = ?",
                    true, false, new Object[] {
            "12:44:31", new java.sql.Date(System.currentTimeMillis())
        }), new sqlStmt("insert into ivtest values ?, ?", true, true,
                        new Object[] {
            "1-10", "10 02:15:30.333"
        }), new sqlStmt(
            "insert into ivtest values CAST (? AS INTERVAL YEAR TO MONTH), CAST (? AS INTERVAL DAY TO SECOND)",
            true, true, new Object[] {
            "1-10", "10 02:15:30.333"
        }), new sqlStmt("script", true, false, null),
        new sqlStmt("drop table public.bintest if exists cascade"),
        new sqlStmt("create cached table bintest(val BIGINT, id BINARY(100))"),
        new sqlStmt("insert into bintest values ?, ?", true, true,
                    new Object[] {
            10L, new byte[] {
                1, 2, 3, 4, 5
            }
        }), new sqlStmt("select val from bintest where id = ?", true, false,
                        new Object[]{ new byte[] {
            1, 2, 3, 4, 5
        } }, 10L),
    };

    public TestPreparedStatements(String name) {
        super(name);
    }

    protected void setUp() {

        String url = "jdbc:hsqldb:test";

        try {
            Class.forName("org.hsqldb.jdbc.JDBCDriver");

            con = java.sql.DriverManager.getConnection(url, "sa", "");
        } catch (Exception e) {}
    }

    public void testA() {

        int i = 0;

        try {
            for (i = 0; i < stmtArray.length; i++) {
                int j;

                System.out.println(" -- #" + i + " ----------------------- ");

                if (stmtArray[i].prepare) {
                    Object[]          stmtArgs = stmtArray[i].args;
                    PreparedStatement ps       = null;

                    System.out.println(" -- preparing\n<<<\n"
                                       + stmtArray[i].command + "\n>>>\n");

                    ps = con.prepareStatement(stmtArray[i].command);

                    if (stmtArgs != null) {
                        System.out.print(" -- setting " + stmtArgs.length
                                         + " Args [");

                        for (j = 0; j < stmtArgs.length; j++) {
                            System.out.print((j > 0 ? "; "
                                                    : "") + stmtArgs[j]);
                            ps.setObject(j + 1, stmtArgs[j]);
                        }

                        System.out.println("]");
                    }

                    System.out.println(" -- executing ");

                    if (stmtArray[i].update) {
                        int r = ps.executeUpdate();

                        System.out.println(" ***** ps.executeUpdate gave me "
                                           + r);
                    } else {
                        boolean b     = ps.execute();
                        int     count = 0;

                        if (b) {
                            ResultSet rs = ps.getResultSet();

                            while (rs.next()) {
                                if (count == 0 && stmtArray[i].ret != null) {
                                    super.assertEquals(stmtArray[i].ret,
                                                       rs.getObject(1));
                                }

                                count++;
                            }

                            System.out.print(
                                " ***** ps.execute returned result row count "
                                + count);
                        } else {
                            System.out.print(" ***** ps.execute gave me " + b);
                        }
                    }
                } else {
                    System.out.println(" -- executing directly\n<<<\n"
                                       + stmtArray[i].command + "\n>>>\n");

                    Statement s = con.createStatement();
                    boolean   b = s.execute(stmtArray[i].command);

                    System.out.println(" ***** st.execute gave me " + b);
                }
            }
        } catch (Exception e) {
            System.out.println(i + " ?? Caught Exception " + e);
            super.fail();
        }

        assertTrue(true);
    }
}
