


package org.hsqldb.test;


public class TestNitin {

    public static void main(String[] args) {

        java.sql.Connection    c  = null;
        java.sql.Statement     s  = null;
        java.io.BufferedReader br = null;

        try {
            Class.forName("org.hsqldb.jdbc.JDBCDriver");

            c = java.sql.DriverManager.getConnection(
                "jdbc:hsqldb:c:/ft/hsqldb_w_1_8_0/oom/my.db", "SA", "");
            s = c.createStatement();
            br = new java.io.BufferedReader(
                new java.io.FileReader("c:/ft/hsqldb_w_1_8_0//oom//my.sql"));

            String line;
            int    lineNo = 0;

            while ((line = br.readLine()) != null) {
                if (line.length() > 0 && line.charAt(0) != '#') {
                    s.execute(line);

                    if (lineNo++ % 100 == 0) {
                        System.out.println(lineNo);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (br != null) {
                    br.close();
                }
            } catch (java.io.IOException ioe) {}

            try {
                if (s != null) {
                    s.close();
                }
            } catch (java.sql.SQLException se) {}

            try {
                if (c != null) {
                    c.close();
                }
            } catch (java.sql.SQLException se) {}
        }

        System.exit(0);
    }
}
