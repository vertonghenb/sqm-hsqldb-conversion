


package org.hsqldb.test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

public class TestUpdatableResults extends TestBase {

    public TestUpdatableResults(String name) {
        super(name);
    }

    public void testQuery() {

        try {
            Connection c = newConnection();
            Statement st = c.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                                             ResultSet.CONCUR_UPDATABLE);
            String s = "CREATE TABLE T (I INTEGER, C CHARACTER(10), B BIT(4) DEFAULT B'')";

            st.execute(s);

            s = "INSERT INTO T VALUES(?,?, DEFAULT)";

            PreparedStatement ps = c.prepareStatement(s);

            for (int i = 1; i <= 20; i++) {
                ps.setInt(1, i);
                ps.setString(2, "TEST " + i);
                ps.execute();
            }

            c.setAutoCommit(false);

            s = "SELECT * FROM T";

            ResultSet rs = st.executeQuery(s);


            rs.absolute(10);
            rs.updateString(2, "UPDATE10");
            rs.updateRow();

            rs.absolute(11);
            rs.deleteRow();

            rs.moveToInsertRow();

            rs.updateInt(1, 1011);
            rs.updateString(2, "INSERT1011");
            rs.updateString(3, "0101");

            rs.insertRow();

            rs.close();

            rs = st.executeQuery(s);

            while (rs.next()) {
                System.out.println("" + rs.getInt(1) + "      "
                                   + rs.getString(2) + "      "
                                   + rs.getString(3));
            }

        } catch (Exception e) {
            System.out.print(e);
        }
    }
}
