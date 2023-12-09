


package org.hsqldb.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class TestMultipleConnections {

    public TestMultipleConnections() {}

    public static void main(String[] args) throws Exception {

        
        TestMultipleConnections hs   = new TestMultipleConnections();
        Connection              con1 = hs.createObject();
        Connection              con2 = hs.createObject();
        Connection              con3 = hs.createObject();

        con1.setAutoCommit(false);

        
        con2.setAutoCommit(false);

        
        con3.setAutoCommit(false);

        
        Statement st = con3.createStatement();

        st.execute("DROP TABLE T IF EXISTS");
        st.execute("CREATE TABLE T (I INT)");
        st.execute("INSERT INTO T VALUES (2)");

        ResultSet rs = st.executeQuery("SELECT * FROM T");

        rs.next();

        int value = rs.getInt(1);

        con2.commit();
        con3.commit();
        con1.commit();

        rs = st.executeQuery("SELECT * FROM T");

        rs.next();

        if (value != rs.getInt(1)) {
            throw new Exception("value doesn't exist");
        }
    }

    
    protected Connection createObject() {

        try {
            Class.forName("org.hsqldb.jdbc.JDBCDriver");

            return DriverManager.getConnection("jdbc:hsqldb:/hsql/test/test",
                                               "sa", "");
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return null;
    }
}
