


package org.hsqldb.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;




public class TestHSQLDB {

    
    public static void main(String[] args) {

        java.sql.DatabaseMetaData metaData = null;
        String databaseURL                 = "jdbc:hsqldb:mem:test";
        String                    driver   = "org.hsqldb.jdbc.JDBCDriver";
        String                    user     = "sa";
        String                    password = "";

        
        String ddlStr =
            "CREATE TABLE USER_(ID VARCHAR(64) NOT NULL PRIMARY KEY,HOME VARCHAR(128),OBJECTTYPE VARCHAR(64),STATUS VARCHAR(64) NOT NULL,PERSONNAME_FIRSTNAME VARCHAR(64),PERSONNAME_MIDDLENAME VARCHAR(64),PERSONNAME_LASTNAME VARCHAR(64),URL VARCHAR(256))";
        String sqlStr =
            "UPDATE User_ SET  id=\'urn:uuid:921284f0-bbed-4a4c-9342-ecaf0625f9d7\',  home=null, objectType=\'urn:uuid:6d07b299-10e7-408f-843d-bb2bc913bfbb\', status=\'urn:uuid:37d17f1b-3245-425b-988d-e0d98200a146\' , personName_firstName=\'Registry\', personName_middleName=null, personName_lastName=\'Operator\', url=\'http://sourceforge.net/projects/ebxmlrr\' WHERE id = \'urn:uuid:921284f0-bbed-4a4c-9342-ecaf0625f9d7\' ";
        Statement stmt = null;

        try {
            Class.forName(driver);

            Connection connection = DriverManager.getConnection(databaseURL,
                user, password);

            stmt = connection.createStatement();

            stmt.addBatch(ddlStr);
            stmt.addBatch(sqlStr);

            int[] updateCounts = stmt.executeBatch();
        } catch (ClassNotFoundException e) {
            System.err.println(e.getClass().getName() + ": "
                               + e.getMessage());
            e.printStackTrace(System.err);
        } catch (SQLException e) {
            System.err.println(e.getClass().getName() + ": "
                               + e.getMessage());
            e.printStackTrace(System.err);
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
            } catch (SQLException e) {}
        }
    }
}
