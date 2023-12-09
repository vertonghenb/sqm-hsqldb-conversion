


package org.hsqldb.test;

import org.hsqldb.types.Collation;


public class TestCollation extends TestBase {

    java.sql.Statement      statement;
    java.sql.Connection     connection;
    org.hsqldb.lib.Iterator collIterator;
    org.hsqldb.lib.Iterator localeIterator;

    
    public TestCollation(String name) {

        super(name);

        super.isNetwork = false;
    }

    protected void setUp() {

        super.setUp();

        try {
            connection = super.newConnection();
            statement  = connection.createStatement();
        } catch (Exception e) {}

        collIterator   = Collation.getCollationsIterator();
        localeIterator = Collation.getLocalesIterator();
    }

    protected void tearDown() {

        try {
            statement = connection.createStatement();

            statement.execute("SHUTDOWN");
        } catch (Exception e) {}

        super.tearDown();
    }

    
    public void testVerifyAvailability() {

        
        try {
            statement.execute(
                getSetCollationStmt(
                    "ThisIsDefinitlyNoValidCollationIdentifier"));
            fail("database did not reject invalid collation name");
        } catch (java.sql.SQLException e) {}

        
        int count = 0;

        while (collIterator.hasNext()) {
            String collationName = (String) collIterator.next();

            try {
                statement.execute(getSetCollationStmt(collationName));
            } catch (java.sql.SQLException e) {
                fail("could not set collation '" + collationName
                     + "'\n  exception message: " + e.getMessage());
            }

            ++count;
        }

        System.out.println("checked " + count
                           + " collations for availability.");

        
        
        
        
        
        
        
        
        
        java.util.Locale[] availableLocales =
            java.util.Locale.getAvailableLocales();
        org.hsqldb.lib.Set existenceCheck = new org.hsqldb.lib.HashSet();

        for (int i = 0; i < availableLocales.length; ++i) {
            String availaleName = availableLocales[i].getLanguage();

            if (availableLocales[i].getCountry().length() > 0) {
                availaleName += "-" + availableLocales[i].getCountry();
            }

            existenceCheck.add(availaleName);
        }

        String notInstalled = "";
        int    expected     = 0,
               failed       = 0;

        while (localeIterator.hasNext()) {
            String localeName = (String) localeIterator.next();

            ++expected;

            if (!existenceCheck.contains(localeName)) {
                if (notInstalled.length() > 0) {
                    notInstalled += "; ";
                }

                notInstalled += localeName;

                ++failed;
            }
        }

        if (notInstalled.length() > 0) {
            fail("the following locales are not installed:\n  " + notInstalled
                 + "\n  (" + failed + " out of " + expected + ")");
        }
    }

    
    public void testVerifyCollation() {

        String failedCollations = "";
        String failMessage      = "";

        while (collIterator.hasNext()) {
            String collationName = (String) collIterator.next();
            String message       = checkSorting(collationName);

            if (message.length() > 0) {
                if (failedCollations.length() > 0) {
                    failedCollations += ", ";
                }

                failedCollations += collationName;
                failMessage      += message;
            }
        }

        if (failedCollations.length() > 0) {
            fail("test failed for following collations:\n" + failedCollations
                 + "\n" + failMessage);
        }
    }

    
    protected final String getSetCollationStmt(String collationName) {

        final String setCollationStmtPre  = "SET DATABASE COLLATION \"";
        final String setCollationStmtPost = "\"";

        return setCollationStmtPre + collationName + setCollationStmtPost;
    }

    
    protected String checkSorting(String collationName) {

        String stmt1 =
            "DROP TABLE WORDLIST IF EXISTS;";
        String stmt2 =
            "CREATE TEXT TABLE WORDLIST ( ID INTEGER, WORD VARCHAR(50) );";
        String stmt3 =
            "SET TABLE WORDLIST SOURCE \"" + collationName
            + ".csv;encoding=UTF-8\"";
        String selectStmt    = "SELECT ID, WORD FROM WORDLIST ORDER BY WORD";
        String returnMessage = "";

        try {

            
            statement.execute(getSetCollationStmt(collationName));
            statement.execute(stmt1);
            statement.execute(stmt2);
            statement.execute(stmt3);

            java.sql.ResultSet results = statement.executeQuery(selectStmt);

            while (results.next()) {
                int expectedPosition = results.getInt(1);
                int foundPosition    = results.getRow();

                if (expectedPosition != foundPosition) {
                    String word = results.getString(2);

                    return "testing collation '" + collationName
                           + "' failed\n" + "  word              : " + word
                           + "\n" + "  expected position : "
                           + expectedPosition + "\n"
                           + "  found position    : " + foundPosition + "\n";
                }
            }
        } catch (java.sql.SQLException e) {
            return "testing collation '" + collationName
                   + "' failed\n  exception message: " + e.getMessage() + "\n";
        }

        return "";
    }

    public static void main(String[] argv) {
        runWithResult(TestCollation.class, "testVerifyAvailability");
        runWithResult(TestCollation.class, "testVerifyCollation");
    }
}
