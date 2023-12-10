package org.hsqldb.test;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import org.hsqldb.error.ErrorCode;
import junit.framework.AssertionFailedError;
public class TestViewAsterisks extends TestBase {
    java.sql.Statement  m_statement;
    java.sql.Connection m_connection;
    public TestViewAsterisks(String testName) {
        super(testName, null, false, false);
    }
    private void setupDatabase() {
        try {
            m_connection = newConnection();
            m_statement  = m_connection.createStatement();
            executeStatement("DROP TABLE ABC IF EXISTS CASCADE");
            executeStatement("DROP TABLE TABLE_A IF EXISTS CASCADE");
            executeStatement("DROP TABLE TABLE_B IF EXISTS CASCADE");
            executeStatement("DROP VIEW V1 IF EXISTS CASCADE"); 
            executeStatement(
                "CREATE TABLE ABC (ID INTEGER NOT NULL PRIMARY KEY, A VARCHAR(50), B VARCHAR(50), C VARCHAR(50))");
            executeStatement("INSERT INTO ABC VALUES (1, 'a', 'b', 'c')");
            executeStatement("INSERT INTO ABC VALUES (2, 'd', 'e', 'f')");
            executeStatement(
                "CREATE TABLE TABLE_A (ID_A INTEGER NOT NULL PRIMARY KEY, NAME_A VARCHAR(50))");
            executeStatement("INSERT INTO TABLE_A VALUES (1, 'first A')");
            executeStatement("INSERT INTO TABLE_A VALUES (2, 'second A')");
            executeStatement(
                "CREATE TABLE TABLE_B (ID_B INTEGER NOT NULL PRIMARY KEY, NAME_B VARCHAR(50))");
            executeStatement("INSERT INTO TABLE_B VALUES (1, 'first B')");
            executeStatement("INSERT INTO TABLE_B VALUES (2, 'second B')");
        } catch (SQLException ex) {
            fail(ex.toString());
        }
    }
    public void setUp() {
        super.setUp();
        setupDatabase();
    }
    protected void tearDown() {
        executeStatement("SHUTDOWN");
        super.tearDown();
    }
    private void executeStatement(String sql) {
        executeStatement(sql, 0);
    }
    private void executeStatement(String sql, int expectedVendorCode) {
        try {
            m_statement.execute(sql);
            assertTrue(
                "executing\n  " + sql
                + "\nwas expected to fail, but it didn't", expectedVendorCode
                    == 0);
        } catch (SQLException ex) {
            if (expectedVendorCode == 0) {
                fail(ex.toString());
            }
            assertEquals(
                "executing\n  " + sql
                + "\ndid not result in the expected error", expectedVendorCode, -ex
                    .getErrorCode());
        }
    }
    private void createView(String viewName, String[] columnList,
                            String viewStatement) throws SQLException {
        StringBuffer colList = new StringBuffer();
        if (columnList != null) {
            colList.append(" (");
            for (int i = 0; i < columnList.length; ++i) {
                colList.append('"').append(columnList[i]).append('"');
                if (i < columnList.length - 1) {
                    colList.append(',');
                }
            }
            colList.append(")");
        }
        executeStatement("CREATE VIEW " + viewName + colList.toString()
                         + " AS " + viewStatement);
        if (columnList != null) {
            ensureTableColumns(viewName, columnList);
        }
    }
    private String getViewStatement(String viewName) throws SQLException {
        ResultSet res = m_statement.executeQuery(
            "SELECT VIEW_DEFINITION FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_NAME = '"
            + viewName + "'");
        res.next();
        String statement = res.getString(1);
        return statement;
    }
    private void ensureEqualContent(String tableNameLHS,
                                    String tableNameRHS) throws SQLException {
        ResultSet lhs = m_statement.executeQuery("SELECT * FROM \""
            + tableNameLHS + "\"");
        ResultSet rhs = m_statement.executeQuery("SELECT * FROM \""
            + tableNameRHS + "\"");
        ResultSetMetaData meta = lhs.getMetaData();
        while (lhs.next() && rhs.next()) {
            for (int col = 1; col <= meta.getColumnCount(); ++col) {
                assertEquals("table content does not match: cp. "
                             + tableNameLHS + "-" + tableNameRHS + ", row "
                             + lhs.getRow() + ", col "
                             + col, lhs.getObject(col), rhs.getObject(col));
            }
        }
        assertTrue("row count does not match: " + tableNameLHS + "-"
                   + tableNameRHS, lhs.isAfterLast() && rhs.isLast());
    }
    private void ensureTableContent(String tableName,
                                    Object[][] tableData) throws SQLException {
        ResultSet lhs = m_statement.executeQuery("SELECT * FROM \""
            + tableName + "\"");
        ResultSetMetaData meta     = lhs.getMetaData();
        int               colCount = meta.getColumnCount();
        while (lhs.next()) {
            int row = lhs.getRow();
            assertEquals(colCount, tableData[row - 1].length);
            for (int col = 1; col <= colCount; ++col) {
                assertEquals(
                    "unexpected table content in " + tableName + " (row "
                    + row + ", col " + col + ")", tableData[row - 1][col - 1],
                                                  lhs.getObject(col));
            }
        }
    }
    private void checkViewTranslationAndContent(String viewName,
            String[] columnList, String viewStatement,
            String expectedTranslatedStatement,
            Object expectedContent) throws SQLException {
        createView(viewName, columnList, viewStatement);
        String actualTranslatedStatement = getViewStatement(viewName);
        if (!actualTranslatedStatement.equals(expectedTranslatedStatement)) {
            StringBuffer message = new StringBuffer();
            message.append(viewName).append(
                "'s statement not translated as expected\n");
            message.append("original statement:\n  ").append(
                viewStatement).append('\n');
            message.append("expected translated statement:\n  ").append(
                expectedTranslatedStatement).append('\n');
            message.append("actual translated statement:\n  ").append(
                actualTranslatedStatement).append('\n');
            throw new AssertionFailedError(message.toString());
        }
        if (expectedContent instanceof Object[][]) {
            ensureTableContent(viewName, (Object[][]) expectedContent);
        }
    }
    private void ensureTableColumns(String tableName,
                                    String[] columnNames) throws SQLException {
        ResultSet res = m_connection.getMetaData().getColumns(null, null,
            tableName, "%");
        while (res.next()) {
            assertEquals(
                "unexpected column name in table \"" + tableName
                + "\" at position "
                + (res.getRow() - 1), res.getString(
                    "COLUMN_NAME"), columnNames[res.getRow() - 1]);
        }
        res.previous();
        assertEquals("not enough columns in table \"" + tableName + "\"",
                     columnNames.length, res.getRow());
    }
    private void checkSimpleViews() throws SQLException {
        checkViewTranslationAndContent(
            "S1", null, "SELECT * FROM ABC",
            "SELECT PUBLIC.ABC.ID,PUBLIC.ABC.A,PUBLIC.ABC.B,PUBLIC.ABC.C FROM PUBLIC.ABC",
            "ABC");
        executeStatement("ALTER TABLE ABC ADD COLUMN D VARCHAR(50)");
        ensureTableColumns("ABC", new String[] {
            "ID", "A", "B", "C", "D"
        });
        ensureTableColumns("S1", new String[] {
            "ID", "A", "B", "C"
        });
        executeStatement("ALTER TABLE ABC DROP COLUMN D");
        executeStatement("ALTER TABLE ABC DROP COLUMN C", ErrorCode.X_42536);
        checkViewTranslationAndContent(
            "S2", null, "SELECT LIMIT 0 2 * FROM ABC ORDER BY ID",
            "SELECT LIMIT 0 2 PUBLIC.ABC.ID,PUBLIC.ABC.A,PUBLIC.ABC.B,PUBLIC.ABC.C FROM PUBLIC.ABC ORDER BY ID",
            "ABC");
        checkViewTranslationAndContent(
            "S3", null, "SELECT TOP 2 * FROM ABC ORDER BY ID",
            "SELECT TOP 2 PUBLIC.ABC.ID,PUBLIC.ABC.A,PUBLIC.ABC.B,PUBLIC.ABC.C FROM PUBLIC.ABC ORDER BY ID",
            "ABC");
        checkViewTranslationAndContent(
            "S4", null, "SELECT DISTINCT * FROM ABC",
            "SELECT DISTINCT PUBLIC.ABC.ID,PUBLIC.ABC.A,PUBLIC.ABC.B,PUBLIC.ABC.C FROM PUBLIC.ABC",
            "ABC");
        checkViewTranslationAndContent(
            "S5", null, "SELECT ABC.* FROM ABC",
            "SELECT  PUBLIC.ABC.ID,PUBLIC.ABC.A,PUBLIC.ABC.B,PUBLIC.ABC.C  FROM PUBLIC.ABC",
            "ABC");
        checkViewTranslationAndContent(
            "S6", null, "SELECT \"A\".* FROM ABC AS A",
            "SELECT A.ID,A.A,A.B,A.C FROM PUBLIC.ABC AS A",
            "ABC");
        checkViewTranslationAndContent(
            "S7", null, "( SELECT * FROM ABC )",
            "(SELECT PUBLIC.ABC.ID,PUBLIC.ABC.A,PUBLIC.ABC.B,PUBLIC.ABC.C FROM PUBLIC.ABC)",
            "ABC");
    }
    private void checkAsterisksCombined() throws SQLException {
        checkViewTranslationAndContent(
            "C1", null, "SELECT * AS \"a2\" FROM ABC",
            "SELECT PUBLIC.ABC.ID,PUBLIC.ABC.A,PUBLIC.ABC.B,PUBLIC.ABC.C AS \"a2\" FROM PUBLIC.ABC",
            new Object[][] {
            new Object[] {
                new Integer(1), "a", "b", "c"
            }, new Object[] {
                new Integer(2), "d", "e", "f"
            }
        });
        checkViewTranslationAndContent(
            "C2", null, "SELECT B AS \"b2\", ABC.* FROM ABC",
            "SELECT B AS \"b2\", PUBLIC.ABC.ID,PUBLIC.ABC.A,PUBLIC.ABC.B,PUBLIC.ABC.C  FROM PUBLIC.ABC",
            new Object[][] {
            new Object[] {
                "b", new Integer(1), "a", "b", "c"
            }, new Object[] {
                "e", new Integer(2), "d", "e", "f"
            }
        });
    }
    private void checkMultipleTables() throws SQLException {
        checkViewTranslationAndContent(
            "M1", null, "SELECT * FROM TABLE_A, TABLE_B",
            "SELECT PUBLIC.TABLE_A.ID_A,PUBLIC.TABLE_A.NAME_A,PUBLIC.TABLE_B.ID_B,PUBLIC.TABLE_B.NAME_B FROM PUBLIC.TABLE_A,PUBLIC.TABLE_B",
            new Object[][] {
            new Object[] {
                new Integer(1), "first A", new Integer(1), "first B"
            }, new Object[] {
                new Integer(1), "first A", new Integer(2), "second B"
            }, new Object[] {
                new Integer(2), "second A", new Integer(1), "first B"
            }, new Object[] {
                new Integer(2), "second A", new Integer(2), "second B"
            }
        });
        checkViewTranslationAndContent(
            "M2", null, "SELECT TABLE_B.*, TABLE_A.* FROM TABLE_A, TABLE_B",
            "SELECT  PUBLIC.TABLE_B.ID_B,PUBLIC.TABLE_B.NAME_B , PUBLIC.TABLE_A.ID_A,PUBLIC.TABLE_A.NAME_A  FROM PUBLIC.TABLE_A,PUBLIC.TABLE_B",
            new Object[][] {
            new Object[] {
                new Integer(1), "first B", new Integer(1), "first A"
            }, new Object[] {
                new Integer(2), "second B", new Integer(1), "first A"
            }, new Object[] {
                new Integer(1), "first B", new Integer(2), "second A"
            }, new Object[] {
                new Integer(2), "second B", new Integer(2), "second A"
            }
        });
        checkViewTranslationAndContent(
            "M3", null, "SELECT \"TABLE_A\".* FROM TABLE_A, TABLE_B",
            "SELECT PUBLIC.TABLE_A.ID_A,PUBLIC.TABLE_A.NAME_A FROM PUBLIC.TABLE_A,PUBLIC.TABLE_B",
            new Object[][] {
            new Object[] {
                new Integer(1), "first A"
            }, new Object[] {
                new Integer(1), "first A"
            }, new Object[] {
                new Integer(2), "second A"
            }, new Object[] {
                new Integer(2), "second A"
            }
        });
    }
    private void checkSubSelects() throws SQLException {
        checkViewTranslationAndContent(
            "Q1", null, "SELECT * FROM ( SELECT * FROM ABC )",
            "SELECT ID,A,B,C FROM(SELECT PUBLIC.ABC.ID,PUBLIC.ABC.A,PUBLIC.ABC.B,PUBLIC.ABC.C FROM PUBLIC.ABC)",
            null);
        checkViewTranslationAndContent(
            "Q2", null,
            "SELECT * FROM ( SELECT * FROM TABLE_A ), ( SELECT * FROM TABLE_B )",
            "SELECT ID_A,NAME_A,ID_B,NAME_B FROM(SELECT PUBLIC.TABLE_A.ID_A,PUBLIC.TABLE_A.NAME_A FROM PUBLIC.TABLE_A),(SELECT PUBLIC.TABLE_B.ID_B,PUBLIC.TABLE_B.NAME_B FROM PUBLIC.TABLE_B)",
            null);
        checkViewTranslationAndContent(
            "Q3", null, "SELECT A.* FROM ( SELECT * FROM TABLE_A ) AS A",
            "SELECT  A.ID_A,A.NAME_A  FROM(SELECT PUBLIC.TABLE_A.ID_A,PUBLIC.TABLE_A.NAME_A FROM PUBLIC.TABLE_A)AS A",
            null);
        checkViewTranslationAndContent(
            "Q4", null,
            "SELECT A.*, B.* FROM ( SELECT * FROM TABLE_A ) AS A, ( SELECT * FROM TABLE_B ) AS B",
            "SELECT  A.ID_A,A.NAME_A , B.ID_B,B.NAME_B  FROM(SELECT PUBLIC.TABLE_A.ID_A,PUBLIC.TABLE_A.NAME_A FROM PUBLIC.TABLE_A)AS A,(SELECT PUBLIC.TABLE_B.ID_B,PUBLIC.TABLE_B.NAME_B FROM PUBLIC.TABLE_B)AS B",
            null);
    }
    private void checkColumnLists() throws SQLException {
        executeStatement("CREATE VIEW IMPOSSIBLE (\"A\") AS SELECT * FROM ABC",
                         ErrorCode.X_42593);
        checkViewTranslationAndContent("L1", new String[] {
            "C1", "C2", "C3", "C4"
        }, "SELECT * FROM ABC", "SELECT PUBLIC.ABC.ID,PUBLIC.ABC.A,PUBLIC.ABC.B,PUBLIC.ABC.C FROM PUBLIC.ABC",
           "ABC");
    }
    private void checkViewsOnViews() throws SQLException {
        checkViewTranslationAndContent(
            "V1", null, "SELECT * FROM S1",
            "SELECT PUBLIC.S1.ID,PUBLIC.S1.A,PUBLIC.S1.B,PUBLIC.S1.C FROM PUBLIC.S1", "L1");
    }
    private void checkUnionViews() throws SQLException {
        checkViewTranslationAndContent(
            "U1", null, "SELECT * FROM TABLE_A UNION SELECT * FROM TABLE_B",
            "SELECT PUBLIC.TABLE_A.ID_A,PUBLIC.TABLE_A.NAME_A FROM PUBLIC.TABLE_A UNION SELECT PUBLIC.TABLE_B.ID_B,PUBLIC.TABLE_B.NAME_B FROM PUBLIC.TABLE_B",
            new Object[][] {
            new Object[] {
                new Integer(1), "first A"
            }, new Object[] {
                new Integer(1), "first B"
            }, new Object[] {
                new Integer(2), "second A"
            }, new Object[] {
                new Integer(2), "second B"
            }
        });
        checkViewTranslationAndContent(
            "U2", null,
            "SELECT * FROM ( SELECT * FROM TABLE_A UNION SELECT * FROM TABLE_B )",
            "SELECT ID_A,NAME_A FROM(SELECT PUBLIC.TABLE_A.ID_A,PUBLIC.TABLE_A.NAME_A FROM PUBLIC.TABLE_A UNION SELECT PUBLIC.TABLE_B.ID_B,PUBLIC.TABLE_B.NAME_B FROM PUBLIC.TABLE_B)",
            new Object[][] {
            new Object[] {
                new Integer(1), "first A"
            }, new Object[] {
                new Integer(1), "first B"
            }, new Object[] {
                new Integer(2), "second A"
            }, new Object[] {
                new Integer(2), "second B"
            }
        });
    }
    public void test() {
        try {
            checkSimpleViews();
            checkAsterisksCombined();
            checkMultipleTables();
            checkSubSelects();
            checkColumnLists();
            checkViewsOnViews();
            checkUnionViews();
        } catch (SQLException ex) {
            fail(ex.toString());
        }
    }
    public static void main(String[] argv) {
        runWithResult(TestViewAsterisks.class, "test");
    }
}