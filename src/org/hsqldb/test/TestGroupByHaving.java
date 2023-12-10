package org.hsqldb.test;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.TestCase;
public class TestGroupByHaving extends TestCase {
    private static final String databaseDriver   = "org.hsqldb.jdbc.JDBCDriver";
    private static final String databaseURL      = "jdbc:hsqldb:mem:.";
    private static final String databaseUser     = "sa";
    private static final String databasePassword = "";
    private Connection conn;
    private Statement  stmt;
    public TestGroupByHaving(String s) {
        super(s);
    }
    protected static Connection getJDBCConnection() throws SQLException {
        return DriverManager.getConnection(databaseURL, databaseUser,
                                           databasePassword);
    }
    protected void setUp() throws Exception {
        super.setUp();
        if (conn != null) {
            return;
        }
        Class.forName(databaseDriver);
        conn = getJDBCConnection();
        stmt = conn.createStatement();
        try {
            stmt.execute("drop table employee if exists");
        } catch (Exception x) {}
        stmt.execute("create table employee(id int, "
                     + "firstname VARCHAR(50), " + "lastname VARCHAR(50), "
                     + "salary decimal(10, 2), " + "superior_id int, "
                     + "CONSTRAINT PK_employee PRIMARY KEY (id), "
                     + "CONSTRAINT FK_superior FOREIGN KEY (superior_id) "
                     + "REFERENCES employee(ID))");
        addEmployee(1, "Mike", "Smith", 160000, -1);
        addEmployee(2, "Mary", "Smith", 140000, -1);
        addEmployee(10, "Joe", "Divis", 50000, 1);
        addEmployee(11, "Peter", "Mason", 45000, 1);
        addEmployee(12, "Steve", "Johnson", 40000, 1);
        addEmployee(13, "Jim", "Hood", 35000, 1);
        addEmployee(20, "Jennifer", "Divis", 60000, 2);
        addEmployee(21, "Helen", "Mason", 50000, 2);
        addEmployee(22, "Daisy", "Johnson", 40000, 2);
        addEmployee(23, "Barbara", "Hood", 30000, 2);
    }
    protected void tearDown() throws Exception {
        try {
            stmt.execute("drop table employee if exists");
        } catch (Exception x) {}
        if (stmt != null) {
            stmt.close();
            stmt = null;
        }
        if (conn != null) {
            conn.close();
            conn = null;
        }
        super.tearDown();
    }
    private void addEmployee(int id, String firstName, String lastName,
                             double salary, int superiorId) throws Exception {
        stmt.execute("insert into employee values(" + id + ", '" + firstName
                     + "', '" + lastName + "', " + salary + ", "
                     + (superiorId <= 0 ? "null"
                                        : ("" + superiorId)) + ")");
    }
    public void testAggregatedGroupBy() throws SQLException {
        String sql = "select avg(salary), max(id) from employee "
                     + "group by superior_id " + "order by superior_id " + "";
        Object[][] expected = new Object[][] {
            {
                new Double(150000), new Integer(2)
            }, {
                new Double(42500), new Integer(13)
            }, {
                new Double(45000), new Integer(23)
            },
        };
        compareResults(sql, expected, "00000");
    }
    public void testAggregatedGroupByHaving1() throws SQLException {
        String sql = "select avg(salary), max(id) from employee "
                     + "group by superior_id " + "having max(id) > 5 "
                     + "order by superior_id " + "";
        Object[][] expected = new Object[][] {
            {
                new Double(42500), new Integer(13)
            }, {
                new Double(45000), new Integer(23)
            },
        };
        compareResults(sql, expected, "00000");
    }
    public void testAggregatedGroupByHaving2() throws SQLException {
        String sql = "select avg(salary), max(id) from employee "
                     + "group by superior_id "
                     + "having superior_id is not null "
                     + "order by superior_id " + "";
        Object[][] expected = new Object[][] {
            {
                new Double(42500), new Integer(13)
            }, {
                new Double(45000), new Integer(23)
            },
        };
        compareResults(sql, expected, "00000");
    }
    public void testHavingWithoutGroupBy1() throws SQLException {
        String sql = "select avg(salary), max(id) from employee "
                     + "having avg(salary) > 1000 " + "";
        Object[][] expected = new Object[][] {
            {
                new Double(65000), new Integer(23)
            },
        };
        compareResults(sql, expected, "00000");
    }
    public void testHavingWithoutGroupBy2() throws SQLException {
        String sql = "select avg(salary), max(id) from employee "
                     + "having avg(salary) > 1000000 " + "";
        Object[][] expected = new Object[][]{};
        compareResults(sql, expected, "00000");
    }
    public void testInvalidHaving() throws SQLException {
        String sql = "select avg(salary), max(id) from employee "
                     + "group by lastname "
                     + "having (max(id) > 1) and (superior_id > 1) " + "";
        Object[][] expected = new Object[][]{};
        compareResults(sql, expected, "42573");
    }
    private void compareResults(String sql, Object[][] rows,
                                String sqlState) throws SQLException {
        ResultSet rs = null;
        try {
            rs = stmt.executeQuery(sql);
            assertTrue("Statement <" + sql + "> \nexpecting error code: "
                       + sqlState, ("00000".equals(sqlState)));
        } catch (SQLException sqlx) {
            if (!sqlx.getSQLState().equals(sqlState)) {
                sqlx.printStackTrace();
            }
            assertTrue("Statement <" + sql + "> \nthrows wrong error code: "
                       + sqlx.getErrorCode() + " expecting error code: "
                       + sqlState, (sqlx.getSQLState().equals(sqlState)));
            return;
        }
        int rowCount = 0;
        int colCount = rows.length > 0 ? rows[0].length
                                       : 0;
        while (rs.next()) {
            assertTrue("Statement <" + sql + "> \nreturned too many rows.",
                       (rowCount < rows.length));
            Object[] columns = rows[rowCount];
            for (int col = 1, i = 0; i < colCount; i++, col++) {
                Object result   = null;
                Object expected = columns[i];
                if (expected == null) {
                    result = rs.getString(col);
                    result = rs.wasNull() ? null
                                          : result;
                } else if (expected instanceof String) {
                    result = rs.getString(col);
                } else if (expected instanceof Double) {
                    result = new Double(rs.getString(col));
                } else if (expected instanceof Integer) {
                    result = new Integer(rs.getInt(col));
                }
                assertEquals("Statement <" + sql
                             + "> \nreturned wrong value.", columns[i],
                                 result);
            }
            rowCount++;
        }
        assertEquals("Statement <" + sql
                     + "> \nreturned wrong number of rows.", rows.length,
                         rowCount);
    }
}