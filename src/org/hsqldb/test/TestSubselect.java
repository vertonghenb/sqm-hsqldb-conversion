package org.hsqldb.test;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.TestCase;
public class TestSubselect extends TestCase {
    private static final String databaseDriver = "org.hsqldb.jdbc.JDBCDriver";
    private static final String databaseURL =
        "jdbc:hsqldb:/hsql/test/subselect";
    private static final String databaseUser     = "sa";
    private static final String databasePassword = "";
    private Connection jdbcConnection;
    public TestSubselect(String s) {
        super(s);
    }
    protected static Connection getJDBCConnection() throws SQLException {
        return DriverManager.getConnection(databaseURL, databaseUser,
                                           databasePassword);
    }
    protected void setUp() throws Exception {
        TestUtil.deleteDatabase("/hsql/test/subselect");
        Class.forName(databaseDriver);
        jdbcConnection = getJDBCConnection();
        createDataset();
    }
    protected void tearDown() throws Exception {
        jdbcConnection.close();
        jdbcConnection = null;
        super.tearDown();
    }
    void createDataset() throws SQLException {
        Statement statement = jdbcConnection.createStatement();
        statement.execute("drop table colors if exists; "
                          + "drop table sizes if exists; "
                          + "drop table fruits if exists; "
                          + "drop table trees if exists; ");
        statement.execute("create table colors(id int, val varchar(10)); ");
        statement.execute("insert into colors values(1,'red'); "
                          + "insert into colors values(2,'green'); "
                          + "insert into colors values(3,'orange'); "
                          + "insert into colors values(4,'indigo'); ");
        statement.execute("create table sizes(id int, val varchar(10)); ");
        statement.execute("insert into sizes values(1,'small'); "
                          + "insert into sizes values(2,'medium'); "
                          + "insert into sizes values(3,'large'); "
                          + "insert into sizes values(4,'odd'); ");
        statement.execute(
            "create table fruits(id int, name varchar(20), color_id int); ");
        statement.execute(
            "insert into fruits values(1, 'golden delicious',2); "
            + "insert into fruits values(2, 'macintosh',1); "
            + "insert into fruits values(3, 'red delicious',1); "
            + "insert into fruits values(4, 'granny smith',2); "
            + "insert into fruits values(5, 'tangerine',4);");
        statement.execute(
            "create table trees(id int, name varchar(30), fruit_id int, size_id int); ");
        statement.execute(
            "insert into trees values(1, 'small golden delicious tree',1,1); "
            + "insert into trees values(2, 'large macintosh tree',2,3); "
            + "insert into trees values(3, 'large red delicious tree',3,3); "
            + "insert into trees values(4, 'small red delicious tree',3,1); "
            + "insert into trees values(5, 'medium granny smith tree',4,2); ");
        statement.close();
    }
    private static void compareResults(String sql, String[] expected,
                                       Connection jdbcConnection)
                                       throws SQLException {
        Statement statement = jdbcConnection.createStatement();
        ResultSet results   = statement.executeQuery(sql);
        int       rowCount  = 0;
        while (results.next()) {
            assertTrue("Statement <" + sql + "> returned too many rows.",
                       (rowCount < expected.length));
            assertEquals("Statement <" + sql + "> returned wrong value.",
                         expected[rowCount], results.getString(1));
            rowCount++;
        }
        assertEquals("Statement <" + sql + "> returned wrong number of rows.",
                     expected.length, rowCount);
    }
    public void testSimpleJoin() throws SQLException {
        String sql =
            "select trees.id, trees.name, sizes.val, fruits.name, colors.val"
            + " from trees, sizes, fruits, colors"
            + " where trees.size_id = sizes.id"
            + " and trees.fruit_id = fruits.id"
            + " and fruits.color_id = colors.id" + " order by 1";
        int      expectedRows  = 5;
        String[] expectedTrees = new String[] {
            "small golden delicious tree", "large macintosh tree",
            "large red delicious tree", "small red delicious tree",
            "medium granny smith tree"
        };
        String[] expectedSizes  = new String[] {
            "small", "large", "large", "small", "medium"
        };
        String[] expectedFruits = new String[] {
            "golden delicious", "macintosh", "red delicious", "red delicious",
            "granny smith"
        };
        String[]  expectedColors = new String[] {
            "green", "red", "red", "red", "green"
        };
        Statement statement      = jdbcConnection.createStatement();
        ResultSet results        = statement.executeQuery(sql);
        String[]  trees          = new String[expectedRows];
        String[]  fruits         = new String[expectedRows];
        String[]  sizes          = new String[expectedRows];
        String[]  colors         = new String[expectedRows];
        int       rowCount       = 0;
        while (results.next()) {
            assertTrue("Statement <" + sql + "> returned too many rows.",
                       (rowCount <= expectedRows));
            assertEquals("Statement <" + sql
                         + "> returned rows in wrong order.", (1 + rowCount),
                             results.getInt(1));
            assertEquals("Statement <" + sql + "> returned wrong value.",
                         expectedTrees[rowCount], results.getString(2));
            assertEquals("Statement <" + sql + "> returned wrong value.",
                         expectedSizes[rowCount], results.getString(3));
            assertEquals("Statement <" + sql + "> returned wrong value.",
                         expectedFruits[rowCount], results.getString(4));
            assertEquals("Statement <" + sql + "> returned wrong value.",
                         expectedColors[rowCount], results.getString(5));
            rowCount++;
        }
        assertEquals("Statement <" + sql + "> returned wrong number of rows.",
                     expectedRows, rowCount);
    }
    public void testWhereClausesColliding() throws SQLException {
        String sql =
            "select name from fruits where id in (select fruit_id from trees where id < 3) order by name";
        String[] expected = new String[] {
            "golden delicious", "macintosh"
        };
        compareResults(sql, expected, jdbcConnection);
    }
    public void testWhereClausesCollidingWithAliases() throws SQLException {
        String sql =
            "select a.name from fruits a where a.id in (select b.fruit_id from trees b where b.id < 3) order by name";
        String[] expected = new String[] {
            "golden delicious", "macintosh"
        };
        compareResults(sql, expected, jdbcConnection);
    }
    public void testHiddenCollision() throws SQLException {
        String sql =
            "select name from fruits where id in (select fruit_id from trees) order by name";
        String[] expected = new String[] {
            "golden delicious", "granny smith", "macintosh", "red delicious"
        };
        compareResults(sql, expected, jdbcConnection);
    }
    public void testHiddenCollisionWithAliases() throws SQLException {
        String sql =
            "select a.name from fruits a where a.id in (select b.fruit_id from trees b) order by a.name";
        String[] expected = new String[] {
            "golden delicious", "granny smith", "macintosh", "red delicious"
        };
        compareResults(sql, expected, jdbcConnection);
    }
    public void testWhereSelectColliding() throws SQLException {
        String sql =
            "select val from colors where id in (select id from trees where fruit_id = 3) order by val";
        String[] expected = new String[] {
            "indigo", "orange"
        };
        compareResults(sql, expected, jdbcConnection);
    }
    public void testWhereSelectCollidingWithAliases() throws SQLException {
        String sql =
            "select a.val from colors a where a.id in (select b.id from trees b where b.fruit_id = 3) order by a.val";
        String[] expected = new String[] {
            "indigo", "orange"
        };
        compareResults(sql, expected, jdbcConnection);
    }
    public void testSameTable() throws SQLException {
        String sql =
            "select name from trees where id in (select id from trees where fruit_id = 3) order by name";
        String[] expected = new String[] {
            "large red delicious tree", "small red delicious tree"
        };
        compareResults(sql, expected, jdbcConnection);
    }
    public void testSameTableWithAliases() throws SQLException {
        String sql =
            "select a.name from trees a where a.id in (select b.id from trees b where b.fruit_id = 3) order by a.name";
        String[] expected = new String[] {
            "large red delicious tree", "small red delicious tree"
        };
        compareResults(sql, expected, jdbcConnection);
    }
    public void testSameTableWithJoin() throws SQLException {
        String sql =
            "select sizes.val from trees, sizes where sizes.id = trees.size_id and trees.id in (select id from trees where fruit_id = 3) order by sizes.val";
        String[] expected = new String[] {
            "large", "small"
        };
        compareResults(sql, expected, jdbcConnection);
    }
    public void testAndedSubselects() throws SQLException {
        String sql =
            "select name from trees where size_id in (select id from sizes where val = 'large') and fruit_id in (select id from fruits where color_id = 1) order by name";
        String[] expected = new String[] {
            "large macintosh tree", "large red delicious tree"
        };
        compareResults(sql, expected, jdbcConnection);
    }
    public void testNestedSubselects() throws SQLException {
        String sql =
            "select name from trees where fruit_id in (select id from fruits where color_id in (select id from colors where val = 'red')) order by name";
        String[] expected = new String[] {
            "large macintosh tree", "large red delicious tree",
            "small red delicious tree"
        };
        compareResults(sql, expected, jdbcConnection);
    }
    public void testNotIn() throws SQLException {
        String sql =
            "select name from fruits where id not in (select fruit_id from trees) order by name";
        String[] expected = new String[]{ "tangerine" };
        compareResults(sql, expected, jdbcConnection);
    }
    public void testNotInSameTableAndColumn() throws SQLException {
        String sql =
            "select name from fruits where id not in (select id from fruits where color_id > 1 ) order by name";
        String[] expected = new String[] {
            "macintosh", "red delicious"
        };
        compareResults(sql, expected, jdbcConnection);
    }
    public void testAliasScope() throws SQLException {
        String sql =
            "select a.val, b.name from sizes a, trees b where a.id = b.size_id and b.id in (select a.id from trees a, fruits b where a.fruit_id = b.id and b.name='red delicious') order by a.val";
        String[] expectedSizes = new String[] {
            "large", "small"
        };
        String[] expectedTrees = new String[] {
            "large red delicious tree", "small red delicious tree"
        };
        assertEquals(
            "Programmer error: expected arrays should be of equal length.",
            expectedSizes.length, expectedTrees.length);
        Statement statement = jdbcConnection.createStatement();
        ResultSet results   = statement.executeQuery(sql);
        int       rowCount  = 0;
        while (results.next()) {
            assertTrue("Statement <" + sql + "> returned too many rows.",
                       (rowCount < expectedSizes.length));
            assertEquals("Statement <" + sql + "> returned wrong value.",
                         expectedSizes[rowCount], results.getString(1));
            assertEquals("Statement <" + sql + "> returned wrong value.",
                         expectedTrees[rowCount], results.getString(2));
            rowCount++;
        }
        assertEquals("Statement <" + sql + "> returned wrong number of rows.",
                     expectedSizes.length, rowCount);
    }
}