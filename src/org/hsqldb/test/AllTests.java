package org.hsqldb.test;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
public class AllTests extends TestCase {
    public AllTests(String s) {
        super(s);
    }
    public static Test suite() {
        TestSuite suite = new TestSuite();
        suite.addTestSuite(org.hsqldb.test.TestBatchExecution.class);
        suite.addTestSuite(org.hsqldb.test.TestBug1191815.class);
        suite.addTestSuite(org.hsqldb.test.TestBug778213.class);
        suite.addTestSuite(org.hsqldb.test.TestBug785429.class);
        suite.addTestSuite(org.hsqldb.test.TestBug808460.class);
        suite.addTestSuite(org.hsqldb.test.TestCollation.class);
        suite.addTestSuite(org.hsqldb.test.TestDatabaseMetaData.class);
        suite.addTestSuite(org.hsqldb.test.TestDateTime.class);
        suite.addTestSuite(org.hsqldb.test.TestINPredicateParameterizationAndCorrelation.class);
        suite.addTestSuite(org.hsqldb.test.TestJDBCGeneratedColumns.class);
        suite.addTestSuite(org.hsqldb.test.TestJDBCSavepoints.class);
        suite.addTestSuite(org.hsqldb.test.TestLikePredicateOptimizations.class);
        suite.addTestSuite(org.hsqldb.test.TestLobs.class);
        suite.addTestSuite(org.hsqldb.test.TestMerge.class);
        suite.addTestSuite(org.hsqldb.test.TestMultiInsert.class);
        suite.addTestSuite(org.hsqldb.test.TestPreparedStatements.class);
        suite.addTestSuite(org.hsqldb.test.TestPreparedSubQueries.class);
        suite.addTestSuite(org.hsqldb.test.TestSql.class);
        suite.addTestSuite(org.hsqldb.test.TestStoredProcedure.class);
        suite.addTestSuite(org.hsqldb.test.TestSubselect.class);
        suite.addTestSuite(org.hsqldb.test.TestTextTable.class);
        suite.addTestSuite(org.hsqldb.test.TestTextTables.class);
        suite.addTestSuite(org.hsqldb.test.TestViewAsterisks.class);
        suite.addTestSuite(org.hsqldb.test.TestCascade.class);
        suite.addTestSuite(org.hsqldb.test.TestGroupByHaving.class);
        suite.addTestSuite(org.hsqldb.test.TestSqlPersistent.class);
        suite.addTestSuite(org.hsqldb.test.TestUpdatableResults.class);
        suite.addTestSuite(org.hsqldb.test.TestUpdatableResultSets.class);
        suite.addTestSuite(org.hsqldb.test.TestTriggers.class);
        return suite;
    }
    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }
}