package org.hsqldb.test;
public class AllSimpleTests {
    public AllSimpleTests() {
        try {
            jbInit();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
    String[] args = new String[0];
    void doTests() throws Exception {
        System.out.println("*********** " + HSQLBug.class.getName());
        HSQLBug.main(args);
        System.out.println("*********** "
                           + TestBatchBug.class.getClass().getName());
        TestBatchBug.main(args);
        System.out.println("*********** " + TestDima.class.getName());
        TestDima.main(args);
        System.out.println("*********** " + TestHSQLDB.class.getName());
        TestHSQLDB.main(args);
        System.out.println("*********** " + TestObjectSize.class.getName());
        TestObjectSize.main(args);
        System.out.println(
            "*********** "
            + TestSubQueriesInPreparedStatements.class.getName());
        TestSubQueriesInPreparedStatements.main(args);
    }
    public static void main(String[] Args) throws Exception {
        AllSimpleTests ast = new AllSimpleTests();
        ast.doTests();
    }
    private void jbInit() throws Exception {}
}