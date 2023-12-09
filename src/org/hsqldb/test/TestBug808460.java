


package org.hsqldb.test;

import java.sql.Connection;
import java.sql.Statement;

import junit.framework.TestCase;
import junit.framework.TestResult;


public class TestBug808460 extends TestBase {

    public TestBug808460(String name) {
        super(name);
    }

    
    public void test() throws Exception {

        Connection conn = newConnection();
        Statement  stmt = conn.createStatement();

        stmt.executeQuery("SELECT * FROM INFORMATION_SCHEMA.SYSTEM_SESSIONS");
        conn.close();

        conn = newConnection();
        stmt = conn.createStatement();

        stmt.executeQuery("SELECT * FROM INFORMATION_SCHEMA.SYSTEM_SESSIONS");
        conn.close();
    }

    
    public static void main(String[] args) throws Exception {

        TestResult            result;
        TestCase              test;
        java.util.Enumeration failures;
        int                   count;

        result = new TestResult();
        test   = new TestBug808460("test");

        test.run(result);

        count = result.failureCount();

        System.out.println("TestBug808460 failure count: " + count);

        failures = result.failures();

        while (failures.hasMoreElements()) {
            System.out.println(failures.nextElement());
        }
    }
}
