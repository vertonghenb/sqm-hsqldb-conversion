


package org.hsqldb.test;

public class BlaineTrig implements org.hsqldb.Trigger {

    public void fire(int i, String name, String table, Object[] row1,
                     Object[] row2) {
        System.err.println("Hello World.  There is a fire");
    }

    public static String capitalize(String s) {
        return s.toUpperCase();
    }
}
