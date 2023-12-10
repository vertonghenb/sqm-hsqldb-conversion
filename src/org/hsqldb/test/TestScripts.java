package org.hsqldb.test;
import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.HashMap;
class TestScripts extends TestUtil {
    public static void main(String[] argv) {
        if (argv.length > 0 && argv[0].equals("--help")) {
            System.err.println(SYNTAX_MSG);
            System.exit(2);
        }
        ArrayList scripts   = new ArrayList();
        ArrayList connIds   = new ArrayList();
        ArrayList retains   = new ArrayList();
        int       i         = -1;
        int       curscript = 0;
        connIds.add(null);
        retains.add(null);
        String newName = null;
        while (++i < argv.length) {
            if (argv[i].startsWith("--ephConnId=")) {
                newName = getIdName(argv[i]);
                if (newName == null
                        || connIds.set(connIds.size() - 1, getIdName(argv[i]))
                           != null) {
                    System.err.println(SYNTAX_MSG);
                    System.exit(2);
                }
                if (retains.set(retains.size() - 1, Boolean.FALSE) != null) {
                    System.err.println(SYNTAX_MSG);
                    System.exit(2);
                }
            } else if (argv[i].startsWith("--persistConnId=")) {
                newName = getIdName(argv[i]);
                if (newName == null
                        || connIds.set(connIds.size() - 1, newName) != null) {
                    System.err.println(SYNTAX_MSG);
                    System.exit(2);
                }
                if (retains.set(retains.size() - 1, Boolean.TRUE) != null) {
                    System.err.println(SYNTAX_MSG);
                    System.exit(2);
                }
            } else if (argv[i].startsWith("-")) {
                System.err.println(SYNTAX_MSG);
                System.exit(2);
            } else {
                scripts.add(argv[i]);
                connIds.add(null);
                retains.add(null);
            }
        }
        test(DEF_URL, DEF_USER, DEF_PASSWORD, DEF_DB,
             (String[]) scripts.toArray(new String[0]),
             (String[]) connIds.toArray(new String[0]),
             (Boolean[]) retains.toArray(new Boolean[0]));
    }
    private static String getIdName(String s) {
        int nameStart = s.indexOf('=') + 1;
        if (nameStart < 1) {
            return null;
        }
        if (nameStart == s.length()) {
            throw new RuntimeException(
                "Leave off '=' if you do not want to name a connection");
        }
        return s.substring(nameStart);
    }
    private static final String SYNTAX_MSG = "SYNTAX  java "
        + TestScripts.class.getName()
        + " [--ephConnId=x | --persistConnId=x] file1.txt...";
    static String DEF_DB = "test3";
    static String DEF_URL = "jdbc:hsqldb:" + DEF_DB
                            + ";sql.enforce_strict_size=true";
    static String DEF_USER     = "SA";
    static String DEF_PASSWORD = "";
    static void test(String url, String user, String password, String db,
                     String[] scriptList, String[] idList,
                     Boolean[] persistList) {
        if (scriptList.length < 1) {
            System.err.println("Nothing to do.  No scripts specified.");
            return;
        }
        HashMap connMap = new HashMap();
        if (db != null) {
            deleteDatabase(db);
        }
        try {
            DriverManager.registerDriver(new org.hsqldb.jdbc.JDBCDriver());
            Connection cConnection = null;
            String     id;
            for (int i = 0; i < scriptList.length; i++) {
                id = idList[i];
                System.out.println("ID: " + id);
                cConnection = ((id == null) ? null
                                            : (Connection) connMap.get(id));
                if (cConnection == null) {
                    System.out.println("Getting NEW conn");
                    cConnection = DriverManager.getConnection(url, user,
                            password);
                    if (id != null) {
                        connMap.put(id, cConnection);
                        System.out.println("Storing NEW conn");
                    }
                }
                testScript(cConnection, scriptList[i]);
                if (persistList[i] == null ||!persistList[i].booleanValue()) {
                    if (id != null) {
                        connMap.remove(id);
                        System.out.println("Removed conn");
                    }
                    cConnection.close();
                    System.out.println("Closed conn");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            print("TestSelf init error: " + e.getMessage());
        }
    }
}