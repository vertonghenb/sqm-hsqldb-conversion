package org.hsqldb.test;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.io.File;
import java.io.Reader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.FileReader;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.lang.reflect.Method;
import org.hsqldb.test.TestUtil;
import org.hsqldb.lib.RCData;
class TestScriptRunner {
    protected static final String DEFAULT_RCFILE = "testscriptrunner.rc";
    public static String LS = System.getProperty("line.separator");
    public static String SYNTAX_MSG =
        "java " + TestScriptRunner.class.getName()
        + " [--optionalSwitches...] --urlid=URLID1 [script1.tsql [[--urlid=URLIDX] scriptY.tsql...]...]"
        + LS
        + "    Specify one input file name as '-' to read from stdin." + LS
        + "    No scripts specified will read from only stdin." + LS
        + "    Simple single-threaded example with RC file '" + DEFAULT_RCFILE
        + "':" + LS
        + "java " + TestScriptRunner.class.getName()
        + "--urlid=URLID script1.tsql script2.tsql" + LS + LS
        + "OPTIONAL SWITCHES:" + LS
        + "    --verbose        Obviously..." + LS
        + "    --threads        Each script runs in a parallel thread (dflt. sequential)."
        + LS
        + "    --rcfile=/path/to/file.rc   (Defaults to '" + DEFAULT_RCFILE
        + "')" + LS
        + "    --populate       Use TestCacheSize class to populate one database" + LS
        + "    --sqltool=URLID  Invoke an interactive SqlTool session on given URLID" + LS
        + "(This last is useful for troubleshooting and interactive script dev).";
    public boolean verbose = false;
    public boolean threaded = false;
    public static void main(String[] sa) throws IOException, SQLException {
        int argIndex = 0;
        boolean threaded = false;
        boolean verbose = false;
        boolean populate = false;
        String rcFile = DEFAULT_RCFILE;
        Map scriptFileMap = new HashMap(); 
        String currentUrlid = null;
        String sqlToolUrlid = null;
        Method sqlToolMainMethod = null;
        try {
            for (int i = 0; i < sa.length; i++) {
                if (sa[i].equals("--verbose")) {
                    verbose = true;
                    continue;
                }
                if (sa[i].equals("--threads")) {
                    threaded = true;
                    continue;
                }
                if (sa[i].equals("--populate")) {
                    populate = true;
                    continue;
                }
                if (sa[i].startsWith("--rcfile=")) {
                    rcFile = sa[i].substring("--rcfile=".length());
                    continue;
                }
                if (sa[i].startsWith("--urlid=")) {
                    currentUrlid = sa[i].substring("--urlid=".length()); continue;
                }
                if (sa[i].startsWith("--sqltool=")) {
                    sqlToolUrlid = sa[i].substring("--sqltool=".length());
                    continue;
                }
                if (currentUrlid == null) {
                    throw new IllegalArgumentException(
                            "You must specify 'urlid' before script files.");
                }
                if (scriptFileMap.containsKey(sa[i]))
                    throw new IllegalArgumentException(
                            TestScriptRunner.class.getName()
                            + " can't handle the same script name twice.  "
                            + "(Just copy or sym-link the script).");
                scriptFileMap.put(sa[i], currentUrlid);
            }
            if (currentUrlid == null) throw new IllegalArgumentException();
            if (scriptFileMap.size() < 1) {
                scriptFileMap.put("-", currentUrlid);
            }
        } catch (IllegalArgumentException e) {
            if (e.getMessage() != null) System.err.println(e.getMessage());
            System.err.println(SYNTAX_MSG);
            System.exit(2);
        }
        if (sqlToolUrlid != null) {
            Class sqlToolClass = null;
            try {
                sqlToolClass = Class.forName("org.hsqldb.util.SqlTool");
            } catch (Exception e) {
                System.err.println("SqlTool class not accessible.  "
                        + "Re-run without '--sqltool' switch.");
                System.exit(3);
            }
            try {
                sqlToolMainMethod = sqlToolClass.
                        getMethod("objectMain", new Class[] {sa.getClass()} );
            } catch (Exception e) {
                System.err.println("SqlTool integration failure: " + e);
                System.exit(3);
            }
        }
        TestScriptRunner runner = new TestScriptRunner(rcFile, scriptFileMap);
        runner.setVerbose(verbose);
        runner.setThreaded(threaded);
        TestCacheSize tcs = populate ? populate() : null;
        runner.establishConnections();
        boolean success = runner.runScripts();
        if (sqlToolMainMethod != null) try {
            sqlToolMainMethod.invoke(null, new Object[] { new String[] {
                "--rcfile=" + rcFile, sqlToolUrlid }});
        } catch (Exception e) {
            System.err.println("SqlTool failed: " + e);
            e.printStackTrace();
        }
        if (tcs != null) tcs.tearDown();
        System.exit(success ? 0 : 1);
    }
    List scriptRuns = new ArrayList();
    private class ScriptRun extends Thread {
        private Reader reader;
        private Connection conn = null;
        private RCData rcdata;
        private boolean success = false;
        public ScriptRun(String name, Reader reader, RCData rcdata) {
            super(name);
            this.reader = reader;
            this.rcdata = rcdata;
        }
        public boolean getSuccess() {
            return success;
        }
        public void connect() throws SQLException {
            if (conn != null) {
                throw new IllegalStateException("Thread '" + getName()
                        + "' has already been connected");
            }
            try {
                conn = rcdata.getConnection();
            } catch (Exception e) {
                throw new RuntimeException(
                        "Failed to connect to get JDBC connection for '"
                        + getName() + "'", e);
            }
            conn.setAutoCommit(false);
            System.out.println("ScriptRun '" + getName() + "' connected with "
                    + RCData.tiToString(conn.getTransactionIsolation()) + '.');
        }
        public void run() {
            try {
                TestUtil.testScript(conn, getName(), reader);
                success = true;
            } catch (TestUtil.TestRuntimeException tre) {
                System.err.println("Script '" + getName() + "' failed");
            } catch (IOException ioe) {
                System.err.println("Aborting thread for script '" + getName()
                        + "' due to: " + ioe);
                throw new RuntimeException(ioe);
            } catch (SQLException se) {
                System.err.println("Aborting thread for script '" + getName()
                        + "' due to: " + se);
                throw new RuntimeException(se);
            } finally { try {
                conn.close();
            } catch (SQLException se) {
                System.err.println("Failed to close JDBC connection for '"
                        + getName() + "': " + se);
            } }
        }
    }
    public void setVerbose(boolean verbose) {
        this.verbose = verbose;
    }
    public void setThreaded(boolean threaded) {
        this.threaded = threaded;
    }
    public TestScriptRunner(String rcFileString, Map scriptFileMap)
            throws IOException {
        TestUtil.setAbortOnErr(true);
        Map rcdataMap = new HashMap();
        File rcFile = new File(rcFileString);
        if (!rcFile.isFile())
            throw new IllegalArgumentException(
                    "RC file '" + rcFileString + "' not a file");
        String scriptPath, urlid;
        Iterator it;
        File file;
        Reader reader = null;
        it = scriptFileMap.values().iterator();
        while (it.hasNext()) {
            urlid = (String) it.next();
            if (rcdataMap.containsKey(urlid)) continue;
            try {
                rcdataMap.put(urlid, new RCData(rcFile, urlid));
            } catch (Exception e) {
                throw new RuntimeException(
                        "Failed to instantiate RCData with file '"
                        + rcFile + "' for urlid '" + urlid + "'", e);
            }
        }
        it = scriptFileMap.keySet().iterator();
        while (it.hasNext()) {
            scriptPath = (String) it.next();
            urlid = (String) scriptFileMap.get(scriptPath);
            if (scriptPath.equals("-")) {
                reader = new InputStreamReader(System.in);
            } else {
                file = new File(scriptPath);
                if (!file.isFile()) throw new IOException("'" + file
                        + "' is not a file");
                if (!file.canRead()) throw new IOException("'" + file
                        + "' is not readable");
                reader = new FileReader(file);
            }
            scriptRuns.add(new ScriptRun(scriptPath,
                    reader, (RCData) rcdataMap.get(urlid)));
        }
    }
    public void establishConnections() throws SQLException {
        for (int i = 0; i < scriptRuns.size(); i++)
            ((ScriptRun) scriptRuns.get(i)).connect();
        if (verbose) System.out.println(Integer.toString(scriptRuns.size())
                    + " connection threads connected");
    }
    public boolean runScripts() {
        ScriptRun scriptRun;
        for (int i = 0; i < scriptRuns.size(); i++) {
            scriptRun = (ScriptRun) scriptRuns.get(i);
            if (verbose) System.out.print("Starting " + (++i) + " / "
                + scriptRuns.size() + "...");
            scriptRun.start();
            if (verbose) System.out.println("  +");
            if (!threaded) try {
                scriptRun.join();
            } catch (InterruptedException ie) {
                throw new RuntimeException(
                        "Interrupted while waiting for script '"
                        + scriptRun.getName() + "' to execute", ie);
            }
        }
        if (threaded) {
            if (verbose)
                System.out.println(
                        "All scripts started.  Will now wait for them.");
            for (int i = 0; i < scriptRuns.size(); i++) try {
                ((ScriptRun) scriptRuns.get(i)).join();
            } catch (InterruptedException ie) {
                throw new RuntimeException(
                        "Interrupted while waiting for script to execute", ie);
            }
        }
        for (int i = 0; i < scriptRuns.size(); i++) {
            if (!((ScriptRun) scriptRuns.get(i)).getSuccess()) return false;
        }
        return true;
    }
    static protected TestCacheSize populate() {
        TestCacheSize  test  = new TestCacheSize();
        test.filepath = "mem:test";
        test.filedb   = false;
        test.shutdown = false;
        test.setUp();
        test.testFillUp();
        return test;
    }
}