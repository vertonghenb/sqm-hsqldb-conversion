package org.hsqldb.server;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Enumeration;
import java.util.StringTokenizer;
import org.hsqldb.DatabaseManager;
import org.hsqldb.DatabaseURL;
import org.hsqldb.HsqlDateTime;
import org.hsqldb.HsqlException;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.lib.FileUtil;
import org.hsqldb.lib.HashSet;
import org.hsqldb.lib.IntKeyHashMap;
import org.hsqldb.lib.Iterator;
import org.hsqldb.lib.StopWatch;
import org.hsqldb.lib.StringUtil;
import org.hsqldb.lib.WrapperIterator;
import org.hsqldb.lib.java.JavaSystem;
import org.hsqldb.persist.HsqlDatabaseProperties;
import org.hsqldb.persist.HsqlProperties;
import org.hsqldb.resources.BundleHandler;
import org.hsqldb.result.Result;
import org.hsqldb.result.ResultConstants;
public class Server implements HsqlSocketRequestHandler {
    protected static final int serverBundleHandle =
        BundleHandler.getBundleHandle("org_hsqldb_Server_messages", null);
    ServerProperties serverProperties;
    HashSet serverConnSet;
    protected String[]         dbAlias;
    protected String[]         dbType;
    protected String[]         dbPath;
    protected HsqlProperties[] dbProps;
    protected int[]            dbID;
    protected long[]           dbActionSequence;
    HashSet aliasSet = new HashSet();
    protected int maxConnections;
    volatile long actionSequence;
    protected String            serverId;
    protected int               serverProtocol;
    protected ThreadGroup       serverConnectionThreadGroup;
    protected HsqlSocketFactory socketFactory;
    protected ServerSocket      socket;
    private Thread             serverThread;
    private Throwable          serverError;
    private volatile int       serverState;
    private volatile boolean   isSilent;
    protected volatile boolean isRemoteOpen;
    protected boolean          isDaemon;
    private PrintWriter        logWriter;
    private PrintWriter        errWriter;
    private ServerAcl          acl = null;    
    private volatile boolean   isShuttingDown;
    private class ServerThread extends Thread {
        ServerThread(String name) {
            super(name);
            setName(name + '@' + Integer.toString(Server.this.hashCode(), 16));
        }
        public void run() {
            Server.this.run();
            printWithThread("ServerThread.run() exited");
        }
    }
    public Thread getServerThread() {
        return serverThread;
    }
    public Server() {
        this(ServerConstants.SC_PROTOCOL_HSQL);
    }
    protected Server(int protocol) {
        init(protocol);
    }
    public void checkRunning(boolean running) {
        int     state;
        boolean error;
        printWithThread("checkRunning(" + running + ") entered");
        state = getState();
        error = (running && state != ServerConstants.SERVER_STATE_ONLINE)
                || (!running
                    && state != ServerConstants.SERVER_STATE_SHUTDOWN);
        if (error) {
            String msg = "server is " + (running ? "not "
                                                 : "") + "running";
            throw Error.error(ErrorCode.GENERAL_ERROR, msg);
        }
        printWithThread("checkRunning(" + running + ") exited");
    }
    public synchronized void signalCloseAllServerConnections() {
        Iterator it;
        printWithThread("signalCloseAllServerConnections() entered");
        synchronized (serverConnSet) {
            it = new WrapperIterator(serverConnSet.toArray(null));
        }
        for (; it.hasNext(); ) {
            ServerConnection sc = (ServerConnection) it.next();
            printWithThread("Closing " + sc);
            sc.signalClose();
        }
        printWithThread("signalCloseAllServerConnections() exited");
    }
    protected void finalize() throws Throwable {
        if (serverThread != null) {
            releaseServerSocket();
        }
    }
    public String getAddress() {
        return socket == null
               ? serverProperties.getProperty(ServerProperties.sc_key_address)
               : socket.getInetAddress().getHostAddress();
    }
    public String getDatabaseName(int index, boolean asconfigured) {
        if (asconfigured) {
            return serverProperties.getProperty(ServerProperties.sc_key_dbname
                                                + "." + index);
        } else if (getState() == ServerConstants.SERVER_STATE_ONLINE) {
            return (dbAlias == null || index < 0 || index >= dbAlias.length)
                   ? null
                   : dbAlias[index];
        } else {
            return null;
        }
    }
    public String getDatabasePath(int index, boolean asconfigured) {
        if (asconfigured) {
            return serverProperties.getProperty(
                ServerProperties.sc_key_database + "." + index);
        } else if (getState() == ServerConstants.SERVER_STATE_ONLINE) {
            return (dbPath == null || index < 0 || index >= dbPath.length)
                   ? null
                   : dbPath[index];
        } else {
            return null;
        }
    }
    public String getDatabaseType(int index) {
        return (dbType == null || index < 0 || index >= dbType.length) ? null
                                                                       : dbType[index];
    }
    public String getDefaultWebPage() {
        return "[IGNORED]";
    }
    public String getHelpString() {
        return BundleHandler.getString(serverBundleHandle, "server.help");
    }
    public PrintWriter getErrWriter() {
        return errWriter;
    }
    public PrintWriter getLogWriter() {
        return logWriter;
    }
    public int getPort() {
        return serverProperties.getIntegerProperty(
            ServerProperties.sc_key_port,
            ServerConfiguration.getDefaultPort(serverProtocol, isTls()));
    }
    public String getProductName() {
        return "HSQLDB server";
    }
    public String getProductVersion() {
        return HsqlDatabaseProperties.THIS_VERSION;
    }
    public String getProtocol() {
        return isTls() ? "HSQLS"
                       : "HSQL";
    }
    public Throwable getServerError() {
        return serverError;
    }
    public String getServerId() {
        return serverId;
    }
    public int getState() {
        return serverState;
    }
    public String getStateDescriptor() {
        String    state;
        Throwable t = getServerError();
        switch (serverState) {
            case ServerConstants.SERVER_STATE_SHUTDOWN :
                state = "SHUTDOWN";
                break;
            case ServerConstants.SERVER_STATE_OPENING :
                state = "OPENING";
                break;
            case ServerConstants.SERVER_STATE_CLOSING :
                state = "CLOSING";
                break;
            case ServerConstants.SERVER_STATE_ONLINE :
                state = "ONLINE";
                break;
            default :
                state = "UNKNOWN";
                break;
        }
        return state;
    }
    public String getWebRoot() {
        return "[IGNORED]";
    }
    public void handleConnection(Socket s) {
        Thread   t;
        Runnable r;
        String   ctn;
        printWithThread("handleConnection(" + s + ") entered");
        if (!allowConnection(s)) {
            try {
                s.close();
            } catch (Exception e) {}
            printWithThread("allowConnection(): connection refused");
            printWithThread("handleConnection() exited");
            return;
        }
        if (socketFactory != null) {
            socketFactory.configureSocket(s);
        }
        if (serverProtocol == ServerConstants.SC_PROTOCOL_HSQL) {
            r   = new ServerConnection(s, this);
            ctn = ((ServerConnection) r).getConnectionThreadName();
            synchronized (serverConnSet) {
                serverConnSet.add(r);
            }
        } else {
            r   = new WebServerConnection(s, (WebServer) this);
            ctn = ((WebServerConnection) r).getConnectionThreadName();
        }
        t = new Thread(serverConnectionThreadGroup, r, ctn);
        t.start();
        printWithThread("handleConnection() exited");
    }
    public boolean isNoSystemExit() {
        return serverProperties.isPropertyTrue(
            ServerProperties.sc_key_no_system_exit);
    }
    public boolean isRestartOnShutdown() {
        return serverProperties.isPropertyTrue(
            ServerProperties.sc_key_autorestart_server);
    }
    public boolean isSilent() {
        return isSilent;
    }
    public boolean isTls() {
        return serverProperties.isPropertyTrue(ServerProperties.sc_key_tls);
    }
    public boolean isTrace() {
        return serverProperties.isPropertyTrue(ServerProperties.sc_key_trace);
    }
    public boolean putPropertiesFromFile(String path) {
        return putPropertiesFromFile(path, ".properties");
    }
    public boolean putPropertiesFromFile(String path, String extension) {
        if (getState() != ServerConstants.SERVER_STATE_SHUTDOWN) {
            throw Error.error(ErrorCode.GENERAL_ERROR, "server properties");
        }
        path = FileUtil.getFileUtil().canonicalOrAbsolutePath(path);
        ServerProperties p = ServerConfiguration.getPropertiesFromFile(
            ServerConstants.SC_PROTOCOL_HSQL, path, extension);
        if (p == null || p.isEmpty()) {
            return false;
        }
        printWithThread("putPropertiesFromFile(): [" + path + ".properties]");
        try {
            setProperties(p);
        } catch (Exception e) {
            throw Error.error(e, ErrorCode.GENERAL_ERROR,
                              ErrorCode.M_Message_Pair,
                              new String[]{ "Failed to set properties" });
        }
        return true;
    }
    public void putPropertiesFromString(String s) {
        if (getState() != ServerConstants.SERVER_STATE_SHUTDOWN) {
            throw Error.error(ErrorCode.GENERAL_ERROR);
        }
        if (StringUtil.isEmpty(s)) {
            return;
        }
        printWithThread("putPropertiesFromString(): [" + s + "]");
        HsqlProperties p = HsqlProperties.delimitedArgPairsToProps(s, "=",
            ";", ServerProperties.sc_key_prefix);
        try {
            setProperties(p);
        } catch (Exception e) {
            throw Error.error(e, ErrorCode.GENERAL_ERROR,
                              ErrorCode.M_Message_Pair,
                              new String[]{ "Failed to set properties" });
        }
    }
    public void setAddress(String address) {
        checkRunning(false);
        if (org.hsqldb.lib.StringUtil.isEmpty(address)) {
            address = ServerConstants.SC_DEFAULT_ADDRESS;
        }
        printWithThread("setAddress(" + address + ")");
        serverProperties.setProperty(ServerProperties.sc_key_address, address);
    }
    public void setDatabaseName(int index, String name) {
        checkRunning(false);
        printWithThread("setDatabaseName(" + index + "," + name + ")");
        serverProperties.setProperty(ServerProperties.sc_key_dbname + "."
                                     + index, name);
    }
    public void setDatabasePath(int index, String path) {
        checkRunning(false);
        printWithThread("setDatabasePath(" + index + "," + path + ")");
        serverProperties.setProperty(ServerProperties.sc_key_database + "."
                                     + index, path);
    }
    public void setDefaultWebPage(String file) {
        checkRunning(false);
        printWithThread("setDefaultWebPage(" + file + ")");
        if (serverProtocol != ServerConstants.SC_PROTOCOL_HTTP) {
            return;
        }
        serverProperties.setProperty(ServerProperties.sc_key_web_default_page,
                                     file);
    }
    public void setPort(int port) {
        checkRunning(false);
        printWithThread("setPort(" + port + ")");
        serverProperties.setProperty(ServerProperties.sc_key_port, port);
    }
    public void setErrWriter(PrintWriter pw) {
        errWriter = pw;
    }
    public void setLogWriter(PrintWriter pw) {
        logWriter = pw;
    }
    public void setNoSystemExit(boolean noExit) {
        printWithThread("setNoSystemExit(" + noExit + ")");
        serverProperties.setProperty(ServerProperties.sc_key_no_system_exit,
                                     noExit);
    }
    public void setRestartOnShutdown(boolean restart) {
        printWithThread("setRestartOnShutdown(" + restart + ")");
        serverProperties.setProperty(
            ServerProperties.sc_key_autorestart_server, restart);
    }
    public void setSilent(boolean silent) {
        printWithThread("setSilent(" + silent + ")");
        serverProperties.setProperty(ServerProperties.sc_key_silent, silent);
        isSilent = silent;
    }
    public void setTls(boolean tls) {
        checkRunning(false);
        printWithThread("setTls(" + tls + ")");
        serverProperties.setProperty(ServerProperties.sc_key_tls, tls);
    }
    public void setTrace(boolean trace) {
        printWithThread("setTrace(" + trace + ")");
        serverProperties.setProperty(ServerProperties.sc_key_trace, trace);
        JavaSystem.setLogToSystem(trace);
    }
    public void setDaemon(boolean daemon) {
        checkRunning(false);
        printWithThread("setDaemon(" + daemon + ")");
        serverProperties.setProperty(ServerProperties.sc_key_daemon, daemon);
    }
    public void setWebRoot(String root) {
        checkRunning(false);
        root = (new File(root)).getAbsolutePath();
        printWithThread("setWebRoot(" + root + ")");
        if (serverProtocol != ServerConstants.SC_PROTOCOL_HTTP) {
            return;
        }
        serverProperties.setProperty(ServerProperties.sc_key_web_root, root);
    }
    public void setProperties(HsqlProperties props)
    throws IOException, ServerAcl.AclFormatException {
        checkRunning(false);
        if (props != null) {
            props.validate();
            String[] errors = props.getErrorKeys();
            if (errors.length > 0) {
                throw Error.error(ErrorCode.SERVER_NO_DATABASE, errors[0]);
            }
            serverProperties.addProperties(props);
        }
        maxConnections = serverProperties.getIntegerProperty(
            ServerProperties.sc_key_max_connections, 16);
        JavaSystem.setLogToSystem(isTrace());
        isSilent =
            serverProperties.isPropertyTrue(ServerProperties.sc_key_silent);
        isRemoteOpen = serverProperties.isPropertyTrue(
            ServerProperties.sc_key_remote_open_db);
        isDaemon =
            serverProperties.isPropertyTrue(ServerProperties.sc_key_daemon);
        String aclFilepath =
            serverProperties.getProperty(ServerProperties.sc_key_acl);
        if (aclFilepath != null) {
            acl = new ServerAcl(new File(aclFilepath));;
            if (logWriter != null && !isSilent) {
                acl.setPrintWriter(logWriter);
            }
        }
    }
    public int start() {
        printWithThread("start() entered");
        int previousState = getState();
        if (serverThread != null) {
            printWithThread("start(): serverThread != null; no action taken");
            return previousState;
        }
        setState(ServerConstants.SERVER_STATE_OPENING);
        serverThread = new ServerThread("HSQLDB Server ");
        if (isDaemon) {
            serverThread.setDaemon(true);
        }
        serverThread.start();
        while (getState() == ServerConstants.SERVER_STATE_OPENING) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {}
        }
        printWithThread("start() exiting");
        return previousState;
    }
    public int stop() {
        printWithThread("stop() entered");
        int previousState = getState();
        if (serverThread == null) {
            printWithThread("stop() serverThread is null; no action taken");
            return previousState;
        }
        releaseServerSocket();
        printWithThread("stop() exiting");
        return previousState;
    }
    protected boolean allowConnection(Socket socket) {
        if (isShuttingDown) {
            return false;
        }
        return (acl == null) ? true
                             : acl.permitAccess(
                                 socket.getInetAddress().getAddress());
    }
    protected void init(int protocol) {
        serverState      = ServerConstants.SERVER_STATE_SHUTDOWN;
        serverConnSet    = new HashSet();
        serverId         = toString();
        serverId         = serverId.substring(serverId.lastIndexOf('.') + 1);
        serverProtocol   = protocol;
        serverProperties = ServerConfiguration.newDefaultProperties(protocol);
        logWriter        = new PrintWriter(System.out);
        errWriter        = new PrintWriter(System.err);
        JavaSystem.setLogToSystem(isTrace());
    }
    protected synchronized void setState(int state) {
        serverState = state;
    }
    public final void notify(int action, int id) {
        printWithThread("notifiy(" + action + "," + id + ") entered");
        if (action != ServerConstants.SC_DATABASE_SHUTDOWN) {
            return;
        }
        releaseDatabase(id);
        boolean shutdown = true;
        for (int i = 0; i < dbID.length; i++) {
            if (dbAlias[i] != null) {
                shutdown = false;
            }
        }
        if (!isRemoteOpen && shutdown) {
            stop();
        }
    }
    final synchronized void releaseDatabase(int id) {
        Iterator it;
        boolean  found = false;
        printWithThread("releaseDatabase(" + id + ") entered");
        for (int i = 0; i < dbID.length; i++) {
            if (dbID[i] == id && dbAlias[i] != null) {
                dbID[i]             = 0;
                dbActionSequence[i] = 0;
                dbAlias[i]          = null;
                dbPath[i]           = null;
                dbType[i]           = null;
                dbProps[i]          = null;
            }
        }
        synchronized (serverConnSet) {
            it = new WrapperIterator(serverConnSet.toArray(null));
        }
        while (it.hasNext()) {
            ServerConnection sc = (ServerConnection) it.next();
            if (sc.dbID == id) {
                sc.signalClose();
                serverConnSet.remove(sc);
            }
        }
        printWithThread("releaseDatabase(" + id + ") exiting");
    }
    protected void print(String msg) {
        PrintWriter writer = logWriter;
        if (writer != null) {
            writer.println("[" + serverId + "]: " + msg);
            writer.flush();
        }
    }
    final void printResource(String key) {
        String          resource;
        StringTokenizer st;
        if (serverBundleHandle < 0) {
            return;
        }
        resource = BundleHandler.getString(serverBundleHandle, key);
        if (resource == null) {
            return;
        }
        st = new StringTokenizer(resource, "\n\r");
        while (st.hasMoreTokens()) {
            print(st.nextToken());
        }
    }
    protected void printStackTrace(Throwable t) {
        if (errWriter != null) {
            t.printStackTrace(errWriter);
            errWriter.flush();
        }
    }
    final void printWithTimestamp(String msg) {
        print(HsqlDateTime.getSystemTimeString() + " " + msg);
    }
    protected void printWithThread(String msg) {
        if (!isSilent()) {
            print("[" + Thread.currentThread() + "]: " + msg);
        }
    }
    protected void printError(String msg) {
        PrintWriter writer = errWriter;
        if (writer != null) {
            writer.print("[" + serverId + "]: ");
            writer.print("[" + Thread.currentThread() + "]: ");
            writer.println(msg);
            writer.flush();
        }
    }
    final void printRequest(int cid, Result r) {
        if (isSilent()) {
            return;
        }
        StringBuffer sb = new StringBuffer();
        sb.append(cid);
        sb.append(':');
        switch (r.getType()) {
            case ResultConstants.PREPARE : {
                sb.append("SQLCLI:SQLPREPARE ");
                sb.append(r.getMainString());
                break;
            }
            case ResultConstants.EXECDIRECT : {
                sb.append(r.getMainString());
                break;
            }
            case ResultConstants.EXECUTE_INVALID :
            case ResultConstants.EXECUTE : {
                sb.append("SQLCLI:SQLEXECUTE:");
                sb.append(r.getStatementID());
                break;
            }
            case ResultConstants.BATCHEXECUTE :
                sb.append("SQLCLI:SQLEXECUTE:");
                sb.append("BATCHMODE:");
                sb.append(r.getStatementID());
                break;
            case ResultConstants.UPDATE_RESULT : {
                sb.append("SQLCLI:RESULTUPDATE:");
                sb.append(r.getStatementID());
                break;
            }
            case ResultConstants.FREESTMT : {
                sb.append("SQLCLI:SQLFREESTMT:");
                sb.append(r.getStatementID());
                break;
            }
            case ResultConstants.GETSESSIONATTR : {
                sb.append("HSQLCLI:GETSESSIONATTR");
                break;
            }
            case ResultConstants.SETSESSIONATTR : {
                sb.append("HSQLCLI:SETSESSIONATTR:");
                break;
            }
            case ResultConstants.ENDTRAN : {
                sb.append("SQLCLI:SQLENDTRAN:");
                switch (r.getActionType()) {
                    case ResultConstants.TX_COMMIT :
                        sb.append("COMMIT");
                        break;
                    case ResultConstants.TX_ROLLBACK :
                        sb.append("ROLLBACK");
                        break;
                    case ResultConstants.TX_SAVEPOINT_NAME_RELEASE :
                        sb.append("SAVEPOINT_NAME_RELEASE ");
                        sb.append(r.getMainString());
                        break;
                    case ResultConstants.TX_SAVEPOINT_NAME_ROLLBACK :
                        sb.append("SAVEPOINT_NAME_ROLLBACK ");
                        sb.append(r.getMainString());
                        break;
                    default :
                        sb.append(r.getActionType());
                }
                break;
            }
            case ResultConstants.STARTTRAN : {
                sb.append("SQLCLI:SQLSTARTTRAN");
                break;
            }
            case ResultConstants.DISCONNECT : {
                sb.append("SQLCLI:SQLDISCONNECT");
                break;
            }
            case ResultConstants.SETCONNECTATTR : {
                sb.append("SQLCLI:SQLSETCONNECTATTR:");
                switch (r.getConnectionAttrType()) {
                    case ResultConstants.SQL_ATTR_SAVEPOINT_NAME : {
                        sb.append("SQL_ATTR_SAVEPOINT_NAME ");
                        sb.append(r.getMainString());
                        break;
                    }
                    default : {
                        sb.append(r.getConnectionAttrType());
                    }
                }
                break;
            }
            case ResultConstants.CLOSE_RESULT : {
                sb.append("HQLCLI:CLOSE_RESULT:RESULT_ID ");
                sb.append(r.getResultId());
                break;
            }
            case ResultConstants.REQUESTDATA : {
                sb.append("HQLCLI:REQUESTDATA:RESULT_ID ");
                sb.append(r.getResultId());
                sb.append(" ROWOFFSET ");
                sb.append(r.getUpdateCount());
                sb.append(" ROWCOUNT ");
                sb.append(r.getFetchSize());
                break;
            }
            default : {
                sb.append("SQLCLI:MODE:");
                sb.append(r.getType());
                break;
            }
        }
        print(sb.toString());
    }
    synchronized final int getDBIndex(String aliasPath) {
        int    semipos  = aliasPath.indexOf(';');
        String alias    = aliasPath;
        String filepath = null;
        if (semipos != -1) {
            alias    = aliasPath.substring(0, semipos);
            filepath = aliasPath.substring(semipos + 1);
        }
        int dbIndex = ArrayUtil.find(dbAlias, alias);
        if (dbIndex == -1) {
            if (filepath == null) {
                HsqlException e = Error.error(ErrorCode.GENERAL_ERROR,
                                              "database alias does not exist");
                printError("database alias=" + alias + " does not exist");
                setServerError(e);
                throw e;
            } else {
                return openDatabase(alias, filepath);
            }
        } else {
            return dbIndex;
        }
    }
    final int openDatabase(String alias, String datapath) {
        if (!isRemoteOpen) {
            HsqlException e = Error.error(ErrorCode.GENERAL_ERROR,
                                          "remote open not allowed");
            printError("Remote database open not allowed");
            setServerError(e);
            throw e;
        }
        int i = getFirstEmptyDatabaseIndex();
        if (i < -1) {
            i = closeOldestDatabase();
            if (i < -1) {
                HsqlException e =
                    Error.error(ErrorCode.GENERAL_ERROR,
                                "limit of open databases reached");
                printError("limit of open databases reached");
                setServerError(e);
                throw e;
            }
        }
        HsqlProperties newprops = DatabaseURL.parseURL(datapath, false, false);
        if (newprops == null) {
            HsqlException e = Error.error(ErrorCode.GENERAL_ERROR,
                                          "invalid database path");
            printError("invalid database path");
            setServerError(e);
            throw e;
        }
        String path = newprops.getProperty(DatabaseURL.url_database);
        String type = newprops.getProperty(DatabaseURL.url_connection_type);
        try {
            int dbid = DatabaseManager.getDatabase(type, path, this, newprops);
            dbID[i]             = dbid;
            dbActionSequence[i] = actionSequence;
            dbAlias[i]          = alias;
            dbPath[i]           = path;
            dbType[i]           = type;
            dbProps[i]          = newprops;
            return i;
        } catch (HsqlException e) {
            printError("Database [index=" + i + ", db=" + dbType[i]
                       + dbPath[i] + ", alias=" + dbAlias[i]
                       + "] did not open: " + e.toString());
            setServerError(e);
            throw e;
        }
    }
    final int getFirstEmptyDatabaseIndex() {
        for (int i = 0; i < dbAlias.length; i++) {
            if (dbAlias[i] == null) {
                return i;
            }
        }
        return -1;
    }
    final boolean openDatabases() {
        printWithThread("openDatabases() entered");
        boolean success = false;
        setDBInfoArrays();
        for (int i = 0; i < dbAlias.length; i++) {
            if (dbAlias[i] == null) {
                continue;
            }
            printWithThread("Opening database: [" + dbType[i] + dbPath[i]
                            + "]");
            StopWatch sw = new StopWatch();
            int       id;
            try {
                id = DatabaseManager.getDatabase(dbType[i], dbPath[i], this,
                                                 dbProps[i]);
                dbID[i] = id;
                success = true;
            } catch (HsqlException e) {
                printError("Database [index=" + i + ", db=" + dbType[i]
                           + dbPath[i] + ", alias=" + dbAlias[i]
                           + "] did not open: " + e.toString());
                setServerError(e);
                dbAlias[i] = null;
                dbPath[i]  = null;
                dbType[i]  = null;
                dbProps[i] = null;
                continue;
            }
            sw.stop();
            String msg = "Database [index=" + i + ", id=" + id + ", db="
                         + dbType[i] + dbPath[i] + ", alias=" + dbAlias[i]
                         + "] opened sucessfully";
            print(sw.elapsedTimeToMessage(msg));
        }
        printWithThread("openDatabases() exiting");
        if (isRemoteOpen) {
            success = true;
        }
        if (!success && getServerError() == null) {
            setServerError(Error.error(ErrorCode.SERVER_NO_DATABASE));
        }
        return success;
    }
    private void setDBInfoArrays() {
        IntKeyHashMap dbNumberMap  = getDBNameArray();
        int           maxDatabases = dbNumberMap.size();
        if (serverProperties.isPropertyTrue(
                ServerProperties.sc_key_remote_open_db)) {
            int max = serverProperties.getIntegerProperty(
                ServerProperties.sc_key_max_databases,
                ServerConstants.SC_DEFAULT_MAX_DATABASES);
            if (maxDatabases < max) {
                maxDatabases = max;
            }
        }
        dbAlias          = new String[maxDatabases];
        dbPath           = new String[dbAlias.length];
        dbType           = new String[dbAlias.length];
        dbID             = new int[dbAlias.length];
        dbActionSequence = new long[dbAlias.length];
        dbProps          = new HsqlProperties[dbAlias.length];
        Iterator it = dbNumberMap.keySet().iterator();
        for (int i = 0; it.hasNext(); ) {
            int    dbNumber = it.nextInt();
            String path     = getDatabasePath(dbNumber, true);
            if (path == null) {
                printWithThread("missing database path: "
                                + dbNumberMap.get(dbNumber));
                continue;
            }
            HsqlProperties dbURL = DatabaseURL.parseURL(path, false, false);
            if (dbURL == null) {
                printWithThread("malformed database path: " + path);
                continue;
            }
            dbAlias[i] = (String) dbNumberMap.get(dbNumber);
            dbPath[i]  = dbURL.getProperty("database");
            dbType[i]  = dbURL.getProperty("connection_type");
            dbProps[i] = dbURL;
            i++;
        }
    }
    private IntKeyHashMap getDBNameArray() {
        final String  prefix       = ServerProperties.sc_key_dbname + ".";
        final int     prefixLen    = prefix.length();
        IntKeyHashMap idToAliasMap = new IntKeyHashMap();
        Enumeration   en           = serverProperties.propertyNames();
        for (; en.hasMoreElements(); ) {
            String key = (String) en.nextElement();
            if (!key.startsWith(prefix)) {
                continue;
            }
            int dbNumber;
            try {
                dbNumber = Integer.parseInt(key.substring(prefixLen));
            } catch (NumberFormatException e1) {
                printWithThread("maformed database enumerator: " + key);
                continue;
            }
            String alias = serverProperties.getProperty(key).toLowerCase();
            if (!aliasSet.add(alias)) {
                printWithThread("duplicate alias: " + alias);
            }
            Object existing = idToAliasMap.put(dbNumber, alias);
            if (existing != null) {
                printWithThread("duplicate database enumerator: " + key);
            }
        }
        return idToAliasMap;
    }
    private void openServerSocket() throws Exception {
        String    address;
        int       port;
        String[]  candidateAddrs;
        String    emsg;
        StopWatch sw;
        printWithThread("openServerSocket() entered");
        if (isTls()) {
            printWithThread("Requesting TLS/SSL-encrypted JDBC");
        }
        sw            = new StopWatch();
        socketFactory = HsqlSocketFactory.getInstance(isTls());
        address       = getAddress();
        port          = getPort();
        if (org.hsqldb.lib.StringUtil.isEmpty(address)
                || ServerConstants.SC_DEFAULT_ADDRESS.equalsIgnoreCase(
                    address.trim())) {
            socket = socketFactory.createServerSocket(port);
        } else {
            try {
                socket = socketFactory.createServerSocket(port, address);
            } catch (UnknownHostException e) {
                candidateAddrs =
                    ServerConfiguration.listLocalInetAddressNames();
                int      messageID;
                Object[] messageParameters;
                if (candidateAddrs.length > 0) {
                    messageID = ErrorCode.M_SERVER_OPEN_SERVER_SOCKET_1;
                    StringBuffer sb = new StringBuffer();
                    for (int i = 0; i < candidateAddrs.length; i++) {
                        if (sb.length() > 0) {
                            sb.append(", ");
                        }
                        sb.append(candidateAddrs[i]);
                    }
                    messageParameters = new Object[] {
                        address, sb.toString()
                    };
                } else {
                    messageID = ErrorCode.M_SERVER_OPEN_SERVER_SOCKET_2;
                    messageParameters = new Object[]{ address };
                }
                throw new UnknownHostException(Error.getMessage(messageID, 0,
                        messageParameters));
            }
        }
        socket.setSoTimeout(1000);
        printWithThread("Got server socket: " + socket);
        print(sw.elapsedTimeToMessage("Server socket opened successfully"));
        if (socketFactory.isSecure()) {
            print("Using TLS/SSL-encrypted JDBC");
        }
        printWithThread("openServerSocket() exiting");
    }
    private void printServerOnlineMessage() {
        String s = getProductName() + " " + getProductVersion()
                   + " is online on port " + this.getPort();
        ;
        printWithTimestamp(s);
        printResource("online.help");
    }
    protected void printProperties() {
        Enumeration e;
        String      key;
        String      value;
        if (isSilent()) {
            return;
        }
        e = serverProperties.propertyNames();
        while (e.hasMoreElements()) {
            key   = (String) e.nextElement();
            value = serverProperties.getProperty(key);
            printWithThread(key + "=" + value);
        }
    }
    private void releaseServerSocket() {
        printWithThread("releaseServerSocket() entered");
        if (socket != null) {
            printWithThread("Releasing server socket: [" + socket + "]");
            setState(ServerConstants.SERVER_STATE_CLOSING);
            try {
                socket.close();
            } catch (IOException e) {
                printError("Exception closing server socket");
                printError("releaseServerSocket(): " + e);
            }
            socket = null;
        }
        printWithThread("releaseServerSocket() exited");
    }
    private void run() {
        StopWatch   sw;
        ThreadGroup tg;
        String      tgName;
        printWithThread("run() entered");
        print("Initiating startup sequence...");
        printProperties();
        sw = new StopWatch();
        setServerError(null);
        try {
            openServerSocket();
        } catch (Exception e) {
            setServerError(e);
            printError("run()/openServerSocket(): ");
            printStackTrace(e);
            shutdown(true);
            return;
        }
        tgName = "HSQLDB Connections @"
                 + Integer.toString(this.hashCode(), 16);
        tg = new ThreadGroup(tgName);
        tg.setDaemon(false);
        serverConnectionThreadGroup = tg;
        if (!openDatabases()) {
            setServerError(null);
            printError("Shutting down because there are no open databases");
            shutdown(true);
            return;
        }
        setState(ServerConstants.SERVER_STATE_ONLINE);
        print(sw.elapsedTimeToMessage("Startup sequence completed"));
        printServerOnlineMessage();
        isShuttingDown = false;    
        try {
            while (socket != null) {
                try {
                    handleConnection(socket.accept());
                } catch (java.io.InterruptedIOException iioe) {}
            }
        } catch (IOException ioe) {
            if (getState() == ServerConstants.SERVER_STATE_ONLINE) {
                setServerError(ioe);
                printError(this + ".run()/handleConnection(): ");
                printStackTrace(ioe);
            }
        } catch (Throwable t) {
            printWithThread(t.toString());
        } finally {
            shutdown(false);    
        }
    }
    protected void setServerError(Throwable t) {
        serverError = t;
    }
    public void shutdownCatalogs(int shutdownMode) {
        DatabaseManager.shutdownDatabases(this, shutdownMode);
    }
    public void shutdownWithCatalogs(int shutdownMode) {
        isShuttingDown = true;
        DatabaseManager.shutdownDatabases(this, shutdownMode);
        shutdown(false);
        isShuttingDown = false;
    }
    public void shutdown() {
        shutdown(false);
    }
    protected synchronized void shutdown(boolean error) {
        if (serverState == ServerConstants.SERVER_STATE_SHUTDOWN) {
            return;
        }
        StopWatch sw;
        printWithThread("shutdown() entered");
        sw = new StopWatch();
        print("Initiating shutdown sequence...");
        releaseServerSocket();
        DatabaseManager.deRegisterServer(this);
        if (dbPath != null) {
            for (int i = 0; i < dbPath.length; i++) {
                releaseDatabase(dbID[i]);
            }
        }
        if (serverConnectionThreadGroup != null) {
            if (!serverConnectionThreadGroup.isDestroyed()) {
                for (int i = 0; serverConnectionThreadGroup.activeCount() > 0;
                        i++) {
                    int count;
                    try {
                        Thread.sleep(100);
                    } catch (Exception e) {
                    }
                }
                try {
                    serverConnectionThreadGroup.destroy();
                    printWithThread(serverConnectionThreadGroup.getName()
                                    + " destroyed");
                } catch (Throwable t) {
                    printWithThread(serverConnectionThreadGroup.getName()
                                    + " not destroyed");
                    printWithThread(t.toString());
                }
            }
            serverConnectionThreadGroup = null;
        }
        serverThread = null;
        setState(ServerConstants.SERVER_STATE_SHUTDOWN);
        print(sw.elapsedTimeToMessage("Shutdown sequence completed"));
        if (isNoSystemExit()) {
            printWithTimestamp("SHUTDOWN : System.exit() was not called");
            printWithThread("shutdown() exited");
        } else {
            printWithTimestamp("SHUTDOWN : System.exit() is called next");
            printWithThread("shutdown() exiting...");
            try {
                System.exit(0);
            } catch (Throwable t) {
                printWithThread(t.toString());
            }
        }
    }
    synchronized void setActionSequence(int dbIndex) {
        dbActionSequence[dbIndex] = actionSequence++;
    }
    protected int closeOldestDatabase() {
        return -1;
    }
    protected static void printHelp(String key) {
        System.out.println(BundleHandler.getString(serverBundleHandle, key));
    }
    public static void main(String[] args) {
        HsqlProperties argProps = null;
        argProps = HsqlProperties.argArrayToProps(args,
                ServerProperties.sc_key_prefix);
        String[] errors = argProps.getErrorKeys();
        if (errors.length != 0) {
            System.out.println("no value for argument:" + errors[0]);
            printHelp("server.help");
            return;
        }
        String propsPath = argProps.getProperty(ServerProperties.sc_key_props);
        String propsExtension = "";
        if (propsPath == null) {
            propsPath      = "server";
            propsExtension = ".properties";
        } else {
            argProps.removeProperty(ServerProperties.sc_key_props);
        }
        propsPath = FileUtil.getFileUtil().canonicalOrAbsolutePath(propsPath);
        ServerProperties fileProps = ServerConfiguration.getPropertiesFromFile(
            ServerConstants.SC_PROTOCOL_HSQL, propsPath, propsExtension);
        ServerProperties props =
            fileProps == null
            ? new ServerProperties(ServerConstants.SC_PROTOCOL_HSQL)
            : fileProps;
        props.addProperties(argProps);
        ServerConfiguration.translateDefaultDatabaseProperty(props);
        ServerConfiguration.translateDefaultNoSystemExitProperty(props);
        ServerConfiguration.translateAddressProperty(props);
        Server server = new Server();
        try {
            server.setProperties(props);
        } catch (Exception e) {
            server.printError("Failed to set properties");
            server.printStackTrace(e);
            return;
        }
        server.print("Startup sequence initiated from main() method");
        if (fileProps != null) {
            server.print("Loaded properties from [" + propsPath
                         + propsExtension + "]");
        } else {
            server.print("Could not load properties from file");
            server.print("Using cli/default properties only");
        }
        server.start();
    }
}