package org.hsqldb;
import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.dbinfo.DatabaseInformation;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.FrameworkLogger;
import org.hsqldb.lib.HashMappedList;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.HsqlTimer;
import org.hsqldb.persist.HsqlDatabaseProperties;
import org.hsqldb.persist.HsqlProperties;
import org.hsqldb.persist.LobManager;
import org.hsqldb.persist.Logger;
import org.hsqldb.persist.PersistentStoreCollectionDatabase;
import org.hsqldb.result.Result;
import org.hsqldb.rights.GranteeManager;
import org.hsqldb.rights.User;
import org.hsqldb.rights.UserManager;
import org.hsqldb.store.ValuePool;
import org.hsqldb.types.Collation;
public class Database {
    int                        databaseID;
    String                     databaseUniqueName;
    String                     databaseType;
    private final String       canonicalPath;
    public HsqlProperties      urlProperties;
    private final String       path;
    public Collation           collation;
    public DatabaseInformation dbInfo;
    private volatile int dbState;
    public Logger        logger;
    boolean databaseReadOnly;
    private boolean filesReadOnly;
    private boolean filesInJar;
    public boolean                sqlEnforceTypes        = false;
    public boolean                sqlEnforceRefs         = false;
    public boolean                sqlEnforceSize         = true;
    public boolean                sqlEnforceNames        = false;
    public boolean                sqlEnforceTDCD         = true;
    public boolean                sqlEnforceTDCU         = true;
    public boolean                sqlTranslateTTI        = true;
    public boolean                sqlConcatNulls         = true;
    public boolean                sqlUniqueNulls         = true;
    public boolean                sqlNullsFirst          = true;
    public boolean                sqlConvertTruncate     = true;
    public int                    sqlAvgScale            = 0;
    public boolean                sqlDoubleNaN           = true;
    public boolean                sqlLongvarIsLob        = false;
    public boolean                sqlSyntaxDb2           = false;
    public boolean                sqlSyntaxMss           = false;
    public boolean                sqlSyntaxMys           = false;
    public boolean                sqlSyntaxOra           = false;
    public boolean                sqlSyntaxPgs           = false;
    private boolean               isReferentialIntegrity = true;
    public HsqlDatabaseProperties databaseProperties;
    private final boolean         shutdownOnNoConnection;
    int                           resultMaxMemoryRows;
    public UserManager     userManager;
    public GranteeManager  granteeManager;
    public HsqlNameManager nameManager;
    public SessionManager     sessionManager;
    public TransactionManager txManager;
    public int defaultIsolationLevel = SessionInterface.TX_READ_COMMITTED;
    public boolean            txConflictRollback = true;
    public SchemaManager schemaManager;
    public PersistentStoreCollectionDatabase persistentStoreCollection;
    public LobManager lobManager;
    public CheckpointRunner checkpointRunner;
    public static final int DATABASE_ONLINE       = 1;
    public static final int DATABASE_OPENING      = 2;
    public static final int DATABASE_CLOSING      = 3;
    public static final int DATABASE_SHUTDOWN     = 4;
    public static final int CLOSEMODE_IMMEDIATELY = 1;
    public static final int CLOSEMODE_NORMAL      = 2;
    public static final int CLOSEMODE_COMPACT     = 3;
    public static final int CLOSEMODE_SCRIPT      = 4;
    Database(String type, String path, String canonicalPath,
             HsqlProperties props) {
        setState(Database.DATABASE_SHUTDOWN);
        this.databaseType  = type;
        this.path          = path;
        this.canonicalPath = canonicalPath;
        this.urlProperties = props;
        if (databaseType == DatabaseURL.S_RES) {
            filesInJar    = true;
            filesReadOnly = true;
        }
        logger = new Logger(this);
        shutdownOnNoConnection =
            urlProperties.isPropertyTrue(HsqlDatabaseProperties.url_shutdown);
        lobManager = new LobManager(this);
    }
    synchronized void open() {
        if (!isShutdown()) {
            return;
        }
        reopen();
    }
    void reopen() {
        boolean isNew = false;
        setState(DATABASE_OPENING);
        try {
            nameManager    = new HsqlNameManager(this);
            granteeManager = new GranteeManager(this);
            userManager    = new UserManager(this);
            schemaManager  = new SchemaManager(this);
            persistentStoreCollection =
                new PersistentStoreCollectionDatabase();
            isReferentialIntegrity = true;
            sessionManager         = new SessionManager(this);
            collation              = collation.getDatabaseInstance();
            dbInfo = DatabaseInformation.newDatabaseInformation(this);
            txManager              = new TransactionManager2PL(this);
            lobManager.createSchema();
            sessionManager.getSysLobSession().setSchema(
                SqlInvariants.LOBS_SCHEMA);
            schemaManager.setSchemaChangeTimestamp();
            schemaManager.createSystemTables();
            logger.openPersistence();
            isNew = logger.isNewDatabase;
            if (isNew) {
                String username = urlProperties.getProperty("user", "SA");
                String password = urlProperties.getProperty("password", "");
                userManager.createFirstUser(username, password);
                schemaManager.createPublicSchema();
                lobManager.initialiseLobSpace();
                logger.checkpoint(false);
            }
            lobManager.open();
            dbInfo.setWithContent(true);
            checkpointRunner = new CheckpointRunner();
        } catch (Throwable e) {
            logger.closePersistence(Database.CLOSEMODE_IMMEDIATELY);
            logger.releaseLock();
            setState(DATABASE_SHUTDOWN);
            clearStructures();
            DatabaseManager.removeDatabase(this);
            if (!(e instanceof HsqlException)) {
                e = Error.error(ErrorCode.GENERAL_ERROR, e);
            }
            logger.logSevereEvent("could not reopen database", e);
            throw (HsqlException) e;
        }
        setState(DATABASE_ONLINE);
    }
    void clearStructures() {
        if (schemaManager != null) {
            schemaManager.clearStructures();
        }
        granteeManager   = null;
        userManager      = null;
        nameManager      = null;
        schemaManager    = null;
        sessionManager   = null;
        dbInfo           = null;
        checkpointRunner = null;
    }
    public int getDatabaseID() {
        return this.databaseID;
    }
    public String getUniqueName() {
        return databaseUniqueName;
    }
    public void setUniqueName(String name) {
        databaseUniqueName = name;
    }
    public String getType() {
        return databaseType;
    }
    public String getPath() {
        return path;
    }
    public HsqlName getCatalogName() {
        return nameManager.getCatalogName();
    }
    public HsqlDatabaseProperties getProperties() {
        return databaseProperties;
    }
    public SessionManager getSessionManager() {
        return sessionManager;
    }
    public boolean isReadOnly() {
        return databaseReadOnly;
    }
    boolean isShutdown() {
        return dbState == DATABASE_SHUTDOWN;
    }
    synchronized Session connect(String username, String password,
                                 String zoneString, int timeZoneSeconds) {
        if (username.equalsIgnoreCase("SA")) {
            username = "SA";
        }
        User user = userManager.getUser(username, password);
        Session session = sessionManager.newSession(this, user,
            databaseReadOnly, true, zoneString, timeZoneSeconds);
        return session;
    }
    public void setReadOnly() {
        databaseReadOnly = true;
        filesReadOnly    = true;
    }
    public void setFilesReadOnly() {
        filesReadOnly = true;
    }
    public boolean isFilesReadOnly() {
        return filesReadOnly;
    }
    public boolean isFilesInJar() {
        return filesInJar;
    }
    public UserManager getUserManager() {
        return userManager;
    }
    public GranteeManager getGranteeManager() {
        return granteeManager;
    }
    public void setReferentialIntegrity(boolean ref) {
        isReferentialIntegrity = ref;
    }
    public boolean isReferentialIntegrity() {
        return isReferentialIntegrity;
    }
    public int getResultMaxMemoryRows() {
        return resultMaxMemoryRows;
    }
    public void setResultMaxMemoryRows(int size) {
        resultMaxMemoryRows = size;
    }
    public void setStrictNames(boolean mode) {
        sqlEnforceNames = mode;
    }
    public void setStrictColumnSize(boolean mode) {
        sqlEnforceSize = mode;
    }
    public void setStrictReferences(boolean mode) {
        sqlEnforceRefs = mode;
    }
    public void setStrictTypes(boolean mode) {
        sqlEnforceTypes = mode;
    }
    public void setStrictTDCD(boolean mode) {
        sqlEnforceTDCD = mode;
    }
    public void setStrictTDCU(boolean mode) {
        sqlEnforceTDCU = mode;
    }
    public void setTranslateTTI(boolean mode) {
        sqlTranslateTTI = mode;
    }
    public void setNullsFirst(boolean mode) {
        sqlNullsFirst = mode;
    }
    public void setConcatNulls(boolean mode) {
        sqlConcatNulls = mode;
    }
    public void setUniqueNulls(boolean mode) {
        sqlUniqueNulls = mode;
    }
    public void setConvertTrunc(boolean mode) {
        sqlConvertTruncate = mode;
    }
    public void setDoubleNaN(boolean mode) {
        sqlDoubleNaN = mode;
    }
    public void setAvgScale(int scale) {
        sqlAvgScale = scale;
    }
    public void setLongVarIsLob(boolean mode) {
        sqlLongvarIsLob = mode;
    }
    public void setSyntaxDb2(boolean mode) {
        sqlSyntaxDb2 = mode;
    }
    public void setSyntaxMss(boolean mode) {
        sqlSyntaxMss = mode;
    }
    public void setSyntaxMys(boolean mode) {
        sqlSyntaxMys = mode;
    }
    public void setSyntaxOra(boolean mode) {
        sqlSyntaxOra = mode;
    }
    public void setSyntaxPgs(boolean mode) {
        sqlSyntaxPgs = mode;
    }
    protected void finalize() {
        if (getState() != DATABASE_ONLINE) {
            return;
        }
        try {
            close(CLOSEMODE_IMMEDIATELY);
        } catch (HsqlException e) {    
        }
    }
    void closeIfLast() {
        if (shutdownOnNoConnection && sessionManager.isEmpty()
                && dbState == this.DATABASE_ONLINE) {
            try {
                close(CLOSEMODE_NORMAL);
            } catch (HsqlException e) {}
        }
    }
    public void close(int closemode) {
        HsqlException he = null;
        synchronized (this) {
            if (getState() != DATABASE_ONLINE) {
                return;
            }
            setState(DATABASE_CLOSING);
        }
        sessionManager.closeAllSessions();
        if (filesReadOnly) {
            closemode = CLOSEMODE_IMMEDIATELY;
        }
        logger.closePersistence(closemode);
        lobManager.close();
        sessionManager.close();
        try {
            if (closemode == CLOSEMODE_COMPACT) {
                clearStructures();
                reopen();
                setState(DATABASE_CLOSING);
                logger.closePersistence(CLOSEMODE_NORMAL);
                lobManager.close();
            }
        } catch (Throwable t) {
            if (t instanceof HsqlException) {
                he = (HsqlException) t;
            } else {
                he = Error.error(ErrorCode.GENERAL_ERROR, t);
            }
        }
        checkpointRunner.stop();
        logger.releaseLock();
        setState(DATABASE_SHUTDOWN);
        clearStructures();
        DatabaseManager.removeDatabase(this);
        FrameworkLogger.clearLoggers("hsqldb.db." + getUniqueName());
        if (he != null) {
            throw he;
        }
    }
    private void setState(int state) {
        dbState = state;
    }
    int getState() {
        return dbState;
    }
    String getStateString() {
        int state = getState();
        switch (state) {
            case DATABASE_CLOSING :
                return "DATABASE_CLOSING";
            case DATABASE_ONLINE :
                return "DATABASE_ONLINE";
            case DATABASE_OPENING :
                return "DATABASE_OPENING";
            case DATABASE_SHUTDOWN :
                return "DATABASE_SHUTDOWN";
            default :
                return "UNKNOWN";
        }
    }
    public String[] getSettingsSQL() {
        HsqlArrayList list = new HsqlArrayList();
        if (!getCatalogName().name.equals(
                HsqlNameManager.DEFAULT_CATALOG_NAME)) {
            String name = getCatalogName().statementName;
            list.add("ALTER CATALOG PUBLIC RENAME TO " + name);
        }
        if (!collation.isDefaultCollation()) {
            String name = collation.getName().statementName;
            list.add("SET DATABASE COLLATION " + name);
        }
        HashMappedList lobTables =
            schemaManager.getTables(SqlInvariants.LOBS_SCHEMA);
        for (int i = 0; i < lobTables.size(); i++) {
            Table table = (Table) lobTables.get(i);
            if (table.isCached()) {
                StringBuffer sb = new StringBuffer();
                sb.append(Tokens.T_SET).append(' ').append(Tokens.T_TABLE);
                sb.append(' ');
                sb.append(table.getName().getSchemaQualifiedStatementName());
                sb.append(' ').append(Tokens.T_TYPE).append(' ');
                sb.append(Tokens.T_CACHED);
                list.add(sb.toString());
            }
        }
        String[] array = new String[list.size()];
        list.toArray(array);
        return array;
    }
    public Result getScript(boolean indexRoots) {
        Result r = Result.newSingleColumnResult("COMMAND");
        String[] list = logger.getPropertiesSQL();
        addRows(r, list);
        list = getSettingsSQL();
        addRows(r, list);
        list = getGranteeManager().getSQL();
        addRows(r, list);
        list = schemaManager.getSQLArray();
        addRows(r, list);
        list = schemaManager.getCommentsArray();
        addRows(r, list);
        if (indexRoots) {
            list = schemaManager.getIndexRootsSQL();
            addRows(r, list);
        }
        list = schemaManager.getTablePropsSQL(!indexRoots);
        addRows(r, list);
        list = getUserManager().getAuthenticationSQL();
        addRows(r, list);
        list = getUserManager().getInitialSchemaSQL();
        addRows(r, list);
        list = getGranteeManager().getRightstSQL();
        addRows(r, list);
        return r;
    }
    private static void addRows(Result r, String[] sql) {
        if (sql == null) {
            return;
        }
        for (int i = 0; i < sql.length; i++) {
            String[] s = new String[1];
            s[0] = sql[i];
            r.initialiseNavigator().add(s);
        }
    }
    public String getURI() {
        return databaseType + canonicalPath;
    }
    public String getCanonicalPath() {
        return canonicalPath;
    }
    public HsqlProperties getURLProperties() {
        return urlProperties;
    }
    class CheckpointRunner implements Runnable {
        private volatile boolean waiting;
        private Object           timerTask;
        public void run() {
            try {
                Session sysSession = sessionManager.newSysSession();
                Statement checkpoint =
                    ParserCommand.getAutoCheckpointStatement(Database.this);
                sysSession.executeCompiledStatement(
                    checkpoint, ValuePool.emptyObjectArray);
                sysSession.close();
                waiting = false;
            } catch (Exception e) {
            }
        }
        public void start() {
            if (!logger.isLogged()) {
                return;
            }
            synchronized (this) {
                if (waiting) {
                    return;
                }
                waiting = true;
            }
            timerTask = DatabaseManager.getTimer().scheduleAfter(0, this);
        }
        public void stop() {
            HsqlTimer.cancel(timerTask);
            timerTask = null;
            waiting   = false;
        }
    }
}