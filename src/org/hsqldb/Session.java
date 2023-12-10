package org.hsqldb;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.Random;
import java.util.TimeZone;
import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.jdbc.JDBCConnection;
import org.hsqldb.jdbc.JDBCDriver;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.lib.CountUpDownLatch;
import org.hsqldb.lib.HashMap;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.HsqlDeque;
import org.hsqldb.lib.Iterator;
import org.hsqldb.lib.OrderedHashSet;
import org.hsqldb.lib.SimpleLog;
import org.hsqldb.lib.java.JavaSystem;
import org.hsqldb.navigator.RowSetNavigator;
import org.hsqldb.navigator.RowSetNavigatorClient;
import org.hsqldb.persist.HsqlDatabaseProperties;
import org.hsqldb.persist.HsqlProperties;
import org.hsqldb.persist.PersistentStore;
import org.hsqldb.result.Result;
import org.hsqldb.result.ResultConstants;
import org.hsqldb.result.ResultLob;
import org.hsqldb.result.ResultProperties;
import org.hsqldb.rights.Grantee;
import org.hsqldb.rights.User;
import org.hsqldb.store.ValuePool;
import org.hsqldb.types.BlobDataID;
import org.hsqldb.types.ClobDataID;
import org.hsqldb.types.TimeData;
import org.hsqldb.types.TimestampData;
import org.hsqldb.types.Type;
import org.hsqldb.types.Type.TypedComparator;
public class Session implements SessionInterface {
    private volatile boolean isClosed;
    public Database    database;
    private final User sessionUser;
    private User       user;
    private Grantee    role;
    public boolean          isReadOnlyDefault;
    int isolationLevelDefault = SessionInterface.TX_READ_COMMITTED;
    int isolationLevel        = SessionInterface.TX_READ_COMMITTED;
    boolean                 isReadOnlyIsolation;
    int                     actionIndex;
    long                    actionTimestamp;
    long                    transactionTimestamp;
    long                    transactionEndTimestamp;
    boolean                 txConflictRollback;
    boolean                 isPreTransaction;
    boolean                 isTransaction;
    boolean                 isBatch;
    volatile boolean        abortTransaction;
    volatile boolean        redoAction;
    HsqlArrayList           rowActionList;
    volatile boolean        tempUnlocked;
    public OrderedHashSet   waitedSessions;
    public OrderedHashSet   waitingSessions;
    OrderedHashSet          tempSet;
    public CountUpDownLatch latch = new CountUpDownLatch();
    Statement               lockStatement;
    final String       zoneString;
    final int          sessionTimeZoneSeconds;
    int                timeZoneSeconds;
    boolean            isNetwork;
    private int        sessionMaxRows;
    private final long sessionId;
    int                sessionTxId = -1;
    private boolean    script;
    boolean            ignoreCase;
    private JDBCConnection intConnection;
    private JDBCConnection extConnection;
    public HsqlName currentSchema;
    public HsqlName loggedSchema;
    ParserCommand         parser;
    boolean               isProcessingScript;
    boolean               isProcessingLog;
    public SessionContext sessionContext;
    int                   resultMaxMemoryRows;
    public SessionData sessionData;
    public StatementManager statementManager;
    Session(Database db, User user, boolean autocommit, boolean readonly,
            long id, String zoneString, int timeZoneSeconds) {
        sessionId                   = id;
        database                    = db;
        this.user                   = user;
        this.sessionUser            = user;
        this.zoneString             = zoneString;
        this.sessionTimeZoneSeconds = timeZoneSeconds;
        this.timeZoneSeconds        = timeZoneSeconds;
        rowActionList               = new HsqlArrayList(32, true);
        waitedSessions              = new OrderedHashSet();
        waitingSessions             = new OrderedHashSet();
        tempSet                     = new OrderedHashSet();
        isolationLevelDefault       = database.defaultIsolationLevel;
        isolationLevel              = isolationLevelDefault;
        txConflictRollback          = database.txConflictRollback;
        isReadOnlyDefault           = readonly;
        isReadOnlyIsolation = isolationLevel
                              == SessionInterface.TX_READ_UNCOMMITTED;
        sessionContext              = new SessionContext(this);
        sessionContext.isAutoCommit = autocommit ? Boolean.TRUE
                                                 : Boolean.FALSE;
        sessionContext.isReadOnly   = isReadOnlyDefault ? Boolean.TRUE
                                                        : Boolean.FALSE;
        parser                      = new ParserCommand(this, new Scanner());
        setResultMemoryRowCount(database.getResultMaxMemoryRows());
        resetSchema();
        sessionData      = new SessionData(database, this);
        statementManager = new StatementManager(database);
    }
    void resetSchema() {
        loggedSchema  = null;
        currentSchema = user.getInitialOrDefaultSchema();
    }
    public long getId() {
        return sessionId;
    }
    public synchronized void close() {
        if (isClosed) {
            return;
        }
        rollback(false);
        try {
            database.logger.writeOtherStatement(this, Tokens.T_DISCONNECT);
        } catch (HsqlException e) {}
        sessionData.closeAllNavigators();
        sessionData.persistentStoreCollection.clearAllTables();
        statementManager.reset();
        database.sessionManager.removeSession(this);
        database.closeIfLast();
        rowActionList.clear();
        database                    = null;
        user                        = null;
        sessionContext.savepoints   = null;
        sessionContext.lastIdentity = null;
        intConnection               = null;
        isClosed                    = true;
    }
    public boolean isClosed() {
        return isClosed;
    }
    public synchronized void setIsolationDefault(int level) {
        if (level == SessionInterface.TX_READ_UNCOMMITTED) {
            level = SessionInterface.TX_READ_COMMITTED;
        }
        if (level == isolationLevelDefault) {
            return;
        }
        isolationLevelDefault = level;
        if (!isInMidTransaction()) {
            isolationLevel = isolationLevelDefault;
            isReadOnlyIsolation = level
                                  == SessionInterface.TX_READ_UNCOMMITTED;
        }
    }
    public void setIsolation(int level) {
        if (isInMidTransaction()) {
            throw Error.error(ErrorCode.X_25001);
        }
        if (level == SessionInterface.TX_READ_UNCOMMITTED) {
            level = SessionInterface.TX_READ_COMMITTED;
        }
        if (isolationLevel != level) {
            isolationLevel = level;
            isReadOnlyIsolation = level
                                  == SessionInterface.TX_READ_UNCOMMITTED;
        }
    }
    public synchronized int getIsolation() {
        return isolationLevel;
    }
    void setLastIdentity(Number i) {
        sessionContext.lastIdentity = i;
    }
    public Number getLastIdentity() {
        return sessionContext.lastIdentity;
    }
    public Database getDatabase() {
        return database;
    }
    public String getUsername() {
        return user.getName().getNameString();
    }
    public User getUser() {
        return (User) user;
    }
    public Grantee getGrantee() {
        return user;
    }
    public Grantee getRole() {
        return role;
    }
    public void setUser(User user) {
        this.user = user;
    }
    public void setRole(Grantee role) {
        this.role = role;
    }
    int getMaxRows() {
        return sessionContext.currentMaxRows;
    }
    void setSQLMaxRows(int rows) {
        sessionMaxRows = rows;
    }
    void checkAdmin() {
        user.checkAdmin();
    }
    void checkReadWrite() {
        if (sessionContext.isReadOnly.booleanValue() || isReadOnlyIsolation) {
            throw Error.error(ErrorCode.X_25006);
        }
    }
    void checkDDLWrite() {
        if (isProcessingScript || isProcessingLog) {
            return;
        }
        checkReadWrite();
    }
    public long getActionTimestamp() {
        return actionTimestamp;
    }
    public void addDeleteAction(Table table, Row row, int[] colMap) {
        if (abortTransaction) {
        }
        database.txManager.addDeleteAction(this, table, row, colMap);
    }
    void addInsertAction(Table table, PersistentStore store, Row row,
                         int[] changedColumns) {
        database.txManager.addInsertAction(this, table, store, row,
                                           changedColumns);
        if (abortTransaction) {
        }
    }
    public synchronized void setAutoCommit(boolean autocommit) {
        if (isClosed) {
            return;
        }
        if (sessionContext.isAutoCommit.booleanValue() != autocommit) {
            commit(false);
            sessionContext.isAutoCommit = ValuePool.getBoolean(autocommit);
        }
    }
    public void beginAction(Statement cs) {
        actionIndex = rowActionList.size();
        database.txManager.beginAction(this, cs);
        database.txManager.beginActionResume(this);
    }
    public void endAction(Result result) {
        sessionData.persistentStoreCollection.clearStatementTables();
        if (result.mode == ResultConstants.ERROR) {
            sessionData.persistentStoreCollection.clearResultTables(
                actionTimestamp);
            database.txManager.rollbackAction(this);
        } else {
            sessionContext
                .diagnosticsVariables[ExpressionColumn.idx_row_count] =
                    result.mode == ResultConstants.UPDATECOUNT
                    ? Integer.valueOf(result.getUpdateCount())
                    : ValuePool.INTEGER_0;
            database.txManager.completeActions(this);
        }
    }
    public boolean hasLocks(Statement statement) {
        if (lockStatement == statement) {
            if (isolationLevel == SessionInterface.TX_REPEATABLE_READ
                    || isolationLevel == SessionInterface.TX_SERIALIZABLE) {
                return true;
            }
            if (statement.getTableNamesForRead().length == 0) {
                return true;
            }
        }
        return false;
    }
    public void startTransaction() {
        database.txManager.beginTransaction(this);
    }
    public synchronized void startPhasedTransaction() {}
    public synchronized void prepareCommit() {
        if (isClosed) {
            throw Error.error(ErrorCode.X_08003);
        }
        if (!database.txManager.prepareCommitActions(this)) {
            rollback(false);
            throw Error.error(ErrorCode.X_40001);
        }
    }
    public synchronized void commit(boolean chain) {
        if (isClosed) {
            return;
        }
        if (sessionContext.depth > 0) {
            return;
        }
        if (!isTransaction && rowActionList.size() == 0) {
            sessionContext.isReadOnly = isReadOnlyDefault ? Boolean.TRUE
                                                          : Boolean.FALSE;
            setIsolation(isolationLevelDefault);
            return;
        }
        if (!database.txManager.commitTransaction(this)) {
            rollback(chain);
            throw Error.error(ErrorCode.X_40001);
        }
        endTransaction(true, chain);
        if (database != null && !sessionUser.isSystem()
                && database.logger.needsCheckpointReset()) {
            database.checkpointRunner.start();
        }
    }
    public synchronized void rollback(boolean chain) {
        if (isClosed) {
            return;
        }
        if (sessionContext.depth > 0) {
            return;
        }
        database.txManager.rollback(this);
        endTransaction(false, chain);
    }
    private void endTransaction(boolean commit, boolean chain) {
        sessionContext.savepoints.clear();
        sessionContext.savepointTimestamps.clear();
        rowActionList.clear();
        sessionData.persistentStoreCollection.clearTransactionTables();
        sessionData.closeAllTransactionNavigators();
        sessionData.clearNewLobIDs();
        lockStatement = null;
        logSequences();
        if (!chain) {
            sessionContext.isReadOnly = isReadOnlyDefault ? Boolean.TRUE
                                                          : Boolean.FALSE;
            setIsolation(isolationLevelDefault);
        }
        Statement endTX = commit ? StatementSession.commitNoChainStatement
                                 : StatementSession.rollbackNoChainStatement;
        if (database.logger.getSqlEventLogLevel() > 0) {
            database.logger.logStatementEvent(this, endTX, null,
                                              SimpleLog.LOG_NORMAL);
        }
    }
    public synchronized void resetSession() {
        if (isClosed) {
            return;
        }
        rollback(false);
        sessionData.closeAllNavigators();
        sessionData.persistentStoreCollection.clearAllTables();
        sessionData.clearNewLobIDs();
        statementManager.reset();
        sessionContext.lastIdentity = ValuePool.INTEGER_0;
        setResultMemoryRowCount(database.getResultMaxMemoryRows());
        user = sessionUser;
        resetSchema();
        setZoneSeconds(sessionTimeZoneSeconds);
        sessionMaxRows = 0;
        ignoreCase     = false;
        setIsolation(isolationLevelDefault);
        txConflictRollback = database.txConflictRollback;
    }
    public synchronized void savepoint(String name) {
        int index = sessionContext.savepoints.getIndex(name);
        if (index != -1) {
            sessionContext.savepoints.remove(name);
            sessionContext.savepointTimestamps.remove(index);
        }
        sessionContext.savepoints.add(name,
                                      ValuePool.getInt(rowActionList.size()));
        sessionContext.savepointTimestamps.addLast(actionTimestamp);
    }
    public synchronized void rollbackToSavepoint(String name) {
        if (isClosed) {
            return;
        }
        int index = sessionContext.savepoints.getIndex(name);
        if (index < 0) {
            throw Error.error(ErrorCode.X_3B001, name);
        }
        database.txManager.rollbackSavepoint(this, index);
    }
    public synchronized void rollbackToSavepoint() {
        if (isClosed) {
            return;
        }
        String name = (String) sessionContext.savepoints.getKey(0);
        database.txManager.rollbackSavepoint(this, 0);
    }
    public synchronized void releaseSavepoint(String name) {
        int index = sessionContext.savepoints.getIndex(name);
        if (index < 0) {
            throw Error.error(ErrorCode.X_3B001, name);
        }
        while (sessionContext.savepoints.size() > index) {
            sessionContext.savepoints.remove(sessionContext.savepoints.size()
                                             - 1);
            sessionContext.savepointTimestamps.removeLast();
        }
    }
    public boolean isInMidTransaction() {
        return isTransaction;
    }
    public void setNoSQL() {
        sessionContext.noSQL = Boolean.TRUE;
    }
    public void setIgnoreCase(boolean mode) {
        ignoreCase = mode;
    }
    public boolean isIgnorecase() {
        return ignoreCase;
    }
    public void setReadOnly(boolean readonly) {
        if (!readonly && database.databaseReadOnly) {
            throw Error.error(ErrorCode.DATABASE_IS_READONLY);
        }
        if (isInMidTransaction()) {
            throw Error.error(ErrorCode.X_25001);
        }
        sessionContext.isReadOnly = readonly ? Boolean.TRUE
                                             : Boolean.FALSE;
    }
    public synchronized void setReadOnlyDefault(boolean readonly) {
        if (!readonly && database.databaseReadOnly) {
            throw Error.error(ErrorCode.DATABASE_IS_READONLY);
        }
        isReadOnlyDefault = readonly;
        if (!isInMidTransaction()) {
            sessionContext.isReadOnly = isReadOnlyDefault ? Boolean.TRUE
                                                          : Boolean.FALSE;
        }
    }
    public boolean isReadOnly() {
        return sessionContext.isReadOnly.booleanValue() || isReadOnlyIsolation;
    }
    public synchronized boolean isReadOnlyDefault() {
        return isReadOnlyDefault;
    }
    public synchronized boolean isAutoCommit() {
        return sessionContext.isAutoCommit.booleanValue();
    }
    public synchronized int getStreamBlockSize() {
        return lobStreamBlockSize;
    }
    void setScripting(boolean script) {
        this.script = script;
    }
    boolean isScripting() {
        return script;
    }
    JDBCConnection getInternalConnection() {
        if (intConnection == null) {
            intConnection = new JDBCConnection(this);
        }
        JDBCDriver.driverInstance.threadConnection.set(intConnection);
        return intConnection;
    }
    void releaseInternalConnection() {
        if (sessionContext.depth == 0) {
            JDBCDriver.driverInstance.threadConnection.set(null);
        }
    }
    public JDBCConnection getJDBCConnection() {
        return extConnection;
    }
    public void setJDBCConnection(JDBCConnection connection) {
        extConnection = connection;
    }
    public String getDatabaseUniqueName() {
        return database.getUniqueName();
    }
    private final long connectTime = System.currentTimeMillis();
    public boolean isAdmin() {
        return user.isAdmin();
    }
    public long getConnectTime() {
        return connectTime;
    }
    public int getTransactionSize() {
        return rowActionList.size();
    }
    public long getTransactionTimestamp() {
        return transactionTimestamp;
    }
    public Statement compileStatement(String sql, int props) {
        parser.reset(sql);
        Statement cs = parser.compileStatement(props);
        return cs;
    }
    public Statement compileStatement(String sql) {
        parser.reset(sql);
        Statement cs =
            parser.compileStatement(ResultProperties.defaultPropsValue);
        cs.setCompileTimestamp(Long.MAX_VALUE);
        return cs;
    }
    public synchronized Result execute(Result cmd) {
        if (isClosed) {
            return Result.newErrorResult(Error.error(ErrorCode.X_08503));
        }
        sessionContext.currentMaxRows = 0;
        isBatch                       = false;
        JavaSystem.gc();
        switch (cmd.mode) {
            case ResultConstants.LARGE_OBJECT_OP : {
                return performLOBOperation((ResultLob) cmd);
            }
            case ResultConstants.EXECUTE : {
                int maxRows = cmd.getUpdateCount();
                if (maxRows == -1) {
                    sessionContext.currentMaxRows = 0;
                } else {
                    sessionContext.currentMaxRows = maxRows;
                }
                Statement cs = cmd.statement;
                if (cs == null
                        || cs.compileTimestamp
                           < database.schemaManager.schemaChangeTimestamp) {
                    long csid = cmd.getStatementID();
                    cs = statementManager.getStatement(this, csid);
                    cmd.setStatement(cs);
                    if (cs == null) {
                        return Result.newErrorResult(
                            Error.error(ErrorCode.X_07502));
                    }
                }
                Object[] pvals  = (Object[]) cmd.valueData;
                Result   result = executeCompiledStatement(cs, pvals);
                result = performPostExecute(cmd, result);
                return result;
            }
            case ResultConstants.BATCHEXECUTE : {
                isBatch = true;
                Result result = executeCompiledBatchStatement(cmd);
                result = performPostExecute(cmd, result);
                return result;
            }
            case ResultConstants.EXECDIRECT : {
                Result result = executeDirectStatement(cmd);
                result = performPostExecute(cmd, result);
                return result;
            }
            case ResultConstants.BATCHEXECDIRECT : {
                isBatch = true;
                Result result = executeDirectBatchStatement(cmd);
                result = performPostExecute(cmd, result);
                return result;
            }
            case ResultConstants.PREPARE : {
                Statement cs;
                try {
                    cs = statementManager.compile(this, cmd);
                } catch (Throwable t) {
                    String errorString = cmd.getMainString();
                    if (database.getProperties().getErrorLevel()
                            == HsqlDatabaseProperties.NO_MESSAGE) {
                        errorString = null;
                    }
                    return Result.newErrorResult(t, errorString);
                }
                Result result = Result.newPrepareResponse(cs);
                if (cs.getType() == StatementTypes.SELECT_CURSOR
                        || cs.getType() == StatementTypes.CALL) {
                    sessionData.setResultSetProperties(cmd, result);
                }
                result = performPostExecute(cmd, result);
                return result;
            }
            case ResultConstants.CLOSE_RESULT : {
                closeNavigator(cmd.getResultId());
                return Result.updateZeroResult;
            }
            case ResultConstants.UPDATE_RESULT : {
                Result result = this.executeResultUpdate(cmd);
                result = performPostExecute(cmd, result);
                return result;
            }
            case ResultConstants.FREESTMT : {
                statementManager.freeStatement(cmd.getStatementID());
                return Result.updateZeroResult;
            }
            case ResultConstants.GETSESSIONATTR : {
                int id = cmd.getStatementType();
                return getAttributesResult(id);
            }
            case ResultConstants.SETSESSIONATTR : {
                return setAttributes(cmd);
            }
            case ResultConstants.ENDTRAN : {
                switch (cmd.getActionType()) {
                    case ResultConstants.TX_COMMIT :
                        try {
                            commit(false);
                        } catch (Throwable t) {
                            return Result.newErrorResult(t);
                        }
                        break;
                    case ResultConstants.TX_COMMIT_AND_CHAIN :
                        try {
                            commit(true);
                        } catch (Throwable t) {
                            return Result.newErrorResult(t);
                        }
                        break;
                    case ResultConstants.TX_ROLLBACK :
                        rollback(false);
                        break;
                    case ResultConstants.TX_ROLLBACK_AND_CHAIN :
                        rollback(true);
                        break;
                    case ResultConstants.TX_SAVEPOINT_NAME_RELEASE :
                        try {
                            String name = cmd.getMainString();
                            releaseSavepoint(name);
                        } catch (Throwable t) {
                            return Result.newErrorResult(t);
                        }
                        break;
                    case ResultConstants.TX_SAVEPOINT_NAME_ROLLBACK :
                        try {
                            rollbackToSavepoint(cmd.getMainString());
                        } catch (Throwable t) {
                            return Result.newErrorResult(t);
                        }
                        break;
                }
                return Result.updateZeroResult;
            }
            case ResultConstants.SETCONNECTATTR : {
                switch (cmd.getConnectionAttrType()) {
                    case ResultConstants.SQL_ATTR_SAVEPOINT_NAME :
                        try {
                            savepoint(cmd.getMainString());
                        } catch (Throwable t) {
                            return Result.newErrorResult(t);
                        }
                }
                return Result.updateZeroResult;
            }
            case ResultConstants.REQUESTDATA : {
                return sessionData.getDataResultSlice(cmd.getResultId(),
                                                      cmd.getUpdateCount(),
                                                      cmd.getFetchSize());
            }
            case ResultConstants.DISCONNECT : {
                close();
                return Result.updateZeroResult;
            }
            default : {
                return Result.newErrorResult(
                    Error.runtimeError(ErrorCode.U_S0500, "Session"));
            }
        }
    }
    private Result performPostExecute(Result command, Result result) {
        if (result.mode == ResultConstants.DATA) {
            result = sessionData.getDataResultHead(command, result, isNetwork);
        }
        if (sqlWarnings != null && sqlWarnings.size() > 0) {
            if (result.mode == ResultConstants.UPDATECOUNT) {
                result = new Result(ResultConstants.UPDATECOUNT,
                                    result.getUpdateCount());
            }
            HsqlException[] warnings = getAndClearWarnings();
            result.addWarnings(warnings);
        }
        return result;
    }
    public RowSetNavigatorClient getRows(long navigatorId, int offset,
                                         int blockSize) {
        return sessionData.getRowSetSlice(navigatorId, offset, blockSize);
    }
    public synchronized void closeNavigator(long id) {
        sessionData.closeNavigator(id);
    }
    public Result executeDirectStatement(Result cmd) {
        String        sql = cmd.getMainString();
        HsqlArrayList list;
        int           maxRows = cmd.getUpdateCount();
        if (maxRows == -1) {
            sessionContext.currentMaxRows = 0;
        } else if (sessionMaxRows == 0) {
            sessionContext.currentMaxRows = maxRows;
        } else {
            sessionContext.currentMaxRows = sessionMaxRows;
            sessionMaxRows                = 0;
        }
        try {
            list = parser.compileStatements(sql, cmd);
        } catch (Exception e) {
            return Result.newErrorResult(e);
        }
        Result result = null;
        for (int i = 0; i < list.size(); i++) {
            Statement cs = (Statement) list.get(i);
            cs.setGeneratedColumnInfo(cmd.getGeneratedResultType(),
                                      cmd.getGeneratedResultMetaData());
            result = executeCompiledStatement(cs, ValuePool.emptyObjectArray);
            if (result.mode == ResultConstants.ERROR) {
                break;
            }
        }
        return result;
    }
    public Result executeDirectStatement(String sql) {
        try {
            Statement cs = compileStatement(sql);
            Result result = executeCompiledStatement(cs,
                ValuePool.emptyObjectArray);
            return result;
        } catch (HsqlException e) {
            return Result.newErrorResult(e);
        }
    }
    public Result executeCompiledStatement(Statement cs, Object[] pvals) {
        Result r;
        if (abortTransaction) {
            rollback(false);
            return Result.newErrorResult(Error.error(ErrorCode.X_40001));
        }
        if (sessionContext.depth > 0) {
            if (sessionContext.noSQL.booleanValue()
                    || cs.isAutoCommitStatement()) {
                return Result.newErrorResult(Error.error(ErrorCode.X_46000));
            }
        }
        if (cs.isAutoCommitStatement()) {
            if (isReadOnly()) {
                return Result.newErrorResult(Error.error(ErrorCode.X_25006));
            }
            try {
                commit(false);
            } catch (HsqlException e) {
                database.logger.logInfoEvent("Exception at commit");
            }
        }
        sessionContext.currentStatement = cs;
        boolean isTX = cs.isTransactionStatement();
        if (!isTX) {
            if (database.logger.getSqlEventLogLevel() > 0) {
                sessionContext.setDynamicArguments(pvals);
            }
            if (database.logger.getSqlEventLogLevel() > 0) {
                database.logger.logStatementEvent(this, cs, pvals,
                                                  SimpleLog.LOG_DETAIL);
            }
            r                               = cs.execute(this);
            sessionContext.currentStatement = null;
            return r;
        }
        while (true) {
            actionIndex = rowActionList.size();
            database.txManager.beginAction(this, cs);
            cs = sessionContext.currentStatement;
            if (cs == null) {
                return Result.newErrorResult(Error.error(ErrorCode.X_07502));
            }
            if (abortTransaction) {
                rollback(false);
                sessionContext.currentStatement = null;
                return Result.newErrorResult(Error.error(ErrorCode.X_40001));
            }
            try {
                latch.await();
            } catch (InterruptedException e) {
                abortTransaction = true;
            }
            if (abortTransaction) {
                rollback(false);
                sessionContext.currentStatement = null;
                return Result.newErrorResult(Error.error(ErrorCode.X_40001));
            }
            database.txManager.beginActionResume(this);
            sessionContext.setDynamicArguments(pvals);
            if (database.logger.getSqlEventLogLevel() > 0) {
                database.logger.logStatementEvent(this, cs, pvals,
                                                  SimpleLog.LOG_DETAIL);
            }
            r             = cs.execute(this);
            lockStatement = sessionContext.currentStatement;
            endAction(r);
            if (abortTransaction) {
                rollback(false);
                sessionContext.currentStatement = null;
                return Result.newErrorResult(Error.error(r.getException(),
                        ErrorCode.X_40001, null));
            }
            if (redoAction) {
                redoAction = false;
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    abortTransaction = true;
                }
            } else {
                break;
            }
        }
        if (sessionContext.depth == 0
                && (sessionContext.isAutoCommit.booleanValue()
                    || cs.isAutoCommitStatement())) {
            try {
                if (r.mode == ResultConstants.ERROR) {
                    rollback(false);
                } else {
                    commit(false);
                }
            } catch (Exception e) {
                sessionContext.currentStatement = null;
                return Result.newErrorResult(Error.error(ErrorCode.X_40001,
                        e));
            }
        }
        sessionContext.currentStatement = null;
        return r;
    }
    private Result executeCompiledBatchStatement(Result cmd) {
        long      csid;
        Statement cs;
        int[]     updateCounts;
        int       count;
        cs = cmd.statement;
        if (cs == null
                || cs.compileTimestamp
                   < database.schemaManager.schemaChangeTimestamp) {
            csid = cmd.getStatementID();
            cs   = statementManager.getStatement(this, csid);
            if (cs == null) {
                return Result.newErrorResult(Error.error(ErrorCode.X_07502));
            }
        }
        count = 0;
        RowSetNavigator nav = cmd.initialiseNavigator();
        updateCounts = new int[nav.getSize()];
        Result generatedResult = null;
        if (cs.hasGeneratedColumns()) {
            generatedResult =
                Result.newGeneratedDataResult(cs.generatedResultMetaData());
        }
        Result error = null;
        while (nav.hasNext()) {
            Object[] pvals = (Object[]) nav.getNext();
            Result   in    = executeCompiledStatement(cs, pvals);
            if (in.isUpdateCount()) {
                if (cs.hasGeneratedColumns()) {
                    RowSetNavigator navgen =
                        in.getChainedResult().getNavigator();
                    while (navgen.hasNext()) {
                        Object[] generatedRow = navgen.getNext();
                        generatedResult.getNavigator().add(generatedRow);
                    }
                }
                updateCounts[count++] = in.getUpdateCount();
            } else if (in.isData()) {
                updateCounts[count++] = ResultConstants.SUCCESS_NO_INFO;
            } else if (in.mode == ResultConstants.ERROR) {
                updateCounts = ArrayUtil.arraySlice(updateCounts, 0, count);
                error        = in;
                break;
            } else {
                throw Error.runtimeError(ErrorCode.U_S0500, "Session");
            }
        }
        return Result.newBatchedExecuteResponse(updateCounts, generatedResult,
                error);
    }
    private Result executeDirectBatchStatement(Result cmd) {
        int[] updateCounts;
        int   count;
        count = 0;
        RowSetNavigator nav = cmd.initialiseNavigator();
        updateCounts = new int[nav.getSize()];
        Result error = null;
        while (nav.hasNext()) {
            Result   in;
            Object[] data = (Object[]) nav.getNext();
            String   sql  = (String) data[0];
            try {
                in = executeDirectStatement(sql);
            } catch (Throwable t) {
                in = Result.newErrorResult(t);
            }
            if (in.isUpdateCount()) {
                updateCounts[count++] = in.getUpdateCount();
            } else if (in.isData()) {
                updateCounts[count++] = ResultConstants.SUCCESS_NO_INFO;
            } else if (in.mode == ResultConstants.ERROR) {
                updateCounts = ArrayUtil.arraySlice(updateCounts, 0, count);
                error        = in;
                break;
            } else {
                throw Error.runtimeError(ErrorCode.U_S0500, "Session");
            }
        }
        return Result.newBatchedExecuteResponse(updateCounts, null, error);
    }
    private Result executeResultUpdate(Result cmd) {
        long   id         = cmd.getResultId();
        int    actionType = cmd.getActionType();
        Result result     = sessionData.getDataResult(id);
        if (result == null) {
            return Result.newErrorResult(Error.error(ErrorCode.X_24501));
        }
        Object[]        pvals     = (Object[]) cmd.valueData;
        Type[]          types     = cmd.metaData.columnTypes;
        StatementQuery  statement = (StatementQuery) result.getStatement();
        QueryExpression qe        = statement.queryExpression;
        Table           baseTable = qe.getBaseTable();
        int[]           columnMap = qe.getBaseTableColumnMap();
        sessionContext.rowUpdateStatement.setRowActionProperties(result,
                actionType, baseTable, types, columnMap);
        Result resultOut =
            executeCompiledStatement(sessionContext.rowUpdateStatement, pvals);
        return resultOut;
    }
    long                  currentDateSCN;
    long                  currentTimestampSCN;
    long                  currentMillis;
    private TimestampData currentDate;
    private TimestampData currentTimestamp;
    private TimestampData localTimestamp;
    private TimeData      currentTime;
    private TimeData      localTime;
    public synchronized TimestampData getCurrentDate() {
        resetCurrentTimestamp();
        if (currentDate == null) {
            currentDate = (TimestampData) Type.SQL_DATE.getValue(currentMillis
                    / 1000, 0, getZoneSeconds());
        }
        return currentDate;
    }
    synchronized TimeData getCurrentTime(boolean withZone) {
        resetCurrentTimestamp();
        if (withZone) {
            if (currentTime == null) {
                int seconds =
                    (int) (HsqlDateTime.getNormalisedTime(currentMillis))
                    / 1000;
                int nanos = (int) (currentMillis % 1000) * 1000000;
                currentTime = new TimeData(seconds, nanos, getZoneSeconds());
            }
            return currentTime;
        } else {
            if (localTime == null) {
                int seconds =
                    (int) (HsqlDateTime.getNormalisedTime(
                        currentMillis + getZoneSeconds() * 1000)) / 1000;
                int nanos = (int) (currentMillis % 1000) * 1000000;
                localTime = new TimeData(seconds, nanos, 0);
            }
            return localTime;
        }
    }
    synchronized TimestampData getCurrentTimestamp(boolean withZone) {
        resetCurrentTimestamp();
        if (withZone) {
            if (currentTimestamp == null) {
                int nanos = (int) (currentMillis % 1000) * 1000000;
                currentTimestamp = new TimestampData((currentMillis / 1000),
                                                     nanos, getZoneSeconds());
            }
            return currentTimestamp;
        } else {
            if (localTimestamp == null) {
                int nanos = (int) (currentMillis % 1000) * 1000000;
                localTimestamp = new TimestampData(currentMillis / 1000
                                                   + getZoneSeconds(), nanos,
                                                       0);
            }
            return localTimestamp;
        }
    }
    private void resetCurrentTimestamp() {
        if (currentTimestampSCN != actionTimestamp) {
            currentTimestampSCN = actionTimestamp;
            currentMillis       = System.currentTimeMillis();
            currentDate         = null;
            currentTimestamp    = null;
            localTimestamp      = null;
            currentTime         = null;
            localTime           = null;
        }
    }
    public int getZoneSeconds() {
        return timeZoneSeconds;
    }
    public void setZoneSeconds(int seconds) {
        if (seconds == sessionTimeZoneSeconds) {
            calendar        = null;
            timeZoneSeconds = sessionTimeZoneSeconds;
        } else {
            TimeZone zone = TimeZone.getDefault();
            zone.setRawOffset(seconds * 1000);
            calendar        = new GregorianCalendar(zone);
            timeZoneSeconds = seconds;
        }
    }
    private Result getAttributesResult(int id) {
        Result   r    = Result.newSessionAttributesResult();
        Object[] data = r.getSingleRowData();
        data[SessionInterface.INFO_ID] = ValuePool.getInt(id);
        switch (id) {
            case SessionInterface.INFO_ISOLATION :
                data[SessionInterface.INFO_INTEGER] =
                    ValuePool.getInt(isolationLevel);
                break;
            case SessionInterface.INFO_AUTOCOMMIT :
                data[SessionInterface.INFO_BOOLEAN] =
                    sessionContext.isAutoCommit;
                break;
            case SessionInterface.INFO_CONNECTION_READONLY :
                data[SessionInterface.INFO_BOOLEAN] =
                    sessionContext.isReadOnly;
                break;
            case SessionInterface.INFO_CATALOG :
                data[SessionInterface.INFO_VARCHAR] =
                    database.getCatalogName().name;
                break;
        }
        return r;
    }
    private Result setAttributes(Result r) {
        Object[] row = r.getSessionAttributes();
        int      id  = ((Integer) row[SessionInterface.INFO_ID]).intValue();
        try {
            switch (id) {
                case SessionInterface.INFO_AUTOCOMMIT : {
                    boolean value =
                        ((Boolean) row[SessionInterface.INFO_BOOLEAN])
                            .booleanValue();
                    this.setAutoCommit(value);
                    break;
                }
                case SessionInterface.INFO_CONNECTION_READONLY : {
                    boolean value =
                        ((Boolean) row[SessionInterface.INFO_BOOLEAN])
                            .booleanValue();
                    this.setReadOnlyDefault(value);
                    break;
                }
                case SessionInterface.INFO_ISOLATION : {
                    int value =
                        ((Integer) row[SessionInterface.INFO_INTEGER])
                            .intValue();
                    this.setIsolationDefault(value);
                    break;
                }
                case SessionInterface.INFO_CATALOG : {
                    String value =
                        ((String) row[SessionInterface.INFO_VARCHAR]);
                    this.setCatalog(value);
                }
            }
        } catch (HsqlException e) {
            return Result.newErrorResult(e);
        }
        return Result.updateZeroResult;
    }
    public synchronized Object getAttribute(int id) {
        switch (id) {
            case SessionInterface.INFO_ISOLATION :
                return ValuePool.getInt(isolationLevel);
            case SessionInterface.INFO_AUTOCOMMIT :
                return sessionContext.isAutoCommit;
            case SessionInterface.INFO_CONNECTION_READONLY :
                return isReadOnlyDefault ? Boolean.TRUE
                                         : Boolean.FALSE;
            case SessionInterface.INFO_CATALOG :
                return database.getCatalogName().name;
        }
        return null;
    }
    public synchronized void setAttribute(int id, Object object) {
        switch (id) {
            case SessionInterface.INFO_AUTOCOMMIT : {
                boolean value = ((Boolean) object).booleanValue();
                this.setAutoCommit(value);
                break;
            }
            case SessionInterface.INFO_CONNECTION_READONLY : {
                boolean value = ((Boolean) object).booleanValue();
                this.setReadOnlyDefault(value);
                break;
            }
            case SessionInterface.INFO_ISOLATION : {
                int value = ((Integer) object).intValue();
                this.setIsolationDefault(value);
                break;
            }
            case SessionInterface.INFO_CATALOG : {
                String value = ((String) object);
                this.setCatalog(value);
            }
        }
    }
    public BlobDataID createBlob(long length) {
        long lobID = database.lobManager.createBlob(this, length);
        if (lobID == 0) {
            throw Error.error(ErrorCode.X_0F502);
        }
        sessionData.registerNewLob(lobID);
        return new BlobDataID(lobID);
    }
    public ClobDataID createClob(long length) {
        long lobID = database.lobManager.createClob(this, length);
        if (lobID == 0) {
            throw Error.error(ErrorCode.X_0F502);
        }
        sessionData.registerNewLob(lobID);
        return new ClobDataID(lobID);
    }
    public void registerResultLobs(Result result) {
        sessionData.registerLobForResult(result);
    }
    public void allocateResultLob(ResultLob result, InputStream inputStream) {
        sessionData.allocateLobForResult(result, inputStream);
    }
    Result performLOBOperation(ResultLob cmd) {
        long id        = cmd.getLobID();
        int  operation = cmd.getSubType();
        switch (operation) {
            case ResultLob.LobResultTypes.REQUEST_GET_LOB : {
                return database.lobManager.getLob(id, cmd.getOffset(),
                                                  cmd.getBlockLength());
            }
            case ResultLob.LobResultTypes.REQUEST_GET_LENGTH : {
                return database.lobManager.getLength(id);
            }
            case ResultLob.LobResultTypes.REQUEST_GET_BYTES : {
                return database.lobManager.getBytes(
                    id, cmd.getOffset(), (int) cmd.getBlockLength());
            }
            case ResultLob.LobResultTypes.REQUEST_SET_BYTES : {
                return database.lobManager.setBytes(
                    id, cmd.getOffset(), cmd.getByteArray(),
                    (int) cmd.getBlockLength());
            }
            case ResultLob.LobResultTypes.REQUEST_GET_CHARS : {
                return database.lobManager.getChars(
                    id, cmd.getOffset(), (int) cmd.getBlockLength());
            }
            case ResultLob.LobResultTypes.REQUEST_SET_CHARS : {
                return database.lobManager.setChars(id, cmd.getOffset(),
                                                    cmd.getCharArray());
            }
            case ResultLob.LobResultTypes.REQUEST_TRUNCATE : {
                return database.lobManager.truncate(id, cmd.getOffset());
            }
            case ResultLob.LobResultTypes.REQUEST_DUPLICATE_LOB : {
                return database.lobManager.createDuplicateLob(id);
            }
            case ResultLob.LobResultTypes.REQUEST_CREATE_BYTES :
            case ResultLob.LobResultTypes.REQUEST_CREATE_CHARS :
            case ResultLob.LobResultTypes.REQUEST_GET_BYTE_PATTERN_POSITION :
            case ResultLob.LobResultTypes.REQUEST_GET_CHAR_PATTERN_POSITION : {
                throw Error.error(ErrorCode.X_0A501);
            }
            default : {
                throw Error.runtimeError(ErrorCode.U_S0500, "Session");
            }
        }
    }
    public String getInternalConnectionURL() {
        return DatabaseURL.S_URL_PREFIX + database.getURI();
    }
    boolean isProcessingScript() {
        return isProcessingScript;
    }
    boolean isProcessingLog() {
        return isProcessingLog;
    }
    public void setSchema(String schema) {
        currentSchema = database.schemaManager.getSchemaHsqlName(schema);
    }
    public void setCatalog(String catalog) {
        if (database.getCatalogName().name.equals(catalog)) {
            return;
        }
        throw Error.error(ErrorCode.X_3D000);
    }
    HsqlName getSchemaHsqlName(String name) {
        return name == null ? currentSchema
                            : database.schemaManager.getSchemaHsqlName(name);
    }
    public String getSchemaName(String name) {
        return name == null ? currentSchema.name
                            : database.schemaManager.getSchemaName(name);
    }
    public void setCurrentSchemaHsqlName(HsqlName name) {
        currentSchema = name;
    }
    public HsqlName getCurrentSchemaHsqlName() {
        return currentSchema;
    }
    public int getResultMemoryRowCount() {
        return resultMaxMemoryRows;
    }
    public void setResultMemoryRowCount(int count) {
        if (database.logger.getTempDirectoryPath() != null) {
            if (count < 0) {
                count = 0;
            }
            resultMaxMemoryRows = count;
        }
    }
    HsqlDeque sqlWarnings;
    public void addWarning(HsqlException warning) {
        if (sqlWarnings == null) {
            sqlWarnings = new HsqlDeque();
        }
        if (sqlWarnings.size() > 9) {
            sqlWarnings.removeFirst();
        }
        int index = sqlWarnings.indexOf(warning);
        if (index >= 0) {
            sqlWarnings.remove(index);
        }
        sqlWarnings.add(warning);
    }
    public HsqlException[] getAndClearWarnings() {
        if (sqlWarnings == null) {
            return HsqlException.emptyArray;
        }
        HsqlException[] array = new HsqlException[sqlWarnings.size()];
        sqlWarnings.toArray(array);
        sqlWarnings.clear();
        return array;
    }
    public HsqlException getLastWarning() {
        if (sqlWarnings == null || sqlWarnings.size() == 0) {
            return null;
        }
        return (HsqlException) sqlWarnings.getLast();
    }
    public void clearWarnings() {
        if (sqlWarnings != null) {
            sqlWarnings.clear();
        }
    }
    Calendar calendar;
    public Calendar getCalendar() {
        if (calendar == null) {
            if (zoneString == null) {
                calendar = new GregorianCalendar();
            } else {
                TimeZone zone = TimeZone.getTimeZone(zoneString);
                calendar = new GregorianCalendar(zone);
            }
        }
        return calendar;
    }
    TypedComparator  typedComparator;
    Scanner          secondaryScanner;
    SimpleDateFormat simpleDateFormat;
    SimpleDateFormat simpleDateFormatGMT;
    Random           randomGenerator = new Random();
    long             seed            = -1;
    public TypedComparator getComparator() {
        if (typedComparator == null) {
            typedComparator = Type.newComparator(this);
        }
        return typedComparator;
    }
    public double random(long seed) {
        if (this.seed != seed) {
            randomGenerator.setSeed(seed);
            this.seed = seed;
        }
        return randomGenerator.nextDouble();
    }
    public double random() {
        return randomGenerator.nextDouble();
    }
    public Scanner getScanner() {
        if (secondaryScanner == null) {
            secondaryScanner = new Scanner();
        }
        return secondaryScanner;
    }
    HsqlProperties clientProperties;
    public HsqlProperties getClientProperties() {
        if (clientProperties == null) {
            clientProperties = new HsqlProperties();
            clientProperties.setProperty(
                HsqlDatabaseProperties.jdbc_translate_tti_types,
                database.sqlTranslateTTI);
        }
        return clientProperties;
    }
    public SimpleDateFormat getSimpleDateFormatGMT() {
        if (simpleDateFormatGMT == null) {
            simpleDateFormatGMT = new SimpleDateFormat("MMMM", Locale.ENGLISH);
            Calendar cal = new GregorianCalendar(TimeZone.getTimeZone("GMT"));
            simpleDateFormatGMT.setCalendar(cal);
        }
        return simpleDateFormatGMT;
    }
    void logSequences() {
        HashMap map = sessionData.sequenceUpdateMap;
        if (map == null || map.isEmpty()) {
            return;
        }
        Iterator it = map.keySet().iterator();
        for (int i = 0, size = map.size(); i < size; i++) {
            NumberSequence sequence = (NumberSequence) it.next();
            database.logger.writeSequenceStatement(this, sequence);
        }
        sessionData.sequenceUpdateMap.clear();
    }
    String getStartTransactionSQL() {
        StringBuffer sb = new StringBuffer();
        sb.append(Tokens.T_START).append(' ').append(Tokens.T_TRANSACTION);
        if (isolationLevel != isolationLevelDefault) {
            sb.append(' ');
            appendIsolationSQL(sb, isolationLevel);
        }
        return sb.toString();
    }
    String getTransactionIsolationSQL() {
        StringBuffer sb = new StringBuffer();
        sb.append(Tokens.T_SET).append(' ').append(Tokens.T_TRANSACTION);
        sb.append(' ');
        appendIsolationSQL(sb, isolationLevel);
        return sb.toString();
    }
    String getSessionIsolationSQL() {
        StringBuffer sb = new StringBuffer();
        sb.append(Tokens.T_SET).append(' ').append(Tokens.T_SESSION);
        sb.append(' ').append(Tokens.T_CHARACTERISTICS).append(' ');
        sb.append(Tokens.T_AS).append(' ').append(Tokens.T_TRANSACTION).append(
            ' ');
        appendIsolationSQL(sb, isolationLevelDefault);
        return sb.toString();
    }
    static void appendIsolationSQL(StringBuffer sb, int isolationLevel) {
        sb.append(Tokens.T_ISOLATION).append(' ');
        sb.append(Tokens.T_LEVEL).append(' ');
        sb.append(getIsolationString(isolationLevel));
    }
    static String getIsolationString(int isolationLevel) {
        switch (isolationLevel) {
            case SessionInterface.TX_READ_UNCOMMITTED :
            case SessionInterface.TX_READ_COMMITTED :
                StringBuffer sb = new StringBuffer();
                sb.append(Tokens.T_READ).append(' ');
                sb.append(Tokens.T_COMMITTED);
                return sb.toString();
            case SessionInterface.TX_REPEATABLE_READ :
            case SessionInterface.TX_SERIALIZABLE :
            default :
                return Tokens.T_SERIALIZABLE;
        }
    }
    String getSetSchemaStatement() {
        return "SET SCHEMA " + currentSchema.statementName;
    }
}