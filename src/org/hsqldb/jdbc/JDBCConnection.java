package org.hsqldb.jdbc;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.Calendar;
import java.util.Map;
import java.sql.Savepoint;
import java.sql.Array;
import java.sql.SQLClientInfoException;
import java.sql.NClob;
import java.sql.SQLXML;
import java.sql.Struct;
import java.util.Properties;
import org.hsqldb.DatabaseManager;
import org.hsqldb.DatabaseURL;
import org.hsqldb.ClientConnection;
import org.hsqldb.ClientConnectionHTTP;
import org.hsqldb.HsqlDateTime;
import org.hsqldb.HsqlException;
import org.hsqldb.SessionInterface;
import org.hsqldb.Tokens;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.StringUtil;
import org.hsqldb.persist.HsqlProperties;
import org.hsqldb.result.Result;
import org.hsqldb.result.ResultConstants;
import org.hsqldb.result.ResultProperties;
import org.hsqldb.types.Type;
public class JDBCConnection implements Connection {
    public synchronized Statement createStatement() throws SQLException {
        checkClosed();
        int props =
            ResultProperties.getValueForJDBC(JDBCResultSet.TYPE_FORWARD_ONLY,
                JDBCResultSet.CONCUR_READ_ONLY, rsHoldability);
        Statement stmt = new JDBCStatement(this, props);
        return stmt;
    }
    public synchronized PreparedStatement prepareStatement(
            String sql) throws SQLException {
        checkClosed();
        try {
            return new JDBCPreparedStatement(this, sql,
                    JDBCResultSet.TYPE_FORWARD_ONLY,
                    JDBCResultSet.CONCUR_READ_ONLY, rsHoldability,
                    ResultConstants.RETURN_NO_GENERATED_KEYS, null, null);
        } catch (HsqlException e) {
            throw Util.sqlException(e);
        }
    }
    public synchronized CallableStatement prepareCall(
            String sql) throws SQLException {
        CallableStatement stmt;
        checkClosed();
        try {
            stmt = new JDBCCallableStatement(this, sql,
                    JDBCResultSet.TYPE_FORWARD_ONLY,
                    JDBCResultSet.CONCUR_READ_ONLY, rsHoldability);
            return stmt;
        } catch (HsqlException e) {
            throw Util.sqlException(e);
        }
    }
    public synchronized String nativeSQL(
            final String sql) throws SQLException {
        checkClosed();
        if (sql == null || sql.length() == 0 || sql.indexOf('{') == -1) {
            return sql;
        }
        boolean      changed = false;
        int          state   = 0;
        int          len     = sql.length();
        int          nest    = 0;
        StringBuffer sb      = null;
        String       msg;
        final int outside_all                         = 0;
        final int outside_escape_inside_single_quotes = 1;
        final int outside_escape_inside_double_quotes = 2;
        final int inside_escape                      = 3;
        final int inside_escape_inside_single_quotes = 4;
        final int inside_escape_inside_double_quotes = 5;
        int tail = 0;
        for (int i = 0; i < len; i++) {
            char c = sql.charAt(i);
            switch (state) {
                case outside_all :                            
                    if (c == '\'') {
                        state = outside_escape_inside_single_quotes;
                    } else if (c == '"') {
                        state = outside_escape_inside_double_quotes;
                    } else if (c == '{') {
                        if (sb == null) {
                            sb = new StringBuffer(sql.length());
                        }
                        sb.append(sql.substring(tail, i));
                        i       = onStartEscapeSequence(sql, sb, i);
                        tail    = i;
                        changed = true;
                        nest++;
                        state = inside_escape;
                    }
                    break;
                case outside_escape_inside_single_quotes :    
                case inside_escape_inside_single_quotes :     
                    if (c == '\'') {
                        state -= 1;
                    }
                    break;
                case outside_escape_inside_double_quotes :    
                case inside_escape_inside_double_quotes :     
                    if (c == '"') {
                        state -= 2;
                    }
                    break;
                case inside_escape :                          
                    if (c == '\'') {
                        state = inside_escape_inside_single_quotes;
                    } else if (c == '"') {
                        state = inside_escape_inside_double_quotes;
                    } else if (c == '}') {
                        sb.append(sql.substring(tail, i));
                        sb.append(' ');
                        i++;
                        tail    = i;
                        changed = true;
                        nest--;
                        state = (nest == 0) ? outside_all
                                : inside_escape;
                    } else if (c == '{') {
                        sb.append(sql.substring(tail, i));
                        i       = onStartEscapeSequence(sql, sb, i);
                        tail    = i;
                        changed = true;
                        nest++;
                        state = inside_escape;
                    }
            }
        }
        if (!changed) {
            return sql;
        }
        sb.append(sql.substring(tail, sql.length()));
        return sb.toString();
    }
    public synchronized void setAutoCommit(
            boolean autoCommit) throws SQLException {
        checkClosed();
        try {
            sessionProxy.setAutoCommit(autoCommit);
        } catch (HsqlException e) {
            throw Util.sqlException(e);
        }
    }
    public synchronized boolean getAutoCommit() throws SQLException {
        checkClosed();
        try {
            return sessionProxy.isAutoCommit();
        } catch (HsqlException e) {
            throw Util.sqlException(e);
        }
    }
    public synchronized void commit() throws SQLException {
        checkClosed();
        try {
            sessionProxy.commit(false);
        } catch (HsqlException e) {
            throw Util.sqlException(e);
        }
    }
    public synchronized void rollback() throws SQLException {
        checkClosed();
        try {
            sessionProxy.rollback(false);
        } catch (HsqlException e) {
            throw Util.sqlException(e);
        }
    }
    public synchronized void close() throws SQLException {
        if (isInternal || isClosed) {
            return;
        }
        isClosed       = true;
        rootWarning    = null;
        connProperties = null;
        if (isPooled) {
            if (poolEventListener != null) {
                poolEventListener.connectionClosed();
                poolEventListener = null;
            }
        } else if (sessionProxy != null) {
            sessionProxy.close();
            sessionProxy = null;
        }
    }
    public synchronized boolean isClosed() throws SQLException {
        return isClosed;
    }
    public synchronized DatabaseMetaData getMetaData() throws SQLException {
        checkClosed();
        return new JDBCDatabaseMetaData(this);
    }
    public synchronized void setReadOnly(
            boolean readOnly) throws SQLException {
        checkClosed();
        try {
            sessionProxy.setReadOnlyDefault(readOnly);
        } catch (HsqlException e) {
            throw Util.sqlException(e);
        }
    }
    public synchronized boolean isReadOnly() throws SQLException {
        checkClosed();
        try {
            return sessionProxy.isReadOnlyDefault();
        } catch (HsqlException e) {
            throw Util.sqlException(e);
        }
    }
    public synchronized void setCatalog(String catalog) throws SQLException {
        checkClosed();
        try {
            sessionProxy.setAttribute(SessionInterface.INFO_CATALOG, catalog);
        } catch (HsqlException e) {
            throw Util.sqlException(e);
        }
    }
    public synchronized String getCatalog() throws SQLException {
        checkClosed();
        try {
            return (String) sessionProxy.getAttribute(
                SessionInterface.INFO_CATALOG);
        } catch (HsqlException e) {
            throw Util.sqlException(e);
        }
    }
    public synchronized void setTransactionIsolation(
            int level) throws SQLException {
        checkClosed();
        switch (level) {
            case TRANSACTION_READ_UNCOMMITTED :
            case TRANSACTION_READ_COMMITTED :
            case TRANSACTION_REPEATABLE_READ :
            case TRANSACTION_SERIALIZABLE :
                break;
            default :
                throw Util.invalidArgument();
        }
        try {
            sessionProxy.setIsolationDefault(level);
        } catch (HsqlException e) {
            throw Util.sqlException(e);
        }
    }
    public synchronized int getTransactionIsolation() throws SQLException {
        checkClosed();
        try {
            return sessionProxy.getIsolation();
        } catch (HsqlException e) {
            throw Util.sqlException(e);
        }
    }
    public synchronized SQLWarning getWarnings() throws SQLException {
        checkClosed();
        return rootWarning;
    }
    public synchronized void clearWarnings() throws SQLException {
        checkClosed();
        rootWarning = null;
    }
    public synchronized Statement createStatement(int resultSetType,
            int resultSetConcurrency) throws SQLException {
        checkClosed();
        int props = ResultProperties.getValueForJDBC(resultSetType,
            resultSetConcurrency, rsHoldability);
        return new JDBCStatement(this, props);
    }
    public synchronized PreparedStatement prepareStatement(String sql,
            int resultSetType, int resultSetConcurrency) throws SQLException {
        checkClosed();
        try {
            return new JDBCPreparedStatement(this, sql, resultSetType,
                    resultSetConcurrency, rsHoldability,
                    ResultConstants.RETURN_NO_GENERATED_KEYS, null, null);
        } catch (HsqlException e) {
            throw Util.sqlException(e);
        }
    }
    public synchronized CallableStatement prepareCall(String sql,
            int resultSetType, int resultSetConcurrency) throws SQLException {
        checkClosed();
        try {
            return new JDBCCallableStatement(this, sql, resultSetType,
                    resultSetConcurrency, rsHoldability);
        } catch (HsqlException e) {
            throw Util.sqlException(e);
        }
    }
    public synchronized java.util
            .Map<java.lang.String,
                 java.lang.Class<?>> getTypeMap() throws SQLException {
        checkClosed();
        return new java.util.HashMap<java.lang.String, java.lang.Class<?>>();
    }
    public synchronized void setTypeMap(Map<String,
            Class<?>> map) throws SQLException {
        checkClosed();
        throw Util.notSupported();
    }
    public synchronized void setHoldability(
            int holdability) throws SQLException {
        checkClosed();
        switch (holdability) {
            case JDBCResultSet.HOLD_CURSORS_OVER_COMMIT :
            case JDBCResultSet.CLOSE_CURSORS_AT_COMMIT :
                break;
            default :
                throw Util.invalidArgument();
        }
        rsHoldability = holdability;
    }
    public synchronized int getHoldability() throws SQLException {
        checkClosed();
        return rsHoldability;
    }
    public synchronized Savepoint setSavepoint() throws SQLException {
        checkClosed();
        if (JDBCDatabaseMetaData.JDBC_MAJOR >= 4 && getAutoCommit()) {
            throw Util.sqlException(ErrorCode.X_3B001);
        }
        JDBCSavepoint savepoint = new JDBCSavepoint(this);
        try {
            sessionProxy.savepoint(savepoint.name);
        } catch (HsqlException e) {
            Util.throwError(e);
        }
        return savepoint;
    }
    public synchronized Savepoint setSavepoint(
            String name) throws SQLException {
        checkClosed();
        if (JDBCDatabaseMetaData.JDBC_MAJOR >= 4 && getAutoCommit()) {
            throw Util.sqlException(ErrorCode.X_3B001);
        }
        if (name == null) {
            throw Util.nullArgument();
        }
        if (name.startsWith("SYSTEM_SAVEPOINT_")) {
            throw Util.invalidArgument();
        }
        try {
            sessionProxy.savepoint(name);
        } catch (HsqlException e) {
            Util.throwError(e);
        }
        return new JDBCSavepoint(name, this);
    }
    public synchronized void rollback(
            Savepoint savepoint) throws SQLException {
        JDBCSavepoint sp;
        checkClosed();
        if (savepoint == null) {
            throw Util.nullArgument();
        }
        if (!(savepoint instanceof JDBCSavepoint)) {
            String msg = Error.getMessage(ErrorCode.X_3B001);
            throw Util.invalidArgument(msg);
        }
        sp = (JDBCSavepoint) savepoint;
        if (JDBCDatabaseMetaData.JDBC_MAJOR >= 4 && sp.name == null) {
            String msg = Error.getMessage(ErrorCode.X_3B001);
            throw Util.invalidArgument(msg);
        }
        if (this != sp.connection) {
            String msg = Error.getMessage(ErrorCode.X_3B001);
            throw Util.invalidArgument(msg);
        }
        if (JDBCDatabaseMetaData.JDBC_MAJOR >= 4 && getAutoCommit()) {
            sp.name       = null;
            sp.connection = null;
            throw Util.sqlException(ErrorCode.X_3B001);
        }
        try {
            sessionProxy.rollbackToSavepoint(sp.name);
            if (JDBCDatabaseMetaData.JDBC_MAJOR >= 4) {
                sp.connection = null;
                sp.name       = null;
            }
        } catch (HsqlException e) {
            throw Util.sqlException(e);
        }
    }
    public synchronized void releaseSavepoint(
            Savepoint savepoint) throws SQLException {
        JDBCSavepoint sp;
        Result        req;
        checkClosed();
        if (savepoint == null) {
            throw Util.nullArgument();
        }
        if (!(savepoint instanceof JDBCSavepoint)) {
            String msg = Error.getMessage(ErrorCode.X_3B001);
            throw Util.invalidArgument(msg);
        }
        sp = (JDBCSavepoint) savepoint;
        if (JDBCDatabaseMetaData.JDBC_MAJOR >= 4 && sp.name == null) {
            String msg = Error.getMessage(ErrorCode.X_3B001);
            throw Util.invalidArgument(msg);
        }
        if (this != sp.connection) {
            String msg = Error.getMessage(ErrorCode.X_3B001);
            throw Util.invalidArgument(msg);
        }
        if (JDBCDatabaseMetaData.JDBC_MAJOR >= 4 && getAutoCommit()) {
            sp.name       = null;
            sp.connection = null;
            throw Util.sqlException(ErrorCode.X_3B001);
        }
        try {
            sessionProxy.releaseSavepoint(sp.name);
            if (JDBCDatabaseMetaData.JDBC_MAJOR >= 4) {
                sp.connection = null;
                sp.name       = null;
            }
        } catch (HsqlException e) {
            throw Util.sqlException(e);
        }
    }
    public synchronized Statement createStatement(int resultSetType,
            int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {
        checkClosed();
        int props = ResultProperties.getValueForJDBC(resultSetType,
            resultSetConcurrency, resultSetHoldability);
        return new JDBCStatement(this, props);
    }
    public synchronized PreparedStatement prepareStatement(String sql,
            int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {
        checkClosed();
        try {
            return new JDBCPreparedStatement(this, sql, resultSetType,
                    resultSetConcurrency, resultSetHoldability,
                    ResultConstants.RETURN_NO_GENERATED_KEYS, null, null);
        } catch (HsqlException e) {
            throw Util.sqlException(e);
        }
    }
    public synchronized CallableStatement prepareCall(String sql,
            int resultSetType, int resultSetConcurrency,
            int resultSetHoldability) throws SQLException {
        checkClosed();
        try {
            return new JDBCCallableStatement(this, sql, resultSetType,
                    resultSetConcurrency, resultSetHoldability);
        } catch (HsqlException e) {
            throw Util.sqlException(e);
        }
    }
    public synchronized PreparedStatement prepareStatement(String sql,
            int autoGeneratedKeys) throws SQLException {
        checkClosed();
        try {
            if (autoGeneratedKeys != ResultConstants.RETURN_GENERATED_KEYS
                    && autoGeneratedKeys
                       != ResultConstants.RETURN_NO_GENERATED_KEYS) {
                throw Util.invalidArgument("autoGeneratedKeys");
            }
            return new JDBCPreparedStatement(this, sql,
                    JDBCResultSet.TYPE_FORWARD_ONLY,
                    JDBCResultSet.CONCUR_READ_ONLY, rsHoldability,
                    autoGeneratedKeys, null, null);
        } catch (HsqlException e) {
            throw Util.sqlException(e);
        }
    }
    public synchronized PreparedStatement prepareStatement(String sql,
            int[] columnIndexes) throws SQLException {
        checkClosed();
        try {
            return new JDBCPreparedStatement(this, sql,
                    JDBCResultSet.TYPE_FORWARD_ONLY,
                    JDBCResultSet.CONCUR_READ_ONLY, rsHoldability,
                    ResultConstants.RETURN_GENERATED_KEYS_COL_INDEXES,
                    columnIndexes, null);
        } catch (HsqlException e) {
            throw Util.sqlException(e);
        }
    }
    public synchronized PreparedStatement prepareStatement(String sql,
            String[] columnNames) throws SQLException {
        checkClosed();
        try {
            return new JDBCPreparedStatement(this, sql,
                    JDBCResultSet.TYPE_FORWARD_ONLY,
                    JDBCResultSet.CONCUR_READ_ONLY, rsHoldability,
                    ResultConstants.RETURN_GENERATED_KEYS_COL_NAMES, null,
                    columnNames);
        } catch (HsqlException e) {
            throw Util.sqlException(e);
        }
    }
    public Clob createClob() throws SQLException {
        checkClosed();
        return new JDBCClob();
    }
    public Blob createBlob() throws SQLException {
        checkClosed();
        return new JDBCBlob();
    }
    public NClob createNClob() throws SQLException {
        checkClosed();
        return new JDBCNClob();
    }
    public SQLXML createSQLXML() throws SQLException {
        checkClosed();
        return new JDBCSQLXML();
    }
    public boolean isValid(int timeout) throws SQLException {
        if (timeout < 0) {
            throw Util.outOfRangeArgument("timeout: " + timeout);
        }
        if (this.isInternal) {
            return true;
        } else if (!this.isNetConn) {
            return !this.isClosed();
        } else if (this.isClosed()) {
            return false;
        }
        final boolean[] flag = new boolean[] { true };
        Thread          t    = new Thread() {
            public void run() {
                try {
                    getMetaData().getDatabaseMajorVersion();
                } catch (Throwable e) {
                    flag[0] = false;
                }
            }
        };
        if (timeout > 60) {
            timeout = 60;
        }
        timeout *= 1000;
        try {
            t.start();
            final long start = System.currentTimeMillis();
            t.join(timeout);
            try {
                t.setContextClassLoader(null);
            } catch (Throwable th) {
            }
            if (timeout == 0) {
                return flag[0];
            }
            return flag[0] && (System.currentTimeMillis() - start) < timeout;
        } catch (Throwable e) {
            return false;
        }
    }
    public void setClientInfo(String name,
                              String value) throws SQLClientInfoException {
        SQLClientInfoException ex = new SQLClientInfoException();
        ex.initCause(Util.notSupported());
        throw ex;
    }
    public void setClientInfo(
            Properties properties) throws SQLClientInfoException {
        if (!this.isClosed && (properties == null || properties.isEmpty())) {
            return;
        }
        SQLClientInfoException ex = new SQLClientInfoException();
        if (this.isClosed) {
            ex.initCause(Util.connectionClosedException());
        } else {
            ex.initCause(Util.notSupported());
        }
        throw ex;
    }
    public String getClientInfo(String name) throws SQLException {
        checkClosed();
        return null;
    }
    public Properties getClientInfo() throws SQLException {
        checkClosed();
        return null;
    }
    public Array createArrayOf(String typeName,
                               Object[] elements) throws SQLException {
        checkClosed();
        if (typeName == null) {
            throw Util.nullArgument();
        }
        typeName = typeName.toUpperCase();
        int typeCode = Type.getTypeNr(typeName);
        if (typeCode < 0) {
            throw Util.invalidArgument(typeName);
        }
        Type type = Type.getDefaultType(typeCode);
        if (type.isArrayType() || type.isLobType() || type.isRowType()) {
            throw Util.invalidArgument(typeName);
        }
        Object[] newData = new Object[elements.length];
        try {
            for (int i = 0; i < elements.length; i++) {
                Object o = type.convertJavaToSQL(sessionProxy, elements[i]);
                newData[i] = type.convertToTypeLimits(sessionProxy, o);
            }
        } catch (HsqlException e) {
            throw Util.sqlException(e);
        }
        return new JDBCArray(newData, type, this);
    }
    public Struct createStruct(String typeName,
                               Object[] attributes) throws SQLException {
        checkClosed();
        throw Util.notSupported();
    }
    @SuppressWarnings("unchecked")
    public <T>T unwrap(java.lang.Class<T> iface) throws java.sql.SQLException {
        checkClosed();
        if (isWrapperFor(iface)) {
            return (T) this;
        }
        throw Util.invalidArgument("iface: " + iface);
    }
    public boolean isWrapperFor(
            java.lang.Class<?> iface) throws java.sql.SQLException {
        checkClosed();
        return (iface != null && iface.isAssignableFrom(this.getClass()));
    }
    public void setSchema(String schema) throws SQLException {
        checkClosed();
        if (schema == null) {
            Util.nullArgument("schema");
        } else if (schema.length() == 0) {
            Util.invalidArgument("Zero-length schema");
        } else {
            (new JDBCDatabaseMetaData(this)).setConnectionDefaultSchema(
                schema);
        }
    }
    public String getSchema() throws SQLException {
        checkClosed();
        return (new JDBCDatabaseMetaData(this)).getConnectionDefaultSchema();
    }
    public void abort(
            java.util.concurrent.Executor executor) throws SQLException {
        if (executor == null) {
            throw Util.nullArgument("executor");
        }
        close();
    }
    public void setNetworkTimeout(java.util.concurrent.Executor executor,
                                  int milliseconds) throws SQLException {
        checkClosed();
        throw Util.notSupported();
    }
    public int getNetworkTimeout() throws SQLException {
        return 0;
    }
    int rsHoldability = JDBCResultSet.HOLD_CURSORS_OVER_COMMIT;
    HsqlProperties connProperties;
    HsqlProperties clientProperties;
    SessionInterface sessionProxy;
    boolean isInternal;
    protected boolean isNetConn;
    boolean isClosed;
    private SQLWarning rootWarning;
    private final Object rootWarning_mutex = new Object();
    private int savepointIDSequence;
    int                         incarnation;
    boolean                     isPooled;
    JDBCConnectionEventListener poolEventListener;
    public JDBCConnection(HsqlProperties props) throws SQLException {
        String user     = props.getProperty("user");
        String password = props.getProperty("password");
        String connType = props.getProperty("connection_type");
        String host     = props.getProperty("host");
        int    port     = props.getIntegerProperty("port", 0);
        String path     = props.getProperty("path");
        String database = props.getProperty("database");
        boolean isTLS = (connType == DatabaseURL.S_HSQLS
                         || connType == DatabaseURL.S_HTTPS);
        if (user == null) {
            user = "SA";
        }
        if (password == null) {
            password = "";
        }
        Calendar cal         = Calendar.getInstance();
        int      zoneSeconds = HsqlDateTime.getZoneSeconds(cal);
        try {
            if (DatabaseURL.isInProcessDatabaseType(connType)) {
                sessionProxy = DatabaseManager.newSession(connType, database,
                        user, password, props, null, zoneSeconds);
            } else if (connType == DatabaseURL.S_HSQL
                       || connType == DatabaseURL.S_HSQLS) {
                sessionProxy = new ClientConnection(host, port, path,
                        database, isTLS, user, password, zoneSeconds);
                isNetConn = true;
            } else if (connType == DatabaseURL.S_HTTP
                       || connType == DatabaseURL.S_HTTPS) {
                sessionProxy = new ClientConnectionHTTP(host, port, path,
                        database, isTLS, user, password, zoneSeconds);
                isNetConn = true;
            } else {    
                throw Util.invalidArgument(connType);
            }
            sessionProxy.setJDBCConnection(this);
            connProperties   = props;
            clientProperties = sessionProxy.getClientProperties();
        } catch (HsqlException e) {
            throw Util.sqlException(e);
        }
    }
    public JDBCConnection(SessionInterface c) {
        isInternal   = true;
        sessionProxy = c;
    }
    public JDBCConnection(JDBCConnection c,
                          JDBCConnectionEventListener eventListener) {
        sessionProxy      = c.sessionProxy;
        connProperties    = c.connProperties;
        clientProperties  = c.clientProperties;
        isPooled          = true;
        poolEventListener = eventListener;
    }
    protected void finalize() {
        try {
            close();
        } catch (SQLException e) {
        }
    }
    synchronized int getSavepointID() {
        return savepointIDSequence++;
    }
    synchronized String getURL() throws SQLException {
        checkClosed();
        return isInternal ? sessionProxy.getInternalConnectionURL()
                          : connProperties.getProperty("url");
    }
    synchronized void checkClosed() throws SQLException {
        if (isClosed) {
            throw Util.connectionClosedException();
        }
    }
    void addWarning(SQLWarning w) {
        synchronized (rootWarning_mutex) {
            if (rootWarning == null) {
                rootWarning = w;
            } else {
                rootWarning.setNextWarning(w);
            }
        }
    }
    void setWarnings(SQLWarning w) {
        synchronized (rootWarning_mutex) {
            rootWarning = w;
        }
    }
    public void reset() throws SQLException {
        try {
            incarnation++;
            this.sessionProxy.resetSession();
        } catch (HsqlException e) {
            throw Util.sqlException(ErrorCode.X_08006, e.getMessage(), e);
        }
    }
    public void closeFully() {
        try {
            close();
        } catch (Throwable t) {
        }
        try {
            if (sessionProxy != null) {
                sessionProxy.close();
                sessionProxy = null;
            }
        } catch (Throwable t) {
        }
    }
    public SessionInterface getSession() {
        return sessionProxy;
    }
    private int onStartEscapeSequence(String sql, StringBuffer sb,
                                      int i) throws SQLException {
        sb.append(' ');
        i++;
        i = StringUtil.skipSpaces(sql, i);
        if (sql.regionMatches(true, i, "fn ", 0, 3)
                || sql.regionMatches(true, i, "oj ", 0, 3)) {
            i += 2;
        } else if (sql.regionMatches(true, i, "ts ", 0, 3)) {
            sb.append(Tokens.T_TIMESTAMP);
            i += 2;
        } else if (sql.regionMatches(true, i, "d ", 0, 2)) {
            sb.append(Tokens.T_DATE);
            i++;
        } else if (sql.regionMatches(true, i, "t ", 0, 2)) {
            sb.append(Tokens.T_TIME);
            i++;
        } else if (sql.regionMatches(true, i, "call ", 0, 5)) {
            sb.append(Tokens.T_CALL);
            i += 4;
        } else if (sql.regionMatches(true, i, "?= call ", 0, 8)) {
            sb.append(Tokens.T_CALL);
            i += 7;
        } else if (sql.regionMatches(true, i, "? = call ", 0, 8)) {
            sb.append(Tokens.T_CALL);
            i += 8;
        } else if (sql.regionMatches(true, i, "escape ", 0, 7)) {
            i += 6;
        } else {
            i--;
            throw Util.sqlException(
                Error.error(
                    ErrorCode.JDBC_CONNECTION_NATIVE_SQL, sql.substring(i)));
        }
        return i;
    }
}