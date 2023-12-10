package org.hsqldb.jdbc;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import org.hsqldb.HsqlException;
import org.hsqldb.StatementTypes;
import org.hsqldb.navigator.RowSetNavigator;
import org.hsqldb.result.Result;
import org.hsqldb.result.ResultConstants;
import org.hsqldb.result.ResultProperties;
public class JDBCStatement extends JDBCStatementBase implements Statement,
        java.sql.Wrapper {
    public synchronized ResultSet executeQuery(
            String sql) throws SQLException {
        fetchResult(sql, StatementTypes.RETURN_RESULT,
                    JDBCStatementBase.NO_GENERATED_KEYS, null, null);
        return getResultSet();
    }
    public synchronized int executeUpdate(String sql) throws SQLException {
        fetchResult(sql, StatementTypes.RETURN_COUNT,
                    JDBCStatementBase.NO_GENERATED_KEYS, null, null);
        return resultIn.getUpdateCount();
    }
    public synchronized void close() throws SQLException {
        if (isClosed) {
            return;
        }
        closeResultData();
        batchResultOut = null;
        connection     = null;
        resultIn       = null;
        resultOut      = null;
        isClosed       = true;
    }
    public synchronized int getMaxFieldSize() throws SQLException {
        checkClosed();
        return 0;
    }
    public void setMaxFieldSize(int max) throws SQLException {
        checkClosed();
        if (max < 0) {
            throw Util.outOfRangeArgument();
        }
    }
    public synchronized int getMaxRows() throws SQLException {
        checkClosed();
        return maxRows;
    }
    public synchronized void setMaxRows(int max) throws SQLException {
        checkClosed();
        if (max < 0) {
            throw Util.outOfRangeArgument();
        }
        maxRows = max;
    }
    public void setEscapeProcessing(boolean enable) throws SQLException {
        checkClosed();
        isEscapeProcessing = enable;
    }
    public synchronized int getQueryTimeout() throws SQLException {
        checkClosed();
        return 0;
    }
    public void setQueryTimeout(int seconds) throws SQLException {
        checkClosed();
        if (seconds < 0 || seconds > Short.MAX_VALUE) {
            throw Util.outOfRangeArgument();
        }
        queryTimeout = seconds;
    }
    public synchronized void cancel() throws SQLException {
        checkClosed();
    }
    public synchronized SQLWarning getWarnings() throws SQLException {
        checkClosed();
        return rootWarning;
    }
    public synchronized void clearWarnings() throws SQLException {
        checkClosed();
        rootWarning = null;
    }
    public void setCursorName(String name) throws SQLException {
        checkClosed();
    }
    public synchronized boolean execute(String sql) throws SQLException {
        fetchResult(sql, StatementTypes.RETURN_ANY,
                    JDBCStatementBase.NO_GENERATED_KEYS, null, null);
        return currentResultSet != null;
    }
    public synchronized ResultSet getResultSet() throws SQLException {
        return super.getResultSet();
    }
    public synchronized int getUpdateCount() throws SQLException {
        return super.getUpdateCount();
    }
    public synchronized boolean getMoreResults() throws SQLException {
        return getMoreResults(JDBCStatementBase.CLOSE_CURRENT_RESULT);
    }
    public synchronized void setFetchDirection(
            int direction) throws SQLException {
        checkClosed();
        checkClosed();
        switch (direction) {
            case JDBCResultSet.FETCH_FORWARD :
            case JDBCResultSet.FETCH_REVERSE :
            case JDBCResultSet.FETCH_UNKNOWN :
                fetchDirection = direction;
                break;
            default :
                throw Util.invalidArgument();
        }
    }
    public int getFetchDirection() throws SQLException {
        checkClosed();
        return this.fetchDirection;
    }
    public synchronized void setFetchSize(int rows) throws SQLException {
        checkClosed();
        if (rows < 0) {
            throw Util.outOfRangeArgument();
        }
        fetchSize = rows;
    }
    public synchronized int getFetchSize() throws SQLException {
        checkClosed();
        return fetchSize;
    }
    public synchronized int getResultSetConcurrency() throws SQLException {
        checkClosed();
        return ResultProperties.getJDBCConcurrency(rsProperties);
    }
    public synchronized int getResultSetType() throws SQLException {
        checkClosed();
        return ResultProperties.getJDBCScrollability(rsProperties);
    }
    public synchronized void addBatch(String sql) throws SQLException {
        checkClosed();
        if (isEscapeProcessing) {
            sql = connection.nativeSQL(sql);
        }
        if (batchResultOut == null) {
            batchResultOut = Result.newBatchedExecuteRequest();
        }
        batchResultOut.getNavigator().add(new Object[] { sql });
    }
    public synchronized void clearBatch() throws SQLException {
        checkClosed();
        if (batchResultOut != null) {
            batchResultOut.getNavigator().clear();
        }
    }
    public synchronized int[] executeBatch() throws SQLException {
        checkClosed();
        generatedResult = null;
        if (batchResultOut == null) {
            batchResultOut = Result.newBatchedExecuteRequest();
        }
        int batchCount = batchResultOut.getNavigator().getSize();
        try {
            resultIn = connection.sessionProxy.execute(batchResultOut);
            performPostExecute();
        } catch (HsqlException e) {
            batchResultOut.getNavigator().clear();
            throw Util.sqlException(e);
        }
        batchResultOut.getNavigator().clear();
        if (resultIn.isError()) {
            throw Util.sqlException(resultIn);
        }
        RowSetNavigator navigator    = resultIn.getNavigator();
        int[]           updateCounts = new int[navigator.getSize()];
        for (int i = 0; i < updateCounts.length; i++) {
            Object[] data = (Object[]) navigator.getNext();
            updateCounts[i] = ((Integer) data[0]).intValue();
        }
        if (updateCounts.length != batchCount) {
            if (errorResult == null) {
                throw new BatchUpdateException(updateCounts);
            } else {
                errorResult.getMainString();
                throw new BatchUpdateException(errorResult.getMainString(),
                        errorResult.getSubString(),
                        errorResult.getErrorCode(), updateCounts);
            }
        }
        return updateCounts;
    }
    public synchronized Connection getConnection() throws SQLException {
        checkClosed();
        return connection;
    }
    public synchronized boolean getMoreResults(
            int current) throws SQLException {
        return super.getMoreResults(current);
    }
    public synchronized ResultSet getGeneratedKeys() throws SQLException {
        return getGeneratedResultSet();
    }
    public synchronized int executeUpdate(String sql,
            int autoGeneratedKeys) throws SQLException {
        if (autoGeneratedKeys != Statement.RETURN_GENERATED_KEYS
                && autoGeneratedKeys != Statement.NO_GENERATED_KEYS) {
            throw Util.invalidArgument("autoGeneratedKeys");
        }
        fetchResult(sql, StatementTypes.RETURN_COUNT, autoGeneratedKeys, null,
                    null);
        if (resultIn.isError()) {
            throw Util.sqlException(resultIn);
        }
        return resultIn.getUpdateCount();
    }
    public synchronized int executeUpdate(String sql,
            int[] columnIndexes) throws SQLException {
        if (columnIndexes == null || columnIndexes.length == 0) {
            throw Util.invalidArgument("columnIndexes");
        }
        fetchResult(sql, StatementTypes.RETURN_COUNT,
                    ResultConstants.RETURN_GENERATED_KEYS_COL_INDEXES,
                    columnIndexes, null);
        return resultIn.getUpdateCount();
    }
    public synchronized int executeUpdate(String sql,
            String[] columnNames) throws SQLException {
        if (columnNames == null || columnNames.length == 0) {
            throw Util.invalidArgument("columnIndexes");
        }
        fetchResult(sql, StatementTypes.RETURN_COUNT,
                    ResultConstants.RETURN_GENERATED_KEYS_COL_NAMES, null,
                    columnNames);
        return resultIn.getUpdateCount();
    }
    public synchronized boolean execute(
            String sql, int autoGeneratedKeys) throws SQLException {
        if (autoGeneratedKeys != Statement.RETURN_GENERATED_KEYS
                && autoGeneratedKeys != Statement.NO_GENERATED_KEYS) {
            throw Util.invalidArgument("autoGeneratedKeys");
        }
        fetchResult(sql, StatementTypes.RETURN_ANY, autoGeneratedKeys, null,
                    null);
        return resultIn.isData();
    }
    public synchronized boolean execute(
            String sql, int[] columnIndexes) throws SQLException {
        if (columnIndexes == null || columnIndexes.length == 0) {
            throw Util.invalidArgument("columnIndexes");
        }
        fetchResult(sql, StatementTypes.RETURN_ANY,
                    ResultConstants.RETURN_GENERATED_KEYS_COL_INDEXES,
                    columnIndexes, null);
        return resultIn.isData();
    }
    public synchronized boolean execute(
            String sql, String[] columnNames) throws SQLException {
        if (columnNames == null || columnNames.length == 0) {
            throw Util.invalidArgument("columnIndexes");
        }
        fetchResult(sql, StatementTypes.RETURN_ANY,
                    ResultConstants.RETURN_GENERATED_KEYS_COL_NAMES, null,
                    columnNames);
        return resultIn.isData();
    }
    public synchronized int getResultSetHoldability() throws SQLException {
        return ResultProperties.getJDBCHoldability(rsProperties);
    }
    public synchronized boolean isClosed() throws SQLException {
        return isClosed;
    }
    boolean poolable = false;
    public synchronized void setPoolable(
            boolean poolable) throws SQLException {
        checkClosed();
        this.poolable = poolable;
    }
    public synchronized boolean isPoolable() throws SQLException {
        checkClosed();
        return this.poolable;
    }
    @SuppressWarnings("unchecked")
    public <T>T unwrap(Class<T> iface) throws java.sql.SQLException {
        if (isWrapperFor(iface)) {
            return (T) this;
        }
        throw Util.invalidArgument("iface: " + iface);
    }
    public boolean isWrapperFor(
            java.lang.Class<?> iface) throws java.sql.SQLException {
        return (iface != null && iface.isAssignableFrom(this.getClass()));
    }
    JDBCStatement(JDBCConnection c, int props) {
        resultOut             = Result.newExecuteDirectRequest();
        connection            = c;
        connectionIncarnation = connection.incarnation;
        rsProperties          = props;
    }
    private void fetchResult(String sql, int statementRetType,
                             int generatedKeys, int[] generatedIndexes,
                             String[] generatedNames) throws SQLException {
        checkClosed();
        closeResultData();
        if (isEscapeProcessing) {
            sql = connection.nativeSQL(sql);
        }
        resultOut.setPrepareOrExecuteProperties(sql, maxRows, fetchSize,
                statementRetType, queryTimeout, rsProperties, generatedKeys,
                generatedIndexes, generatedNames);
        try {
            resultIn = connection.sessionProxy.execute(resultOut);
            performPostExecute();
        } catch (HsqlException e) {
            throw Util.sqlException(e);
        }
        if (resultIn.isError()) {
            throw Util.sqlException(resultIn);
        }
        if (resultIn.isData()) {
            currentResultSet = new JDBCResultSet(connection, this, resultIn,
                    resultIn.metaData);
        } else if (resultIn.getStatementType()
                   == StatementTypes.RETURN_RESULT) {
            getMoreResults();
        }
    }
}