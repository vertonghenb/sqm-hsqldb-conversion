package org.hsqldb.jdbc;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.result.Result;
import org.hsqldb.result.ResultConstants;
class JDBCStatementBase {
    volatile boolean isClosed;
    protected boolean isEscapeProcessing = true;
    protected JDBCConnection connection;
    protected int maxRows;
    protected int fetchSize = 0;
    protected int fetchDirection = JDBCResultSet.FETCH_FORWARD;
    protected Result resultIn;
    protected Result errorResult;
    protected Result generatedResult;
    protected int rsProperties;
    protected Result resultOut;
    protected Result batchResultOut;
    protected JDBCResultSet currentResultSet;
    protected JDBCResultSet generatedResultSet;
    protected SQLWarning rootWarning;
    protected int resultSetCounter;
    protected int queryTimeout;
    int connectionIncarnation;
    public synchronized void close() throws SQLException {}
    void checkClosed() throws SQLException {
        if (isClosed) {
            throw Util.sqlException(ErrorCode.X_07501);
        }
        if (connection.isClosed) {
            close();
            throw Util.sqlException(ErrorCode.X_08503);
        }
        if (connectionIncarnation != connection.incarnation ) {
            throw Util.sqlException(ErrorCode.X_08503);
        }
    }
    void performPostExecute() throws SQLException {
        resultOut.clearLobResults();
        generatedResult = null;
        if (resultIn == null) {
            return;
        }
        rootWarning = null;
        Result current = resultIn;
        while (current.getChainedResult() != null) {
            current = current.getUnlinkChainedResult();
            if (current.getType() == ResultConstants.WARNING) {
                SQLWarning w = Util.sqlWarning(current);
                if (rootWarning == null) {
                    rootWarning = w;
                } else {
                    rootWarning.setNextWarning(w);
                }
            } else if (current.getType() == ResultConstants.ERROR) {
                errorResult = current;
            } else if (current.getType() == ResultConstants.GENERATED) {
                generatedResult = current;
            } else if (current.getType() == ResultConstants.DATA) {
                resultIn.addChainedResult(current);
            }
        }
        if (rootWarning != null) {
            connection.setWarnings(rootWarning);
        }
    }
    int getUpdateCount() throws SQLException {
        checkClosed();
        return (resultIn == null || resultIn.isData()) ? -1
                                                       : resultIn
                                                       .getUpdateCount();
    }
    ResultSet getResultSet() throws SQLException {
        checkClosed();
        ResultSet result = currentResultSet;
        currentResultSet = null;
        return result;
    }
    boolean getMoreResults() throws SQLException {
        return getMoreResults(CLOSE_CURRENT_RESULT);
    }
    boolean getMoreResults(int current) throws SQLException {
        checkClosed();
        if (resultIn == null) {
            return false;
        }
        resultIn = resultIn.getChainedResult();
        if (currentResultSet != null && current != KEEP_CURRENT_RESULT) {
            currentResultSet.close();
        }
        currentResultSet = null;
        if (resultIn != null) {
            currentResultSet = new JDBCResultSet(connection,
                                                 this, resultIn,
                                                 resultIn.metaData);
            return true;
        }
        return false;
    }
    ResultSet getGeneratedResultSet() throws SQLException {
        if (generatedResultSet != null) {
            generatedResultSet.close();
        }
        if (generatedResult == null) {
            generatedResult = Result.emptyGeneratedResult;
        }
        generatedResultSet = new JDBCResultSet(connection, null,
                                               generatedResult,
                                               generatedResult.metaData);
        return generatedResultSet;
    }
    void closeResultData() throws SQLException {
        if (currentResultSet != null) {
            currentResultSet.close();
        }
        if (generatedResultSet != null) {
            generatedResultSet.close();
        }
        generatedResultSet = null;
        generatedResult    = null;
        resultIn           = null;
        currentResultSet   = null;
    }
    static final int CLOSE_CURRENT_RESULT  = 1;
    static final int KEEP_CURRENT_RESULT   = 2;
    static final int CLOSE_ALL_RESULTS     = 3;
    static final int SUCCESS_NO_INFO       = -2;
    static final int EXECUTE_FAILED        = -3;
    static final int RETURN_GENERATED_KEYS = 1;
    static final int NO_GENERATED_KEYS     = 2;
    public void closeOnCompletion() throws SQLException {
        checkClosed();
    }
    public boolean isCloseOnCompletion() throws SQLException {
        checkClosed();
        return false;
    }
}