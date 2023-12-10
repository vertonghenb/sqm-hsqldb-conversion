package org.hsqldb.server;
import org.hsqldb.result.Result;
class RecoverableOdbcFailure extends Exception {
    private String clientMessage = null;
    private String sqlStateCode = null;
    private Result errorResult = null;
    public String getSqlStateCode() {
        return sqlStateCode;
    }
    public Result getErrorResult() {
        return errorResult;
    }
    public RecoverableOdbcFailure(Result errorResult) {
        this.errorResult = errorResult;
    }
    public RecoverableOdbcFailure(String m) {
        super(m);
        clientMessage = m;
    }
    public RecoverableOdbcFailure(String m, String sqlStateCode) {
        this(m);
        this.sqlStateCode = sqlStateCode;
    }
    public RecoverableOdbcFailure(
    String ourMessage, String clientMessage, String sqlStateCode) {
        super(ourMessage);
        this.clientMessage = clientMessage;
        this.sqlStateCode = sqlStateCode;
    }
    public String getClientMessage() {
        return clientMessage;
    }
}