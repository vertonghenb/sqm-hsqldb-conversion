


package org.hsqldb;

import org.hsqldb.result.Result;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;


public class HsqlException extends RuntimeException {

    
    public static final HsqlException[] emptyArray = new HsqlException[]{};
    public static final HsqlException noDataCondition =
        Error.error(ErrorCode.N_02000);

    
    private String message;
    private String state;
    private int    code;
    private int    level;
    private int    statementGroup;
    private int    statementCode;

    
    public HsqlException(Throwable t, String message, String state, int code) {

        super(t);

        this.message = message;
        this.state   = state;
        this.code    = code;
    }

    
    public HsqlException(Result r) {

        this.message = r.getMainString();
        this.state   = r.getSubString();
        this.code    = r.getErrorCode();
    }

    public HsqlException(Throwable t, String errorState, int errorCode) {

        super(t);

        this.message = t.toString();
        this.state   = errorState;
        this.code    = errorCode;
    }

    
    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    
    public String getSQLState() {
        return state;
    }

    
    public int getErrorCode() {
        return code;
    }

    public void setLevel(int level) {
        this.level = level;
    }

    public int getLevel() {
        return level;
    }

    public int getStatementCode() {
        return statementCode;
    }

    public void setStatementType(int group, int code) {
        statementGroup = group;
        statementCode  = code;
    }

    public static class HsqlRuntimeMemoryError extends OutOfMemoryError {
        HsqlRuntimeMemoryError() {}
    }

    public int hashCode() {
        return code;
    }

    public boolean equals(Object other) {

        if (other instanceof HsqlException) {
            HsqlException o = (HsqlException) other;

            return code == o.code && equals(state, o.state)
                   && equals(message, o.message);
        }

        return false;
    }

    private static boolean equals(Object a, Object b) {

        if (a == b) {
            return true;
        }

        if (a == null || b == null) {
            return false;
        }

        return a.equals(b);
    }
}
