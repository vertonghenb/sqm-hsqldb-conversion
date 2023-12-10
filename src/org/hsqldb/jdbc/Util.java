package org.hsqldb.jdbc;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLDataException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.SQLInvalidAuthorizationSpecException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLTransactionRollbackException;
import java.sql.SQLTransientConnectionException;
import org.hsqldb.HsqlException;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.result.Result;
public class Util {
    static final void throwError(HsqlException e) throws SQLException {
        throw sqlException(e.getMessage(), e.getSQLState(), e.getErrorCode(),
                           e);
    }
    static final void throwError(Result r) throws SQLException {
        throw sqlException(r.getMainString(), r.getSubString(),
                           r.getErrorCode(), r.getException());
    }
    public static final SQLException sqlException(HsqlException e) {
        return sqlException(e.getMessage(), e.getSQLState(), e.getErrorCode(),
                            e);
    }
    public static final SQLException sqlException(HsqlException e,
            Throwable cause) {
        return sqlException(e.getMessage(), e.getSQLState(), e.getErrorCode(),
                            cause);
    }
    public static final SQLException sqlException(int id) {
        return sqlException(Error.error(id));
    }
    public static final SQLException sqlExceptionSQL(int id) {
        return sqlException(Error.error(id));
    }
    public static final SQLException sqlException(int id, String message) {
        return sqlException(Error.error(id, message));
    }
    public static final SQLException sqlException(int id, String message,
            Throwable cause) {
        return sqlException(Error.error(id, message), cause);
    }
    public static final SQLException sqlException(int id, int add) {
        return sqlException(Error.error(id, add));
    }
    static final SQLException sqlException(int id, int subId, Object[] add) {
        return sqlException(Error.error(null, id, subId, add));
    }
    static final SQLException notSupported() {
        HsqlException e = Error.error(ErrorCode.X_0A000);
        return new SQLFeatureNotSupportedException(e.getMessage(),
                e.getSQLState(), -ErrorCode.X_0A000);
    }
    static SQLException notUpdatableColumn() {
        return sqlException(ErrorCode.X_0U000);
    }
    public static SQLException nullArgument() {
        return sqlException(ErrorCode.JDBC_INVALID_ARGUMENT);
    }
    static SQLException nullArgument(String name) {
        return sqlException(ErrorCode.JDBC_INVALID_ARGUMENT, name + ": null");
    }
    public static SQLException invalidArgument() {
        return sqlException(ErrorCode.JDBC_INVALID_ARGUMENT);
    }
    public static SQLException invalidArgument(String name) {
        return sqlException(ErrorCode.JDBC_INVALID_ARGUMENT, name);
    }
    public static SQLException outOfRangeArgument() {
        return sqlException(ErrorCode.JDBC_INVALID_ARGUMENT);
    }
    public static SQLException outOfRangeArgument(String name) {
        return sqlException(ErrorCode.JDBC_INVALID_ARGUMENT, name);
    }
    public static SQLException connectionClosedException() {
        return sqlException(ErrorCode.X_08003);
    }
    public static SQLWarning sqlWarning(Result r) {
        return new SQLWarning(r.getMainString(), r.getSubString(),
                              r.getErrorCode());
    }
    public static SQLException sqlException(Throwable t) {
        return new SQLNonTransientConnectionException(t);
    }
    public static SQLException sqlException(Result r) {
        return sqlException(r.getMainString(), r.getSubString(),
                            r.getErrorCode(), r.getException());
    }
    public static final SQLException sqlException(String msg, String sqlstate,
            int code, Throwable cause) {
        if (sqlstate.startsWith("08")) {
            if (!sqlstate.endsWith("3")) {
                return new SQLTransientConnectionException(msg, sqlstate,
                        code, cause);
            } else {
                return new SQLNonTransientConnectionException(msg, sqlstate,
                        code, cause);
            }
        } else if (sqlstate.startsWith("22")) {
            return new SQLDataException(msg, sqlstate, code, cause);
        } else if (sqlstate.startsWith("23")) {
            return new SQLIntegrityConstraintViolationException(msg, sqlstate,
                    code, cause);
        } else if (sqlstate.startsWith("28")) {
            return new SQLInvalidAuthorizationSpecException(msg, sqlstate,
                    code, cause);
        } else if (sqlstate.startsWith("42") || sqlstate.startsWith("37")
                   || sqlstate.startsWith("2A")) {
            return new SQLSyntaxErrorException(msg, sqlstate, code, cause);
        } else if (sqlstate.startsWith("40")) {
            return new SQLTransactionRollbackException(msg, sqlstate, code,
                    cause);
        } else if (sqlstate.startsWith("0A")) {    
            return new SQLFeatureNotSupportedException(msg, sqlstate, code,
                    cause);
        } else {
            return new SQLException(msg, sqlstate, code, cause);
        }
    }
}