


package org.hsqldb.error;

import java.lang.reflect.Field;

import org.hsqldb.HsqlException;
import org.hsqldb.lib.StringUtil;
import org.hsqldb.resources.BundleHandler;
import org.hsqldb.result.Result;


public class Error {

    
    public static boolean TRACE          = false;
    public static boolean TRACESYSTEMOUT = false;

    
    private static final String errPropsName = "sql-state-messages";
    private static final int bundleHandle =
        BundleHandler.getBundleHandle(errPropsName, null);
    private static final String MESSAGE_TAG      = "$$";
    private static final int    SQL_STATE_DIGITS = 5;
    private static final int    SQL_CODE_DIGITS  = 4;
    private static final int    ERROR_CODE_BASE  = 11;

    public static RuntimeException runtimeError(int code, String add) {

        HsqlException e = error(code, add);

        return new RuntimeException(e.getMessage());
    }

    public static HsqlException error(int code, String add) {
        return error((Throwable) null, code, add);
    }

    public static HsqlException error(Throwable t, int code, String add) {

        String s = getMessage(code);

        if (add != null) {
            s += ": " + add.toString();
        }

        return new HsqlException(t, s.substring(SQL_STATE_DIGITS + 1),
                                 s.substring(0, SQL_STATE_DIGITS), -code);
    }

    public static HsqlException parseError(int code, String add,
                                           int lineNumber) {

        String s = getMessage(code);

        if (add != null) {
            s = s + ": " + add;
        }

        if (lineNumber > 1) {
            add = getMessage(ErrorCode.M_parse_line);
            s   = s + " :" + add + String.valueOf(lineNumber);
        }

        return new HsqlException(null, s.substring(SQL_STATE_DIGITS + 1),
                                 s.substring(0, SQL_STATE_DIGITS), -code);
    }

    public static HsqlException error(int code) {
        return error(null, code, 0, null);
    }

    public static HsqlException error(int code, Throwable t) {

        String message = getMessage(code, 0, null);

        return new HsqlException(t, message.substring(0, SQL_STATE_DIGITS),
                                 -code);
    }

    
    public static HsqlException error(Throwable t, int code, int subCode,
                                      final Object[] add) {

        String message = getMessage(code, subCode, add);
        int    sqlCode = subCode < ERROR_CODE_BASE ? code
                                                   : subCode;

        return new HsqlException(t, message.substring(SQL_STATE_DIGITS + 1),
                                 message.substring(0, SQL_STATE_DIGITS),
                                 -sqlCode);
    }

    public static HsqlException parseError(int code, int subCode,
                                           int lineNumber,
                                           final Object[] add) {

        String message = getMessage(code, subCode, add);

        if (lineNumber > 1) {
            String sub = getMessage(ErrorCode.M_parse_line);

            message = message + " :" + sub + String.valueOf(lineNumber);
        }

        int sqlCode = subCode < ERROR_CODE_BASE ? code
                                                : subCode;

        return new HsqlException(null,
                                 message.substring(SQL_STATE_DIGITS + 1),
                                 message.substring(0, SQL_STATE_DIGITS),
                                 -sqlCode);
    }

    public static HsqlException error(int code, int code2) {
        return error(code, getMessage(code2));
    }

    
    public static HsqlException error(String message, String sqlState) {

        int code = getCode(sqlState);

        if (code < 1000) {
            code = ErrorCode.X_45000;
        }

        if (message == null) {
            message = getMessage(code);
        }

        return new HsqlException(null, message, sqlState, code);
    }

    
    private static String insertStrings(String message, Object[] add) {

        StringBuffer sb        = new StringBuffer(message.length() + 32);
        int          lastIndex = 0;
        int          escIndex  = message.length();

        
        
        
        
        for (int i = 0; i < add.length; i++) {
            escIndex = message.indexOf(MESSAGE_TAG, lastIndex);

            if (escIndex == -1) {
                break;
            }

            sb.append(message.substring(lastIndex, escIndex));
            sb.append(add[i] == null ? "null exception message"
                                     : add[i].toString());

            lastIndex = escIndex + MESSAGE_TAG.length();
        }

        escIndex = message.length();

        sb.append(message.substring(lastIndex, escIndex));

        return sb.toString();
    }

    
    public static String getMessage(final int errorCode) {
        return getResourceString(errorCode);
    }

    
    public static String getStateString(final int errorCode) {
        return getMessage(errorCode, 0, null).substring(0, SQL_STATE_DIGITS);
    }

    
    public static String getMessage(final int code, int subCode,
                                    final Object[] add) {

        String message = getResourceString(code);

        if (subCode != 0) {
            message += getResourceString(subCode);
        }

        if (add != null) {
            message = insertStrings(message, add);
        }

        return message;
    }

    private static String getResourceString(int code) {

        String key = StringUtil.toZeroPaddedString(code, SQL_CODE_DIGITS,
            SQL_CODE_DIGITS);

        return BundleHandler.getString(bundleHandle, key);
    }

    public static HsqlException error(final Result result) {
        return new HsqlException(result);
    }

    
    public static void printSystemOut(String message) {

        if (TRACESYSTEMOUT) {
            System.out.println(message);
        }
    }

    public static int getCode(String sqlState) {

        try {
            Field[] fields = ErrorCode.class.getDeclaredFields();

            for (int i = 0; i < fields.length; i++) {
                String name = fields[i].getName();

                if (name.length() == 7 && name.endsWith(sqlState)) {
                    return fields[i].getInt(ErrorCode.class);
                }
            }
        } catch (IllegalAccessException e) {}

        return -1;
    }
}
