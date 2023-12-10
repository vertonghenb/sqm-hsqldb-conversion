package org.hsqldb.types;
import org.hsqldb.Session;
import org.hsqldb.SessionInterface;
import org.hsqldb.Tokens;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.jdbc.JDBCClobClient;
import org.hsqldb.lib.StringConverter;
public final class ClobType extends CharacterType {
    public static final long maxClobPrecision = 1024L * 1024 * 1024 * 1024;
    public static final int  defaultClobSize  = 1024 * 1024 * 16;
    public ClobType(long precision) {
        super(Types.SQL_CLOB, precision);
    }
    public int displaySize() {
        return precision > Integer.MAX_VALUE ? Integer.MAX_VALUE
                                             : (int) precision;
    }
    public int getJDBCTypeCode() {
        return Types.CLOB;
    }
    public Class getJDBCClass() {
        return java.sql.Clob.class;
    }
    public String getJDBCClassName() {
        return "java.sql.Clob";
    }
    public int getSQLGenericTypeCode() {
        return typeCode;
    }
    public String getDefinition() {
        long   factor     = precision;
        String multiplier = null;
        if (precision % (1024) == 0) {
            if (precision % (1024 * 1024 * 1024) == 0) {
                factor     = precision / (1024 * 1024 * 1024);
                multiplier = Tokens.T_G_FACTOR;
            } else if (precision % (1024 * 1024) == 0) {
                factor     = precision / (1024 * 1024);
                multiplier = Tokens.T_M_FACTOR;
            } else {
                factor     = precision / (1024);
                multiplier = Tokens.T_K_FACTOR;
            }
        }
        StringBuffer sb = new StringBuffer(16);
        sb.append(getNameString());
        sb.append('(');
        sb.append(factor);
        if (multiplier != null) {
            sb.append(multiplier);
        }
        sb.append(')');
        return sb.toString();
    }
    public long getMaxPrecision() {
        return maxClobPrecision;
    }
    public boolean isLobType() {
        return true;
    }
    public int compare(Session session, Object a, Object b) {
        if (a == b) {
            return 0;
        }
        if (a == null) {
            return -1;
        }
        if (b == null) {
            return 1;
        }
        if (b instanceof String) {
            return session.database.lobManager.compare((ClobData) a,
                    (String) b);
        }
        return session.database.lobManager.compare((ClobData) a, (ClobData) b);
    }
    public Object convertToDefaultType(SessionInterface session, Object a) {
        if (a == null) {
            return null;
        }
        if (a instanceof ClobData) {
            return a;
        }
        if (a instanceof String) {
            ClobData clob = session.createClob(((String) a).length());
            clob.setString(session, 0, (String) a);
            return clob;
        }
        throw Error.error(ErrorCode.X_42561);
    }
    public String convertToString(Object a) {
        if (a == null) {
            return null;
        }
        return ((ClobData) a).toString();
    }
    public String convertToSQLString(Object a) {
        if (a == null) {
            return Tokens.T_NULL;
        }
        String s = convertToString(a);
        return StringConverter.toQuotedString(s, '\'', true);
    }
    public Object convertJavaToSQL(SessionInterface session, Object a) {
        if (a == null) {
            return null;
        }
        if (a instanceof JDBCClobClient) {
            return ((JDBCClobClient) a).getClob();
        }
        throw Error.error(ErrorCode.X_42561);
    }
    public Object convertSQLToJava(SessionInterface session, Object a) {
        if (a == null) {
            return null;
        }
        if (a instanceof ClobDataID) {
            ClobDataID clob = (ClobDataID) a;
            return new JDBCClobClient(session, clob);
        }
        throw Error.error(ErrorCode.X_42561);
    }
    public long position(SessionInterface session, Object data,
                         Object otherData, Type otherType, long start) {
        if (otherType.typeCode == Types.SQL_CLOB) {
            return ((ClobData) data).position(session, (ClobData) otherData,
                                              start);
        } else if (otherType.isCharacterType()) {
            return ((ClobData) data).position(session, (String) otherData,
                                              start);
        } else {
            throw Error.runtimeError(ErrorCode.U_S0500, "ClobType");
        }
    }
}