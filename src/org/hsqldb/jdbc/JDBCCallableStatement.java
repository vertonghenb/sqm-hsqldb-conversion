package org.hsqldb.jdbc;
import java.io.Reader;
import java.io.InputStream;
import java.io.StringReader;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Date;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;
import java.sql.NClob;
import java.sql.RowId;
import java.sql.SQLXML;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.HsqlException;
import org.hsqldb.HsqlDateTime;
import org.hsqldb.SchemaObject;
import org.hsqldb.error.Error;
import org.hsqldb.lib.IntValueHashMap;
import org.hsqldb.result.ResultConstants;
import org.hsqldb.types.BinaryData;
import org.hsqldb.types.BlobDataID;
import org.hsqldb.types.ClobDataID;
import org.hsqldb.types.DateTimeType;
import org.hsqldb.types.JavaObjectData;
import org.hsqldb.types.TimeData;
import org.hsqldb.types.TimestampData;
import org.hsqldb.types.Type;
import org.hsqldb.types.Types;
public class JDBCCallableStatement extends JDBCPreparedStatement implements CallableStatement {
    public synchronized void registerOutParameter(int parameterIndex,
            int sqlType) throws SQLException {
        checkGetParameterIndex(parameterIndex);
        if (parameterModes[--parameterIndex]
                == SchemaObject.ParameterModes.PARAM_IN) {
            throw Util.invalidArgument();
        }
    }
    public synchronized void registerOutParameter(int parameterIndex,
            int sqlType, int scale) throws SQLException {
        registerOutParameter(parameterIndex, sqlType);
    }
    public synchronized boolean wasNull() throws SQLException {
        if (isClosed || connection.isClosed) {
            checkClosed();
        }
        return wasNullValue;
    }
    public synchronized String getString(
            int parameterIndex) throws SQLException {
        return (String) getColumnInType(parameterIndex, Type.SQL_VARCHAR);
    }
    public synchronized boolean getBoolean(
            int parameterIndex) throws SQLException {
        Object o = getColumnInType(parameterIndex, Type.SQL_BOOLEAN);
        return o == null ? false
                         : ((Boolean) o).booleanValue();
    }
    public synchronized byte getByte(int parameterIndex) throws SQLException {
        Object o = getColumnInType(parameterIndex, Type.TINYINT);
        return o == null ? 0
                         : ((Number) o).byteValue();
    }
    public synchronized short getShort(
            int parameterIndex) throws SQLException {
        Object o = getColumnInType(parameterIndex, Type.SQL_SMALLINT);
        return o == null ? 0
                         : ((Number) o).shortValue();
    }
    public synchronized int getInt(int parameterIndex) throws SQLException {
        Object o = getColumnInType(parameterIndex, Type.SQL_INTEGER);
        return o == null ? 0
                         : ((Number) o).intValue();
    }
    public synchronized long getLong(int parameterIndex) throws SQLException {
        Object o = getColumnInType(parameterIndex, Type.SQL_BIGINT);
        return o == null ? 0
                         : ((Number) o).longValue();
    }
    public synchronized float getFloat(
            int parameterIndex) throws SQLException {
        Object o = getColumnInType(parameterIndex, Type.SQL_DOUBLE);
        return o == null ? (float) 0.0
                         : ((Number) o).floatValue();
    }
    public synchronized double getDouble(
            int parameterIndex) throws SQLException {
        Object o = getColumnInType(parameterIndex, Type.SQL_DOUBLE);
        return o == null ? 0.0
                         : ((Number) o).doubleValue();
    }
    public synchronized BigDecimal getBigDecimal(int parameterIndex,
            int scale) throws SQLException {
        if (isClosed || connection.isClosed) {
            checkClosed();
        }
        if (scale < 0) {
            throw Util.outOfRangeArgument();
        }
        BigDecimal bd = getBigDecimal(parameterIndex);
        if (bd != null) {
            bd = bd.setScale(scale, BigDecimal.ROUND_DOWN);
        }
        return bd;
    }
    public synchronized byte[] getBytes(
            int parameterIndex) throws SQLException {
        Object x = getColumnInType(parameterIndex, Type.SQL_VARBINARY);
        if (x == null) {
            return null;
        }
        return ((BinaryData) x).getBytes();
    }
    public synchronized Date getDate(int parameterIndex) throws SQLException {
        TimestampData t = (TimestampData) getColumnInType(parameterIndex,
            Type.SQL_DATE);
        if (t == null) {
            return null;
        }
        return (Date) Type.SQL_DATE.convertSQLToJava(session, t);
    }
    public synchronized Time getTime(int parameterIndex) throws SQLException {
        TimeData t = (TimeData) getColumnInType(parameterIndex, Type.SQL_TIME);
        if (t == null) {
            return null;
        }
        return (Time) Type.SQL_TIME.convertSQLToJava(session, t);
    }
    public synchronized Timestamp getTimestamp(
            int parameterIndex) throws SQLException {
        TimestampData t = (TimestampData) getColumnInType(parameterIndex,
            Type.SQL_TIMESTAMP);
        if (t == null) {
            return null;
        }
        return (Timestamp) Type.SQL_TIMESTAMP.convertSQLToJava(session, t);
    }
    public synchronized Object getObject(
            int parameterIndex) throws SQLException {
        checkGetParameterIndex(parameterIndex);
        Type sourceType = parameterTypes[parameterIndex - 1];
        switch (sourceType.typeCode) {
            case Types.SQL_ARRAY :
                return getArray(parameterIndex);
            case Types.SQL_DATE :
                return getDate(parameterIndex);
            case Types.SQL_TIME :
            case Types.SQL_TIME_WITH_TIME_ZONE :
                return getTime(parameterIndex);
            case Types.SQL_TIMESTAMP :
            case Types.SQL_TIMESTAMP_WITH_TIME_ZONE :
                return getTimestamp(parameterIndex);
            case Types.SQL_BINARY :
            case Types.SQL_VARBINARY :
                return getBytes(parameterIndex);
            case Types.SQL_BIT : {
                boolean b = getBoolean(parameterIndex);
                return wasNull() ? null
                                 : b ? Boolean.TRUE
                                     : Boolean.FALSE;
            }
            case Types.SQL_CLOB :
                return getClob(parameterIndex);
            case Types.SQL_BLOB :
                return getBlob(parameterIndex);
            case Types.OTHER :
            case Types.JAVA_OBJECT : {
                Object o = getColumnInType(parameterIndex, sourceType);
                if (o == null) {
                    return null;
                }
                try {
                    return ((JavaObjectData) o).getObject();
                } catch (HsqlException e) {
                    throw Util.sqlException(e);
                }
            }
            default :
                return getColumnInType(parameterIndex, sourceType);
        }
    }
    public synchronized BigDecimal getBigDecimal(
            int parameterIndex) throws SQLException {
        if (isClosed || connection.isClosed) {
            checkClosed();
        }
        Type targetType = parameterMetaData.columnTypes[parameterIndex - 1];
        switch (targetType.typeCode) {
            case Types.SQL_NUMERIC :
            case Types.SQL_DECIMAL :
                break;
            case Types.TINYINT :
            case Types.SQL_SMALLINT :
            case Types.SQL_INTEGER :
            case Types.SQL_BIGINT :
                targetType = Type.SQL_DECIMAL;
                break;
            case Types.SQL_DOUBLE :
            default :
                targetType = Type.SQL_DECIMAL_DEFAULT;
                break;
        }
        return (BigDecimal) getColumnInType(parameterIndex, targetType);
    }
    public Object getObject(int parameterIndex,
                            Map<String, Class<?>> map) throws SQLException {
        checkGetParameterIndex(parameterIndex);
        throw Util.notSupported();
    }
    public Ref getRef(int parameterIndex) throws SQLException {
        checkGetParameterIndex(parameterIndex);
        throw Util.notSupported();
    }
    public synchronized Blob getBlob(int parameterIndex) throws SQLException {
        checkGetParameterIndex(parameterIndex);
        Type   sourceType = parameterMetaData.columnTypes[parameterIndex - 1];
        Object o          = getColumnInType(parameterIndex, sourceType);
        if (o == null) {
            return null;
        }
        if (o instanceof BlobDataID) {
            return new JDBCBlobClient(session, (BlobDataID) o);
        }
        throw Util.sqlException(ErrorCode.X_42561);
    }
    public synchronized Clob getClob(int parameterIndex) throws SQLException {
        checkGetParameterIndex(parameterIndex);
        Type   sourceType = parameterMetaData.columnTypes[parameterIndex - 1];
        Object o          = getColumnInType(parameterIndex, sourceType);
        if (o == null) {
            return null;
        }
        if (o instanceof ClobDataID) {
            return new JDBCClobClient(session, (ClobDataID) o);
        }
        throw Util.sqlException(ErrorCode.X_42561);
    }
    public Array getArray(int parameterIndex) throws SQLException {
        checkGetParameterIndex(parameterIndex);
        Type type = parameterMetaData.columnTypes[parameterIndex - 1];
        if (!type.isArrayType()) {
            throw Util.sqlException(ErrorCode.X_42561);
        }
        Object[] data = (Object[]) parameterValues[parameterIndex - 1];
        if (data == null) {
            return null;
        }
        return new JDBCArray(data, type.collectionBaseType(), type,
                             connection);
    }
    public synchronized Date getDate(int parameterIndex,
                                     Calendar cal) throws SQLException {
        TimestampData t = (TimestampData) getColumnInType(parameterIndex,
            Type.SQL_DATE);
        if (t == null) {
            return null;
        }
        long millis = t.getSeconds() * 1000;
        if (cal != null) {
            millis = HsqlDateTime.convertMillisToCalendar(cal, millis);
        }
        return new Date(millis);
    }
    public synchronized Time getTime(int parameterIndex,
                                     Calendar cal) throws SQLException {
        TimeData t = (TimeData) getColumnInType(parameterIndex, Type.SQL_TIME);
        if (t == null) {
            return null;
        }
        long millis = DateTimeType.normaliseTime(t.getSeconds()) * 1000;
        if (!parameterMetaData.columnTypes[--parameterIndex]
                .isDateTimeTypeWithZone()) {
            Calendar calendar = cal == null ? session.getCalendar()
                    : cal;
            millis = HsqlDateTime.convertMillisToCalendar(calendar, millis);
            millis = HsqlDateTime.getNormalisedTime(millis);
        }
        return new Time(millis);
    }
    public synchronized Timestamp getTimestamp(int parameterIndex,
            Calendar cal) throws SQLException {
        TimestampData t = (TimestampData) getColumnInType(parameterIndex,
            Type.SQL_TIMESTAMP);
        if (t == null) {
            return null;
        }
        long millis = t.getSeconds() * 1000;
        if (!parameterMetaData.columnTypes[--parameterIndex]
                .isDateTimeTypeWithZone()) {
            Calendar calendar = cal == null ? session.getCalendar()
                    : cal;
            if (cal != null) {
                millis = HsqlDateTime.convertMillisToCalendar(calendar,
                        millis);
            }
        }
        Timestamp ts = new Timestamp(millis);
        ts.setNanos(t.getNanos());
        return ts;
    }
    public synchronized void registerOutParameter(int parameterIndex,
            int sqlType, String typeName) throws SQLException {
        registerOutParameter(parameterIndex, sqlType);
    }
    public synchronized void registerOutParameter(String parameterName,
            int sqlType) throws SQLException {
        registerOutParameter(findParameterIndex(parameterName), sqlType);
    }
    public synchronized void registerOutParameter(String parameterName,
            int sqlType, int scale) throws SQLException {
        registerOutParameter(findParameterIndex(parameterName), sqlType);
    }
    public synchronized void registerOutParameter(String parameterName,
            int sqlType, String typeName) throws SQLException {
        registerOutParameter(findParameterIndex(parameterName), sqlType);
    }
    public java.net.URL getURL(int parameterIndex) throws SQLException {
        checkGetParameterIndex(parameterIndex);
        throw Util.notSupported();
    }
    public void setURL(String parameterName,
                       java.net.URL val) throws SQLException {
        setURL(findParameterIndex(parameterName), val);
    }
    public synchronized void setNull(String parameterName,
                                     int sqlType) throws SQLException {
        setNull(findParameterIndex(parameterName), sqlType);
    }
    public synchronized void setBoolean(String parameterName,
                                        boolean x) throws SQLException {
        setBoolean(findParameterIndex(parameterName), x);
    }
    public synchronized void setByte(String parameterName,
                                     byte x) throws SQLException {
        setByte(findParameterIndex(parameterName), x);
    }
    public synchronized void setShort(String parameterName,
                                      short x) throws SQLException {
        setShort(findParameterIndex(parameterName), x);
    }
    public synchronized void setInt(String parameterName,
                                    int x) throws SQLException {
        setInt(findParameterIndex(parameterName), x);
    }
    public synchronized void setLong(String parameterName,
                                     long x) throws SQLException {
        setLong(findParameterIndex(parameterName), x);
    }
    public synchronized void setFloat(String parameterName,
                                      float x) throws SQLException {
        setFloat(findParameterIndex(parameterName), x);
    }
    public synchronized void setDouble(String parameterName,
                                       double x) throws SQLException {
        setDouble(findParameterIndex(parameterName), x);
    }
    public synchronized void setBigDecimal(String parameterName,
            BigDecimal x) throws SQLException {
        setBigDecimal(findParameterIndex(parameterName), x);
    }
    public synchronized void setString(String parameterName,
                                       String x) throws SQLException {
        setString(findParameterIndex(parameterName), x);
    }
    public synchronized void setBytes(String parameterName,
                                      byte[] x) throws SQLException {
        setBytes(findParameterIndex(parameterName), x);
    }
    public synchronized void setDate(String parameterName,
                                     Date x) throws SQLException {
        setDate(findParameterIndex(parameterName), x);
    }
    public synchronized void setTime(String parameterName,
                                     Time x) throws SQLException {
        setTime(findParameterIndex(parameterName), x);
    }
    public synchronized void setTimestamp(String parameterName,
            Timestamp x) throws SQLException {
        setTimestamp(findParameterIndex(parameterName), x);
    }
    public synchronized void setAsciiStream(String parameterName,
            java.io.InputStream x, int length) throws SQLException {
        setAsciiStream(findParameterIndex(parameterName), x, length);
    }
    public synchronized void setBinaryStream(String parameterName,
            java.io.InputStream x, int length) throws SQLException {
        setBinaryStream(findParameterIndex(parameterName), x, length);
    }
    public synchronized void setObject(String parameterName, Object x,
                                       int targetSqlType,
                                       int scale) throws SQLException {
        setObject(findParameterIndex(parameterName), x, targetSqlType, scale);
    }
    public synchronized void setObject(String parameterName, Object x,
                                       int targetSqlType) throws SQLException {
        setObject(findParameterIndex(parameterName), x, targetSqlType);
    }
    public synchronized void setObject(String parameterName,
                                       Object x) throws SQLException {
        setObject(findParameterIndex(parameterName), x);
    }
    public synchronized void setCharacterStream(String parameterName,
            java.io.Reader reader, int length) throws SQLException {
        setCharacterStream(findParameterIndex(parameterName), reader, length);
    }
    public synchronized void setDate(String parameterName, Date x,
                                     Calendar cal) throws SQLException {
        setDate(findParameterIndex(parameterName), x, cal);
    }
    public synchronized void setTime(String parameterName, Time x,
                                     Calendar cal) throws SQLException {
        setTime(findParameterIndex(parameterName), x, cal);
    }
    public synchronized void setTimestamp(String parameterName, Timestamp x,
            Calendar cal) throws SQLException {
        setTimestamp(findParameterIndex(parameterName), x, cal);
    }
    public synchronized void setNull(String parameterName, int sqlType,
                                     String typeName) throws SQLException {
        setNull(findParameterIndex(parameterName), sqlType, typeName);
    }
    public synchronized String getString(
            String parameterName) throws SQLException {
        return getString(findParameterIndex(parameterName));
    }
    public synchronized boolean getBoolean(
            String parameterName) throws SQLException {
        return getBoolean(findParameterIndex(parameterName));
    }
    public synchronized byte getByte(
            String parameterName) throws SQLException {
        return getByte(findParameterIndex(parameterName));
    }
    public synchronized short getShort(
            String parameterName) throws SQLException {
        return getShort(findParameterIndex(parameterName));
    }
    public synchronized int getInt(String parameterName) throws SQLException {
        return getInt(findParameterIndex(parameterName));
    }
    public synchronized long getLong(
            String parameterName) throws SQLException {
        return getLong(findParameterIndex(parameterName));
    }
    public synchronized float getFloat(
            String parameterName) throws SQLException {
        return getFloat(findParameterIndex(parameterName));
    }
    public synchronized double getDouble(
            String parameterName) throws SQLException {
        return getDouble(findParameterIndex(parameterName));
    }
    public synchronized byte[] getBytes(
            String parameterName) throws SQLException {
        return getBytes(findParameterIndex(parameterName));
    }
    public synchronized Date getDate(
            String parameterName) throws SQLException {
        return getDate(findParameterIndex(parameterName));
    }
    public synchronized Time getTime(
            String parameterName) throws SQLException {
        return getTime(findParameterIndex(parameterName));
    }
    public synchronized Timestamp getTimestamp(
            String parameterName) throws SQLException {
        return getTimestamp(findParameterIndex(parameterName));
    }
    public synchronized Object getObject(
            String parameterName) throws SQLException {
        return getObject(findParameterIndex(parameterName));
    }
    public synchronized BigDecimal getBigDecimal(
            String parameterName) throws SQLException {
        return getBigDecimal(findParameterIndex(parameterName));
    }
    @SuppressWarnings("unchecked")
    public synchronized Object getObject(String parameterName,
            Map map) throws SQLException {
        return getObject(findParameterIndex(parameterName), map);
    }
    public synchronized Ref getRef(String parameterName) throws SQLException {
        return getRef(findParameterIndex(parameterName));
    }
    public synchronized Blob getBlob(
            String parameterName) throws SQLException {
        return getBlob(findParameterIndex(parameterName));
    }
    public synchronized Clob getClob(
            String parameterName) throws SQLException {
        return getClob(findParameterIndex(parameterName));
    }
    public synchronized Array getArray(
            String parameterName) throws SQLException {
        return getArray(findParameterIndex(parameterName));
    }
    public synchronized Date getDate(String parameterName,
                                     Calendar cal) throws SQLException {
        return getDate(findParameterIndex(parameterName), cal);
    }
    public synchronized Time getTime(String parameterName,
                                     Calendar cal) throws SQLException {
        return getTime(findParameterIndex(parameterName), cal);
    }
    public synchronized Timestamp getTimestamp(String parameterName,
            Calendar cal) throws SQLException {
        return getTimestamp(findParameterIndex(parameterName), cal);
    }
    public java.net.URL getURL(String parameterName) throws SQLException {
        return getURL(findParameterIndex(parameterName));
    }
    public RowId getRowId(int parameterIndex) throws SQLException {
        checkGetParameterIndex(parameterIndex);
        throw Util.notSupported();
    }
    public synchronized RowId getRowId(
            String parameterName) throws SQLException {
        return getRowId(findParameterIndex(parameterName));
    }
    public synchronized void setRowId(String parameterName,
                                      RowId x) throws SQLException {
        super.setRowId(findParameterIndex(parameterName), x);
    }
    public synchronized void setNString(String parameterName,
                                        String value) throws SQLException {
        super.setNString(findParameterIndex(parameterName), value);
    }
    public synchronized void setNCharacterStream(String parameterName,
            Reader value, long length) throws SQLException {
        super.setNCharacterStream(findParameterIndex(parameterName), value,
                                  length);
    }
    public synchronized void setNClob(String parameterName,
                                      NClob value) throws SQLException {
        super.setNClob(findParameterIndex(parameterName), value);
    }
    public synchronized void setClob(String parameterName, Reader reader,
                                     long length) throws SQLException {
        super.setClob(findParameterIndex(parameterName), reader, length);
    }
    public synchronized void setBlob(String parameterName,
                                     InputStream inputStream,
                                     long length) throws SQLException {
        super.setBlob(findParameterIndex(parameterName), inputStream, length);
    }
    public synchronized void setNClob(String parameterName, Reader reader,
                                      long length) throws SQLException {
        super.setNClob(findParameterIndex(parameterName), reader, length);
    }
    public NClob getNClob(int parameterIndex) throws SQLException {
        checkGetParameterIndex(parameterIndex);
        throw Util.notSupported();
    }
    public synchronized NClob getNClob(
            String parameterName) throws SQLException {
        return getNClob(findParameterIndex(parameterName));
    }
    public synchronized void setSQLXML(String parameterName,
                                       SQLXML xmlObject) throws SQLException {
        super.setSQLXML(findParameterIndex(parameterName), xmlObject);
    }
    public SQLXML getSQLXML(int parameterIndex) throws SQLException {
        checkGetParameterIndex(parameterIndex);
        throw Util.notSupported();
    }
    public synchronized SQLXML getSQLXML(
            String parameterName) throws SQLException {
        return getSQLXML(findParameterIndex(parameterName));
    }
    public String getNString(int parameterIndex) throws SQLException {
        checkGetParameterIndex(parameterIndex);
        throw Util.notSupported();
    }
    public synchronized String getNString(
            String parameterName) throws SQLException {
        return getNString(findParameterIndex(parameterName));
    }
    public Reader getNCharacterStream(int parameterIndex) throws SQLException {
        checkGetParameterIndex(parameterIndex);
        throw Util.notSupported();
    }
    public synchronized Reader getNCharacterStream(
            String parameterName) throws SQLException {
        return getNCharacterStream(findParameterIndex(parameterName));
    }
    public Reader getCharacterStream(int parameterIndex) throws SQLException {
        checkGetParameterIndex(parameterIndex);
        Type   sourceType = parameterMetaData.columnTypes[parameterIndex - 1];
        Object o          = getColumnInType(parameterIndex, sourceType);
        if (o == null) {
            return null;
        }
        if (o instanceof ClobDataID) {
            return ((ClobDataID) o).getCharacterStream(session);
        } else if (o instanceof Clob) {
            return ((Clob) o).getCharacterStream();
        } else if (o instanceof String) {
            return new StringReader((String) o);
        }
        throw Util.sqlException(ErrorCode.X_42561);
    }
    public synchronized Reader getCharacterStream(
            String parameterName) throws SQLException {
        return getCharacterStream(findParameterIndex(parameterName));
    }
    public synchronized void setBlob(String parameterName,
                                     Blob x) throws SQLException {
        super.setBlob(findParameterIndex(parameterName), x);
    }
    public synchronized void setClob(String parameterName,
                                     Clob x) throws SQLException {
        super.setClob(findParameterIndex(parameterName), x);
    }
    public synchronized void setAsciiStream(String parameterName,
            java.io.InputStream x, long length) throws SQLException {
        if (length > Integer.MAX_VALUE) {
            String msg = "Maximum ASCII input octet length exceeded: "
                         + length;    
            throw Util.sqlException(ErrorCode.JDBC_INPUTSTREAM_ERROR, msg);
        }
        this.setAsciiStream(parameterName, x, (int) length);
    }
    public synchronized void setBinaryStream(String parameterName,
            java.io.InputStream x, long length) throws SQLException {
        if (length > Integer.MAX_VALUE) {
            String msg = "Maximum Binary input octet length exceeded: "
                         + length;    
            throw Util.sqlException(ErrorCode.JDBC_INPUTSTREAM_ERROR, msg);
        }
        setBinaryStream(parameterName, x, (int) length);
    }
    public synchronized void setCharacterStream(String parameterName,
            java.io.Reader reader, long length) throws SQLException {
        if (length > Integer.MAX_VALUE) {
            String msg = "Maximum character input length exceeded: " + length;    
            throw Util.sqlException(ErrorCode.JDBC_INPUTSTREAM_ERROR, msg);
        }
        setCharacterStream(parameterName, reader, (int) length);
    }
    public synchronized void setAsciiStream(String parameterName,
            java.io.InputStream x) throws SQLException {
        super.setAsciiStream(findParameterIndex(parameterName), x);
    }
    public synchronized void setBinaryStream(String parameterName,
            java.io.InputStream x) throws SQLException {
        super.setBinaryStream(findParameterIndex(parameterName), x);
    }
    public synchronized void setCharacterStream(String parameterName,
            java.io.Reader reader) throws SQLException {
        super.setCharacterStream(findParameterIndex(parameterName), reader);
    }
    public synchronized void setNCharacterStream(String parameterName,
            Reader value) throws SQLException {
        super.setNCharacterStream(findParameterIndex(parameterName), value);
    }
    public synchronized void setClob(String parameterName,
                                     Reader reader) throws SQLException {
        super.setClob(findParameterIndex(parameterName), reader);
    }
    public synchronized void setBlob(
            String parameterName,
            InputStream inputStream) throws SQLException {
        super.setBlob(findParameterIndex(parameterName), inputStream);
    }
    public synchronized void setNClob(String parameterName,
                                      Reader reader) throws SQLException {
        super.setNClob(findParameterIndex(parameterName), reader);
    }
    public <T>T getObject(int parameterIndex,
                          Class<T> type) throws SQLException {
        return (T) this.getObject(parameterIndex);
    }
    public <T>T getObject(String parameterName,
                          Class<T> type) throws SQLException {
        return getObject(this.findParameterIndex(parameterName), type);
    }
    private IntValueHashMap parameterNameMap;
    private boolean         wasNullValue;
    public JDBCCallableStatement(
            JDBCConnection c, String sql, int resultSetType,
            int resultSetConcurrency,
            int resultSetHoldability) throws HsqlException, SQLException {
        super(c, sql, resultSetType, resultSetConcurrency,
              resultSetHoldability, ResultConstants.RETURN_NO_GENERATED_KEYS,
              null, null);
        String[] names;
        String   name;
        parameterNameMap = new IntValueHashMap();
        if (parameterMetaData != null) {
            names = parameterMetaData.columnLabels;
            for (int i = 0; i < names.length; i++) {
                name = names[i];
                if (name == null || name.length() == 0) {
                    continue;    
                }
                parameterNameMap.put(name, i);
            }
        }
    }
    void fetchResult() throws SQLException {
        super.fetchResult();
        if (resultIn.getType() == ResultConstants.CALL_RESPONSE) {
            Object[] data = resultIn.getParameterData();
            for (int i = 0; i < parameterValues.length; i++) {
                parameterValues[i] = data[i];
            }
        }
    }
    int findParameterIndex(String parameterName) throws SQLException {
        if (isClosed || connection.isClosed) {
            checkClosed();
        }
        int index = parameterNameMap.get(parameterName, -1);
        if (index >= 0) {
            return index + 1;
        }
        throw Util.sqlException(ErrorCode.JDBC_COLUMN_NOT_FOUND,
                                parameterName);
    }
    public synchronized void close() throws SQLException {
        if (isClosed()) {
            return;
        }
        parameterNameMap = null;
        super.close();
    }
    private Object getColumnInType(int columnIndex,
                                   Type targetType) throws SQLException {
        checkGetParameterIndex(columnIndex);
        Type   sourceType;
        Object value;
        sourceType = parameterTypes[--columnIndex];
        value      = parameterValues[columnIndex];
        if (trackNull(value)) {
            return null;
        }
        if (sourceType.typeCode != targetType.typeCode) {
            try {
                value = targetType.convertToTypeJDBC(session, value,
                        sourceType);
            } catch (HsqlException e) {
                String stringValue =
                    (value instanceof Number || value instanceof String
                     || value instanceof java.util.Date) ? value.toString()
                        : "instance of " + value.getClass().getName();
                String msg = "from SQL type " + sourceType.getNameString()
                             + " to " + targetType.getJDBCClassName()
                             + ", value: " + stringValue;
                HsqlException err = Error.error(ErrorCode.X_42561, msg);
                throw Util.sqlException(err, e);
            }
        }
        return value;
    }
    private boolean trackNull(Object o) {
        return (wasNullValue = (o == null));
    }
    public void closeOnCompletion() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    public boolean isCloseOnCompletion() throws SQLException {
        throw new UnsupportedOperationException("Not supported yet.");
    }
    public synchronized ResultSet executeQuery() throws SQLException {
        fetchResult();
        ResultSet rs = getResultSet();
        if (rs != null) {
            return rs;
        }
        if (getMoreResults()) {
            return getResultSet();
        }
        throw Util.sqlException(ErrorCode.X_07504);
    }
}