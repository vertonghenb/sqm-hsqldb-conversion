


package org.hsqldb.jdbc;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;


import java.sql.NClob;
import java.sql.RowId;
import java.sql.SQLXML;


import org.hsqldb.ColumnBase;
import org.hsqldb.persist.HsqlDatabaseProperties;
import org.hsqldb.HsqlDateTime;
import org.hsqldb.HsqlException;
import org.hsqldb.SessionInterface;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.IntValueHashMap;
import org.hsqldb.lib.StringInputStream;
import org.hsqldb.navigator.RowSetNavigator;
import org.hsqldb.result.Result;
import org.hsqldb.result.ResultConstants;
import org.hsqldb.result.ResultMetaData;
import org.hsqldb.result.ResultProperties;
import org.hsqldb.types.BinaryData;
import org.hsqldb.types.BlobDataID;
import org.hsqldb.types.ClobDataID;
import org.hsqldb.types.DateTimeType;
import org.hsqldb.types.IntervalType;
import org.hsqldb.types.JavaObjectData;
import org.hsqldb.types.TimeData;
import org.hsqldb.types.TimestampData;
import org.hsqldb.types.Type;
import org.hsqldb.types.Types;











public class JDBCResultSet implements ResultSet {

    
    public boolean next() throws SQLException {

        checkClosed();

        rootWarning = null;

        return navigator.next();
    }

    
    public void close() throws SQLException {

        if (navigator == null) {
            return;
        }
        navigator.release();

        navigator = null;

        if (autoClose && statement != null) {
            statement.close();
        }
    }

    
    public boolean wasNull() throws SQLException {

        checkClosed();

        return wasNullValue;
    }

    
    
    

    
    public String getString(int columnIndex) throws SQLException {

        checkColumn(columnIndex);

        Type sourceType = resultMetaData.columnTypes[columnIndex - 1];

        if (sourceType.typeCode == Types.SQL_CLOB) {
            ClobDataID x = (ClobDataID) getColumnInType(columnIndex,
                sourceType);

            if (x == null) {
                return null;
            }

            long length = x.length(session);

            if (length > Integer.MAX_VALUE) {
                Util.throwError(Error.error(ErrorCode.X_42561));
            }

            return x.getSubString(session, 0, (int) length);
        }

        return (String) getColumnInType(columnIndex, Type.SQL_VARCHAR);
    }

    
    public boolean getBoolean(int columnIndex) throws SQLException {

        Object o = getColumnInType(columnIndex, Type.SQL_BOOLEAN);

        return o == null ? false
                         : ((Boolean) o).booleanValue();
    }

    
    public byte getByte(int columnIndex) throws SQLException {

        Object o = getColumnInType(columnIndex, Type.TINYINT);

        return o == null ? 0
                         : ((Number) o).byteValue();
    }

    
    public short getShort(int columnIndex) throws SQLException {

        Object o = getColumnInType(columnIndex, Type.SQL_SMALLINT);

        return o == null ? 0
                         : ((Number) o).shortValue();
    }

    
    public int getInt(int columnIndex) throws SQLException {

        Object o = getColumnInType(columnIndex, Type.SQL_INTEGER);

        return o == null ? 0
                         : ((Number) o).intValue();
    }

    
    public long getLong(int columnIndex) throws SQLException {

        Object o = getColumnInType(columnIndex, Type.SQL_BIGINT);

        return o == null ? 0
                         : ((Number) o).longValue();
    }

    
    public float getFloat(int columnIndex) throws SQLException {

        Object o = getColumnInType(columnIndex, Type.SQL_DOUBLE);

        return o == null ? (float) 0.0
                         : ((Number) o).floatValue();
    }

    
    public double getDouble(int columnIndex) throws SQLException {

        Object o = getColumnInType(columnIndex, Type.SQL_DOUBLE);

        return o == null ? 0.0
                         : ((Number) o).doubleValue();
    }

    


    public BigDecimal getBigDecimal(int columnIndex,
                                    int scale) throws SQLException {

        if (scale < 0) {
            throw Util.outOfRangeArgument();
        }

        BigDecimal bd = getBigDecimal(columnIndex);

        if (bd != null) {
            bd = bd.setScale(scale, BigDecimal.ROUND_DOWN);
        }

        return bd;
    }



    
    public byte[] getBytes(int columnIndex) throws SQLException {

        checkColumn(columnIndex);

        Type sourceType = resultMetaData.columnTypes[columnIndex - 1];

        if (sourceType.typeCode == Types.SQL_BLOB) {
            BlobDataID x = (BlobDataID) getColumnInType(columnIndex,
                sourceType);

            if (x == null) {
                return null;
            }

            long length = x.length(session);

            if (length > Integer.MAX_VALUE) {
                Util.throwError(Error.error(ErrorCode.X_42561));
            }

            return x.getBytes(session, 0, (int) length);
        }

        Object x = getColumnInType(columnIndex, Type.SQL_VARBINARY);

        if (x == null) {
            return null;
        }

        return ((BinaryData) x).getBytes();
    }

    
    public Date getDate(int columnIndex) throws SQLException {

        Object t = getColumnInType(columnIndex, Type.SQL_DATE);

        if (t == null) {
            return null;
        }

        return (Date) Type.SQL_DATE.convertSQLToJava(session, t);
    }

    
    public Time getTime(int columnIndex) throws SQLException {

        Object t = getColumnInType(columnIndex, Type.SQL_TIME);

        if (t == null) {
            return null;
        }

        return (Time) Type.SQL_TIME.convertSQLToJava(session, t);
    }

    
    public Timestamp getTimestamp(int columnIndex) throws SQLException {

        Object t = getColumnInType(columnIndex, Type.SQL_TIMESTAMP);

        if (t == null) {
            return null;
        }

        return (Timestamp) Type.SQL_TIMESTAMP.convertSQLToJava(session, t);
    }

    
    public java.io.InputStream getAsciiStream(
            int columnIndex) throws SQLException {

        String s = getString(columnIndex);

        if (s == null) {
            return null;
        }

        try {
            return new ByteArrayInputStream(s.getBytes("US-ASCII"));
        } catch (IOException e) {
            return null;
        }
    }

    


    public java.io.InputStream getUnicodeStream(
            int columnIndex) throws SQLException {

        String s = getString(columnIndex);

        if (s == null) {
            return null;
        }

        return new StringInputStream(s);
    }



    
    public java.io.InputStream getBinaryStream(
            int columnIndex) throws SQLException {

        checkColumn(columnIndex);

        Type   sourceType = resultMetaData.columnTypes[columnIndex - 1];
        Object o          = getColumnInType(columnIndex, sourceType);

        if (o == null) {
            return null;
        }

        if (o instanceof BlobDataID) {
            return ((BlobDataID) o).getBinaryStream(session);
        } else if (o instanceof Blob) {
            return ((Blob) o).getBinaryStream();
        } else if (o instanceof BinaryData) {
            byte[] b = getBytes(columnIndex);

            return new ByteArrayInputStream(b);
        }

        throw Util.sqlException(ErrorCode.X_42561);
    }

    
    
    

    
    public String getString(String columnLabel) throws SQLException {
        return getString(findColumn(columnLabel));
    }

    
    public boolean getBoolean(String columnLabel) throws SQLException {
        return getBoolean(findColumn(columnLabel));
    }

    
    public byte getByte(String columnLabel) throws SQLException {
        return getByte(findColumn(columnLabel));
    }

    
    public short getShort(String columnLabel) throws SQLException {
        return getShort(findColumn(columnLabel));
    }

    
    public int getInt(String columnLabel) throws SQLException {
        return getInt(findColumn(columnLabel));
    }

    
    public long getLong(String columnLabel) throws SQLException {
        return getLong(findColumn(columnLabel));
    }

    
    public float getFloat(String columnLabel) throws SQLException {
        return getFloat(findColumn(columnLabel));
    }

    
    public double getDouble(String columnLabel) throws SQLException {
        return getDouble(findColumn(columnLabel));
    }

    


    public BigDecimal getBigDecimal(String columnLabel,
                                    int scale) throws SQLException {
        return getBigDecimal(findColumn(columnLabel), scale);
    }



    
    public byte[] getBytes(String columnLabel) throws SQLException {
        return getBytes(findColumn(columnLabel));
    }

    
    public Date getDate(String columnLabel) throws SQLException {
        return getDate(findColumn(columnLabel));
    }

    
    public Time getTime(String columnLabel) throws SQLException {
        return getTime(findColumn(columnLabel));
    }

    
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return getTimestamp(findColumn(columnLabel));
    }

    
    public java.io.InputStream getAsciiStream(
            String columnLabel) throws SQLException {
        return getAsciiStream(findColumn(columnLabel));
    }

    


    public java.io.InputStream getUnicodeStream(
            String columnLabel) throws SQLException {
        return getUnicodeStream(findColumn(columnLabel));
    }



    
    public java.io.InputStream getBinaryStream(
            String columnLabel) throws SQLException {
        return getBinaryStream(findColumn(columnLabel));
    }

    
    
    

    
    public SQLWarning getWarnings() throws SQLException {

        checkClosed();

        return rootWarning;
    }

    
    public void clearWarnings() throws SQLException {

        checkClosed();

        rootWarning = null;
    }

    
    public String getCursorName() throws SQLException {

        checkClosed();

        if (result == null) {
            return "";
        }

        return result.getMainString();
    }

    
    public ResultSetMetaData getMetaData() throws SQLException {

        checkClosed();

        if (resultSetMetaData == null) {
            resultSetMetaData = new JDBCResultSetMetaData(resultMetaData,
                    isUpdatable, isInsertable, connection);
        }

        return resultSetMetaData;
    }

    
    public Object getObject(int columnIndex) throws SQLException {

        checkColumn(columnIndex);

        Type sourceType = resultMetaData.columnTypes[columnIndex - 1];

        switch (sourceType.typeCode) {

            case Types.SQL_ARRAY :
                return getArray(columnIndex);
            case Types.SQL_DATE :
                return getDate(columnIndex);
            case Types.SQL_TIME :
            case Types.SQL_TIME_WITH_TIME_ZONE :
                return getTime(columnIndex);
            case Types.SQL_TIMESTAMP :
            case Types.SQL_TIMESTAMP_WITH_TIME_ZONE :
                return getTimestamp(columnIndex);
            case Types.SQL_BINARY :
            case Types.SQL_VARBINARY :
                return getBytes(columnIndex);
            case Types.SQL_BIT : {
                boolean b = getBoolean(columnIndex);

                return wasNull() ? null
                                 : b ? Boolean.TRUE
                                     : Boolean.FALSE;
            }
            case Types.SQL_CLOB :
                return getClob(columnIndex);
            case Types.SQL_BLOB :
                return getBlob(columnIndex);
            case Types.OTHER :
            case Types.JAVA_OBJECT : {
                Object o = getColumnInType(columnIndex, sourceType);

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
                return getColumnInType(columnIndex, sourceType);
        }
    }

    
    public Object getObject(String columnLabel) throws SQLException {
        return getObject(findColumn(columnLabel));
    }

    

    
    public int findColumn(final String columnLabel) throws SQLException {

        checkClosed();

        if (columnLabel == null) {
            throw Util.nullArgument();
        }

        int columnIndex;

        
        if (columnMap != null) {
            columnIndex = columnMap.get(columnLabel, -1);

            if (columnIndex != -1) {
                return columnIndex;
            }
        }

        final String[] colLabels = resultMetaData.columnLabels;

        columnIndex = -1;

        
        for (int i = 0; i < columnCount; i++) {
            if (columnLabel.equalsIgnoreCase(colLabels[i])) {
                columnIndex = i;

                break;
            }
        }

        final ColumnBase[] columns = resultMetaData.columns;

        
        
        if (columnIndex < 0) {
            for (int i = 0; i < columnCount; i++) {
                if (columnLabel.equalsIgnoreCase(columns[i].getNameString())) {
                    columnIndex = i;

                    break;
                }
            }
        }

        
        
        
        
        if (columnIndex < 0) {
            int position = columnLabel.indexOf('.');

            if (position < 0) {
                throw Util.sqlException(ErrorCode.JDBC_COLUMN_NOT_FOUND,
                                        columnLabel);
            }

            for (int i = 0; i < columnCount; i++) {
                final String tabName = columns[i].getTableNameString();

                if (tabName == null || tabName.length() == 0) {
                    continue;
                }

                final String colName = columns[i].getNameString();

                if (columnLabel.equalsIgnoreCase(tabName + '.' + colName)) {
                    columnIndex = i;

                    break;
                }

                final String schemName = columns[i].getSchemaNameString();

                if (schemName == null || schemName.length() == 0) {
                    continue;
                }

                String match = new StringBuffer(schemName).append('.').append(
                    tabName).append('.').append(colName).toString();

                if (columnLabel.equalsIgnoreCase(match)) {
                    columnIndex = i;

                    break;
                }
            }
        }

        if (columnIndex < 0) {
            throw Util.sqlException(ErrorCode.JDBC_COLUMN_NOT_FOUND,
                                    columnLabel);
        }
        columnIndex++;

        if (columnMap == null) {
            columnMap = new IntValueHashMap();
        }
        columnMap.put(columnLabel, columnIndex);

        return columnIndex;
    }

    
    
    
    

    
    public java.io.Reader getCharacterStream(
            int columnIndex) throws SQLException {

        checkColumn(columnIndex);

        Type   sourceType = resultMetaData.columnTypes[columnIndex - 1];
        Object o          = getColumnInType(columnIndex, sourceType);

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

    
    public java.io.Reader getCharacterStream(
            String columnLabel) throws SQLException {
        return getCharacterStream(findColumn(columnLabel));
    }

    
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {

        checkColumn(columnIndex);

        Type targetType = resultMetaData.columnTypes[columnIndex - 1];

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

        return (BigDecimal) getColumnInType(columnIndex, targetType);
    }

    
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return getBigDecimal(findColumn(columnLabel));
    }

    
    
    

    
    public boolean isBeforeFirst() throws SQLException {

        checkClosed();

        if (isOnInsertRow) {
            return false;
        }

        return navigator.isBeforeFirst();
    }

    
    public boolean isAfterLast() throws SQLException {

        
        
        
        checkClosed();

        if (isOnInsertRow) {
            return false;
        }

        return navigator.isAfterLast();
    }

    
    public boolean isFirst() throws SQLException {

        checkClosed();

        if (isOnInsertRow) {
            return false;
        }

        return navigator.isFirst();
    }

    
    public boolean isLast() throws SQLException {

        checkClosed();

        if (isOnInsertRow) {
            return false;
        }

        return navigator.isLast();
    }

    
    public void beforeFirst() throws SQLException {

        checkClosed();
        checkNotForwardOnly();

        if (isOnInsertRow || isRowUpdated) {
            throw Util.sqlExceptionSQL(ErrorCode.X_24513);
        }
        navigator.beforeFirst();
    }

    
    public void afterLast() throws SQLException {

        checkClosed();
        checkNotForwardOnly();

        if (isOnInsertRow || isRowUpdated) {
            throw Util.sqlExceptionSQL(ErrorCode.X_24513);
        }
        navigator.afterLast();
    }

    
    public boolean first() throws SQLException {

        checkClosed();
        checkNotForwardOnly();

        if (isOnInsertRow || isRowUpdated) {
            throw Util.sqlExceptionSQL(ErrorCode.X_24513);
        }

        return navigator.first();
    }

    
    public boolean last() throws SQLException {

        checkClosed();
        checkNotForwardOnly();

        if (isOnInsertRow || isRowUpdated) {
            throw Util.sqlExceptionSQL(ErrorCode.X_24513);
        }

        return navigator.last();
    }

    
    public int getRow() throws SQLException {

        checkClosed();

        if (navigator.isAfterLast()) {
            return 0;
        }

        return navigator.getRowNumber() + 1;
    }

    
    public boolean absolute(int row) throws SQLException {

        checkClosed();
        checkNotForwardOnly();

        if (isOnInsertRow || isRowUpdated) {
            throw Util.sqlExceptionSQL(ErrorCode.X_24513);
        }

        if (row > 0) {
            row--;
        } else if (row == 0) {
            return navigator.beforeFirst();
        }

        return navigator.absolute(row);
    }

    
    public boolean relative(int rows) throws SQLException {

        checkClosed();
        checkNotForwardOnly();

        if (isOnInsertRow || isRowUpdated) {
            throw Util.sqlExceptionSQL(ErrorCode.X_24513);
        }

        return navigator.relative(rows);
    }

    
    public boolean previous() throws SQLException {

        checkClosed();
        checkNotForwardOnly();

        if (isOnInsertRow || isRowUpdated) {
            throw Util.sqlExceptionSQL(ErrorCode.X_24513);
        }
        rootWarning = null;

        return navigator.previous();
    }

    
    
    

    
    public void setFetchDirection(int direction) throws SQLException {

        checkClosed();

        switch (direction) {

            case FETCH_FORWARD : {
                break;
            }
            case FETCH_REVERSE : {
                checkNotForwardOnly();

                break;
            }
            case FETCH_UNKNOWN : {
                checkNotForwardOnly();

                break;
            }
            default : {
                throw Util.notSupported();
            }
        }
    }

    
    public int getFetchDirection() throws SQLException {

        checkClosed();

        return FETCH_FORWARD;
    }

    
    public void setFetchSize(int rows) throws SQLException {

        if (rows < 0) {
            throw Util.outOfRangeArgument();
        }
    }

    
    public int getFetchSize() throws SQLException {

        checkClosed();

        return fetchSize;
    }

    
    public int getType() throws SQLException {

        checkClosed();

        return ResultProperties.getJDBCScrollability(rsProperties);
    }

    
    public int getConcurrency() throws SQLException {

        checkClosed();

        return ResultProperties.getJDBCConcurrency(rsProperties);
    }

    
    
    

    
    public boolean rowUpdated() throws SQLException {

        checkClosed();

        return isRowUpdated;
    }

    
    public boolean rowInserted() throws SQLException {

        checkClosed();

        return false;
    }

    
    public boolean rowDeleted() throws SQLException {

        checkClosed();

        return false;
    }

    
    public void updateNull(int columnIndex) throws SQLException {
        startUpdate(columnIndex);
        preparedStatement.setParameter(columnIndex, null);
    }

    
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {

        Boolean value = x ? Boolean.TRUE
                          : Boolean.FALSE;

        startUpdate(columnIndex);
        preparedStatement.setParameter(columnIndex, value);
    }

    
    public void updateByte(int columnIndex, byte x) throws SQLException {
        startUpdate(columnIndex);
        preparedStatement.setIntParameter(columnIndex, x);
    }

    
    public void updateShort(int columnIndex, short x) throws SQLException {
        startUpdate(columnIndex);
        preparedStatement.setIntParameter(columnIndex, x);
    }

    
    public void updateInt(int columnIndex, int x) throws SQLException {
        startUpdate(columnIndex);
        preparedStatement.setIntParameter(columnIndex, x);
    }

    
    public void updateLong(int columnIndex, long x) throws SQLException {
        startUpdate(columnIndex);
        preparedStatement.setLongParameter(columnIndex, x);
    }

    
    public void updateFloat(int columnIndex, float x) throws SQLException {

        Double value = new Double(x);

        startUpdate(columnIndex);
        preparedStatement.setParameter(columnIndex, value);
    }

    
    public void updateDouble(int columnIndex, double x) throws SQLException {

        Double value = new Double(x);

        startUpdate(columnIndex);
        preparedStatement.setParameter(columnIndex, value);
    }

    
    public void updateBigDecimal(int columnIndex,
                                 BigDecimal x) throws SQLException {
        startUpdate(columnIndex);
        preparedStatement.setParameter(columnIndex, x);
    }

    
    public void updateString(int columnIndex, String x) throws SQLException {
        startUpdate(columnIndex);
        preparedStatement.setParameter(columnIndex, x);
    }

    
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        startUpdate(columnIndex);
        preparedStatement.setParameter(columnIndex, x);
    }

    
    public void updateDate(int columnIndex, Date x) throws SQLException {
        startUpdate(columnIndex);
        preparedStatement.setParameter(columnIndex, x);
    }

    
    public void updateTime(int columnIndex, Time x) throws SQLException {
        startUpdate(columnIndex);
        preparedStatement.setParameter(columnIndex, x);
    }

    
    public void updateTimestamp(int columnIndex,
                                Timestamp x) throws SQLException {
        startUpdate(columnIndex);
        preparedStatement.setParameter(columnIndex, x);
    }

    
    public void updateAsciiStream(int columnIndex, java.io.InputStream x,
                                  int length) throws SQLException {
        startUpdate(columnIndex);
        preparedStatement.setAsciiStream(columnIndex, x, length);
    }

    
    public void updateBinaryStream(int columnIndex, java.io.InputStream x,
                                   int length) throws SQLException {
        startUpdate(columnIndex);
        preparedStatement.setBinaryStream(columnIndex, x, length);
    }

    
    public void updateCharacterStream(int columnIndex, java.io.Reader x,
                                      int length) throws SQLException {
        startUpdate(columnIndex);
        preparedStatement.setCharacterStream(columnIndex, x, length);
    }

    
    public void updateObject(int columnIndex, Object x,
                             int scaleOrLength) throws SQLException {
        startUpdate(columnIndex);
        preparedStatement.setObject(columnIndex, x, 0, scaleOrLength);
    }

    
    public void updateObject(int columnIndex, Object x) throws SQLException {
        startUpdate(columnIndex);
        preparedStatement.setParameter(columnIndex, x);
    }

    
    public void updateNull(String columnLabel) throws SQLException {
        updateNull(findColumn(columnLabel));
    }

    
    public void updateBoolean(String columnLabel,
                              boolean x) throws SQLException {
        updateBoolean(findColumn(columnLabel), x);
    }

    
    public void updateByte(String columnLabel, byte x) throws SQLException {
        updateByte(findColumn(columnLabel), x);
    }

    
    public void updateShort(String columnLabel, short x) throws SQLException {
        updateShort(findColumn(columnLabel), x);
    }

    
    public void updateInt(String columnLabel, int x) throws SQLException {
        updateInt(findColumn(columnLabel), x);
    }

    
    public void updateLong(String columnLabel, long x) throws SQLException {
        updateLong(findColumn(columnLabel), x);
    }

    
    public void updateFloat(String columnLabel, float x) throws SQLException {
        updateFloat(findColumn(columnLabel), x);
    }

    
    public void updateDouble(String columnLabel,
                             double x) throws SQLException {
        updateDouble(findColumn(columnLabel), x);
    }

    
    public void updateBigDecimal(String columnLabel,
                                 BigDecimal x) throws SQLException {
        updateBigDecimal(findColumn(columnLabel), x);
    }

    
    public void updateString(String columnLabel,
                             String x) throws SQLException {
        updateString(findColumn(columnLabel), x);
    }

    
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        updateBytes(findColumn(columnLabel), x);
    }

    
    public void updateDate(String columnLabel, Date x) throws SQLException {
        updateDate(findColumn(columnLabel), x);
    }

    
    public void updateTime(String columnLabel, Time x) throws SQLException {
        updateTime(findColumn(columnLabel), x);
    }

    
    public void updateTimestamp(String columnLabel,
                                Timestamp x) throws SQLException {
        updateTimestamp(findColumn(columnLabel), x);
    }

    
    public void updateAsciiStream(String columnLabel, java.io.InputStream x,
                                  int length) throws SQLException {
        updateAsciiStream(findColumn(columnLabel), x, length);
    }

    
    public void updateBinaryStream(String columnLabel, java.io.InputStream x,
                                   int length) throws SQLException {
        updateBinaryStream(findColumn(columnLabel), x, length);
    }

    
    public void updateCharacterStream(String columnLabel,
                                      java.io.Reader reader,
                                      int length) throws SQLException {
        updateCharacterStream(findColumn(columnLabel), reader, length);
    }

    
    public void updateObject(String columnLabel, Object x,
                             int scaleOrLength) throws SQLException {
        updateObject(findColumn(columnLabel), x, scaleOrLength);
    }

    
    public void updateObject(String columnLabel,
                             Object x) throws SQLException {
        updateObject(findColumn(columnLabel), x);
    }

    
    public void insertRow() throws SQLException {
        performInsert();
    }

    
    public void updateRow() throws SQLException {
        performUpdate();
    }

    
    public void deleteRow() throws SQLException {
        performDelete();
    }

    

    
    public void refreshRow() throws SQLException {
        clearUpdates();
    }

    
    public void cancelRowUpdates() throws SQLException {
        clearUpdates();
    }

    
    public void moveToInsertRow() throws SQLException {
        startInsert();
    }

    
    public void moveToCurrentRow() throws SQLException {
        endInsert();
    }

    
    public Statement getStatement() throws SQLException {

        checkClosed();

        return (Statement) statement;
    }

    
    public Object getObject(int columnIndex, Map map) throws SQLException {
        return getObject(columnIndex);
    }

    
    public Ref getRef(int columnIndex) throws SQLException {
        throw Util.notSupported();
    }

    
    public Blob getBlob(int columnIndex) throws SQLException {

        checkColumn(columnIndex);

        Type   sourceType = resultMetaData.columnTypes[columnIndex - 1];
        Object o          = getColumnInType(columnIndex, sourceType);

        if (o == null) {
            return null;
        }

        if (o instanceof BlobDataID) {
            JDBCBlobClient blob = new JDBCBlobClient(session, (BlobDataID) o);

            if (isUpdatable) {
                if (resultMetaData.colIndexes[columnIndex - 1] > 0
                        && resultMetaData.columns[columnIndex - 1]
                            .isWriteable()) {
                    blob.setWritable(this, columnIndex - 1);
                }
            }

            return blob;
        } else if (o instanceof Blob) {
            return (Blob) o;
        } else if (o instanceof BinaryData) {
            byte[] b = getBytes(columnIndex);

            return new JDBCBlob(b);
        }

        throw Util.sqlException(ErrorCode.X_42561);
    }

    
    public Clob getClob(int columnIndex) throws SQLException {

        checkColumn(columnIndex);

        Type   sourceType = resultMetaData.columnTypes[columnIndex - 1];
        Object o          = getColumnInType(columnIndex, sourceType);

        if (o == null) {
            return null;
        }

        if (o instanceof ClobDataID) {
            JDBCClobClient clob = new JDBCClobClient(session, (ClobDataID) o);

            if (isUpdatable) {
                if (resultMetaData.colIndexes[columnIndex - 1] > 0
                        && resultMetaData.columns[columnIndex - 1]
                            .isWriteable()) {
                    clob.setWritable(this, columnIndex - 1);
                }
            }

            return clob;
        } else if (o instanceof Clob) {
            return (Clob) o;
        } else if (o instanceof String) {
            return new JDBCClob((String) o);
        }

        throw Util.sqlException(ErrorCode.X_42561);
    }

    
    public Array getArray(int columnIndex) throws SQLException {

        checkColumn(columnIndex);

        Type     type = resultMetaData.columnTypes[columnIndex - 1];
        Object[] data = (Object[]) getCurrent()[columnIndex - 1];

        if (!type.isArrayType()) {
            throw Util.sqlException(ErrorCode.X_42561);
        }

        if (trackNull(data)) {
            return null;
        }

        return new JDBCArray(data, type.collectionBaseType(), type,
                             connection);
    }

    
    public Object getObject(String columnLabel, Map map) throws SQLException {
        return getObject(findColumn(columnLabel), map);
    }

    
    public Ref getRef(String columnLabel) throws SQLException {
        return getRef(findColumn(columnLabel));
    }

    
    public Blob getBlob(String columnLabel) throws SQLException {
        return getBlob(findColumn(columnLabel));
    }

    
    public Clob getClob(String columnLabel) throws SQLException {
        return getClob(findColumn(columnLabel));
    }

    
    public Array getArray(String columnLabel) throws SQLException {
        return getArray(findColumn(columnLabel));
    }

    
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {

        TimestampData t = (TimestampData) getColumnInType(columnIndex,
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

    
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return getDate(findColumn(columnLabel), cal);
    }

    
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {

        TimeData t = (TimeData) getColumnInType(columnIndex, Type.SQL_TIME);

        if (t == null) {
            return null;
        }

        long millis = DateTimeType.normaliseTime(t.getSeconds()) * 1000;

        if (!resultMetaData.columnTypes[--columnIndex]
                .isDateTimeTypeWithZone()) {
            Calendar calendar = cal == null ? session.getCalendar()
                    : cal;

            millis = HsqlDateTime.convertMillisToCalendar(calendar, millis);
            millis = HsqlDateTime.getNormalisedTime(millis);
        }

        return new Time(millis);
    }

    
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        return getTime(findColumn(columnLabel), cal);
    }

    
    public Timestamp getTimestamp(int columnIndex,
                                  Calendar cal) throws SQLException {

        TimestampData t = (TimestampData) getColumnInType(columnIndex,
            Type.SQL_TIMESTAMP);

        if (t == null) {
            return null;
        }

        long millis = t.getSeconds() * 1000;

        if (!resultMetaData.columnTypes[--columnIndex]
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

    
    public Timestamp getTimestamp(String columnLabel,
                                  Calendar cal) throws SQLException {
        return getTimestamp(findColumn(columnLabel), cal);
    }

    

    

    public java.net.URL getURL(int columnIndex) throws SQLException {
        throw Util.notSupported();
    }



    

    public java.net.URL getURL(String columnLabel) throws SQLException {
        throw Util.notSupported();
    }



    

    public void updateRef(int columnIndex,
                          java.sql.Ref x) throws SQLException {
        throw Util.notSupported();
    }



    

    public void updateRef(String columnLabel,
                          java.sql.Ref x) throws SQLException {
        throw Util.notSupported();
    }



    

    public void updateBlob(int columnIndex,
                           java.sql.Blob x) throws SQLException {
        startUpdate(columnIndex);
        preparedStatement.setBlobParameter(columnIndex, x);
    }



    

    public void updateBlob(String columnLabel,
                           java.sql.Blob x) throws SQLException {

        int columnIndex = findColumn(columnLabel);

        updateBlob(columnIndex, x);
    }



    

    public void updateClob(int columnIndex,
                           java.sql.Clob x) throws SQLException {
        startUpdate(columnIndex);
        preparedStatement.setClobParameter(columnIndex, x);
    }



    

    public void updateClob(String columnLabel,
                           java.sql.Clob x) throws SQLException {

        int columnIndex = findColumn(columnLabel);

        updateClob(columnIndex, x);
    }



    

    public void updateArray(int columnIndex,
                            java.sql.Array x) throws SQLException {
        startUpdate(columnIndex);
        preparedStatement.setParameter(columnIndex, x);
    }



    

    public void updateArray(String columnLabel,
                            java.sql.Array x) throws SQLException {

        int columnIndex = findColumn(columnLabel);

        updateArray(columnIndex, x);
    }


    

    

    public RowId getRowId(int columnIndex) throws SQLException {
        throw Util.notSupported();
    }



    

    public RowId getRowId(String columnLabel) throws SQLException {
        throw Util.notSupported();
    }



    

    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw Util.notSupported();
    }



    

    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        throw Util.notSupported();
    }



    
    public int getHoldability() throws SQLException {

        checkClosed();

        return ResultProperties.getJDBCHoldability(rsProperties);
    }

    
    public boolean isClosed() throws SQLException {
        return navigator == null;
    }

    


    public void updateNString(int columnIndex,
                              String nString) throws SQLException {
        updateString(columnIndex, nString);
    }



    

    public void updateNString(String columnLabel,
                              String nString) throws SQLException {
        updateString(columnLabel, nString);
    }



    

    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        updateClob(columnIndex, nClob);
    }



    

    public void updateNClob(String columnLabel,
                            NClob nClob) throws SQLException {
        updateClob(columnLabel, nClob);
    }



    

    public NClob getNClob(int columnIndex) throws SQLException {

        String s = getString(columnIndex);

        return s == null ? null
                         : new JDBCNClob(s);
    }



    

    public NClob getNClob(String columnLabel) throws SQLException {
        return getNClob(findColumn(columnLabel));
    }



    

    public SQLXML getSQLXML(int columnIndex) throws SQLException {

        checkColumn(columnIndex);

        SQLXML sqlxml;
        int    type = resultMetaData.columnTypes[columnIndex - 1].typeCode;

        switch (type) {

            case Types.SQL_XML : {
                Object object = getObject(columnIndex);

                if (object == null) {
                    sqlxml = null;
                } else if (object instanceof SQLXML) {
                    sqlxml = (SQLXML) object;
                } else {
                    throw Util.notSupported();
                }

                break;
            }
            case Types.SQL_CLOB : {
                Clob clob = getClob(columnIndex);

                if (clob == null) {
                    sqlxml = null;
                } else {
                    sqlxml = new JDBCSQLXML(clob.getCharacterStream());
                }

                break;
            }
            case Types.SQL_CHAR :
            case Types.SQL_VARCHAR :
            case Types.VARCHAR_IGNORECASE : {
                java.io.Reader reader = getCharacterStream(columnIndex);

                if (reader == null) {
                    sqlxml = null;
                } else {
                    sqlxml = new JDBCSQLXML(reader);
                }

                break;
            }
            case Types.SQL_NCHAR :
            case Types.SQL_NVARCHAR : {
                java.io.Reader nreader = getNCharacterStream(columnIndex);

                if (nreader == null) {
                    sqlxml = null;
                } else {
                    sqlxml = new JDBCSQLXML(nreader);
                }

                break;
            }
            case Types.SQL_BLOB : {
                Blob blob = getBlob(columnIndex);

                if (blob == null) {
                    sqlxml = null;
                } else {
                    sqlxml = new JDBCSQLXML(blob.getBinaryStream());
                }

                break;
            }
            case Types.SQL_BINARY :
            case Types.SQL_VARBINARY : {
                java.io.InputStream inputStream = getBinaryStream(columnIndex);

                if (inputStream == null) {
                    sqlxml = null;
                } else {
                    sqlxml = new JDBCSQLXML(inputStream);
                }

                break;
            }
            case Types.OTHER :
            case Types.JAVA_OBJECT : {
                Object data = getObject(columnIndex);

                if (data == null) {
                    sqlxml = null;
                } else if (data instanceof SQLXML) {
                    sqlxml = (SQLXML) data;
                } else if (data instanceof String) {
                    sqlxml = new JDBCSQLXML((String) data);
                } else if (data instanceof byte[]) {
                    sqlxml = new JDBCSQLXML((byte[]) data);
                } else if (data instanceof Blob) {
                    Blob blob = (Blob) data;

                    sqlxml = new JDBCSQLXML(blob.getBinaryStream());
                } else if (data instanceof Clob) {
                    Clob clob = (Clob) data;

                    sqlxml = new JDBCSQLXML(clob.getCharacterStream());
                } else {
                    throw Util.notSupported();
                }

                break;
            }
            default : {
                throw Util.notSupported();
            }
        }

        return sqlxml;
    }



    

    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        return getSQLXML(findColumn(columnLabel));
    }



    

    public void updateSQLXML(int columnIndex,
                             SQLXML xmlObject) throws SQLException {
        startUpdate(columnIndex);
        preparedStatement.setSQLXML(columnIndex, xmlObject);
    }



    

    public void updateSQLXML(String columnLabel,
                             SQLXML xmlObject) throws SQLException {
        updateSQLXML(findColumn(columnLabel), xmlObject);
    }



    

    public String getNString(int columnIndex) throws SQLException {
        return getString(columnIndex);
    }



    

    public String getNString(String columnLabel) throws SQLException {
        return getString(findColumn(columnLabel));
    }



    

    public java.io.Reader getNCharacterStream(
            int columnIndex) throws SQLException {
        return getCharacterStream(columnIndex);
    }



    

    public java.io.Reader getNCharacterStream(
            String columnLabel) throws SQLException {
        return getCharacterStream(findColumn(columnLabel));
    }



    

    public void updateNCharacterStream(int columnIndex, java.io.Reader x,
                                       long length) throws SQLException {
        startUpdate(columnIndex);
        preparedStatement.setCharacterStream(columnIndex, x, length);
    }



    

    public void updateNCharacterStream(String columnLabel,
                                       java.io.Reader reader,
                                       long length) throws SQLException {
        updateCharacterStream(columnLabel, reader, length);
    }




    

    public void updateAsciiStream(int columnIndex, java.io.InputStream x,
                                  long length) throws SQLException {
        startUpdate(columnIndex);
        preparedStatement.setAsciiStream(columnIndex, x, length);
    }



    

    public void updateBinaryStream(int columnIndex, java.io.InputStream x,
                                   long length) throws SQLException {
        startUpdate(columnIndex);
        preparedStatement.setBinaryStream(columnIndex, x, length);
    }



    

    public void updateCharacterStream(int columnIndex, java.io.Reader x,
                                      long length) throws SQLException {
        startUpdate(columnIndex);
        preparedStatement.setCharacterStream(columnIndex, x, length);
    }



    

    public void updateAsciiStream(String columnLabel, java.io.InputStream x,
                                  long length) throws SQLException {

        int columnIndex = findColumn(columnLabel);

        startUpdate(columnIndex);
        preparedStatement.setAsciiStream(columnIndex, x, length);
    }



    

    public void updateBinaryStream(String columnLabel, java.io.InputStream x,
                                   long length) throws SQLException {

        int columnIndex = findColumn(columnLabel);

        startUpdate(columnIndex);
        preparedStatement.setBinaryStream(columnIndex, x, length);
    }



    

    public void updateCharacterStream(String columnLabel,
                                      java.io.Reader reader,
                                      long length) throws SQLException {

        int columnIndex = findColumn(columnLabel);

        startUpdate(columnIndex);
        preparedStatement.setCharacterStream(columnIndex, reader, length);
    }



    

    public void updateBlob(int columnIndex, InputStream inputStream,
                           long length) throws SQLException {
        startUpdate(columnIndex);
        preparedStatement.setBlob(columnIndex, inputStream, length);
    }



    

    public void updateBlob(String columnLabel, InputStream inputStream,
                           long length) throws SQLException {

        int columnIndex = findColumn(columnLabel);

        startUpdate(columnIndex);
        preparedStatement.setBlob(columnIndex, inputStream, length);
    }



    

    public void updateClob(int columnIndex, Reader reader,
                           long length) throws SQLException {
        startUpdate(columnIndex);
        preparedStatement.setClob(columnIndex, reader, length);
    }



    

    public void updateClob(String columnLabel, Reader reader,
                           long length) throws SQLException {

        int columnIndex = findColumn(columnLabel);

        startUpdate(columnIndex);
        preparedStatement.setClob(columnIndex, reader, length);
    }



    

    public void updateNClob(int columnIndex, Reader reader,
                            long length) throws SQLException {
        startUpdate(columnIndex);
        preparedStatement.setClob(columnIndex, reader, length);
    }



    

    public void updateNClob(String columnLabel, Reader reader,
                            long length) throws SQLException {

        int columnIndex = findColumn(columnLabel);

        startUpdate(columnIndex);
        preparedStatement.setClob(columnIndex, reader, length);
    }



    
    public void updateNCharacterStream(
            int columnIndex, java.io.Reader reader) throws SQLException {
        startUpdate(columnIndex);
        preparedStatement.setCharacterStream(columnIndex, reader);
    }

    
    public void updateNCharacterStream(
            String columnLabel, java.io.Reader reader) throws SQLException {

        int columnIndex = findColumn(columnLabel);

        startUpdate(columnIndex);
        preparedStatement.setCharacterStream(columnIndex, reader);
    }

    
    public void updateAsciiStream(int columnIndex,
                                  java.io.InputStream x) throws SQLException {
        startUpdate(columnIndex);
        preparedStatement.setAsciiStream(columnIndex, x);
    }

    
    public void updateBinaryStream(int columnIndex,
                                   java.io.InputStream x) throws SQLException {
        startUpdate(columnIndex);
        preparedStatement.setBinaryStream(columnIndex, x);
    }

    
    public void updateCharacterStream(int columnIndex,
                                      java.io.Reader x) throws SQLException {
        startUpdate(columnIndex);
        preparedStatement.setCharacterStream(columnIndex, x);
    }

    
    public void updateAsciiStream(String columnLabel,
                                  java.io.InputStream x) throws SQLException {

        int columnIndex = findColumn(columnLabel);

        startUpdate(columnIndex);
        preparedStatement.setAsciiStream(columnIndex, x);
    }

    
    public void updateBinaryStream(String columnLabel,
                                   java.io.InputStream x) throws SQLException {

        int columnIndex = findColumn(columnLabel);

        startUpdate(columnIndex);
        preparedStatement.setBinaryStream(columnIndex, x);
    }

    
    public void updateCharacterStream(
            String columnLabel, java.io.Reader reader) throws SQLException {

        int columnIndex = findColumn(columnLabel);

        startUpdate(columnIndex);
        preparedStatement.setCharacterStream(columnIndex, reader);
    }

    
    public void updateBlob(int columnIndex,
                           InputStream inputStream) throws SQLException {
        startUpdate(columnIndex);
        preparedStatement.setBlob(columnIndex, inputStream);
    }

    
    public void updateBlob(String columnLabel,
                           InputStream inputStream) throws SQLException {

        int columnIndex = findColumn(columnLabel);

        startUpdate(columnIndex);
        preparedStatement.setBlob(columnIndex, inputStream);
    }

    
    public void updateClob(int columnIndex,
                           Reader reader) throws SQLException {
        startUpdate(columnIndex);
        preparedStatement.setClob(columnIndex, reader);
    }

    
    public void updateClob(String columnLabel,
                           Reader reader) throws SQLException {

        int columnIndex = findColumn(columnLabel);

        startUpdate(columnIndex);
        preparedStatement.setClob(columnIndex, reader);
    }

    
    public void updateNClob(int columnIndex,
                            Reader reader) throws SQLException {
        startUpdate(columnIndex);
        preparedStatement.setClob(columnIndex, reader);
    }

    
    public void updateNClob(String columnLabel,
                            Reader reader) throws SQLException {

        int columnIndex = findColumn(columnLabel);

        startUpdate(columnIndex);
        preparedStatement.setClob(columnIndex, reader);
    }

    

    

    @SuppressWarnings("unchecked")
    public <T>T unwrap(java.lang.Class<T> iface) throws java.sql.SQLException {

        if (isWrapperFor(iface)) {
            return (T) this;
        }

        throw Util.invalidArgument("iface: " + iface);
    }



    

    public boolean isWrapperFor(
            java.lang.Class<?> iface) throws java.sql.SQLException {
        return (iface != null && iface.isAssignableFrom(this.getClass()));
    }




    

    public <T>T getObject(int columnIndex, Class<T> type) throws SQLException {
        return (T) getObject(columnIndex);
    }



    

    public <T>T getObject(String columnLabel,
                          Class<T> type) throws SQLException {
        return getObject(findColumn(columnLabel), type);
    }




    
    private RowSetNavigator navigator;

    
    protected ResultMetaData resultMetaData;

    
    private boolean translateTTIType;

    
    private int columnCount;

    
    private boolean wasNullValue;

    
    private ResultSetMetaData resultSetMetaData;

    
    private IntValueHashMap columnMap;

    
    private SQLWarning rootWarning;

    

    
    JDBCStatementBase statement;

    
    SessionInterface  session;

    
    JDBCConnection connection;

    
    boolean isScrollable;

    
    boolean isReadOnly;

    
    boolean isUpdatable;

    
    boolean isInsertable;
    int     rsProperties;
    int     fetchSize;

    
    boolean autoClose;

    
    public Result result;

    
    

    
    public static final int FETCH_FORWARD = 1000;

    
    public static final int FETCH_REVERSE = 1001;

    
    public static final int FETCH_UNKNOWN = 1002;

    
    public static final int TYPE_FORWARD_ONLY = 1003;

    
    public static final int TYPE_SCROLL_INSENSITIVE = 1004;

    
    public static final int TYPE_SCROLL_SENSITIVE = 1005;

    
    public static final int CONCUR_READ_ONLY = 1007;

    
    public static final int CONCUR_UPDATABLE = 1008;

    
    public static final int HOLD_CURSORS_OVER_COMMIT = 1;

    
    public static final int CLOSE_CURSORS_AT_COMMIT = 2;

    

    
    protected Object[] getCurrent() throws SQLException {

        final RowSetNavigator lnavigator = this.navigator;

        if (lnavigator == null) {
            throw Util.sqlException(ErrorCode.X_24501);
        } else if (lnavigator.isEmpty()) {
            throw Util.sqlException(ErrorCode.X_24504, ErrorCode.M_RS_EMPTY);
        } else if (lnavigator.isBeforeFirst()) {
            throw Util.sqlException(ErrorCode.X_24504,
                                    ErrorCode.M_RS_BEFORE_FIRST);
        } else if (lnavigator.isAfterLast()) {
            throw Util.sqlException(ErrorCode.X_24504,
                                    ErrorCode.M_RS_AFTER_LAST);
        }

        Object[] data = lnavigator.getCurrent();

        if (data == null) {
            throw Util.sqlException(ErrorCode.X_24501);
        }

        return data;
    }

    
    private void checkClosed() throws SQLException {

        if (navigator == null) {
            throw Util.sqlException(ErrorCode.X_24501);
        }
    }

    
    protected void checkColumn(int columnIndex) throws SQLException {

        if (navigator == null) {
            throw Util.sqlException(ErrorCode.X_24501);
        }

        if (columnIndex < 1 || columnIndex > columnCount) {
            throw Util.sqlException(ErrorCode.JDBC_COLUMN_NOT_FOUND,
                                    String.valueOf(columnIndex));
        }
    }

    
    protected boolean trackNull(Object o) {
        return (wasNullValue = (o == null));
    }

    
    protected Object getColumnInType(int columnIndex,
                                     Type targetType) throws SQLException {

        Object[] rowData = getCurrent();
        Type     sourceType;
        Object   value;

        checkColumn(columnIndex);

        sourceType = resultMetaData.columnTypes[--columnIndex];
        value      = rowData[columnIndex];

        if (trackNull(value)) {
            return null;
        }

        if (translateTTIType && targetType.isIntervalType()) {
            targetType = ((IntervalType) targetType).getCharacterType();
        }


        if (sourceType.typeCode != targetType.typeCode) {
            try {
                value = targetType.convertToTypeJDBC(session, value,
                        sourceType);
            } catch (Exception e) {
                String stringValue = (value instanceof Number
                                      || value
                                         instanceof String) ? value.toString()
                        : "instance of " + value.getClass().getName();
                String msg = "from SQL type " + sourceType.getNameString()
                             + " to " + targetType.getJDBCClassName()
                             + ", value: " + stringValue;

                Util.throwError(Error.error(ErrorCode.X_42561, msg));
            }
        }

        return value;
    }

    private void checkNotForwardOnly() throws SQLException {

        if (!isScrollable) {
            throw Util.notSupported();
        }
    }

    
    JDBCPreparedStatement preparedStatement;
    boolean               isRowUpdated;
    boolean               isOnInsertRow;

    
    int currentUpdateRowNumber;

    private void checkUpdatable() throws SQLException {

        checkClosed();

        if (!isUpdatable) {
            throw Util.notUpdatableColumn();
        }
    }

    private void checkUpdatable(int columnIndex) throws SQLException {

        checkClosed();
        checkColumn(columnIndex);

        if (!isUpdatable) {
            throw Util.notUpdatableColumn();
        }

        if (resultMetaData.colIndexes[--columnIndex] == -1) {
            throw Util.notUpdatableColumn();
        }

        if (!resultMetaData.columns[columnIndex].isWriteable()) {
            throw Util.notUpdatableColumn();
        }
    }

    void startUpdate(int columnIndex) throws SQLException {

        checkUpdatable(columnIndex);

        if (currentUpdateRowNumber != navigator.getRowNumber()) {
            preparedStatement.clearParameters();
        }
        currentUpdateRowNumber = navigator.getRowNumber();
        isRowUpdated           = true;
    }

    private void clearUpdates() throws SQLException {

        checkUpdatable();
        preparedStatement.clearParameters();

        isRowUpdated = false;
    }

    private void startInsert() throws SQLException {

        checkUpdatable();

        
        isOnInsertRow = true;
    }

    private void endInsert() throws SQLException {

        checkUpdatable();
        preparedStatement.clearParameters();

        isOnInsertRow = false;
    }

    private void performUpdate() throws SQLException {

        preparedStatement.parameterValues[columnCount] =
            getCurrent()[columnCount];

        for (int i = 0; i < columnCount; i++) {
            boolean set = preparedStatement.parameterSet[i] != null;

            preparedStatement.resultOut.metaData.columnTypes[i] = set
                    ? preparedStatement.parameterTypes[i]
                    : Type.SQL_ALL_TYPES;
        }
        preparedStatement.resultOut.setActionType(
            ResultConstants.UPDATE_CURSOR);
        preparedStatement.fetchResult();
        preparedStatement.clearParameters();

        rootWarning = preparedStatement.getWarnings();

        preparedStatement.clearWarnings();

        isRowUpdated = false;
    }

    private void performInsert() throws SQLException {

        checkUpdatable();

        for (int i = 0; i < columnCount; i++) {
            boolean set = preparedStatement.parameterSet[i] != null;

            if (!set) {
                throw Util.sqlException(ErrorCode.X_24515);
            }
            preparedStatement.resultOut.metaData.columnTypes[i] =
                preparedStatement.parameterTypes[i];
        }
        preparedStatement.resultOut.setActionType(
            ResultConstants.INSERT_CURSOR);
        preparedStatement.fetchResult();
        preparedStatement.clearParameters();

        rootWarning = preparedStatement.getWarnings();

        preparedStatement.clearWarnings();
    }

    private void performDelete() throws SQLException {

        checkUpdatable();

        preparedStatement.parameterValues[columnCount] =
            getCurrent()[columnCount];
        preparedStatement.resultOut.metaData.columnTypes[columnCount] =
            resultMetaData.columnTypes[columnCount];

        preparedStatement.resultOut.setActionType(
            ResultConstants.DELETE_CURSOR);
        preparedStatement.fetchResult();
        preparedStatement.clearParameters();

        rootWarning = preparedStatement.getWarnings();

        preparedStatement.clearWarnings();
    }

    
    
    RowSetNavigator getNavigator() {
        return navigator;
    }

    void setNavigator(RowSetNavigator navigator) {
        this.navigator = navigator;
    }

    

    
    public JDBCResultSet(JDBCConnection conn, JDBCStatementBase s, Result r,
                         ResultMetaData metaData) {

        this.session    = conn == null ? null
                                       : conn.sessionProxy;
        this.statement  = s;
        this.result     = r;
        this.connection = conn;
        rsProperties    = r.rsProperties;
        navigator       = r.getNavigator();
        resultMetaData  = metaData;
        columnCount     = resultMetaData.getColumnCount();
        isScrollable    = ResultProperties.isScrollable(rsProperties);

        if (ResultProperties.isUpdatable(rsProperties)) {
            isUpdatable  = true;
            isInsertable = true;

            for (int i = 0; i < metaData.colIndexes.length; i++) {
                if (metaData.colIndexes[i] < 0) {
                    isInsertable = false;

                    break;
                }
            }
            preparedStatement = new JDBCPreparedStatement(s.connection,
                    result);
        }

        if (conn != null && conn.clientProperties != null) {
            translateTTIType = conn.clientProperties.isPropertyTrue(
                HsqlDatabaseProperties.jdbc_translate_tti_types);
        }
    }

    public JDBCResultSet(JDBCConnection conn, Result r,
                         ResultMetaData metaData) {

        this.session    = conn == null ? null
                                       : conn.sessionProxy;
        this.result     = r;
        this.connection = conn;
        rsProperties    = 0;
        navigator       = r.getNavigator();
        resultMetaData  = metaData;
        columnCount     = resultMetaData.getColumnCount();

        if (conn != null && conn.clientProperties != null) {
            translateTTIType = conn.clientProperties.isPropertyTrue(
                HsqlDatabaseProperties.jdbc_translate_tti_types);
        }
    }

    
    public static JDBCResultSet newJDBCResultSet(Result r,
            ResultMetaData metaData) {
        return new JDBCResultSetBasic(r, metaData);
    }

    static class JDBCResultSetBasic extends JDBCResultSet {

        JDBCResultSetBasic(Result r, ResultMetaData metaData) {
            super(null, r, metaData);
        }

        protected Object getColumnInType(int columnIndex,
                Type targetType) throws SQLException {

            Object[] rowData = getCurrent();
            Type     sourceType;
            Object   value;

            checkColumn(columnIndex);

            sourceType = resultMetaData.columnTypes[--columnIndex];
            value      = rowData[columnIndex];

            if (trackNull(value)) {
                return null;
            }

            if (sourceType.typeCode != targetType.typeCode) {
                Util.throwError(Error.error(ErrorCode.X_42561));
            }

            return value;
        }

        public Date getDate(int columnIndex) throws SQLException {
            return (Date) getColumnInType(columnIndex, Type.SQL_DATE);
        }

        public Time getTime(int columnIndex) throws SQLException {
            return (Time) getColumnInType(columnIndex, Type.SQL_DATE);
        }

        public Timestamp getTimestamp(int columnIndex) throws SQLException {
            return (Timestamp) getColumnInType(columnIndex, Type.SQL_DATE);
        }

        public java.io.InputStream getBinaryStream(
                int columnIndex) throws SQLException {
            throw Util.notSupported();
        }

        public java.io.Reader getCharacterStream(
                int columnIndex) throws SQLException {
            throw Util.notSupported();
        }

        public Blob getBlob(int columnIndex) throws SQLException {

            checkColumn(columnIndex);

            Type   sourceType = resultMetaData.columnTypes[columnIndex - 1];
            Object o          = getColumnInType(columnIndex, sourceType);

            if (o == null) {
                return null;
            }

            if (o instanceof Blob) {
                return (Blob) o;
            } else if (o instanceof byte[]) {
                return new JDBCBlob((byte[]) o);
            }

            throw Util.sqlException(ErrorCode.X_42561);
        }

        public Clob getClob(int columnIndex) throws SQLException {

            checkColumn(columnIndex);

            Type   sourceType = resultMetaData.columnTypes[columnIndex - 1];
            Object o          = getColumnInType(columnIndex, sourceType);

            if (o == null) {
                return null;
            }

            if (o instanceof Clob) {
                return (Clob) o;
            } else if (o instanceof String) {
                return new JDBCClob((String) o);
            }

            throw Util.sqlException(ErrorCode.X_42561);
        }

        public Time getTime(int columnIndex,
                            Calendar cal) throws SQLException {
            throw Util.notSupported();
        }

        public Timestamp getTimestamp(int columnIndex,
                                      Calendar cal) throws SQLException {
            throw Util.notSupported();
        }
    }
}
