


package org.hsqldb.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.Serializable;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.sql.Array;
import java.sql.BatchUpdateException;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;


import java.sql.ParameterMetaData;
import java.util.ArrayList;



import java.sql.NClob;
import java.sql.RowId;
import java.sql.SQLXML;


import org.hsqldb.HsqlDateTime;
import org.hsqldb.HsqlException;
import org.hsqldb.SchemaObject;
import org.hsqldb.SessionInterface;
import org.hsqldb.StatementTypes;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.lib.CharArrayWriter;
import org.hsqldb.lib.CountdownInputStream;
import org.hsqldb.lib.HsqlByteArrayOutputStream;
import org.hsqldb.lib.StringConverter;
import org.hsqldb.navigator.RowSetNavigator;
import org.hsqldb.result.Result;
import org.hsqldb.result.ResultConstants;
import org.hsqldb.result.ResultLob;
import org.hsqldb.result.ResultMetaData;
import org.hsqldb.result.ResultProperties;
import org.hsqldb.types.BinaryData;
import org.hsqldb.types.BlobDataID;
import org.hsqldb.types.BlobInputStream;
import org.hsqldb.types.ClobDataID;
import org.hsqldb.types.ClobInputStream;
import org.hsqldb.types.JavaObjectData;
import org.hsqldb.types.TimeData;
import org.hsqldb.types.TimestampData;
import org.hsqldb.types.Type;
import org.hsqldb.types.Types;












































public class JDBCPreparedStatement extends JDBCStatementBase implements PreparedStatement {

    
    public synchronized ResultSet executeQuery() throws SQLException {

        if (statementRetType != StatementTypes.RETURN_RESULT) {
            checkStatementType(StatementTypes.RETURN_RESULT);
        }
        fetchResult();

        return getResultSet();
    }

    
    public synchronized int executeUpdate() throws SQLException {

        if (statementRetType != StatementTypes.RETURN_COUNT) {
            checkStatementType(StatementTypes.RETURN_COUNT);
        }
        fetchResult();

        return resultIn.getUpdateCount();
    }

    
    public synchronized void setNull(int parameterIndex,
                                     int sqlType) throws SQLException {
        setParameter(parameterIndex, null);
    }

    
    public synchronized void setBoolean(int parameterIndex,
                                        boolean x) throws SQLException {

        Boolean b = x ? Boolean.TRUE
                      : Boolean.FALSE;

        setParameter(parameterIndex, b);
    }

    
    public synchronized void setByte(int parameterIndex,
                                     byte x) throws SQLException {
        setIntParameter(parameterIndex, x);
    }

    
    public synchronized void setShort(int parameterIndex,
                                      short x) throws SQLException {

        if (isClosed || connection.isClosed) {
            checkClosed();
        }
        checkSetParameterIndex(parameterIndex);

        if (parameterTypes[parameterIndex - 1].typeCode
                == Types.SQL_SMALLINT) {
            parameterValues[--parameterIndex] = Integer.valueOf(x);
            parameterSet[parameterIndex]      = Boolean.TRUE;

            return;
        }
        setIntParameter(parameterIndex, x);
    }

    
    public synchronized void setInt(int parameterIndex,
                                    int x) throws SQLException {

        if (isClosed || connection.isClosed) {
            checkClosed();
        }
        checkSetParameterIndex(parameterIndex);

        if (parameterTypes[parameterIndex - 1].typeCode == Types.SQL_INTEGER) {
            parameterValues[--parameterIndex] = Integer.valueOf(x);
            parameterSet[parameterIndex]      = Boolean.TRUE;

            return;
        }
        setIntParameter(parameterIndex, x);
    }

    
    public synchronized void setLong(int parameterIndex,
                                     long x) throws SQLException {

        if (isClosed || connection.isClosed) {
            checkClosed();
        }
        checkSetParameterIndex(parameterIndex);

        if (parameterTypes[parameterIndex - 1].typeCode == Types.SQL_BIGINT) {
            parameterValues[--parameterIndex] = Long.valueOf(x);
            parameterSet[parameterIndex]      = Boolean.TRUE;

            return;
        }
        setLongParameter(parameterIndex, x);
    }

    
    public synchronized void setFloat(int parameterIndex,
                                      float x) throws SQLException {
        setDouble(parameterIndex, (double) x);
    }

    
    public synchronized void setDouble(int parameterIndex,
                                       double x) throws SQLException {

        Double d = new Double(x);

        setParameter(parameterIndex, d);
    }

    
    public synchronized void setBigDecimal(int parameterIndex,
            BigDecimal x) throws SQLException {
        setParameter(parameterIndex, x);
    }

    
    public synchronized void setString(int parameterIndex,
                                       String x) throws SQLException {
        setParameter(parameterIndex, x);
    }

    
    public synchronized void setBytes(int parameterIndex,
                                      byte[] x) throws SQLException {
        setParameter(parameterIndex, x);
    }

    
    public synchronized void setDate(int parameterIndex,
                                     Date x) throws SQLException {
        setParameter(parameterIndex, x);
    }

    
    public synchronized void setTime(int parameterIndex,
                                     Time x) throws SQLException {
        setParameter(parameterIndex, x);
    }

    
    public synchronized void setTimestamp(int parameterIndex,
            Timestamp x) throws SQLException {
        setParameter(parameterIndex, x);
    }

    

    
    public synchronized void setAsciiStream(int parameterIndex,
            java.io.InputStream x, int length) throws SQLException {
        setAsciiStream(parameterIndex, x, (long) length);
    }

    


    public synchronized void setUnicodeStream(int parameterIndex,
            java.io.InputStream x, int length) throws SQLException {

        checkSetParameterIndex(parameterIndex);

        String    msg = null;
        final int ver = JDBCDatabaseMetaData.JDBC_MAJOR;

        if (x == null) {
            throw Util.nullArgument("x");
        }

        
        if ((ver < 4) && (length % 2 != 0)) {
            msg = "Odd length argument for UTF16 encoded stream: " + length;

            throw Util.invalidArgument(msg);
        }

        String       encoding = (ver < 4) ? "UTF16"
                : "UTF8";
        StringWriter writer   = new StringWriter();

        try {
            CountdownInputStream cis    = new CountdownInputStream(x);
            InputStreamReader    reader = new InputStreamReader(cis, encoding);
            char[]               buff   = new char[1024];
            int                  charsRead;

            cis.setCount(length);

            while (-1 != (charsRead = reader.read(buff))) {
                writer.write(buff, 0, charsRead);
            }
        } catch (IOException ex) {
            throw Util.sqlException(ErrorCode.SERVER_TRANSFER_CORRUPTED,
                                    ex.toString(), ex);
        }
        setParameter(parameterIndex, writer.toString());
    }



    

    
    public synchronized void setBinaryStream(int parameterIndex,
            java.io.InputStream x, int length) throws SQLException {
        setBinaryStream(parameterIndex, x, (long) length);
    }

    
    public synchronized void clearParameters() throws SQLException {

        if (isClosed || connection.isClosed) {
            checkClosed();
        }
        ArrayUtil.fillArray(parameterValues, null);
        ArrayUtil.fillArray(parameterSet, null);
        ArrayUtil.clearArray(ArrayUtil.CLASS_CODE_LONG, streamLengths, 0,
                             streamLengths.length);
    }

    
    

    
    public synchronized void setObject(int parameterIndex, Object x,
                                       int targetSqlType,
                                       int scaleOrLength) throws SQLException {

        if (x instanceof InputStream) {
            setBinaryStream(parameterIndex, (InputStream) x, scaleOrLength);
        } else if (x instanceof Reader) {
            setCharacterStream(parameterIndex, (Reader) x, scaleOrLength);
        } else {
            setObject(parameterIndex, x);
        }
    }

    
    public synchronized void setObject(int parameterIndex, Object x,
                                       int targetSqlType) throws SQLException {
        setObject(parameterIndex, x);
    }

    
    public synchronized void setObject(int parameterIndex,
                                       Object x) throws SQLException {
        setParameter(parameterIndex, x);
    }

    
    public synchronized boolean execute() throws SQLException {

        fetchResult();

        return statementRetType == StatementTypes.RETURN_RESULT;
    }

    

    
    public synchronized void addBatch() throws SQLException {

        if (isClosed || connection.isClosed) {
            checkClosed();
        }
        checkParametersSet();

        if (!isBatch) {
            resultOut.setBatchedPreparedExecuteRequest();

            isBatch = true;
        }

        try {
            performPreExecute();
        } catch (HsqlException e) {
            throw Util.sqlException(e);
        }

        int      len              = parameterValues.length;
        Object[] batchParamValues = new Object[len];

        System.arraycopy(parameterValues, 0, batchParamValues, 0, len);
        resultOut.addBatchedPreparedExecuteRequest(batchParamValues);
    }

    

    
    public synchronized void setCharacterStream(int parameterIndex,
            java.io.Reader reader, int length) throws SQLException {
        setCharacterStream(parameterIndex, reader, (long) length);
    }

    
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        throw Util.notSupported();
    }

    
    public synchronized void setBlob(int parameterIndex,
                                     Blob x) throws SQLException {

        checkSetParameterIndex(parameterIndex);

        Type outType = parameterTypes[parameterIndex - 1];

        switch (outType.typeCode) {

            case Types.SQL_BINARY :
            case Types.SQL_VARBINARY :
                setBlobForBinaryParameter(parameterIndex, x);

                return;
            case Types.SQL_BLOB :
                setBlobParameter(parameterIndex, x);

                break;
            default :
                throw Util.invalidArgument();
        }
    }

    
    private void setBlobForBinaryParameter(int parameterIndex,
            Blob x) throws SQLException {

        if (x instanceof JDBCBlob) {
            setParameter(parameterIndex, ((JDBCBlob) x).data());

            return;
        } else if (x == null) {
            setParameter(parameterIndex, null);

            return;
        }

        final long length = x.length();

        if (length > Integer.MAX_VALUE) {
            String msg = "Maximum Blob input octet length exceeded: " + length;    

            throw Util.sqlException(ErrorCode.JDBC_INPUTSTREAM_ERROR, msg);
        }

        try {
            java.io.InputStream in = x.getBinaryStream();
            HsqlByteArrayOutputStream out = new HsqlByteArrayOutputStream(in,
                (int) length);

            setParameter(parameterIndex, out.toByteArray());
        } catch (Throwable e) {
            throw Util.sqlException(ErrorCode.JDBC_INPUTSTREAM_ERROR,
                                    e.toString(), e);
        }
    }

    
    public synchronized void setClob(int parameterIndex,
                                     Clob x) throws SQLException {

        checkSetParameterIndex(parameterIndex);

        Type outType = parameterTypes[parameterIndex - 1];

        switch (outType.typeCode) {

            case Types.SQL_CHAR :
            case Types.SQL_VARCHAR :
                setClobForStringParameter(parameterIndex, x);

                return;
            case Types.SQL_CLOB :
                setClobParameter(parameterIndex, x);

                return;
            default :
                throw Util.invalidArgument();
        }
    }

    private void setClobForStringParameter(int parameterIndex,
            Clob x) throws SQLException {

        if (x instanceof JDBCClob) {
            setParameter(parameterIndex, ((JDBCClob) x).data());

            return;
        } else if (x == null) {
            setParameter(parameterIndex, null);

            return;
        }

        final long length = x.length();

        if (length > Integer.MAX_VALUE) {
            String msg = "Max Clob input character length exceeded: " + length;    

            throw Util.sqlException(ErrorCode.JDBC_INPUTSTREAM_ERROR, msg);
        }

        try {
            java.io.Reader  reader = x.getCharacterStream();
            CharArrayWriter writer = new CharArrayWriter(reader, (int) length);

            setParameter(parameterIndex, writer.toString());
        } catch (Throwable e) {
            throw Util.sqlException(ErrorCode.SERVER_TRANSFER_CORRUPTED,
                                    e.toString(), e);
        }
    }

    
    public synchronized void setArray(int parameterIndex,
                                      Array x) throws SQLException {

        checkParameterIndex(parameterIndex);

        Type type = this.parameterMetaData.columnTypes[parameterIndex - 1];

        if (!type.isArrayType()) {
            throw Util.sqlException(ErrorCode.X_42561);
        }

        if (x == null) {
            setParameter(parameterIndex, null);

            return;
        }

        Object[] data = null;

        if (x instanceof JDBCArray) {
            data = (Object[]) ((JDBCArray) x).getArrayInternal();
        } else {
            Object object = x.getArray();

            if (object instanceof Object[]) {
                Type     baseType = type.collectionBaseType();
                Object[] array    = (Object[]) object;

                data = new Object[array.length];

                for (int i = 0; i < data.length; i++) {
                    data[i] = baseType.convertJavaToSQL(session, array[i]);
                }
            } else {

                
                throw Util.notSupported();
            }
        }
        parameterValues[parameterIndex - 1] = data;
        parameterSet[parameterIndex - 1]    = Boolean.TRUE;

        return;
    }

    
    public synchronized ResultSetMetaData getMetaData() throws SQLException {

        if (isClosed || connection.isClosed) {
            checkClosed();
        }

        if (statementRetType != StatementTypes.RETURN_RESULT) {
            return null;
        }

        if (resultSetMetaData == null) {
            boolean isUpdatable  = ResultProperties.isUpdatable(rsProperties);
            boolean isInsertable = isUpdatable;

            if (isInsertable) {
                for (int i = 0; i < resultMetaData.colIndexes.length; i++) {
                    if (resultMetaData.colIndexes[i] < 0) {
                        isInsertable = false;

                        break;
                    }
                }
            }
            resultSetMetaData = new JDBCResultSetMetaData(resultMetaData,
                    isUpdatable, isInsertable, connection);
        }

        return resultSetMetaData;
    }

    
    public synchronized void setDate(int parameterIndex, Date x,
                                     Calendar cal) throws SQLException {

        checkSetParameterIndex(parameterIndex);

        int i = parameterIndex - 1;

        if (x == null) {
            parameterValues[i] = null;
            parameterSet[i]    = Boolean.TRUE;

            return;
        }

        Type outType = parameterTypes[i];
        long millis  = HsqlDateTime.convertMillisFromCalendar(cal,
            x.getTime());

        millis = HsqlDateTime.getNormalisedDate(millis);

        switch (outType.typeCode) {

            case Types.SQL_DATE :
            case Types.SQL_TIMESTAMP :
                parameterValues[i] = new TimestampData(millis / 1000);

                break;
            case Types.SQL_TIMESTAMP_WITH_TIME_ZONE :
                int zoneOffset = HsqlDateTime.getZoneMillis(cal, millis);

                parameterValues[i] = new TimestampData(millis / 1000, 0,
                        zoneOffset / 1000);

                break;
            default :
                throw Util.sqlException(ErrorCode.X_42561);
        }
        parameterSet[i] = Boolean.TRUE;
    }

    
    public synchronized void setTime(int parameterIndex, Time x,
                                     Calendar cal) throws SQLException {

        checkSetParameterIndex(parameterIndex);

        int i = parameterIndex - 1;

        if (x == null) {
            parameterValues[i] = null;
            parameterSet[i]    = Boolean.TRUE;

            return;
        }

        Type     outType    = parameterTypes[i];
        long     millis     = x.getTime();
        int      zoneOffset = 0;
        Calendar calendar   = cal == null ? session.getCalendar()
                : cal;

        millis = HsqlDateTime.convertMillisFromCalendar(calendar, millis);
        millis = HsqlDateTime.convertToNormalisedTime(millis);

        switch (outType.typeCode) {

            case Types.SQL_TIME :
                break;
            case Types.SQL_TIME_WITH_TIME_ZONE :
                zoneOffset = HsqlDateTime.getZoneMillis(calendar, millis);

                break;
            default :
                throw Util.sqlException(ErrorCode.X_42561);
        }
        parameterValues[i] = new TimeData((int) (millis / 1000), 0,
                zoneOffset / 1000);
        parameterSet[i] = Boolean.TRUE;
    }

    
    public synchronized void setTimestamp(int parameterIndex, Timestamp x,
            Calendar cal) throws SQLException {

        checkSetParameterIndex(parameterIndex);

        int i = parameterIndex - 1;

        if (x == null) {
            parameterValues[i] = null;
            parameterSet[i]    = Boolean.TRUE;

            return;
        }

        Type     outType    = parameterTypes[i];
        long     millis     = x.getTime();
        int      zoneOffset = 0;
        Calendar calendar   = cal == null ? session.getCalendar()
                : cal;

        millis = HsqlDateTime.convertMillisFromCalendar(calendar, millis);

        switch (outType.typeCode) {

            case Types.SQL_TIMESTAMP_WITH_TIME_ZONE :
                zoneOffset = HsqlDateTime.getZoneMillis(calendar, millis);

            
            case Types.SQL_TIMESTAMP :
                parameterValues[i] = new TimestampData(millis / 1000,
                        x.getNanos(), zoneOffset / 1000);

                break;
            case Types.SQL_TIME :
                millis = HsqlDateTime.getNormalisedTime(millis);
                parameterValues[i] = new TimeData((int) (millis / 1000),
                        x.getNanos(), 0);

                break;
            case Types.SQL_TIME_WITH_TIME_ZONE :
                zoneOffset = HsqlDateTime.getZoneMillis(calendar, millis);
                parameterValues[i] = new TimeData((int) (millis / 1000),
                        x.getNanos(), zoneOffset / 1000);

                break;
            case Types.SQL_DATE :
                millis             = HsqlDateTime.getNormalisedDate(millis);
                parameterValues[i] = new TimestampData(millis / 1000);

                break;
            default :
                throw Util.sqlException(ErrorCode.X_42561);
        }
        parameterSet[i] = Boolean.TRUE;
    }

    
    public synchronized void setNull(int parameterIndex, int sqlType,
                                     String typeName) throws SQLException {
        setParameter(parameterIndex, null);
    }

    

    
    public synchronized int[] executeBatch() throws SQLException {

        if (isClosed || connection.isClosed) {
            checkClosed();
        }
        checkStatementType(StatementTypes.RETURN_COUNT);

        if (!isBatch) {
            throw Util.sqlExceptionSQL(ErrorCode.X_07506);
        }
        generatedResult = null;

        int batchCount = resultOut.getNavigator().getSize();

        resultIn = null;

        try {
            resultIn = session.execute(resultOut);
        } catch (HsqlException e) {
            throw Util.sqlException(e);
        } finally {
            performPostExecute();
            resultOut.getNavigator().clear();

            isBatch = false;
        }

        if (resultIn.mode == ResultConstants.ERROR) {
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

    
    public void setEscapeProcessing(boolean enable) throws SQLException {
        checkClosed();
    }

    
    public void addBatch(String sql) throws SQLException {
        throw Util.notSupported();
    }

    
    public synchronized ResultSet executeQuery(
            String sql) throws SQLException {
        throw Util.notSupported();
    }

    
    public boolean execute(String sql) throws SQLException {
        throw Util.notSupported();
    }

    
    public int executeUpdate(String sql) throws SQLException {
        throw Util.notSupported();
    }

    
    public synchronized void close() throws SQLException {

        if (isClosed()) {
            return;
        }
        closeResultData();

        HsqlException he = null;

        try {

            
            
            
            if (!connection.isClosed) {
                session.execute(Result.newFreeStmtRequest(statementID));
            }
        } catch (HsqlException e) {
            he = e;
        }
        parameterValues   = null;
        parameterSet      = null;
        parameterTypes    = null;
        parameterModes    = null;
        resultMetaData    = null;
        parameterMetaData = null;
        resultSetMetaData = null;
        pmd               = null;
        connection        = null;
        resultIn          = null;
        resultOut         = null;
        isClosed          = true;

        if (he != null) {
            throw Util.sqlException(he);
        }
    }

    
    public String toString() {

        StringBuffer sb = new StringBuffer();
        String       sql;
        Object[]     pv;

        sb.append(super.toString());

        sql = this.sql;
        pv  = parameterValues;

        if (sql == null || pv == null) {
            sb.append("[closed]");

            return sb.toString();
        }
        sb.append("[sql=[").append(sql).append("]");

        if (pv.length > 0) {
            sb.append(", parameters=[");

            for (int i = 0; i < pv.length; i++) {
                sb.append('[');
                sb.append(pv[i]);
                sb.append("], ");
            }
            sb.setLength(sb.length() - 2);
            sb.append(']');
        }
        sb.append(']');

        return sb.toString();
    }

    

    

    public void setURL(int parameterIndex,
                       java.net.URL x) throws SQLException {
        throw Util.notSupported();
    }



    

    public synchronized ParameterMetaData getParameterMetaData() throws SQLException {

        checkClosed();

        if (pmd == null) {
            pmd = new JDBCParameterMetaData(connection, parameterMetaData);
        }

        
        return (ParameterMetaData) pmd;
    }



    

    public int executeUpdate(String sql,
                             int autoGeneratedKeys) throws SQLException {
        throw Util.notSupported();
    }

    public boolean execute(String sql,
                           int autoGeneratedKeys) throws SQLException {
        throw Util.notSupported();
    }

    public int executeUpdate(String sql,
                             int[] columnIndexes) throws SQLException {
        throw Util.notSupported();
    }

    public boolean execute(String sql,
                           int[] columnIndexes) throws SQLException {
        throw Util.notSupported();
    }

    public int executeUpdate(String sql,
                             String[] columnNames) throws SQLException {
        throw Util.notSupported();
    }

    public boolean execute(String sql,
                           String[] columnNames) throws SQLException {
        throw Util.notSupported();
    }



    

    public synchronized boolean getMoreResults(
            int current) throws SQLException {
        return super.getMoreResults(current);
    }



    

    public synchronized ResultSet getGeneratedKeys() throws SQLException {
        return getGeneratedResultSet();
    }



    

    public synchronized int getResultSetHoldability() throws SQLException {

        if (isClosed || connection.isClosed) {
            checkClosed();
        }

        return ResultProperties.getJDBCHoldability(rsProperties);
    }


    

    
    public synchronized boolean isClosed() {
        return isClosed;
    }

    


    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw Util.notSupported();
    }



    
    public synchronized void setNString(int parameterIndex,
                                        String value) throws SQLException {
        setString(parameterIndex, value);
    }

    
    public synchronized void setNCharacterStream(int parameterIndex,
            Reader value, long length) throws SQLException {
        setCharacterStream(parameterIndex, value, length);
    }

    


    public synchronized void setNClob(int parameterIndex,
                                      NClob value) throws SQLException {
        setClob(parameterIndex, value);
    }



    

    
    public synchronized void setClob(int parameterIndex, Reader reader,
                                     long length) throws SQLException {
        setCharacterStream(parameterIndex, reader, length);
    }

    

    
    public synchronized void setBlob(int parameterIndex,
                                     InputStream inputStream,
                                     long length) throws SQLException {
        setBinaryStream(parameterIndex, inputStream, length);
    }

    
    public synchronized void setNClob(int parameterIndex, Reader reader,
                                      long length) throws SQLException {
        setClob(parameterIndex, reader, length);
    }

    


    public void setSQLXML(int parameterIndex,
                          SQLXML xmlObject) throws SQLException {
        throw Util.notSupported();
    }




    

    
    public synchronized void setAsciiStream(int parameterIndex,
            java.io.InputStream x, long length) throws SQLException {

        if (length < 0) {
            throw Util.sqlException(ErrorCode.JDBC_INVALID_ARGUMENT,
                                    "length: " + length);
        }
        setAscStream(parameterIndex, x, (long) length);
    }

    void setAscStream(int parameterIndex, java.io.InputStream x,
                      long length) throws SQLException {

        if (length > Integer.MAX_VALUE) {
            Util.sqlException(ErrorCode.X_22001);
        }

        if (x == null) {
            throw Util.nullArgument("x");
        }

        try {
            String s = StringConverter.inputStreamToString(x, "US-ASCII");

            if (length >= 0 && s.length() > length) {
                s = s.substring(0, (int) length);
            }
            setParameter(parameterIndex, s);
        } catch (IOException e) {
            throw Util.sqlException(ErrorCode.JDBC_INPUTSTREAM_ERROR, null, e);
        }
    }

    
    public synchronized void setBinaryStream(int parameterIndex,
            java.io.InputStream x, long length) throws SQLException {

        if (length < 0) {
            throw Util.sqlException(ErrorCode.JDBC_INVALID_ARGUMENT,
                                    "length: " + length);
        }
        setBinStream(parameterIndex, x, length);
    }

    private void setBinStream(int parameterIndex, java.io.InputStream x,
                              long length) throws SQLException {

        if (isClosed || connection.isClosed) {
            checkClosed();
        }

        if (parameterTypes[parameterIndex - 1].typeCode == Types.SQL_BLOB) {
            setBlobParameter(parameterIndex, x, length);

            return;
        }

        if (length > Integer.MAX_VALUE) {
            String msg = "Maximum Blob input length exceeded: " + length;

            throw Util.sqlException(ErrorCode.JDBC_INPUTSTREAM_ERROR, msg);
        }

        try {
            HsqlByteArrayOutputStream output;

            if (length < 0) {
                output = new HsqlByteArrayOutputStream(x);
            } else {
                output = new HsqlByteArrayOutputStream(x, (int) length);
            }
            setParameter(parameterIndex, output.toByteArray());
        } catch (Throwable e) {
            throw Util.sqlException(ErrorCode.JDBC_INPUTSTREAM_ERROR,
                                    e.toString(), e);
        }
    }

    
    public synchronized void setCharacterStream(int parameterIndex,
            java.io.Reader reader, long length) throws SQLException {

        if (length < 0) {
            throw Util.sqlException(ErrorCode.JDBC_INVALID_ARGUMENT,
                                    "length: " + length);
        }
        setCharStream(parameterIndex, reader, length);
    }

    private void setCharStream(int parameterIndex, java.io.Reader reader,
                               long length) throws SQLException {

        checkSetParameterIndex(parameterIndex);

        if (parameterTypes[parameterIndex - 1].typeCode == Types.SQL_CLOB) {
            setClobParameter(parameterIndex, reader, length);

            return;
        }

        if (length > Integer.MAX_VALUE) {
            String msg = "Maximum Clob input length exceeded: " + length;

            throw Util.sqlException(ErrorCode.JDBC_INPUTSTREAM_ERROR, msg);
        }

        try {
            CharArrayWriter writer;

            if (length < 0) {
                writer = new CharArrayWriter(reader);
            } else {
                writer = new CharArrayWriter(reader, (int) length);
            }
            setParameter(parameterIndex, writer.toString());
        } catch (Throwable e) {
            throw Util.sqlException(ErrorCode.JDBC_INPUTSTREAM_ERROR,
                                    e.toString(), e);
        }
    }

    
    public void setAsciiStream(int parameterIndex,
                               java.io.InputStream x) throws SQLException {
        setAscStream(parameterIndex, x, -1);
    }

    

    
    public synchronized void setBinaryStream(int parameterIndex,
            java.io.InputStream x) throws SQLException {
        setBinStream(parameterIndex, x, -1);
    }

    

    
    public void setCharacterStream(int parameterIndex,
                                   java.io.Reader reader) throws SQLException {
        setCharStream(parameterIndex, reader, -1);
    }

    

    
    public void setNCharacterStream(int parameterIndex,
                                    Reader value) throws SQLException {
        setCharStream(parameterIndex, value, -1);
    }

    

    
    public void setClob(int parameterIndex,
                        Reader reader) throws SQLException {
        setCharStream(parameterIndex, reader, -1);
    }

    

    
    public void setBlob(int parameterIndex,
                        InputStream inputStream) throws SQLException {
        setBinStream(parameterIndex, inputStream, -1);
    }

    

    
    public void setNClob(int parameterIndex,
                         Reader reader) throws SQLException {
        setCharStream(parameterIndex, reader, -1);
    }

    
    public synchronized int getMaxFieldSize() throws SQLException {

        if (isClosed || connection.isClosed) {
            checkClosed();
        }

        return 0;
    }

    
    public synchronized void setMaxFieldSize(int max) throws SQLException {

        if (isClosed || connection.isClosed) {
            checkClosed();
        }

        if (max < 0) {
            throw Util.outOfRangeArgument();
        }
    }

    
    public synchronized int getMaxRows() throws SQLException {

        if (isClosed || connection.isClosed) {
            checkClosed();
        }

        return maxRows;
    }

    
    public synchronized void setMaxRows(int max) throws SQLException {

        if (isClosed || connection.isClosed) {
            checkClosed();
        }

        if (max < 0) {
            throw Util.outOfRangeArgument();
        }
        maxRows = max;
    }

    
    public synchronized int getQueryTimeout() throws SQLException {

        if (isClosed || connection.isClosed) {
            checkClosed();
        }

        return 0;
    }

    
    public synchronized void setQueryTimeout(int seconds) throws SQLException {

        if (isClosed || connection.isClosed) {
            checkClosed();
        }

        if (seconds < 0 || seconds > Short.MAX_VALUE) {
            throw Util.outOfRangeArgument();
        }
        queryTimeout = seconds;
    }

    
    public void cancel() throws SQLException {
        checkClosed();
    }

    
    public synchronized SQLWarning getWarnings() throws SQLException {

        if (isClosed || connection.isClosed) {
            checkClosed();
        }

        return rootWarning;
    }

    
    public synchronized void clearWarnings() throws SQLException {

        if (isClosed || connection.isClosed) {
            checkClosed();
        }
        rootWarning = null;
    }

    

    
    public void setCursorName(String name) throws SQLException {
        checkClosed();
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

        if (isClosed || connection.isClosed) {
            checkClosed();
        }

        if (direction != JDBCResultSet.FETCH_FORWARD
                && direction != JDBCResultSet.FETCH_REVERSE
                && direction != JDBCResultSet.FETCH_UNKNOWN) {
            throw Util.notSupported();
        }
        fetchDirection = direction;
    }

    
    public synchronized int getFetchDirection() throws SQLException {

        if (isClosed || connection.isClosed) {
            checkClosed();
        }

        return fetchDirection;
    }

    
    public synchronized void setFetchSize(int rows) throws SQLException {

        if (isClosed || connection.isClosed) {
            checkClosed();
        }

        if (rows < 0) {
            throw Util.outOfRangeArgument();
        }
        fetchSize = rows;
    }

    
    public synchronized int getFetchSize() throws SQLException {

        if (isClosed || connection.isClosed) {
            checkClosed();
        }

        return fetchSize;
    }

    
    public synchronized int getResultSetConcurrency() throws SQLException {

        if (isClosed || connection.isClosed) {
            checkClosed();
        }

        return ResultProperties.getJDBCConcurrency(rsProperties);
    }

    
    public synchronized int getResultSetType() throws SQLException {

        
        
        if (isClosed || connection.isClosed) {
            checkClosed();
        }

        return ResultProperties.getJDBCScrollability(rsProperties);
    }

    
    public synchronized void clearBatch() throws SQLException {

        if (isClosed || connection.isClosed) {
            checkClosed();
        }

        if (isBatch) {
            resultOut.getNavigator().clear();
        }
    }

    
    public synchronized Connection getConnection() throws SQLException {

        if (isClosed || connection.isClosed) {
            checkClosed();
        }

        return connection;
    }

    

    boolean poolable = true;

    
    public synchronized void setPoolable(
            boolean poolable) throws SQLException {

        if (isClosed || connection.isClosed) {
            checkClosed();
        }
        this.poolable = poolable;
    }

    
    public synchronized boolean isPoolable() throws SQLException {

        if (isClosed || connection.isClosed) {
            checkClosed();
        }

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


    

    
    JDBCPreparedStatement(JDBCConnection c, String sql, int resultSetType,
                          int resultSetConcurrency, int resultSetHoldability,
                          int generatedKeys, int[] generatedIndexes,
                          String[] generatedNames) throws HsqlException,
                              SQLException {

        isResult              = false;
        connection            = c;
        connectionIncarnation = connection.incarnation;
        session               = c.sessionProxy;
        sql                   = c.nativeSQL(sql);

        int[] keyIndexes = null;

        if (generatedIndexes != null) {
            keyIndexes = new int[generatedIndexes.length];

            for (int i = 0; i < generatedIndexes.length; i++) {
                keyIndexes[i] = generatedIndexes[i] - 1;
            }
        }
        resultOut = Result.newPrepareStatementRequest();

        int props = ResultProperties.getValueForJDBC(resultSetType,
            resultSetConcurrency, resultSetHoldability);

        resultOut.setPrepareOrExecuteProperties(sql, 0, 0, 0, queryTimeout,
                props, generatedKeys, generatedIndexes, generatedNames);

        Result in = session.execute(resultOut);

        if (in.mode == ResultConstants.ERROR) {
            throw Util.sqlException(in);
        }
        rootWarning = null;

        Result current = in;

        while (current.getChainedResult() != null) {
            current = current.getUnlinkChainedResult();

            if (current.isWarning()) {
                SQLWarning w = Util.sqlWarning(current);

                if (rootWarning == null) {
                    rootWarning = w;
                } else {
                    rootWarning.setNextWarning(w);
                }
            }
        }
        connection.setWarnings(rootWarning);

        statementID       = in.getStatementID();
        statementRetType  = in.getStatementType();
        resultMetaData    = in.metaData;
        parameterMetaData = in.parameterMetaData;
        parameterTypes    = parameterMetaData.getParameterTypes();
        parameterModes    = parameterMetaData.paramModes;
        rsProperties      = in.rsProperties;

        
        int paramCount = parameterMetaData.getColumnCount();

        parameterValues = new Object[paramCount];
        parameterSet    = new Boolean[paramCount];
        streamLengths   = new long[paramCount];

        
        
        for (int i = 0; i < paramCount; i++) {
            if (parameterTypes[i].isLobType()) {
                hasLOBs = true;

                break;
            }
        }

        
        resultOut = Result.newPreparedExecuteRequest(parameterTypes,
                statementID);

        resultOut.setStatement(in.getStatement());

        
        this.sql = sql;
    }

    
    JDBCPreparedStatement(JDBCConnection c, Result result) {

        isResult              = true;
        connection            = c;
        connectionIncarnation = connection.incarnation;
        session               = c.sessionProxy;

        int paramCount = result.metaData.getExtendedColumnCount();

        parameterMetaData = result.metaData;
        parameterTypes    = result.metaData.columnTypes;
        parameterModes    = new byte[paramCount];
        parameterValues   = new Object[paramCount];
        parameterSet      = new Boolean[paramCount];
        streamLengths     = new long[paramCount];

        
        for (int i = 0; i < paramCount; i++) {
            parameterModes[i] = SchemaObject.ParameterModes.PARAM_IN;

            if (parameterTypes[i].isLobType()) {
                hasLOBs = true;
            }
        }

        
        resultOut = Result.newUpdateResultRequest(parameterTypes,
                result.getResultId());
    }

    
    protected void checkStatementType(int type) throws SQLException {

        if (type != statementRetType) {
            if (statementRetType == StatementTypes.RETURN_COUNT) {
                throw Util.sqlException(ErrorCode.X_07504);
            } else {
                throw Util.sqlException(ErrorCode.X_07503);
            }
        }
    }

    protected void checkParameterIndex(int i) throws SQLException {

        if (isClosed || connection.isClosed) {
            checkClosed();
        }

        if (i < 1 || i > parameterValues.length) {
            String msg = "parameter index out of range: " + i;

            throw Util.outOfRangeArgument(msg);
        }
    }

    
    protected void checkSetParameterIndex(int i) throws SQLException {

        if (isClosed || connection.isClosed) {
            checkClosed();
        }

        if (i < 1 || i > parameterValues.length) {
            String msg = "parameter index out of range: " + i;

            throw Util.outOfRangeArgument(msg);
        }

        if (parameterModes[i - 1] == SchemaObject.ParameterModes.PARAM_OUT) {
            String msg = "Not IN or INOUT mode for parameter: " + i;

            throw Util.invalidArgument(msg);
        }
    }

    
    protected void checkGetParameterIndex(int i) throws SQLException {

        String msg;

        if (isClosed || connection.isClosed) {
            checkClosed();
        }

        if (i < 1 || i > parameterValues.length) {
            msg = "parameter index out of range: " + i;

            throw Util.outOfRangeArgument(msg);
        }

        int mode = parameterModes[i - 1];

        switch (mode) {

            case SchemaObject.ParameterModes.PARAM_UNKNOWN :
            case SchemaObject.ParameterModes.PARAM_OUT :
            case SchemaObject.ParameterModes.PARAM_INOUT :
                break;
            case SchemaObject.ParameterModes.PARAM_IN :
            default :
                msg = "Not OUT or INOUT mode: " + mode + " for parameter: "
                      + i;

                throw Util.invalidArgument(msg);
        }
    }

    
    private void checkParametersSet() throws SQLException {

        if (isResult) {
            return;
        }

        for (int i = 0; i < parameterSet.length; i++) {
            if (parameterModes[i] != SchemaObject.ParameterModes.PARAM_OUT) {
                if (parameterSet[i] == null) {
                    throw Util.sqlException(ErrorCode.JDBC_PARAMETER_NOT_SET);
                }
            }
        }
    }

    
    void setParameter(int i, Object o) throws SQLException {

        checkSetParameterIndex(i);

        i--;

        if (o == null) {
            parameterValues[i] = null;
            parameterSet[i]    = Boolean.TRUE;

            return;
        }

        Type outType = parameterTypes[i];

        switch (outType.typeCode) {

            case Types.OTHER :
                try {
                    if (o instanceof Serializable) {
                        o = new JavaObjectData((Serializable) o);

                        break;
                    }
                } catch (HsqlException e) {
                    Util.throwError(e);
                }
                Util.throwError(Error.error(ErrorCode.X_42563));
            case Types.SQL_BIT :
            case Types.SQL_BIT_VARYING :
                try {
                    if (o instanceof Boolean) {
                        o = outType.convertToDefaultType(session, o);

                        break;
                    }

                    if (o instanceof Integer) {
                        o = outType.convertToDefaultType(session, o);

                        break;
                    }

                    if (o instanceof byte[]) {
                        o = outType.convertToDefaultType(session, o);

                        break;
                    }

                    if (o instanceof String) {
                        o = outType.convertToDefaultType(session, o);

                        break;
                    }
                } catch (HsqlException e) {
                    Util.throwError(e);
                }
                Util.throwError(Error.error(ErrorCode.X_42563));

            
            case Types.SQL_BINARY :
            case Types.SQL_VARBINARY :
                if (o instanceof byte[]) {
                    o = new BinaryData((byte[]) o, !connection.isNetConn);

                    break;
                }

                try {
                    if (o instanceof String) {
                        o = outType.convertToDefaultType(session, o);

                        break;
                    }
                } catch (HsqlException e) {
                    Util.throwError(e);
                }
                Util.throwError(Error.error(ErrorCode.X_42563));

                break;
            case Types.SQL_ARRAY :
                if (o instanceof Array) {
                    setArray(i + 1, (Array) o);

                    return;
                }

                if (o instanceof ArrayList) {
                    o = ((ArrayList) o).toArray();
                }

                if (o instanceof Object[]) {
                    Type     baseType = outType.collectionBaseType();
                    Object[] array    = (Object[]) o;
                    Object[] data     = new Object[array.length];

                    for (int j = 0; j < data.length; j++) {
                        data[j] = baseType.convertJavaToSQL(session, array[j]);
                    }
                    o = data;

                    break;
                }
                Util.throwError(Error.error(ErrorCode.X_42563));
            case Types.SQL_BLOB :
                setBlobParameter(i + 1, o);

                return;
            case Types.SQL_CLOB :
                setClobParameter(i + 1, o);

                return;
            case Types.SQL_DATE :
            case Types.SQL_TIME_WITH_TIME_ZONE :
            case Types.SQL_TIMESTAMP_WITH_TIME_ZONE :
            case Types.SQL_TIME :
            case Types.SQL_TIMESTAMP : {
                try {
                    if (o instanceof String) {
                        o = outType.convertToType(session, o,
                                Type.SQL_VARCHAR);

                        break;
                    }
                    o = outType.convertJavaToSQL(session, o);

                    break;
                } catch (HsqlException e) {
                    Util.throwError(e);
                }
            }
            case Types.TINYINT :
            case Types.SQL_SMALLINT :
            case Types.SQL_INTEGER :
            case Types.SQL_BIGINT :
            case Types.SQL_REAL :
            case Types.SQL_FLOAT :
            case Types.SQL_DOUBLE :
            case Types.SQL_NUMERIC :
            case Types.SQL_DECIMAL :
                try {
                    if (o instanceof String) {
                        o = outType.convertToType(session, o,
                                Type.SQL_VARCHAR);

                        break;
                    } else if (o instanceof Boolean) {
                        boolean value = ((Boolean) o).booleanValue();

                        o = value ? Integer.valueOf(1)
                                  : Integer.valueOf(0);
                    }
                    o = outType.convertToDefaultType(session, o);

                    break;
                } catch (HsqlException e) {
                    Util.throwError(e);
                }
            case Types.SQL_VARCHAR : {
                if (o instanceof String) {
                    break;
                } else {
                    o = outType.convertToDefaultType(session, o);

                    break;
                }
            }
            case Types.SQL_CHAR :
                if (outType.precision == 1) {
                    if (o instanceof Character) {
                        o = new String(new char[] {
                            ((Character) o).charValue() });

                        break;
                    } else if (o instanceof Boolean) {
                        o = ((Boolean) o).booleanValue() ? "1"
                                : "0";

                        break;
                    }
                }

            
            default :
                try {
                    o = outType.convertToDefaultType(session, o);

                    break;
                } catch (HsqlException e) {
                    Util.throwError(e);
                }
        }
        parameterValues[i] = o;
        parameterSet[i]    = Boolean.TRUE;
    }

    
    void setClobParameter(int i, Object o) throws SQLException {
        setClobParameter(i, o, 0);
    }

    void setClobParameter(int i, Object o,
                          long streamLength) throws SQLException {

        if (o instanceof JDBCClobClient) {
            JDBCClobClient clob = (JDBCClobClient) o;

            if (!clob.session.getDatabaseUniqueName().equals(
                    session.getDatabaseUniqueName())) {
                streamLength = clob.length();

                Reader is = clob.getCharacterStream();

                parameterValues[i - 1] = is;
                streamLengths[i - 1]   = streamLength;
                parameterSet[i - 1]    = Boolean.FALSE;

                return;
            }
            parameterValues[i - 1] = o;
            parameterSet[i - 1]    = Boolean.TRUE;

            return;
        } else if (o instanceof Clob) {
            parameterValues[i - 1] = o;
            parameterSet[i - 1]    = Boolean.TRUE;

            return;
        } else if (o instanceof ClobInputStream) {
            ClobInputStream is = (ClobInputStream) o;

            if (is.session.getDatabaseUniqueName().equals(
                    session.getDatabaseUniqueName())) {
                throw Util.sqlException(ErrorCode.JDBC_INVALID_ARGUMENT,
                                        "invalid Reader");
            }
            parameterValues[i - 1] = o;
            streamLengths[i - 1]   = streamLength;
            parameterSet[i - 1]    = Boolean.FALSE;

            return;
        } else if (o instanceof Reader) {
            parameterValues[i - 1] = o;
            streamLengths[i - 1]   = streamLength;
            parameterSet[i - 1]    = Boolean.FALSE;

            return;
        } else if (o instanceof String) {
            JDBCClob clob = new JDBCClob((String) o);

            parameterValues[i - 1] = clob;
            parameterSet[i - 1]    = false;

            return;
        }

        throw Util.invalidArgument();
    }

    
    void setBlobParameter(int i, Object o) throws SQLException {
        setBlobParameter(i, o, 0);
    }

    void setBlobParameter(int i, Object o,
                          long streamLength) throws SQLException {

        if (o instanceof JDBCBlobClient) {
            JDBCBlobClient blob = (JDBCBlobClient) o;

            if (!blob.session.getDatabaseUniqueName().equals(
                    session.getDatabaseUniqueName())) {
                streamLength = blob.length();

                InputStream is = blob.getBinaryStream();

                parameterValues[i - 1] = is;
                streamLengths[i - 1]   = streamLength;
                parameterSet[i - 1]    = Boolean.FALSE;

                return;
            }

            
            parameterValues[i - 1] = o;
            parameterSet[i - 1]    = Boolean.TRUE;

            return;
        } else if (o instanceof Blob) {
            parameterValues[i - 1] = o;
            parameterSet[i - 1]    = Boolean.FALSE;

            return;
        } else if (o instanceof BlobInputStream) {
            BlobInputStream is = (BlobInputStream) o;

            if (is.session.getDatabaseUniqueName().equals(
                    session.getDatabaseUniqueName())) {
                throw Util.sqlException(ErrorCode.JDBC_INVALID_ARGUMENT,
                                        "invalid Reader");
            }

            
            parameterValues[i - 1] = o;
            streamLengths[i - 1]   = streamLength;
            parameterSet[i - 1]    = Boolean.FALSE;

            return;
        } else if (o instanceof InputStream) {
            parameterValues[i - 1] = o;
            streamLengths[i - 1]   = streamLength;
            parameterSet[i - 1]    = Boolean.FALSE;

            return;
        } else if (o instanceof byte[]) {
            JDBCBlob blob = new JDBCBlob((byte[]) o);

            parameterValues[i - 1] = blob;
            parameterSet[i - 1]    = Boolean.TRUE;

            return;
        }

        throw Util.invalidArgument();
    }

    
    void setIntParameter(int i, int value) throws SQLException {

        checkSetParameterIndex(i);

        int outType = parameterTypes[i - 1].typeCode;

        switch (outType) {

            case Types.TINYINT :
            case Types.SQL_SMALLINT :
            case Types.SQL_INTEGER : {
                Object o = Integer.valueOf(value);

                parameterValues[i - 1] = o;
                parameterSet[i - 1]    = Boolean.TRUE;

                break;
            }
            case Types.SQL_BIGINT : {
                Object o = Long.valueOf(value);

                parameterValues[i - 1] = o;
                parameterSet[i - 1]    = Boolean.TRUE;

                break;
            }
            case Types.SQL_BINARY :
            case Types.SQL_VARBINARY :
            case Types.OTHER :
                throw Util.sqlException(Error.error(ErrorCode.X_42563));
            default :
                setParameter(i, new Integer(value));
        }
    }

    
    void setLongParameter(int i, long value) throws SQLException {

        checkSetParameterIndex(i);

        int outType = parameterTypes[i - 1].typeCode;

        switch (outType) {

            case Types.SQL_BIGINT :
                Object o = new Long(value);

                parameterValues[i - 1] = o;
                parameterSet[i - 1]    = Boolean.TRUE;

                break;
            case Types.SQL_BINARY :
            case Types.SQL_VARBINARY :
            case Types.OTHER :
                throw Util.sqlException(Error.error(ErrorCode.X_42563));
            default :
                setParameter(i, new Long(value));
        }
    }

    private void performPreExecute() throws SQLException, HsqlException {

        if (!hasLOBs) {
            return;
        }

        for (int i = 0; i < parameterValues.length; i++) {
            Object value = parameterValues[i];

            if (value == null) {
                continue;
            }

            if (parameterTypes[i].typeCode == Types.SQL_BLOB) {
                long       id;
                BlobDataID blob = null;

                if (value instanceof JDBCBlobClient) {

                    
                    blob = ((JDBCBlobClient) value).blob;
                    id   = blob.getId();
                } else if (value instanceof Blob) {
                    long length = ((Blob) value).length();

                    blob = session.createBlob(length);
                    id   = blob.getId();

                    InputStream stream = ((Blob) value).getBinaryStream();
                    ResultLob resultLob =
                        ResultLob.newLobCreateBlobRequest(session.getId(), id,
                            stream, length);

                    session.allocateResultLob(resultLob, null);
                    resultOut.addLobResult(resultLob);
                } else if (value instanceof InputStream) {
                    long length = streamLengths[i];

                    long createLength = length > 0 ? length : 0;

                    blob = session.createBlob(createLength);
                    id   = blob.getId();

                    InputStream stream = (InputStream) value;
                    ResultLob resultLob =
                        ResultLob.newLobCreateBlobRequest(session.getId(), id,
                            stream, length);

                    session.allocateResultLob(resultLob, null);
                    resultOut.addLobResult(resultLob);
                } else if (value instanceof BlobDataID) {
                    blob = (BlobDataID) value;
                }
                parameterValues[i] = blob;
            } else if (parameterTypes[i].typeCode == Types.SQL_CLOB) {
                long       id;
                ClobDataID clob = null;

                if (value instanceof JDBCClobClient) {

                    
                    clob = ((JDBCClobClient) value).clob;
                    id   = clob.getId();
                } else if (value instanceof Clob) {
                    long   length = ((Clob) value).length();
                    Reader reader = ((Clob) value).getCharacterStream();

                    clob = session.createClob(length);
                    id   = clob.getId();

                    ResultLob resultLob =
                        ResultLob.newLobCreateClobRequest(session.getId(), id,
                            reader, length);

                    session.allocateResultLob(resultLob, null);
                    resultOut.addLobResult(resultLob);
                } else if (value instanceof Reader) {
                    long length = streamLengths[i];

                    long createLength = length > 0 ? length : 0;

                    clob = session.createClob(createLength);
                    id   = clob.getId();

                    Reader reader = (Reader) value;
                    ResultLob resultLob =
                        ResultLob.newLobCreateClobRequest(session.getId(), id,
                            reader, length);

                    session.allocateResultLob(resultLob, null);
                    resultOut.addLobResult(resultLob);
                } else if (value instanceof ClobDataID) {
                    clob = (ClobDataID) value;
                }
                parameterValues[i] = clob;
            }
        }
    }

    
    void fetchResult() throws SQLException {

        if (isClosed || connection.isClosed) {
            checkClosed();
        }
        closeResultData();
        checkParametersSet();

        if (isBatch) {
            throw Util.sqlExceptionSQL(ErrorCode.X_07505);
        }

        
        if (isResult) {
            resultOut.setPreparedResultUpdateProperties(parameterValues);
        } else {
            resultOut.setPreparedExecuteProperties(parameterValues, maxRows,
                    fetchSize, rsProperties);
        }

        try {
            performPreExecute();

            resultIn = session.execute(resultOut);
        } catch (HsqlException e) {
            throw Util.sqlException(e);
        } finally {
            performPostExecute();
        }

        if (resultIn.mode == ResultConstants.ERROR) {
            throw Util.sqlException(resultIn);
        }

        if (resultIn.isData()) {
            currentResultSet = new JDBCResultSet(connection, this, resultIn,
                    resultIn.metaData);
        } else if (statementRetType == StatementTypes.RETURN_RESULT) {
            getMoreResults();
        }
    }

    boolean isAnyParameterSet() {

        for (int i = 0; i < parameterValues.length; i++) {
            if (parameterSet[i] != null) {
                return true;
            }
        }

        return false;
    }

    
    void performPostExecute() throws SQLException {
        super.performPostExecute();
    }

    
    protected Object[] parameterValues;

    
    protected Boolean[] parameterSet;

    
    protected Type[] parameterTypes;

    
    protected byte[] parameterModes;

    
    protected long[] streamLengths;

    
    protected boolean hasStreams;

    
    protected boolean hasLOBs;

    
    protected boolean isBatch;

    
    protected ResultMetaData resultMetaData;

    
    protected ResultMetaData parameterMetaData;

    
    protected JDBCResultSetMetaData resultSetMetaData;

    

    
    protected Object pmd;

    
    protected String sql;

    
    protected long statementID;

    
    protected int statementRetType;

    
    protected final boolean isResult;

    
    protected SessionInterface session;
}
