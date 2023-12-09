


package org.hsqldb.jdbc;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.hsqldb.ColumnBase;
import org.hsqldb.SessionInterface;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.navigator.RowSetNavigatorClient;
import org.hsqldb.result.Result;
import org.hsqldb.result.ResultMetaData;
import org.hsqldb.types.Type;


public class JDBCArray implements Array {

    
    public String getBaseTypeName() throws SQLException {

        checkClosed();

        return elementType.getNameString();
    }

    
    public int getBaseType() throws SQLException {

        checkClosed();

        return elementType.getJDBCTypeCode();
    }

    
    public Object getArray() throws SQLException {

        checkClosed();

        Object[] array = new Object[data.length];

        for (int i = 0; i < data.length; i++) {
            array[i] = elementType.convertSQLToJava(sessionProxy, data[i]);
        }

        return array;
    }

    
    public Object getArray(java.util.Map<String,
            Class<?>> map) throws SQLException {
        return getArray();
    }

    
    public Object getArray(long index, int count) throws SQLException {

        checkClosed();

        if (!JDBCClobClient.isInLimits(data.length, index - 1, count)) {
            throw Util.outOfRangeArgument();
        }

        Object[] slice = new Object[count];

        for (int i = 0; i < count; i++) {
            slice[i] = elementType.convertSQLToJava(sessionProxy,
                    data[(int) index + i - 1]);
        }

        return slice;
    }

    
    public Object getArray(long index, int count,
                           java.util.Map<String,
                               Class<?>> map) throws SQLException {
        return getArray(index, count);
    }

    
    public ResultSet getResultSet() throws SQLException {

        checkClosed();

        Result result = this.newColumnResult(0, data.length);

        return new JDBCResultSet(connection, result, result.metaData);
    }

    
    public ResultSet getResultSet(java.util.Map<String,
            Class<?>> map) throws SQLException {
        return getResultSet();
    }

    
    public ResultSet getResultSet(long index, int count) throws SQLException {

        checkClosed();

        Result result = this.newColumnResult(index - 1, count);

        return new JDBCResultSet(connection, result, result.metaData);
    }

    
    public ResultSet getResultSet(long index, int count,
                                  java.util.Map<String,
                                      Class<?>> map) throws SQLException {
        return getResultSet(index, count);
    }

    
    public String toString() {

        if (arrayType == null) {
            arrayType = Type.getDefaultArrayType(elementType.typeCode);
        }

        return arrayType.convertToString(data);
    }

    
    public void free() throws SQLException {

        if (!closed) {
            closed       = true;
            connection   = null;
            sessionProxy = null;
        }
    }

    
    volatile boolean closed;
    Type             arrayType;
    Type             elementType;
    Object[]         data;
    JDBCConnection   connection;
    SessionInterface sessionProxy;

    public JDBCArray(Object[] data, Type type, Type arrayType,
                     SessionInterface session) {

        this(data, type, arrayType, session.getJDBCConnection());

        this.sessionProxy = session;
    }

    
    JDBCArray(Object[] data, Type type,
              JDBCConnection connection) throws SQLException {
        this(data, type, null, connection);
    }

    JDBCArray(Object[] data, Type type, Type arrayType,
              JDBCConnection connection) {

        this.data         = data;
        this.elementType  = type;
        this.arrayType    = arrayType;
        this.connection   = connection;
        this.sessionProxy = connection.sessionProxy;
    }

    public Object[] getArrayInternal() {
        return data;
    }

    private Result newColumnResult(long position,
                                   int count) throws SQLException {

        if (!JDBCClobClient.isInLimits(data.length, position, count)) {
            throw Util.outOfRangeArgument();
        }

        Type[] types = new Type[2];

        types[0] = Type.SQL_INTEGER;
        types[1] = elementType;

        ResultMetaData meta = ResultMetaData.newSimpleResultMetaData(types);

        meta.columnLabels = new String[] {
            "C1", "C2"
        };
        meta.colIndexes   = new int[] {
            -1, -1
        };
        meta.columns      = new ColumnBase[2];

        ColumnBase column = new ColumnBase("", "", "", "");

        column.setType(types[0]);

        meta.columns[0] = column;
        column          = new ColumnBase("", "", "", "");

        column.setType(types[1]);

        meta.columns[1] = column;

        RowSetNavigatorClient navigator = new RowSetNavigatorClient();

        for (int i = (int) position; i < position + count; i++) {
            Object[] rowData = new Object[2];

            rowData[0] = Integer.valueOf(i + 1);
            rowData[1] = data[i];

            navigator.add(rowData);
        }

        Result result = Result.newDataResult(meta);

        result.setNavigator(navigator);

        return result;
    }

    private void checkClosed() throws SQLException {

        if (closed) {
            throw Util.sqlException(ErrorCode.X_07501);
        }
    }
}
