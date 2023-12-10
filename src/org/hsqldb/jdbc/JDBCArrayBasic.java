package org.hsqldb.jdbc;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.hsqldb.ColumnBase;
import org.hsqldb.navigator.RowSetNavigatorClient;
import org.hsqldb.result.Result;
import org.hsqldb.result.ResultMetaData;
import org.hsqldb.types.Type;
public class JDBCArrayBasic implements Array {
    public String getBaseTypeName() throws SQLException {
        checkClosed();
        return elementType.getNameString();
    }
    public int getBaseType() throws SQLException {
        checkClosed();
        return elementType.getJDBCTypeCode();
    }
    public Object getArray() {
        return data;
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
            slice[i] = data[(int) index + i - 1];
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
        return JDBCResultSet.newJDBCResultSet(result, result.metaData);
    }
    public ResultSet getResultSet(java.util.Map<String,
            Class<?>> map) throws SQLException {
        return getResultSet();
    }
    public ResultSet getResultSet(long index, int count) throws SQLException {
        checkClosed();
        Result result = this.newColumnResult(index - 1, count);
        return new JDBCResultSet(null, result, result.metaData);
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
    }
    Type     arrayType;
    Type     elementType;
    Object[] data;
    public JDBCArrayBasic(Object[] data, Type type) {
        this.data        = data;
        this.elementType = type;
    }
    Object[] getArrayInternal() {
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
        for (int i = 0; i < meta.columns.length; i++) {
            ColumnBase column = new ColumnBase("", "", "", "");
            column.setType(types[i]);
            meta.columns[i] = column;
        }
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
    }
}