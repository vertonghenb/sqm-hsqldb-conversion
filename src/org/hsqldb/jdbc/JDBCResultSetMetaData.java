package org.hsqldb.jdbc;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.persist.HsqlDatabaseProperties;
import org.hsqldb.result.ResultMetaData;
import org.hsqldb.types.CharacterType;
import org.hsqldb.types.DateTimeType;
import org.hsqldb.types.IntervalType;
import org.hsqldb.types.Type;
import org.hsqldb.types.Types;
public class JDBCResultSetMetaData implements ResultSetMetaData {
    public int getColumnCount() throws SQLException {
        return resultMetaData.getColumnCount();
    }
    public boolean isAutoIncrement(int column) throws SQLException {
        checkColumn(column);
        return resultMetaData.columns[--column].isIdentity();
    }
    public boolean isCaseSensitive(int column) throws SQLException {
        checkColumn(column);
        Type type = translateType(resultMetaData.columnTypes[--column]);
        if (type.isCharacterType()) {
            return !((CharacterType) type).isCaseInsensitive();
        }
        return false;
    }
    public boolean isSearchable(int column) throws SQLException {
        checkColumn(column);
        return resultMetaData.columns[--column].isSearchable();
    }
    public boolean isCurrency(int column) throws SQLException {
        checkColumn(column);
        Type type = translateType(resultMetaData.columnTypes[--column]);
        return (type.typeCode == Types.SQL_DECIMAL
                || type.typeCode == Types.SQL_NUMERIC) && type.scale > 0;
    }
    public int isNullable(int column) throws SQLException {
        checkColumn(column);
        return resultMetaData.columns[--column].getNullability();
    }
    public boolean isSigned(int column) throws SQLException {
        checkColumn(column);
        Type type = translateType(resultMetaData.columnTypes[--column]);
        return type.isNumberType();
    }
    public int getColumnDisplaySize(int column) throws SQLException {
        checkColumn(column);
        Type type = translateType(resultMetaData.columnTypes[--column]);
        return type.displaySize();
    }
    public String getColumnLabel(int column) throws SQLException {
        checkColumn(column--);
        String label = resultMetaData.columnLabels[column];
        if (label != null && label.length() > 0) {
            return label;
        }
        return resultMetaData.columns[column].getNameString();
    }
    public String getColumnName(int column) throws SQLException {
        checkColumn(column--);
        if (useColumnName) {
            String name = resultMetaData.columns[column].getNameString();
            if (name != null && name.length() > 0) {
                return name;
            }
        }
        String label = resultMetaData.columnLabels[column];
        return label == null ? resultMetaData.columns[column].getNameString()
                             : label;
    }
    public String getSchemaName(int column) throws SQLException {
        checkColumn(column);
        String name = resultMetaData.columns[--column].getSchemaNameString();;
        return name == null ? ""
                            : name;
    }
    public int getPrecision(int column) throws SQLException {
        checkColumn(column);
        Type type      = translateType(resultMetaData.columnTypes[--column]);
        return type.getJDBCPrecision();
    }
    public int getScale(int column) throws SQLException {
        checkColumn(column);
        Type type = translateType(resultMetaData.columnTypes[--column]);
        return type.getJDBCScale();
    }
    public String getTableName(int column) throws SQLException {
        checkColumn(column);
        String name = resultMetaData.columns[--column].getTableNameString();
        return name == null ? ""
                            : name;
    }
    public String getCatalogName(int column) throws SQLException {
        checkColumn(column);
        String name = resultMetaData.columns[--column].getCatalogNameString();
        return name == null ? ""
                            : name;
    }
    public int getColumnType(int column) throws SQLException {
        checkColumn(column);
        Type type = translateType(resultMetaData.columnTypes[--column]);
        return type.getJDBCTypeCode();
    }
    public String getColumnTypeName(int column) throws SQLException {
        checkColumn(column);
        Type type = translateType(resultMetaData.columnTypes[--column]);
        return type.getNameString();
    }
    public boolean isReadOnly(int column) throws SQLException {
        checkColumn(column);
        return !resultMetaData.columns[--column].isWriteable();
    }
    public boolean isWritable(int column) throws SQLException {
        checkColumn(column);
        return resultMetaData.colIndexes != null
               && resultMetaData.colIndexes[--column] > -1;
    }
    public boolean isDefinitelyWritable(int column) throws SQLException {
        checkColumn(column);
        return resultMetaData.colIndexes != null
               && resultMetaData.colIndexes[--column] > -1;
    }
    public String getColumnClassName(int column) throws SQLException {
        checkColumn(column);
        Type type = translateType(resultMetaData.columnTypes[--column]);
        return type.getJDBCClassName();
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
    private ResultMetaData resultMetaData;
    private boolean useColumnName;
    private boolean translateTTIType;
    private int     columnCount;
    JDBCResultSetMetaData(ResultMetaData meta, boolean isUpdatable,
                          boolean isInsertable,
                          JDBCConnection conn) throws SQLException {
        init(meta, conn);
    }
    void init(ResultMetaData meta, JDBCConnection conn) throws SQLException {
        resultMetaData = meta;
        columnCount    = resultMetaData.getColumnCount();
        useColumnName = true;
        if (conn == null) {
            return;
        }
        if (conn.connProperties != null) {
            useColumnName = conn.connProperties.isPropertyTrue(
                HsqlDatabaseProperties.url_get_column_name, true);
        }
        if (conn.clientProperties != null) {
            translateTTIType = conn.clientProperties.isPropertyTrue(
                HsqlDatabaseProperties.jdbc_translate_tti_types);
        }
    }
    private void checkColumn(int column) throws SQLException {
        if (column < 1 || column > columnCount) {
            throw Util.sqlException(ErrorCode.JDBC_COLUMN_NOT_FOUND,
                                    String.valueOf(column));
        }
    }
    private Type translateType(Type type) {
        if (this.translateTTIType) {
            if (type.isIntervalType()) {
                type = ((IntervalType) type).getCharacterType();
            } else if (type.isDateTimeTypeWithZone()) {
                type = ((DateTimeType) type).getDateTimeTypeWithoutZone();
            }
        }
        return type;
    }
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append(super.toString());
        if (columnCount == 0) {
            sb.append("[columnCount=0]");
            return sb.toString();
        }
        sb.append('[');
        for (int i = 0; i < columnCount; i++) {
            JDBCColumnMetaData meta = getColumnMetaData(i + 1);
            sb.append('\n');
            sb.append("   column_");
            sb.append(i + 1);
            sb.append('=');
            sb.append(meta);
            if (i + 1 < columnCount) {
                sb.append(',');
                sb.append(' ');
            }
        }
        sb.append('\n');
        sb.append(']');
        return sb.toString();
    }
    JDBCColumnMetaData getColumnMetaData(int i) {
        JDBCColumnMetaData meta = new JDBCColumnMetaData();
        try {
            meta.catalogName          = getCatalogName(i);
            meta.columnClassName      = getColumnClassName(i);
            meta.columnDisplaySize    = getColumnDisplaySize(i);
            meta.columnLabel          = getColumnLabel(i);
            meta.columnName           = getColumnName(i);
            meta.columnType           = getColumnType(i);
            meta.isAutoIncrement      = isAutoIncrement(i);
            meta.isCaseSensitive      = isCaseSensitive(i);
            meta.isCurrency           = isCurrency(i);
            meta.isDefinitelyWritable = isDefinitelyWritable(i);
            meta.isNullable           = isNullable(i);
            meta.isReadOnly           = isReadOnly(i);
            meta.isSearchable         = isSearchable(i);
            meta.isSigned             = isSigned(i);
            meta.isWritable           = isWritable(i);
            meta.precision            = getPrecision(i);
            meta.scale                = getScale(i);
            meta.schemaName           = getSchemaName(i);
            meta.tableName            = getTableName(i);
        } catch (SQLException e) {
        }
        return meta;
    }
}