package org.hsqldb.util;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
class HsqldbTransferHelper extends TransferHelper {
    public HsqldbTransferHelper() {
        super();
    }
    public HsqldbTransferHelper(TransferDb database, Traceable t, String q) {
        super(database, t, q);
    }
    int convertFromType(int type) {
        if (type == 100) {
            type = Types.VARCHAR;
            tracer.trace("Converted HSQLDB VARCHAR_IGNORECASE to VARCHAR");
        }
        return (type);
    }
    String fixupColumnDefRead(TransferTable t, ResultSetMetaData meta,
                              String columnType, ResultSet columnDesc,
                              int columnIndex) throws SQLException {
        String CompareString = "INTEGER IDENTITY";
        if (columnType.indexOf(CompareString) >= 0) {
            columnType = "SERIAL";
        }
        return (columnType);
    }
    String fixupColumnDefWrite(TransferTable t, ResultSetMetaData meta,
                               String columnType, ResultSet columnDesc,
                               int columnIndex) throws SQLException {
        if (columnType.indexOf("SERIAL") >= 0) {
            columnType = "INTEGER GENERATED BY DEFAULT AS IDENTITY";
        }
        return (columnType);
    }
    String fixupColumnDefRead(String aTableName, ResultSetMetaData meta,
                              String columnType, ResultSet columnDesc,
                              int columnIndex) throws SQLException {
        return fixupColumnDefRead((TransferTable) null, meta, columnType,
                                  columnDesc, columnIndex);
    }
    String fixupColumnDefWrite(String aTableName, ResultSetMetaData meta,
                               String columnType, ResultSet columnDesc,
                               int columnIndex) throws SQLException {
        return fixupColumnDefWrite((TransferTable) null, meta, columnType,
                                   columnDesc, columnIndex);
    }
    String formatName(String t) {
        return formatIdentifier(t);
    }
}