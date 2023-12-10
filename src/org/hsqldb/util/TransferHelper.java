package org.hsqldb.util;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Clob;
import java.sql.Blob;
import java.sql.Types;
import java.util.Hashtable;
class TransferHelper {
    protected TransferDb db;
    protected Traceable  tracer;
    protected String     sSchema;
    protected JDBCTypes  JDBCT;
    private String       quote;
    TransferHelper() {
        db     = null;
        tracer = null;
        quote  = "\'";
        JDBCT  = new JDBCTypes();
    }
    TransferHelper(TransferDb database, Traceable t, String q) {
        db     = database;
        tracer = t;
        quote  = q;
        JDBCT  = new JDBCTypes();
    }
    void set(TransferDb database, Traceable t, String q) {
        db     = database;
        tracer = t;
        quote  = q;
    }
    String formatIdentifier(String id) {
        if (id == null) {
            return id;
        }
        if (id.equals("")) {
            return id;
        }
        if (!id.toUpperCase().equals(id)) {
            return (quote + id + quote);
        }
        if (!Character.isLetter(id.charAt(0)) || (id.indexOf(' ') != -1)) {
            return (quote + id + quote);
        }
        return id;
    }
    void setSchema(String _Schema) {
        sSchema = _Schema;
    }
    String formatName(String t) {
        String Name = "";
        if ((sSchema != null) && (sSchema.length() > 0)) {
            Name = sSchema + ".";
        }
        Name += formatIdentifier(t);
        return Name;
    }
    int convertFromType(int type) {
        return (type);
    }
    int convertToType(int type) {
        return (type);
    }
    Hashtable getSupportedTypes() {
        Hashtable hTypes = new Hashtable();
        if (db != null) {
            try {
                ResultSet result = db.meta.getTypeInfo();
                while (result.next()) {
                    Integer intobj = new Integer(result.getShort(2));
                    if (hTypes.get(intobj) == null) {
                        try {
                            int typeNumber = result.getShort(2);
                            hTypes.put(intobj, JDBCT.toString(typeNumber));
                        } catch (Exception e) {}
                    }
                }
                result.close();
            } catch (SQLException e) {}
        }
        if (hTypes.isEmpty()) {
            hTypes = JDBCT.getHashtable();
        }
        return hTypes;
    }
    String fixupColumnDefRead(TransferTable t, ResultSetMetaData meta,
                              String columnType, ResultSet columnDesc,
                              int columnIndex) throws SQLException {
        return (columnType);
    }
    String fixupColumnDefWrite(TransferTable t, ResultSetMetaData meta,
                               String columnType, ResultSet columnDesc,
                               int columnIndex) throws SQLException {
        return (columnType);
    }
    boolean needTransferTransaction() {
        return (false);
    }
    Object convertColumnValue(Object value, int column, int type) {
        if (value == null) {
            return value;
        }
        try {
            if (value instanceof Clob) {
                return ((Clob) value).getSubString(
                    1, (int) ((Clob) value).length());
            } else if (value instanceof Blob) {
                return ((Blob) value).getBytes(
                    1, (int) ((Blob) value).length());
            }
        } catch (SQLException e) {
            return null;
        }
        return (value);
    }
    void beginDataTransfer() {}
    void endDataTransfer() {}
    String fixupColumnDefRead(String aTableName, ResultSetMetaData meta,
                              String columnType, ResultSet columnDesc,
                              int columnIndex) throws SQLException {
        return columnType;
    }
    String fixupColumnDefWrite(String aTableName, ResultSetMetaData meta,
                               String columnType, ResultSet columnDesc,
                               int columnIndex) throws SQLException {
        return columnType;
    }
}