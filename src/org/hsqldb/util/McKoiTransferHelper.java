


package org.hsqldb.util;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;


class McKoiTransferHelper extends TransferHelper {

    McKoiTransferHelper() {
        super();
    }

    String fixupColumnDefRead(TransferTable t, ResultSetMetaData meta,
                              String columnType, ResultSet columnDesc,
                              int columnIndex) throws SQLException {

        String CompareString = "UNIQUEKEY(\'" + t.Stmts.sDestTable + "\'";

        if (columnType.indexOf(CompareString) > 0) {

            
            columnType = "SERIAL";
        }

        return (columnType);
    }

    public McKoiTransferHelper(TransferDb database, Traceable t, String q) {
        super(database, t, q);
    }

    String fixupColumnDefWrite(TransferTable t, ResultSetMetaData meta,
                               String columnType, ResultSet columnDesc,
                               int columnIndex) throws SQLException {

        if (columnType.equals("SERIAL")) {
            columnType = "INTEGER DEFAULT UNIQUEKEY (\'"
                         + t.Stmts.sSourceTable + "\')";
        }

        return (columnType);
    }

    boolean needTransferTransaction() {
        return (true);
    }
}
