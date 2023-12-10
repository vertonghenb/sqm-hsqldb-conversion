package org.hsqldb.util;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
class PostgresTransferHelper extends TransferHelper {
    private final int PostgreSQL = 0;
    private final int HSQLDB     = 1;
    String[][]        Funcs      = {
        {
            "now()", "\'now\'"
        }
    };
    PostgresTransferHelper() {
        super();
    }
    PostgresTransferHelper(TransferDb database, Traceable t, String q) {
        super(database, t, q);
    }
    int convertToType(int type) {
        if (type == Types.DECIMAL) {
            type = Types.NUMERIC;
            tracer.trace("Converted DECIMAL to NUMERIC");
        }
        return (type);
    }
    String fixupColumnDefRead(TransferTable t, ResultSetMetaData meta,
                              String columnType, ResultSet columnDesc,
                              int columnIndex) throws SQLException {
        String SeqName   = new String("_" + columnDesc.getString(4) + "_seq");
        int    spaceleft = 31 - SeqName.length();
        if (t.Stmts.sDestTable.length() > spaceleft) {
            SeqName = t.Stmts.sDestTable.substring(0, spaceleft) + SeqName;
        } else {
            SeqName = t.Stmts.sDestTable + SeqName;
        }
        String CompareString = "nextval(\'\"" + SeqName + "\"\'";
        if (columnType.indexOf(CompareString) >= 0) {
            columnType = "SERIAL";
        }
        for (int Idx = 0; Idx < Funcs.length; Idx++) {
            String PostgreSQL_func = Funcs[Idx][PostgreSQL];
            int    iStartPos       = columnType.indexOf(PostgreSQL_func);
            if (iStartPos >= 0) {
                String NewColumnType = columnType.substring(0, iStartPos);
                NewColumnType += Funcs[Idx][HSQLDB];
                NewColumnType +=
                    columnType.substring(iStartPos
                                         + PostgreSQL_func.length());
                columnType = NewColumnType;
            }
        }
        return (columnType);
    }
    String fixupColumnDefWrite(TransferTable t, ResultSetMetaData meta,
                               String columnType, ResultSet columnDesc,
                               int columnIndex) throws SQLException {
        if (columnType.equals("SERIAL")) {
            String SeqName = new String("_" + columnDesc.getString(4)
                                        + "_seq");
            int spaceleft = 31 - SeqName.length();
            if (t.Stmts.sDestTable.length() > spaceleft) {
                SeqName = t.Stmts.sDestTable.substring(0, spaceleft)
                          + SeqName;
            } else {
                SeqName = t.Stmts.sDestTable + SeqName;
            }
            String DropSequence = "DROP SEQUENCE " + SeqName + ";";
            t.Stmts.sDestDrop += DropSequence;
        }
        for (int Idx = 0; Idx < Funcs.length; Idx++) {
            String HSQLDB_func = Funcs[Idx][HSQLDB];
            int    iStartPos   = columnType.indexOf(HSQLDB_func);
            if (iStartPos >= 0) {
                String NewColumnType = columnType.substring(0, iStartPos);
                NewColumnType += Funcs[Idx][PostgreSQL];
                NewColumnType += columnType.substring(iStartPos
                                                      + HSQLDB_func.length());
                columnType = NewColumnType;
            }
        }
        return (columnType);
    }
    void beginDataTransfer() {
        try {
            db.setAutoCommit(false);
        } catch (Exception e) {}
    }
    void endDataTransfer() {
        try {
            db.commit();
            db.execute("VACUUM ANALYZE");
        } catch (Exception e) {}
    }
}