


package org.hsqldb.util;

import java.sql.Types;




class SqlServerTransferHelper extends TransferHelper {

    private boolean firstTinyintRow;
    private boolean firstSmallintRow;

    SqlServerTransferHelper() {
        super();
    }

    SqlServerTransferHelper(TransferDb database, Traceable t, String q) {
        super(database, t, q);
    }

    String formatTableName(String t) {

        if (t == null) {
            return t;
        }

        if (t.equals("")) {
            return t;
        }

        if (t.indexOf(' ') != -1) {
            return ("[" + t + "]");
        } else {
            return (formatIdentifier(t));
        }
    }

    int convertFromType(int type) {

        
        if (type == 11) {
            tracer.trace("Converted DATETIME (type 11) to TIMESTAMP");

            type = Types.TIMESTAMP;
        } else if (type == -9) {
            tracer.trace("Converted NVARCHAR (type -9) to VARCHAR");

            type = Types.VARCHAR;
        } else if (type == -8) {
            tracer.trace("Converted NCHAR (type -8) to VARCHAR");

            type = Types.VARCHAR;
        } else if (type == -10) {
            tracer.trace("Converted NTEXT (type -10) to VARCHAR");

            type = Types.VARCHAR;
        } else if (type == -1) {
            tracer.trace("Converted LONGTEXT (type -1) to LONGVARCHAR");

            type = Types.LONGVARCHAR;
        }

        return (type);
    }

    void beginTransfer() {
        firstSmallintRow = true;
        firstTinyintRow  = true;
    }

    Object convertColumnValue(Object value, int column, int type) {

        
        if ((type == Types.SMALLINT) && (value instanceof Integer)) {
            if (firstSmallintRow) {
                firstSmallintRow = false;

                tracer.trace("SMALLINT: Converted column " + column
                             + " Integer to Short");
            }

            value = new Short((short) ((Integer) value).intValue());
        } else if ((type == Types.TINYINT) && (value instanceof Integer)) {
            if (firstTinyintRow) {
                firstTinyintRow = false;

                tracer.trace("TINYINT: Converted column " + column
                             + " Integer to Byte");
            }

            value = new Byte((byte) ((Integer) value).intValue());
        }

        return (value);
    }
}
