


package org.hsqldb.util;

import java.sql.Types;





class InformixTransferHelper extends TransferHelper {

    public InformixTransferHelper() {
        super();
    }

    public InformixTransferHelper(TransferDb database, Traceable t,
                                  String q) {
        super(database, t, q);
    }

    void setSchema(String _Schema) {
        sSchema = "\"" + _Schema + "\"";
    }

    int convertFromType(int type) {

        
        
        
        if (type == Types.TIMESTAMP) {
            type = Types.TIME;

            tracer.trace("Converted INFORMIX TIMESTAMP to TIME");
        } else if (type == Types.TIME) {
            type = Types.TIMESTAMP;

            tracer.trace("Converted INFORMIX TIME to TIMESTAMP");
        }

        return (type);
    }

    int convertToType(int type) {

        
        
        
        if (type == Types.TIMESTAMP) {
            type = Types.TIME;

            tracer.trace("Converted TIMESTAMP to INFORMIX TIME");
        } else if (type == Types.TIME) {
            type = Types.TIMESTAMP;

            tracer.trace("Converted TIME to INFORMIX TIMESTAMP");
        }

        return (type);
    }
}
