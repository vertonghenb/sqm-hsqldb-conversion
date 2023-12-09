


package org.hsqldb.util;

import java.io.Serializable;
import java.util.Vector;


class DataAccessPoint implements Serializable {

    Traceable      tracer;
    TransferHelper helper;
    String         databaseToConvert;

    public DataAccessPoint() {

        tracer            = null;
        helper            = HelperFactory.getHelper("");
        databaseToConvert = "";
    }

    public DataAccessPoint(Traceable t) {

        tracer = t;
        helper = HelperFactory.getHelper("");

        helper.set(null, t, "\'");

        databaseToConvert = "";
    }

    boolean isConnected() {
        return false;
    }

    boolean getAutoCommit() throws DataAccessPointException {
        return false;
    }

    void commit() throws DataAccessPointException {}

    void rollback() throws DataAccessPointException {}

    void setAutoCommit(boolean flag) throws DataAccessPointException {}

    boolean execute(String statement) throws DataAccessPointException {
        return false;
    }

    TransferResultSet getData(String statement)
    throws DataAccessPointException {
        return null;
    }

    void putData(String statement, TransferResultSet r,
                 int iMaxRows) throws DataAccessPointException {}

    Vector getSchemas() throws DataAccessPointException {
        return new Vector();
    }

    Vector getCatalog() throws DataAccessPointException {
        return new Vector();
    }

    void setCatalog(String sCatalog) throws DataAccessPointException {}

    Vector getTables(String sCatalog,
                     String[] sSchemas) throws DataAccessPointException {
        return new Vector();
    }

    void getTableStructure(TransferTable SQLCommands,
                           DataAccessPoint Dest)
                           throws DataAccessPointException {
        throw new DataAccessPointException("Nothing to Parse");
    }

    void close() throws DataAccessPointException {}

    void beginDataTransfer() throws DataAccessPointException {

        try {
            helper.beginDataTransfer();
        } catch (Exception e) {
            throw new DataAccessPointException(e.getMessage());
        }
    }

    void endDataTransfer() throws DataAccessPointException {

        try {
            helper.endDataTransfer();
        } catch (Exception e) {
            throw new DataAccessPointException(e.getMessage());
        }
    }

    
    public TransferHelper getHelper() {
        return helper;
    }
}
