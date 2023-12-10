package org.hsqldb.util;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.Hashtable;
class TransferTable implements Serializable {
    Hashtable       hTypes;
    DataAccessPoint sourceDb;
    DataAccessPoint destDb;
    SQLStatements   Stmts = null;
    Traceable       tracer;
    TransferTable(DataAccessPoint src, String name, String schema,
                  String type, Traceable t) {
        Stmts         = new SQLStatements();
        sourceDb      = src;
        Stmts.sSchema = "";
        if (schema != null && schema.length() > 0) {
            Stmts.sSchema = schema;
        }
        Stmts.sType              = type;
        Stmts.sDatabaseToConvert = src.databaseToConvert;
        Stmts.sSourceTable       = Stmts.sDestTable = name;
        tracer                   = t;
        if (Stmts.sType.compareTo("TABLE") == 0) {
            Stmts.sSourceSelect = "SELECT * FROM "
                                  + src.helper.formatName(Stmts.sSourceTable);
        } else if (Stmts.sType.compareTo("VIEW") == 0) {
            Stmts.sSourceSelect = "";
        }
    }
    void setDest(String _Schema, DataAccessPoint dest) throws Exception {
        destDb = dest;
        dest.helper.setSchema(_Schema);
    }
    void extractTableStructure(DataAccessPoint Source,
                               DataAccessPoint Destination) throws Exception {
        initTypes();
        Source.getTableStructure(this, Destination);
    }
    void transferStructure() throws Exception {
        String Statement = new String("");
        if (destDb.helper.needTransferTransaction()) {
            try {
                destDb.setAutoCommit(false);
            } catch (Exception e) {}
        }
        if (Stmts.bTransfer == false) {
            tracer.trace("Table " + Stmts.sSourceTable + " not transfered");
            return;
        }
        tracer.trace("Table " + Stmts.sSourceTable + ": start transfer");
        try {
            if (Stmts.bDropIndex) {
                if (Stmts.sDestDropIndex.charAt(Stmts.sDestDropIndex.length()
                                                - 1) != ';') {
                    Stmts.sDestDropIndex += ";";
                }
                int lastsemicolon = 0;
                int nextsemicolon = Stmts.sDestDropIndex.indexOf(';');
                while (nextsemicolon > lastsemicolon) {
                    Statement = Stmts.sDestDropIndex.substring(lastsemicolon,
                            nextsemicolon);
                    while (Statement.charAt(Statement.length() - 1) == ';') {
                        Statement = Statement.substring(0, Statement.length()
                                                        - 1);
                    }
                    try {
                        tracer.trace("Executing " + Statement);
                        destDb.execute(Statement);
                    } catch (Exception e) {
                        tracer.trace("Ignoring error " + e.getMessage());
                    }
                    lastsemicolon = nextsemicolon + 1;
                    nextsemicolon = lastsemicolon
                                    + Stmts.sDestDropIndex.substring(
                                        lastsemicolon).indexOf(';');
                }
            }
            if (Stmts.bDelete) {
                if (Stmts.sDestDelete.charAt(Stmts.sDestDelete.length() - 1)
                        != ';') {
                    Stmts.sDestDelete += ";";
                }
                int lastsemicolon = 0;
                int nextsemicolon = Stmts.sDestDelete.indexOf(';');
                while (nextsemicolon > lastsemicolon) {
                    Statement = Stmts.sDestDelete.substring(lastsemicolon,
                            nextsemicolon);
                    while (Statement.charAt(Statement.length() - 1) == ';') {
                        Statement = Statement.substring(0, Statement.length()
                                                        - 1);
                    }
                    try {
                        tracer.trace("Executing " + Statement);
                        destDb.execute(Statement);
                    } catch (Exception e) {
                        tracer.trace("Ignoring error " + e.getMessage());
                    }
                    lastsemicolon = nextsemicolon + 1;
                    nextsemicolon = lastsemicolon
                                    + Stmts.sDestDelete.substring(
                                        lastsemicolon).indexOf(';');
                }
            }
            if (Stmts.bDrop) {
                if (Stmts.sDestDrop.charAt(Stmts.sDestDrop.length() - 1)
                        != ';') {
                    Stmts.sDestDrop += ";";
                }
                int lastsemicolon = 0;
                int nextsemicolon = Stmts.sDestDrop.indexOf(';');
                while (nextsemicolon > lastsemicolon) {
                    Statement = Stmts.sDestDrop.substring(lastsemicolon,
                                                          nextsemicolon);
                    while (Statement.charAt(Statement.length() - 1) == ';') {
                        Statement = Statement.substring(0, Statement.length()
                                                        - 1);
                    }
                    try {
                        tracer.trace("Executing " + Statement);
                        destDb.execute(Statement);
                    } catch (Exception e) {
                        tracer.trace("Ignoring error " + e.getMessage());
                    }
                    lastsemicolon = nextsemicolon + 1;
                    nextsemicolon = lastsemicolon
                                    + Stmts.sDestDrop.substring(
                                        lastsemicolon).indexOf(';');
                }
            }
            if (Stmts.bCreate) {
                if (Stmts.sDestCreate.charAt(Stmts.sDestCreate.length() - 1)
                        != ';') {
                    Stmts.sDestCreate += ";";
                }
                int lastsemicolon = 0;
                int nextsemicolon = Stmts.sDestCreate.indexOf(';');
                while (nextsemicolon > lastsemicolon) {
                    Statement = Stmts.sDestCreate.substring(lastsemicolon,
                            nextsemicolon);
                    while (Statement.charAt(Statement.length() - 1) == ';') {
                        Statement = Statement.substring(0, Statement.length()
                                                        - 1);
                    }
                    tracer.trace("Executing " + Statement);
                    destDb.execute(Statement);
                    lastsemicolon = nextsemicolon + 1;
                    nextsemicolon = lastsemicolon
                                    + Stmts.sDestCreate.substring(
                                        lastsemicolon).indexOf(';');
                }
            }
        } catch (Exception e) {
            try {
                if (!destDb.getAutoCommit()) {
                    destDb.rollback();
                }
            } catch (Exception e1) {}
            throw (e);
        }
        if (!destDb.getAutoCommit()) {
            destDb.commit();
            try {
                destDb.setAutoCommit(true);
            } catch (Exception e) {}
        }
    }
    void transferData(int iMaxRows) throws Exception, SQLException {
        if (destDb.helper.needTransferTransaction()) {
            try {
                destDb.setAutoCommit(false);
            } catch (Exception e) {}
        }
        try {
            if (Stmts.bInsert) {
                if (destDb.helper.needTransferTransaction()) {
                    try {
                        destDb.setAutoCommit(false);
                    } catch (Exception e) {}
                }
                tracer.trace("Executing " + Stmts.sSourceSelect);
                TransferResultSet r = sourceDb.getData(Stmts.sSourceSelect);
                tracer.trace("Start transfering data...");
                destDb.beginDataTransfer();
                tracer.trace("Executing " + Stmts.sDestInsert);
                destDb.putData(Stmts.sDestInsert, r, iMaxRows);
                destDb.endDataTransfer();
                tracer.trace("Finished");
                if (!destDb.getAutoCommit()) {
                    destDb.commit();
                    try {
                        destDb.setAutoCommit(true);
                    } catch (Exception e) {}
                }
            }
        } catch (Exception e) {
            try {
                if (!destDb.getAutoCommit()) {
                    destDb.rollback();
                }
            } catch (Exception e1) {}
            throw (e);
        }
        if (!destDb.getAutoCommit()) {
            destDb.commit();
            try {
                destDb.setAutoCommit(true);
            } catch (Exception e) {}
        }
    }
    void transferAlter() throws Exception {
        String Statement = new String("");
        if (destDb.helper.needTransferTransaction()) {
            try {
                destDb.setAutoCommit(false);
            } catch (Exception e) {}
        }
        if (Stmts.bTransfer == false) {
            tracer.trace("Table " + Stmts.sSourceTable + " not transfered");
            return;
        }
        tracer.trace("Table " + Stmts.sSourceTable + ": start alter");
        try {
            if (Stmts.bCreateIndex) {
                if (Stmts.sDestCreateIndex.charAt(
                        Stmts.sDestCreateIndex.length() - 1) != ';') {
                    Stmts.sDestCreateIndex += ";";
                }
                int lastsemicolon = 0;
                int nextsemicolon = Stmts.sDestCreateIndex.indexOf(';');
                while (nextsemicolon > lastsemicolon) {
                    Statement = Stmts.sDestCreateIndex.substring(lastsemicolon,
                            nextsemicolon);
                    while (Statement.charAt(Statement.length() - 1) == ';') {
                        Statement = Statement.substring(0, Statement.length()
                                                        - 1);
                    }
                    try {
                        tracer.trace("Executing " + Stmts.sDestCreateIndex);
                        destDb.execute(Statement);
                    } catch (Exception e) {
                        tracer.trace("Ignoring error " + e.getMessage());
                    }
                    lastsemicolon = nextsemicolon + 1;
                    nextsemicolon = lastsemicolon
                                    + Stmts.sDestCreateIndex.substring(
                                        lastsemicolon).indexOf(';');
                }
            }
            if (Stmts.bAlter) {
                if (Stmts.sDestAlter.charAt(Stmts.sDestAlter.length() - 1)
                        != ';') {
                    Stmts.sDestAlter += ";";
                }
                int lastsemicolon = 0;
                int nextsemicolon = Stmts.sDestAlter.indexOf(';');
                while (nextsemicolon > lastsemicolon) {
                    Statement = Stmts.sDestAlter.substring(lastsemicolon,
                                                           nextsemicolon);
                    while (Statement.charAt(Statement.length() - 1) == ';') {
                        Statement = Statement.substring(0, Statement.length()
                                                        - 1);
                    }
                    try {
                        tracer.trace("Executing " + Statement);
                        destDb.execute(Statement);
                    } catch (Exception e) {
                        tracer.trace("Ignoring error " + e.getMessage());
                    }
                    lastsemicolon = nextsemicolon + 1;
                    nextsemicolon = lastsemicolon
                                    + Stmts.sDestAlter.substring(
                                        lastsemicolon).indexOf(';');
                }
            }
        } catch (Exception e) {
            try {
                if (!destDb.getAutoCommit()) {
                    destDb.rollback();
                }
            } catch (Exception e1) {}
            throw (e);
        }
        if (!destDb.getAutoCommit()) {
            destDb.commit();
            try {
                destDb.setAutoCommit(true);
            } catch (Exception e) {}
        }
    }
    private void initTypes() throws SQLException {
        if (hTypes != null) {
            return;
        }
        hTypes = destDb.helper.getSupportedTypes();
    }
}