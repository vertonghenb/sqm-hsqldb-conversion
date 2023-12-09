


package org.hsqldb.scriptio;

import org.hsqldb.Database;
import org.hsqldb.NumberSequence;
import org.hsqldb.Session;
import org.hsqldb.Table;
import org.hsqldb.persist.PersistentStore;


public abstract class ScriptReaderBase {

    public static final int ANY_STATEMENT        = 1;
    public static final int DELETE_STATEMENT     = 2;
    public static final int INSERT_STATEMENT     = 3;
    public static final int COMMIT_STATEMENT     = 4;
    public static final int SESSION_ID           = 5;
    public static final int SET_SCHEMA_STATEMENT = 6;
    Database                database;
    int                     lineCount;

    ScriptReaderBase(Database db) {
        this.database = db;
    }

    public void readAll(Session session) {
        readDDL(session);
        readExistingData(session);
    }

    protected abstract void readDDL(Session session);

    protected abstract void readExistingData(Session session);

    public abstract boolean readLoggedStatement(Session session);

    int             statementType;
    int             sessionNumber;
    boolean         sessionChanged;
    Object[]        rowData;
    long            sequenceValue;
    String          statement;
    Table           currentTable;
    PersistentStore currentStore;
    NumberSequence  currentSequence;
    String          currentSchema;

    public int getStatementType() {
        return statementType;
    }

    public int getSessionNumber() {
        return sessionNumber;
    }

    public Object[] getData() {
        return rowData;
    }

    public String getLoggedStatement() {
        return statement;
    }

    public NumberSequence getCurrentSequence() {
        return currentSequence;
    }

    public long getSequenceValue() {
        return sequenceValue;
    }

    public Table getCurrentTable() {
        return currentTable;
    }

    public String getCurrentSchema() {
        return currentSchema;
    }

    public int getLineNumber() {
        return lineCount;
    }

    public abstract void close();
}
