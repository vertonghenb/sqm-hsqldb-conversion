package org.hsqldb.scriptio;
import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;
import org.hsqldb.Database;
import org.hsqldb.HsqlException;
import org.hsqldb.Session;
import org.hsqldb.Statement;
import org.hsqldb.StatementTypes;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.LineReader;
import org.hsqldb.lib.StringConverter;
import org.hsqldb.result.Result;
import org.hsqldb.rowio.RowInputTextLog;
import org.hsqldb.store.ValuePool;
import org.hsqldb.types.Type;
public class ScriptReaderText extends ScriptReaderBase {
    LineReader      dataStreamIn;
    RowInputTextLog rowIn;
    boolean         isInsert;
    public ScriptReaderText(Database db) {
        super(db);
    }
    public ScriptReaderText(Database db, String fileName,
                            boolean compressed) throws IOException {
        super(db);
        InputStream inputStream =
            database.logger.getFileAccess().openInputStreamElement(fileName);
        inputStream = new BufferedInputStream(inputStream);
        if (compressed) {
            inputStream = new GZIPInputStream(inputStream);
        }
        dataStreamIn = new LineReader(inputStream,
                                      ScriptWriterText.ISO_8859_1);
        rowIn = new RowInputTextLog(db.databaseProperties.isVersion18());
    }
    public ScriptReaderText(Database db, InputStream inputStream) {
        super(db);
        dataStreamIn = new LineReader(inputStream,
                                      ScriptWriterText.ISO_8859_1);
        rowIn = new RowInputTextLog(db.databaseProperties.isVersion18());
    }
    protected void readDDL(Session session) {
        for (; readLoggedStatement(session); ) {
            Statement cs     = null;
            Result    result = null;
            if (rowIn.getStatementType() == INSERT_STATEMENT) {
                isInsert = true;
                break;
            }
            try {
                cs = session.compileStatement(statement);
                result = session.executeCompiledStatement(cs,
                        ValuePool.emptyObjectArray);
            } catch (HsqlException e) {
                result = Result.newErrorResult(e);
            }
            if (result.isError()) {
                if (cs == null) {}
                else if (cs.getType() == StatementTypes.GRANT) {
                    continue;
                } else if (cs.getType() == StatementTypes.CREATE_ROUTINE) {
                    if (result.getMainString().contains(
                            "org.hsqldb.Library")) {
                        continue;
                    }
                }
            }
            if (result.isError()) {
                database.logger.logWarningEvent(result.getMainString(),
                                                result.getException());
                if (cs != null
                        && cs.getType() == StatementTypes.CREATE_ROUTINE) {
                    continue;
                }
                throw Error.error(result.getException(),
                                  ErrorCode.ERROR_IN_SCRIPT_FILE,
                                  ErrorCode.M_DatabaseScriptReader_read,
                                  new Object[] {
                    new Integer(lineCount), result.getMainString()
                });
            }
        }
    }
    protected void readExistingData(Session session) {
        try {
            String tablename = null;
            database.setReferentialIntegrity(false);
            for (; isInsert || readLoggedStatement(session);
                    isInsert = false) {
                if (statementType == SET_SCHEMA_STATEMENT) {
                    session.setSchema(currentSchema);
                    continue;
                } else if (statementType == INSERT_STATEMENT) {
                    if (!rowIn.getTableName().equals(tablename)) {
                        tablename = rowIn.getTableName();
                        String schema = session.getSchemaName(currentSchema);
                        currentTable =
                            database.schemaManager.getUserTable(session,
                                tablename, schema);
                        currentStore =
                            database.persistentStoreCollection.getStore(
                                currentTable);
                    }
                    currentTable.insertFromScript(session, currentStore,
                                                  rowData);
                } else {
                    throw Error.error(ErrorCode.ERROR_IN_SCRIPT_FILE,
                                      statement);
                }
            }
            database.setReferentialIntegrity(true);
        } catch (Throwable t) {
            database.logger.logSevereEvent("readExistingData failed", t);
            throw Error.error(t, ErrorCode.ERROR_IN_SCRIPT_FILE,
                              ErrorCode.M_DatabaseScriptReader_read,
                              new Object[] {
                new Integer(lineCount), t.toString()
            });
        }
    }
    public boolean readLoggedStatement(Session session) {
        if (!sessionChanged) {
            String s;
            try {
                s = dataStreamIn.readLine();
            } catch (EOFException e) {
                return false;
            } catch (IOException e) {
                throw Error.error(e, ErrorCode.FILE_IO_ERROR, null);
            }
            lineCount++;
            statement = StringConverter.unicodeStringToString(s);
            if (statement == null) {
                return false;
            }
        }
        processStatement(session);
        return true;
    }
    void processStatement(Session session) {
        if (statement.startsWith("/*C")) {
            int endid = statement.indexOf('*', 4);
            sessionNumber  = Integer.parseInt(statement.substring(3, endid));
            statement      = statement.substring(endid + 2);
            sessionChanged = true;
            statementType  = SESSION_ID;
            return;
        }
        sessionChanged = false;
        rowIn.setSource(statement);
        statementType = rowIn.getStatementType();
        if (statementType == ANY_STATEMENT) {
            rowData      = null;
            currentTable = null;
            return;
        } else if (statementType == COMMIT_STATEMENT) {
            rowData      = null;
            currentTable = null;
            return;
        } else if (statementType == SET_SCHEMA_STATEMENT) {
            rowData       = null;
            currentTable  = null;
            currentSchema = rowIn.getSchemaName();
            return;
        }
        String name   = rowIn.getTableName();
        String schema = session.getCurrentSchemaHsqlName().name;
        currentTable = database.schemaManager.getUserTable(session, name,
                schema);
        currentStore =
            database.persistentStoreCollection.getStore(currentTable);
        Type[] colTypes;
        if (statementType == INSERT_STATEMENT) {
            colTypes = currentTable.getColumnTypes();
        } else if (currentTable.hasPrimaryKey()) {
            colTypes = currentTable.getPrimaryKeyTypes();
        } else {
            colTypes = currentTable.getColumnTypes();
        }
        try {
            rowData = rowIn.readData(colTypes);
        } catch (IOException e) {
            throw Error.error(e, ErrorCode.FILE_IO_ERROR, null);
        }
    }
    public void close() {
        try {
            dataStreamIn.close();
        } catch (Exception e) {}
    }
}