package org.hsqldb.persist;
import java.io.EOFException;
import java.io.InputStream;
import org.hsqldb.ColumnSchema;
import org.hsqldb.Database;
import org.hsqldb.HsqlException;
import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.Row;
import org.hsqldb.Session;
import org.hsqldb.Statement;
import org.hsqldb.StatementDML;
import org.hsqldb.StatementSchema;
import org.hsqldb.StatementTypes;
import org.hsqldb.Table;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.IntKeyHashMap;
import org.hsqldb.lib.StopWatch;
import org.hsqldb.result.Result;
import org.hsqldb.scriptio.ScriptReaderBase;
import org.hsqldb.scriptio.ScriptReaderDecode;
import org.hsqldb.scriptio.ScriptReaderText;
import org.hsqldb.store.ValuePool;
import org.hsqldb.types.Type;
public class ScriptRunner {
    public static void runScript(Database database, InputStream inputStream) {
        Crypto           crypto = database.logger.getCrypto();
        ScriptReaderBase scr;
        if (crypto == null) {
            scr = new ScriptReaderText(database, inputStream);
        } else {
            try {
                scr = new ScriptReaderDecode(database, inputStream, crypto,
                                             true);
            } catch (Throwable e) {
                database.logger.logSevereEvent("opening log file", e);
                return;
            }
        }
        runScript(database, scr);
    }
    public static void runScript(Database database, String logFilename) {
        Crypto           crypto = database.logger.getCrypto();
        ScriptReaderBase scr;
        try {
            if (crypto == null) {
                scr = new ScriptReaderText(database, logFilename, false);
            } else {
                scr = new ScriptReaderDecode(database, logFilename, crypto,
                                             true);
            }
        } catch (Throwable e) {
            if (e instanceof EOFException) {
            } else {
                database.logger.logSevereEvent("opening log file", e);
            }
            return;
        }
        runScript(database, scr);
    }
    private static void runScript(Database database, ScriptReaderBase scr) {
        IntKeyHashMap sessionMap = new IntKeyHashMap();
        Session       current    = null;
        int           currentId  = 0;
        String        statement;
        int           statementType;
        Statement dummy = new StatementDML(StatementTypes.UPDATE_CURSOR,
                                           StatementTypes.X_SQL_DATA_CHANGE,
                                           null);
        String databaseFile = database.getPath();
        boolean fullReplay = database.getURLProperties().isPropertyTrue(
            HsqlDatabaseProperties.hsqldb_full_log_replay);
        dummy.setCompileTimestamp(Long.MAX_VALUE);
        database.setReferentialIntegrity(false);
        try {
            StopWatch sw = new StopWatch();
            while (scr.readLoggedStatement(current)) {
                int sessionId = scr.getSessionNumber();
                if (current == null || currentId != sessionId) {
                    currentId = sessionId;
                    current   = (Session) sessionMap.get(currentId);
                    if (current == null) {
                        current =
                            database.getSessionManager().newSessionForLog(
                                database);
                        sessionMap.put(currentId, current);
                    }
                }
                if (current.isClosed()) {
                    sessionMap.remove(currentId);
                    continue;
                }
                Result result = null;
                statementType = scr.getStatementType();
                switch (statementType) {
                    case ScriptReaderBase.ANY_STATEMENT :
                        statement = scr.getLoggedStatement();
                        Statement cs;
                        try {
                            cs = current.compileStatement(statement);
                            if (database.getProperties().isVersion18()) {
                                if (cs.getType()
                                        == StatementTypes.CREATE_TABLE) {
                                    Table table =
                                        (Table) ((StatementSchema) cs)
                                            .getArguments()[0];
                                    for (int i = 0; i < table.getColumnCount();
                                            i++) {
                                        ColumnSchema column =
                                            table.getColumn(i);
                                        if (column.getDataType().isBitType()) {
                                            column.setType(Type.SQL_BOOLEAN);
                                        }
                                    }
                                }
                            }
                            result = current.executeCompiledStatement(cs,
                                    ValuePool.emptyObjectArray);
                        } catch (Throwable e) {
                            result = Result.newErrorResult(e);
                        }
                        if (result != null && result.isError()) {
                            if (result.getException() != null) {
                                throw result.getException();
                            }
                            throw Error.error(result);
                        }
                        break;
                    case ScriptReaderBase.COMMIT_STATEMENT :
                        current.commit(false);
                        break;
                    case ScriptReaderBase.INSERT_STATEMENT : {
                        current.sessionContext.currentStatement = dummy;
                        current.beginAction(dummy);
                        Object[] data = scr.getData();
                        scr.getCurrentTable().insertNoCheckFromLog(current,
                                data);
                        current.endAction(Result.updateOneResult);
                        break;
                    }
                    case ScriptReaderBase.DELETE_STATEMENT : {
                        current.sessionContext.currentStatement = dummy;
                        current.beginAction(dummy);
                        Table    table = scr.getCurrentTable();
                        Object[] data  = scr.getData();
                        Row row = table.getDeleteRowFromLog(current, data);
                        if (row != null) {
                            current.addDeleteAction(table, row, null);
                        }
                        current.endAction(Result.updateOneResult);
                        break;
                    }
                    case ScriptReaderBase.SET_SCHEMA_STATEMENT : {
                        HsqlName name =
                            database.schemaManager.findSchemaHsqlName(
                                scr.getCurrentSchema());
                        current.setCurrentSchemaHsqlName(name);
                        break;
                    }
                    case ScriptReaderBase.SESSION_ID : {
                        break;
                    }
                }
                if (current.isClosed()) {
                    sessionMap.remove(currentId);
                }
            }
        } catch (HsqlException e) {
            String error = "statement error processing log " + databaseFile
                           + "line: " + scr.getLineNumber();
            database.logger.logSevereEvent(error, e);
            if (fullReplay) {
                throw Error.error(e, ErrorCode.ERROR_IN_SCRIPT_FILE, error);
            }
        } catch (OutOfMemoryError e) {
            String error = "out of memory processing log" + databaseFile
                           + " line: " + scr.getLineNumber();
            database.logger.logSevereEvent(error, e);
            throw Error.error(ErrorCode.OUT_OF_MEMORY);
        } catch (Throwable e) {
            String error = "statement error processing log " + databaseFile
                           + "line: " + scr.getLineNumber();
            database.logger.logSevereEvent(error, e);
            if (fullReplay) {
                throw Error.error(e, ErrorCode.ERROR_IN_SCRIPT_FILE, error);
            }
        } finally {
            if (scr != null) {
                scr.close();
            }
            database.getSessionManager().closeAllSessions();
            database.setReferentialIntegrity(true);
        }
    }
}