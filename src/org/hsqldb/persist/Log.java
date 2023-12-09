


package org.hsqldb.persist;

import java.io.File;
import java.io.IOException;

import org.hsqldb.Database;
import org.hsqldb.HsqlException;
import org.hsqldb.HsqlNameManager;
import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.NumberSequence;
import org.hsqldb.Row;
import org.hsqldb.Session;
import org.hsqldb.SqlInvariants;
import org.hsqldb.Table;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.FileAccess;
import org.hsqldb.lib.FileArchiver;
import org.hsqldb.scriptio.ScriptReaderBase;
import org.hsqldb.scriptio.ScriptReaderDecode;
import org.hsqldb.scriptio.ScriptReaderText;
import org.hsqldb.scriptio.ScriptWriterBase;
import org.hsqldb.scriptio.ScriptWriterEncode;
import org.hsqldb.scriptio.ScriptWriterText;


public class Log {

    private HsqlDatabaseProperties properties;
    private String                 fileName;
    private Database               database;
    private FileAccess             fa;
    ScriptWriterBase               dbLogWriter;
    private String                 scriptFileName;
    private String                 logFileName;
    private boolean                filesReadOnly;
    private long                   maxLogSize;
    private int                    writeDelay;
    private DataFileCache          cache;

    Log(Database db) {

        database   = db;
        fa         = db.logger.getFileAccess();
        fileName   = db.getPath();
        properties = db.getProperties();
    }

    void initParams() {

        maxLogSize     = database.logger.propLogSize * 1024L * 1024;
        writeDelay     = database.logger.propWriteDelay;
        filesReadOnly  = database.isFilesReadOnly();
        scriptFileName = fileName + Logger.scriptFileExtension;
        logFileName    = fileName + Logger.logFileExtension;
    }

    
    void open() {

        initParams();

        int state = properties.getDBModified();

        switch (state) {

            case HsqlDatabaseProperties.FILES_NEW :
                break;

            case HsqlDatabaseProperties.FILES_MODIFIED :
                deleteNewAndOldFiles();
                deleteOldTempFiles();

                if (properties.isVersion18()) {
                    if (fa.isStreamElement(scriptFileName)) {
                        processScript();
                    } else {
                        database.schemaManager.createPublicSchema();
                    }

                    HsqlName name = database.schemaManager.findSchemaHsqlName(
                        SqlInvariants.PUBLIC_SCHEMA);

                    if (name != null) {
                        database.schemaManager.setDefaultSchemaHsqlName(name);
                    }
                } else {
                    processScript();
                }

                processLog();
                checkpoint();
                break;

            case HsqlDatabaseProperties.FILES_MODIFIED_NEW :
                renameNewDataFile();
                renameNewBackup();
                renameNewScript();
                deleteLog();
                properties.setDBModified(
                    HsqlDatabaseProperties.FILES_NOT_MODIFIED);

            
            
            case HsqlDatabaseProperties.FILES_NOT_MODIFIED :

                
                processScript();

                if (!filesReadOnly && isAnyCacheModified()) {
                    properties.setDBModified(
                        HsqlDatabaseProperties.FILES_MODIFIED);
                    checkpoint();
                }
                break;
        }

        if (!filesReadOnly) {
            openLog();
            properties.setDBModified(HsqlDatabaseProperties.FILES_MODIFIED);
        }
    }

    
    void close(boolean script) {

        closeLog();
        deleteOldDataFiles();
        deleteOldTempFiles();
        deleteTempFileDirectory();
        writeScript(script);
        database.logger.closeAllTextCaches(script);

        if (cache != null) {
            cache.close(true);
        }

        
        properties.setProperty(HsqlDatabaseProperties.hsqldb_script_format,
                               database.logger.propScriptFormat);
        properties.setDBModified(HsqlDatabaseProperties.FILES_MODIFIED_NEW);
        deleteLog();

        if (cache != null) {
            if (script) {
                cache.deleteFile();
                cache.deleteBackup();
            } else {
                cache.backupFile(false);
                cache.renameBackupFile();
            }
        }

        renameNewScript();
        properties.setDBModified(HsqlDatabaseProperties.FILES_NOT_MODIFIED);
    }

    
    void shutdown() {

        if (cache != null) {
            cache.close(false);
        }

        database.logger.closeAllTextCaches(false);
        closeLog();
    }

    
    void deleteNewAndOldFiles() {

        deleteOldDataFiles();
        fa.removeElement(fileName + Logger.dataFileExtension
                         + Logger.newFileExtension);
        fa.removeElement(fileName + Logger.backupFileExtension
                         + Logger.newFileExtension);
        fa.removeElement(scriptFileName + Logger.newFileExtension);
    }

    void deleteBackup() {
        fa.removeElement(fileName + Logger.backupFileExtension);
    }

    void deleteData() {
        fa.removeElement(fileName + Logger.dataFileExtension);
    }

    void backupData() throws IOException {

        if (database.logger.propIncrementBackup) {
            fa.removeElement(fileName + Logger.backupFileExtension);

            return;
        }

        if (fa.isStreamElement(fileName + Logger.dataFileExtension)) {
            FileArchiver.archive(
                fileName + Logger.dataFileExtension,
                fileName + Logger.backupFileExtension
                + Logger.newFileExtension, database.logger.getFileAccess(),
                                           FileArchiver.COMPRESSION_ZIP);
        }
    }

    void renameNewDataFile() {

        if (fa.isStreamElement(fileName + Logger.dataFileExtension
                               + Logger.newFileExtension)) {
            fa.renameElement(fileName + Logger.dataFileExtension
                             + Logger.newFileExtension, fileName
                                 + Logger.dataFileExtension);
        }
    }

    void renameNewBackup() {

        
        fa.removeElement(fileName + Logger.backupFileExtension);

        if (fa.isStreamElement(fileName + Logger.backupFileExtension
                               + Logger.newFileExtension)) {
            fa.renameElement(fileName + Logger.backupFileExtension
                             + Logger.newFileExtension, fileName
                                 + Logger.backupFileExtension);
        }
    }

    void renameNewScript() {

        if (fa.isStreamElement(scriptFileName + Logger.newFileExtension)) {
            fa.renameElement(scriptFileName + Logger.newFileExtension,
                             scriptFileName);
        }
    }

    void deleteNewScript() {
        fa.removeElement(scriptFileName + Logger.newFileExtension);
    }

    void deleteNewBackup() {
        fa.removeElement(fileName + Logger.backupFileExtension
                         + Logger.newFileExtension);
    }

    void deleteLog() {
        fa.removeElement(logFileName);
    }

    
    boolean isAnyCacheModified() {

        if (cache != null && cache.isModified()) {
            return true;
        }

        return database.logger.isAnyTextCacheModified();
    }

    void checkpoint() {

        if (filesReadOnly) {
            return;
        }

        boolean result = checkpointClose();

        if (result) {
            checkpointReopen();
        } else {
            database.logger.logSevereEvent(
                "checkpoint failed - see previous error", null);
        }
    }

    
    void checkpoint(boolean defrag) {

        if (filesReadOnly) {
            return;
        }

        if (cache == null) {
            defrag = false;
        } else if (forceDefrag()) {
            defrag = true;
        }

        if (defrag) {
            try {
                defrag();
                database.sessionManager.resetLoggedSchemas();

                return;
            } catch (Throwable e) {
                database.logger.logSevereEvent("defrag failed", e);

                
            }
        }

        checkpoint();
    }

    
    boolean checkpointClose() {

        if (filesReadOnly) {
            return true;
        }

        database.logger.logInfoEvent("checkpointClose start");
        synchLog();
        database.lobManager.synch();

        if (!database.txManager.isMVCC()) {
            database.lobManager.deleteUnusedLobs();
        }

        deleteOldDataFiles();

        try {
            writeScript(false);
        } catch (Throwable e) {
            deleteNewScript();
            database.logger.logSevereEvent("checkpoint failed - recovered", e);

            return false;
        }

        try {
            if (cache != null) {
                cache.commitChanges();
                cache.backupFile(false);
            }
        } catch (Throwable ee) {

            
            deleteNewScript();
            deleteNewBackup();

            try {
                if (!cache.isFileOpen()) {
                    cache.open(false);
                }
            } catch (Throwable e1) {}

            database.logger.logSevereEvent("checkpoint failed - recovered",
                                           ee);

            return false;
        }

        closeLog();
        properties.setProperty(HsqlDatabaseProperties.hsqldb_script_format,
                               database.logger.propScriptFormat);
        properties.setDBModified(HsqlDatabaseProperties.FILES_MODIFIED_NEW);
        deleteLog();
        renameNewScript();
        renameNewBackup();

        try {
            properties.setDBModified(
                HsqlDatabaseProperties.FILES_NOT_MODIFIED);
        } catch (Throwable e) {}

        database.logger.logInfoEvent("checkpointClose end");

        return true;
    }

    boolean checkpointReopen() {

        if (filesReadOnly) {
            return true;
        }

        database.sessionManager.resetLoggedSchemas();

        try {
            if (cache != null) {
                cache.openShadowFile();
            }

            if (dbLogWriter != null) {
                openLog();
            }

            properties.setDBModified(HsqlDatabaseProperties.FILES_MODIFIED);
        } catch (Throwable e) {
            return false;
        }

        return true;
    }

    
    public void defrag() {

        if (cache.fileFreePosition == cache.initialFreePos) {
            return;
        }

        database.logger.logInfoEvent("defrag start");

        try {





            synchLog();
            database.lobManager.synch();
            deleteOldDataFiles();

            DataFileDefrag dfd = cache.defrag();
        } catch (HsqlException e) {
            throw e;
        } catch (Throwable e) {
            database.logger.logSevereEvent("defrag failure", e);

            throw Error.error(ErrorCode.DATA_FILE_ERROR, e);
        }





        database.logger.logInfoEvent("defrag end");
    }

    
    boolean forceDefrag() {

        long limit = database.logger.propCacheDefragLimit
                     * cache.getFileFreePos() / 100;
        long lostSize = cache.freeBlocks.getLostBlocksSize();

        return limit > 0 && lostSize > limit;
    }

    
    boolean hasCache() {
        return cache != null;
    }

    
    DataFileCache getCache() {

        if (cache == null) {
            cache = new DataFileCache(database, fileName);

            cache.open(filesReadOnly);
        }

        return cache;
    }

    void setLogSize(int megas) {
        maxLogSize = megas * 1024L * 1024;
    }

    
    int getWriteDelay() {
        return writeDelay;
    }

    void setWriteDelay(int delay) {

        writeDelay = delay;

        if (dbLogWriter != null && dbLogWriter.getWriteDelay() != delay) {
            dbLogWriter.forceSync();
            dbLogWriter.stop();
            dbLogWriter.setWriteDelay(delay);
            dbLogWriter.start();
        }
    }

    public void setIncrementBackup(boolean val) {

        if (cache != null) {
            cache.setIncrementBackup(val);
        }
    }

    
    void writeOtherStatement(Session session, String s) {

        try {
            dbLogWriter.writeOtherStatement(session, s);
        } catch (IOException e) {
            throw Error.error(ErrorCode.FILE_IO_ERROR, logFileName);
        }

        if (maxLogSize > 0 && dbLogWriter.size() > maxLogSize) {
            database.logger.setCheckpointRequired();
        }
    }

    void writeInsertStatement(Session session, Row row, Table t) {

        try {
            dbLogWriter.writeInsertStatement(session, row, t);
        } catch (IOException e) {
            throw Error.error(ErrorCode.FILE_IO_ERROR, logFileName);
        }

        if (maxLogSize > 0 && dbLogWriter.size() > maxLogSize) {
            database.logger.setCheckpointRequired();
        }
    }

    void writeDeleteStatement(Session session, Table t, Object[] row) {

        try {
            dbLogWriter.writeDeleteStatement(session, t, row);
        } catch (IOException e) {
            throw Error.error(ErrorCode.FILE_IO_ERROR, logFileName);
        }

        if (maxLogSize > 0 && dbLogWriter.size() > maxLogSize) {
            database.logger.setCheckpointRequired();
        }
    }

    void writeSequenceStatement(Session session, NumberSequence s) {

        try {
            dbLogWriter.writeSequenceStatement(session, s);
        } catch (IOException e) {
            throw Error.error(ErrorCode.FILE_IO_ERROR, logFileName);
        }

        if (maxLogSize > 0 && dbLogWriter.size() > maxLogSize) {
            database.logger.setCheckpointRequired();
        }
    }

    void writeCommitStatement(Session session) {

        try {
            dbLogWriter.writeCommitStatement(session);
        } catch (IOException e) {
            throw Error.error(ErrorCode.FILE_IO_ERROR, logFileName);
        }

        if (maxLogSize > 0 && dbLogWriter.size() > maxLogSize) {
            database.logger.setCheckpointRequired();
        }
    }

    void synchLog() {

        if (dbLogWriter != null) {
            dbLogWriter.forceSync();
        }
    }

    
    void openLog() {

        if (filesReadOnly) {
            return;
        }

        Crypto crypto = database.logger.getCrypto();

        try {
            if (crypto == null) {
                dbLogWriter = new ScriptWriterText(database, logFileName,
                                                   false, false, false);
            } else {
                dbLogWriter = new ScriptWriterEncode(database, logFileName,
                                                     crypto);
            }

            dbLogWriter.setWriteDelay(writeDelay);
            dbLogWriter.start();
        } catch (Throwable e) {
            throw Error.error(ErrorCode.FILE_IO_ERROR, logFileName);
        }
    }

    synchronized void closeLog() {

        if (dbLogWriter != null) {
            database.logger.logDetailEvent("log close size: "
                                           + dbLogWriter.size());
            dbLogWriter.close();
        }
    }

    
    void writeScript(boolean full) {

        deleteNewScript();

        ScriptWriterBase scw;
        Crypto           crypto = database.logger.getCrypto();

        if (crypto == null) {
            boolean compressed = database.logger.propScriptFormat == 3;

            scw = new ScriptWriterText(database,
                                       scriptFileName
                                       + Logger.newFileExtension, full,
                                           compressed);
        } else {
            scw = new ScriptWriterEncode(database,
                                         scriptFileName
                                         + Logger.newFileExtension, full,
                                             crypto);
        }

        scw.writeAll();
        scw.close();

        scw = null;
    }

    
    private void processScript() {

        ScriptReaderBase scr = null;

        try {
            Crypto crypto = database.logger.getCrypto();

            if (crypto == null) {
                boolean compressed = database.logger.propScriptFormat == 3;

                scr = new ScriptReaderText(database, scriptFileName,
                                           compressed);
            } else {
                scr = new ScriptReaderDecode(database, scriptFileName, crypto,
                                             false);
            }

            Session session =
                database.sessionManager.getSysSessionForScript(database);

            scr.readAll(session);
            scr.close();
        } catch (Throwable e) {
            if (scr != null) {
                scr.close();

                if (cache != null) {
                    cache.close(false);
                }

                database.logger.closeAllTextCaches(false);
            }

            database.logger.logWarningEvent("Script processing failure", e);

            if (e instanceof HsqlException) {
                throw (HsqlException) e;
            } else if (e instanceof IOException) {
                throw Error.error(ErrorCode.FILE_IO_ERROR, e);
            } else if (e instanceof OutOfMemoryError) {
                throw Error.error(ErrorCode.OUT_OF_MEMORY);
            } else {
                throw Error.error(ErrorCode.GENERAL_ERROR, e);
            }
        }
    }

    
    private void processLog() {

        if (fa.isStreamElement(logFileName)) {
            ScriptRunner.runScript(database, logFileName);
        }
    }

    void deleteOldDataFiles() {

        if (database.logger.isStoredFileAccess()) {
            return;
        }

        try {
            File   file = new File(database.getCanonicalPath());
            File[] list = file.getParentFile().listFiles();

            for (int i = 0; i < list.length; i++) {
                if (list[i].getName().startsWith(file.getName())
                        && list[i].getName().endsWith(
                            Logger.oldFileExtension)) {
                    list[i].delete();
                }
            }
        } catch (Throwable t) {}
    }

    void deleteOldTempFiles() {

        try {
            if (database.logger.tempDirectoryPath == null) {
                return;
            }

            File   file = new File(database.logger.tempDirectoryPath);
            File[] list = file.listFiles();

            for (int i = 0; i < list.length; i++) {
                list[i].delete();
            }
        } catch (Throwable t) {}
    }

    void deleteTempFileDirectory() {

        try {
            if (database.logger.tempDirectoryPath == null) {
                return;
            }

            File file = new File(database.logger.tempDirectoryPath);

            file.delete();
        } catch (Throwable t) {}
    }

    String getLogFileName() {
        return logFileName;
    }
}
