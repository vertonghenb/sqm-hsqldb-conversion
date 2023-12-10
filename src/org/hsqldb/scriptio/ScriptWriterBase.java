package org.hsqldb.scriptio;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import org.hsqldb.Database;
import org.hsqldb.DatabaseManager;
import org.hsqldb.HsqlNameManager;
import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.NumberSequence;
import org.hsqldb.Row;
import org.hsqldb.SchemaObject;
import org.hsqldb.Session;
import org.hsqldb.Table;
import org.hsqldb.TableBase;
import org.hsqldb.Tokens;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.FileAccess;
import org.hsqldb.lib.FileUtil;
import org.hsqldb.lib.HsqlTimer;
import org.hsqldb.lib.Iterator;
import org.hsqldb.navigator.RowIterator;
import org.hsqldb.navigator.RowSetNavigator;
import org.hsqldb.result.Result;
public abstract class ScriptWriterBase implements Runnable {
    Database            database;
    String              outFile;
    OutputStream        fileStreamOut;
    FileAccess.FileSync outDescriptor;
    int                 tableRowCount;
    HsqlName            schemaToLog;
    boolean             isClosed;
    boolean isCompressed;
    boolean isCrypt;
    boolean          isDump;
    boolean          includeCachedData;
    long             byteCount;
    long             lineCount;
    volatile boolean needsSync;
    private int      syncCount;
    static final int INSERT             = 0;
    static final int INSERT_WITH_SCHEMA = 1;
    Session                      currentSession;
    public static final String[] LIST_SCRIPT_FORMATS = new String[] {
        Tokens.T_TEXT, Tokens.T_BINARY, null, Tokens.T_COMPRESSED
    };
    ScriptWriterBase(Database db, OutputStream outputStream,
                     FileAccess.FileSync descriptor,
                     boolean includeCachedData) {
        initBuffers();
        this.database          = db;
        this.includeCachedData = includeCachedData;
        currentSession         = database.sessionManager.getSysSession();
        schemaToLog = currentSession.loggedSchema =
            currentSession.currentSchema;
        fileStreamOut = new BufferedOutputStream(outputStream, 1 << 14);
        outDescriptor = descriptor;
    }
    ScriptWriterBase(Database db, String file, boolean includeCachedData,
                     boolean isNewFile, boolean isDump) {
        initBuffers();
        boolean exists = false;
        if (isDump) {
            exists = FileUtil.getFileUtil().exists(file);
        } else {
            exists = db.logger.getFileAccess().isStreamElement(file);
        }
        if (exists && isNewFile) {
            throw Error.error(ErrorCode.FILE_IO_ERROR, file);
        }
        this.database          = db;
        this.isDump            = isDump;
        this.includeCachedData = includeCachedData;
        outFile                = file;
        currentSession         = database.sessionManager.getSysSession();
        schemaToLog = currentSession.loggedSchema =
            currentSession.currentSchema;
        openFile();
    }
    protected abstract void initBuffers();
    public void sync() {
        if (isClosed) {
            return;
        }
        if (needsSync) {
            forceSync();
        }
    }
    public void forceSync() {
        if (isClosed) {
            return;
        }
        needsSync = false;
        synchronized (fileStreamOut) {
            try {
                fileStreamOut.flush();
                outDescriptor.sync();
                syncCount++;
            } catch (IOException e) {
                database.logger.logWarningEvent("ScriptWriter synch error: ",
                                                e);
            }
        }
    }
    public void close() {
        stop();
        if (isClosed) {
            return;
        }
        try {
            synchronized (fileStreamOut) {
                finishStream();
                forceSync();
                fileStreamOut.close();
                fileStreamOut = null;
                outDescriptor = null;
                isClosed = true;
            }
        } catch (IOException e) {
            throw Error.error(ErrorCode.FILE_IO_ERROR);
        }
        byteCount = 0;
        lineCount = 0;
    }
    public long size() {
        return byteCount;
    }
    public void writeAll() {
        try {
            writeDDL();
            writeExistingData();
        } catch (IOException e) {
            throw Error.error(ErrorCode.FILE_IO_ERROR);
        }
    }
    protected void openFile() {
        try {
            FileAccess   fa  = isDump ? FileUtil.getFileUtil()
                                      : database.logger.getFileAccess();
            OutputStream fos = fa.openOutputStreamElement(outFile);
            outDescriptor = fa.getFileSync(fos);
            fileStreamOut = fos;
            fileStreamOut = new BufferedOutputStream(fos, 1 << 14);
        } catch (IOException e) {
            throw Error.error(e, ErrorCode.FILE_IO_ERROR,
                              ErrorCode.M_Message_Pair, new Object[] {
                e.toString(), outFile
            });
        }
    }
    protected void finishStream() throws IOException {}
    protected void writeDDL() throws IOException {
        Result ddlPart = database.getScript(!includeCachedData);
        writeSingleColumnResult(ddlPart);
    }
    protected void writeExistingData() throws IOException {
        currentSession.loggedSchema = null;
        String[] schemas = database.schemaManager.getSchemaNamesArray();
        for (int i = 0; i < schemas.length; i++) {
            String schema = schemas[i];
            Iterator tables =
                database.schemaManager.databaseObjectIterator(schema,
                    SchemaObject.TABLE);
            while (tables.hasNext()) {
                Table t = (Table) tables.next();
                boolean script = false;
                switch (t.getTableType()) {
                    case TableBase.MEMORY_TABLE :
                        script = true;
                        break;
                    case TableBase.CACHED_TABLE :
                        script = includeCachedData;
                        break;
                    case TableBase.TEXT_TABLE :
                        script = includeCachedData && !t.isReadOnly();
                        break;
                }
                try {
                    if (script) {
                        schemaToLog = t.getName().schema;
                        writeTableInit(t);
                        RowIterator it =
                            t.rowIteratorClustered(currentSession);
                        while (it.hasNext()) {
                            Row row = it.getNextRow();
                            writeRow(currentSession, row, t);
                        }
                        writeTableTerm(t);
                    }
                } catch (Exception e) {
                    throw Error.error(ErrorCode.FILE_IO_ERROR, e.toString());
                }
            }
        }
        writeDataTerm();
    }
    protected void writeTableInit(Table t) throws IOException {}
    protected void writeTableTerm(Table t) throws IOException {}
    protected void writeSingleColumnResult(Result r) throws IOException {
        RowSetNavigator nav = r.initialiseNavigator();
        while (nav.hasNext()) {
            Object[] data = (Object[]) nav.getNext();
            writeLogStatement(currentSession, (String) data[0]);
        }
    }
    abstract void writeRow(Session session, Row row,
                           Table table) throws IOException;
    protected abstract void writeDataTerm() throws IOException;
    protected abstract void writeSessionIdAndSchema(Session session)
    throws IOException;
    public abstract void writeLogStatement(Session session,
                                           String s) throws IOException;
    public abstract void writeOtherStatement(Session session,
            String s) throws IOException;
    public abstract void writeInsertStatement(Session session, Row row,
            Table table) throws IOException;
    public abstract void writeDeleteStatement(Session session, Table table,
            Object[] data) throws IOException;
    public abstract void writeSequenceStatement(Session session,
            NumberSequence seq) throws IOException;
    public abstract void writeCommitStatement(Session session)
    throws IOException;
    private Object timerTask;
    protected volatile int writeDelay = 60000;
    public void run() {
        try {
            if (writeDelay != 0) {
                sync();
            }
        } catch (Exception e) {
        }
    }
    public void setWriteDelay(int delay) {
        writeDelay = delay;
    }
    public void start() {
        if (writeDelay > 0) {
            timerTask = DatabaseManager.getTimer().schedulePeriodicallyAfter(0,
                    writeDelay, this, false);
        }
    }
    public void stop() {
        if (timerTask != null) {
            HsqlTimer.cancel(timerTask);
            timerTask = null;
        }
    }
    public int getWriteDelay() {
        return writeDelay;
    }
}