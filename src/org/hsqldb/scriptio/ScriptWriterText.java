package org.hsqldb.scriptio;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.zip.GZIPOutputStream;
import org.hsqldb.Database;
import org.hsqldb.HsqlNameManager;
import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.NumberSequence;
import org.hsqldb.Row;
import org.hsqldb.Session;
import org.hsqldb.Table;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.FileAccess;
import org.hsqldb.rowio.RowOutputTextLog;
public class ScriptWriterText extends ScriptWriterBase {
    RowOutputTextLog           rowOut;
    public static final String ISO_8859_1 = "ISO-8859-1";
    public static byte[] BYTES_LINE_SEP;
    static byte[]        BYTES_COMMIT;
    static byte[]        BYTES_INSERT_INTO;
    static byte[]        BYTES_VALUES;
    static byte[]        BYTES_TERM;
    static byte[]        BYTES_DELETE_FROM;
    static byte[]        BYTES_WHERE;
    static byte[]        BYTES_SEQUENCE;
    static byte[]        BYTES_SEQUENCE_MID;
    static byte[]        BYTES_C_ID_INIT;
    static byte[]        BYTES_C_ID_TERM;
    static byte[]        BYTES_SCHEMA;
    static {
        String sLineSep = System.getProperty("line.separator", "\n");
        try {
            BYTES_LINE_SEP     = sLineSep.getBytes();
            BYTES_COMMIT       = "COMMIT".getBytes(ISO_8859_1);
            BYTES_INSERT_INTO  = "INSERT INTO ".getBytes(ISO_8859_1);
            BYTES_VALUES       = " VALUES(".getBytes(ISO_8859_1);
            BYTES_TERM         = ")".getBytes(ISO_8859_1);
            BYTES_DELETE_FROM  = "DELETE FROM ".getBytes(ISO_8859_1);
            BYTES_WHERE        = " WHERE ".getBytes(ISO_8859_1);
            BYTES_SEQUENCE     = "ALTER SEQUENCE ".getBytes(ISO_8859_1);
            BYTES_SEQUENCE_MID = " RESTART WITH ".getBytes(ISO_8859_1);
            BYTES_C_ID_INIT    = "/*C".getBytes(ISO_8859_1);
            BYTES_C_ID_TERM    = "*/".getBytes(ISO_8859_1);
            BYTES_SCHEMA       = "SET SCHEMA ".getBytes(ISO_8859_1);
        } catch (UnsupportedEncodingException e) {
            Error.runtimeError(ErrorCode.U_S0500, "ScriptWriterText");
        }
    }
    public ScriptWriterText(Database db, OutputStream outputStream,
                            FileAccess.FileSync descriptor,
                            boolean includeCachedData) {
        super(db, outputStream, descriptor, includeCachedData);
    }
    public ScriptWriterText(Database db, String file,
                            boolean includeCachedData, boolean newFile,
                            boolean isDump) {
        super(db, file, includeCachedData, newFile, isDump);
    }
    public ScriptWriterText(Database db, String file,
                            boolean includeCachedData, boolean compressed) {
        super(db, file, includeCachedData, true, false);
        if (compressed) {
            isCompressed = true;
            try {
                fileStreamOut = new GZIPOutputStream(fileStreamOut);
            } catch (IOException e) {
                throw Error.error(e, ErrorCode.FILE_IO_ERROR,
                                  ErrorCode.M_Message_Pair, new Object[] {
                    e.toString(), outFile
                });
            }
        }
    }
    protected void initBuffers() {
        rowOut = new RowOutputTextLog();
    }
    protected void writeDataTerm() throws IOException {}
    protected void writeSessionIdAndSchema(Session session)
    throws IOException {
        if (session == null) {
            return;
        }
        if (session != currentSession) {
            rowOut.reset();
            rowOut.write(BYTES_C_ID_INIT);
            rowOut.writeLong(session.getId());
            rowOut.write(BYTES_C_ID_TERM);
            currentSession = session;
            writeRowOutToFile();
        }
        if (schemaToLog != session.loggedSchema) {
            rowOut.reset();
            writeSchemaStatement(schemaToLog);
            session.loggedSchema = schemaToLog;
            writeRowOutToFile();
        }
    }
    private void writeSchemaStatement(HsqlName schema) {
        rowOut.write(BYTES_SCHEMA);
        rowOut.writeString(schema.statementName);
        rowOut.write(BYTES_LINE_SEP);
    }
    public void writeLogStatement(Session session,
                                  String s) throws IOException {
        schemaToLog = session.currentSchema;
        writeSessionIdAndSchema(session);
        rowOut.reset();
        rowOut.writeString(s);
        rowOut.write(BYTES_LINE_SEP);
        writeRowOutToFile();
        needsSync = true;
    }
    protected void writeRow(Session session, Row row,
                            Table table) throws IOException {
        schemaToLog = table.getName().schema;
        writeSessionIdAndSchema(session);
        rowOut.reset();
        ((RowOutputTextLog) rowOut).setMode(RowOutputTextLog.MODE_INSERT);
        rowOut.write(BYTES_INSERT_INTO);
        rowOut.writeString(table.getName().statementName);
        rowOut.write(BYTES_VALUES);
        rowOut.writeData(row, table.getColumnTypes());
        rowOut.write(BYTES_TERM);
        rowOut.write(BYTES_LINE_SEP);
        writeRowOutToFile();
    }
    protected void writeTableInit(Table t) throws IOException {
        if (t.isEmpty(currentSession)) {
            return;
        }
        if (schemaToLog == currentSession.loggedSchema) {
            return;
        }
        rowOut.reset();
        writeSchemaStatement(t.getName().schema);
        writeRowOutToFile();
        currentSession.loggedSchema = schemaToLog;
    }
    public void writeOtherStatement(Session session,
                                  String s) throws IOException {
        writeLogStatement(session, s);
        if (writeDelay == 0) {
            sync();
        }
    }
    public void writeInsertStatement(Session session, Row row,
                                     Table table) throws IOException {
        schemaToLog = table.getName().schema;
        writeRow(session, row, table);
    }
    public void writeDeleteStatement(Session session, Table table,
                                     Object[] data) throws IOException {
        schemaToLog = table.getName().schema;
        writeSessionIdAndSchema(session);
        rowOut.reset();
        ((RowOutputTextLog) rowOut).setMode(RowOutputTextLog.MODE_DELETE);
        rowOut.write(BYTES_DELETE_FROM);
        rowOut.writeString(table.getName().statementName);
        rowOut.write(BYTES_WHERE);
        rowOut.writeData(table.getColumnCount(), table.getColumnTypes(), data,
                         table.columnList, table.getPrimaryKey());
        rowOut.write(BYTES_LINE_SEP);
        writeRowOutToFile();
    }
    public void writeSequenceStatement(Session session,
                                       NumberSequence seq) throws IOException {
        schemaToLog = seq.getName().schema;
        writeSessionIdAndSchema(session);
        rowOut.reset();
        rowOut.write(BYTES_SEQUENCE);
        rowOut.writeString(seq.getSchemaName().statementName);
        rowOut.write('.');
        rowOut.writeString(seq.getName().statementName);
        rowOut.write(BYTES_SEQUENCE_MID);
        rowOut.writeLong(seq.peek());
        rowOut.write(BYTES_LINE_SEP);
        writeRowOutToFile();
        needsSync = true;
    }
    public void writeCommitStatement(Session session) throws IOException {
        writeSessionIdAndSchema(session);
        rowOut.reset();
        rowOut.write(BYTES_COMMIT);
        rowOut.write(BYTES_LINE_SEP);
        writeRowOutToFile();
        needsSync = true;
        if (writeDelay == 0) {
            sync();
        }
    }
    protected void finishStream() throws IOException {
        if (isCompressed) {
            ((GZIPOutputStream) fileStreamOut).finish();
        }
    }
    void writeRowOutToFile() throws IOException {
        synchronized (fileStreamOut) {
            fileStreamOut.write(rowOut.getBuffer(), 0, rowOut.size());
            byteCount += rowOut.size();
            lineCount++;
        }
    }
}