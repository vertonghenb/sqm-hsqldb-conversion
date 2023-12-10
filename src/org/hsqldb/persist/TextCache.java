package org.hsqldb.persist;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import org.hsqldb.Database;
import org.hsqldb.DatabaseURL;
import org.hsqldb.HsqlException;
import org.hsqldb.Table;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.FileUtil;
import org.hsqldb.lib.HsqlByteArrayOutputStream;
import org.hsqldb.lib.IntKeyHashMap;
import org.hsqldb.rowio.RowInputText;
import org.hsqldb.rowio.RowInputTextQuoted;
import org.hsqldb.rowio.RowOutputText;
import org.hsqldb.rowio.RowOutputTextQuoted;
import org.hsqldb.scriptio.ScriptWriterText;
public class TextCache extends DataFileCache {
    TextFileSettings textFileSettings;
    protected String          header;
    protected Table           table;
    private IntKeyHashMap     uncommittedCache;
    HsqlByteArrayOutputStream buffer = new HsqlByteArrayOutputStream(128);
    TextCache(Table table, String name) {
        super(table.database, name);
        this.table       = table;
        uncommittedCache = new IntKeyHashMap();
    }
    protected void initParams(Database database, String fileSettingsString) {
        this.database    = database;
        fa               = FileUtil.getFileUtil();
        textFileSettings = new TextFileSettings(database, fileSettingsString);
        dataFileName     = textFileSettings.getFileName();
        if (dataFileName == null) {
            throw Error.error(ErrorCode.X_S0501);
        }
        dataFileName  = ((FileUtil) fa).canonicalOrAbsolutePath(dataFileName);
        maxCacheRows  = textFileSettings.getMaxCacheRows();
        maxCacheBytes = textFileSettings.getMaxCacheBytes();
        maxDataFileSize  = Integer.MAX_VALUE;
        cachedRowPadding = 1;
        cacheFileScale   = 1;
    }
    protected void initBuffers() {
        if (textFileSettings.isQuoted || textFileSettings.isAllQuoted) {
            rowIn = new RowInputTextQuoted(textFileSettings.fs,
                                           textFileSettings.vs,
                                           textFileSettings.lvs,
                                           textFileSettings.isAllQuoted);
            rowOut = new RowOutputTextQuoted(textFileSettings.fs,
                                             textFileSettings.vs,
                                             textFileSettings.lvs,
                                             textFileSettings.isAllQuoted,
                                             textFileSettings.stringEncoding);
        } else {
            rowIn = new RowInputText(textFileSettings.fs, textFileSettings.vs,
                                     textFileSettings.lvs, false);
            rowOut = new RowOutputText(textFileSettings.fs,
                                       textFileSettings.vs,
                                       textFileSettings.lvs, false,
                                       textFileSettings.stringEncoding);
        }
    }
    public void open(boolean readonly) {
        fileFreePosition = 0;
        try {
            int type = database.getType() == DatabaseURL.S_RES
                       ? ScaledRAFile.DATA_FILE_JAR
                       : ScaledRAFile.DATA_FILE_TEXT;
            dataFile = ScaledRAFile.newScaledRAFile(database, dataFileName,
                    readonly, type);
            fileFreePosition = dataFile.length();
            if (fileFreePosition > Integer.MAX_VALUE) {
                throw Error.error(ErrorCode.DATA_FILE_IS_FULL);
            }
            initBuffers();
            freeBlocks = new DataFileBlockManager(0, cacheFileScale, 0, 0);
        } catch (Throwable t) {
            throw Error.error(t, ErrorCode.FILE_IO_ERROR,
                              ErrorCode.M_TextCache_openning_file_error,
                              new Object[] {
                t.toString(), dataFileName
            });
        }
        cacheReadonly = readonly;
    }
    void reopen() {
        open(cacheReadonly);
    }
    public void close(boolean write) {
        if (dataFile == null) {
            return;
        }
        writeLock.lock();
        try {
            cache.saveAll();
            boolean empty = (dataFile.length()
                             <= TextFileSettings.NL.length());
            dataFile.synch();
            dataFile.close();
            dataFile = null;
            if (empty && !cacheReadonly) {
                FileUtil.getFileUtil().delete(dataFileName);
            }
            uncommittedCache.clear();
        } catch (Throwable t) {
            throw Error.error(t, ErrorCode.FILE_IO_ERROR,
                              ErrorCode.M_TextCache_closing_file_error,
                              new Object[] {
                t.toString(), dataFileName
            });
        } finally {
            writeLock.unlock();
        }
    }
    void purge() {
        writeLock.lock();
        try {
            uncommittedCache.clear();
            if (cacheReadonly) {
                close(false);
            } else {
                if (dataFile != null) {
                    dataFile.close();
                    dataFile = null;
                }
                FileUtil.getFileUtil().delete(dataFileName);
            }
        } catch (Throwable t) {
            throw Error.error(t, ErrorCode.FILE_IO_ERROR,
                              ErrorCode.M_TextCache_purging_file_error,
                              new Object[] {
                t.toString(), dataFileName
            });
        } finally {
            writeLock.unlock();
        }
    }
    int setFilePos(CachedObject r) {
        int  rowSize         = r.getStorageSize();
        long newFreePosition = fileFreePosition + rowSize;
        if (newFreePosition > maxDataFileSize) {
            database.logger.logSevereEvent("data file reached maximum size "
                                           + this.dataFileName, null);
            throw Error.error(ErrorCode.DATA_FILE_IS_FULL);
        }
        int i = (int) fileFreePosition;
        r.setPos(i);
        clearRowImage(r);
        fileFreePosition = newFreePosition;
        return i;
    }
    public void remove(int pos, PersistentStore store) {
        writeLock.lock();
        try {
            CachedObject row = (CachedObject) uncommittedCache.remove(pos);
            if (row != null) {
                return;
            }
            row = cache.release(pos);
        } finally {
            writeLock.unlock();
        }
    }
    public void removePersistence(CachedObject row) {
        writeLock.lock();
        try {
            clearRowImage(row);
        } finally {
            writeLock.unlock();
        }
    }
    private void clearRowImage(CachedObject row) {
        try {
            int length = row.getStorageSize()
                         - ScriptWriterText.BYTES_LINE_SEP.length;
            rowOut.reset();
            HsqlByteArrayOutputStream out = rowOut.getOutputStream();
            out.fill(' ', length);
            out.write(ScriptWriterText.BYTES_LINE_SEP);
            dataFile.seek(row.getPos());
            dataFile.write(out.getBuffer(), 0, out.size());
        } catch (IOException e) {
            throw Error.runtimeError(ErrorCode.U_S0500, e.getMessage());
        }
    }
    public void addInit(CachedObject object) {
        writeLock.lock();
        try {
            cache.put(object.getPos(), object);
        } finally {
            writeLock.unlock();
        }
    }
    public void add(CachedObject object) {
        writeLock.lock();
        try {
            setFilePos(object);
            uncommittedCache.put(object.getPos(), object);
        } finally {
            writeLock.unlock();
        }
    }
    public CachedObject get(CachedObject object, PersistentStore store,
                            boolean keep) {
        if (object == null) {
            return null;
        }
        writeLock.lock();
        try {
            try {
                buffer.reset(object.getStorageSize());
                dataFile.seek(object.getPos());
                dataFile.read(buffer.getBuffer(), 0, object.getStorageSize());
                buffer.setSize(object.getStorageSize());
                String rowString =
                    buffer.toString(textFileSettings.stringEncoding);
                ((RowInputText) rowIn).setSource(rowString, object.getPos(),
                                                 buffer.size());
                store.get(rowIn);
                cache.put(object.getPos(), object);
                return object;
            } catch (IOException err) {
                database.logger.logSevereEvent(dataFileName
                                               + " getFromFile problem "
                                               + object.getPos(), err);
                cache.forceCleanUp();
                System.gc();
                return object;
            }
        } finally {
            writeLock.unlock();
        }
    }
    public CachedObject get(int i, PersistentStore store, boolean keep) {
        throw Error.runtimeError(ErrorCode.U_S0500, "TextCache");
    }
    protected void saveRows(CachedObject[] rows, int offset, int count) {
    }
    public void saveRow(CachedObject row) {
        writeLock.lock();
        try {
            setFileModified();
            saveRowNoLock(row);
            uncommittedCache.remove(row.getPos());
            cache.put(row.getPos(), row);
        } catch (Throwable e) {
            database.logger.logSevereEvent("saveRow failed", e);
            throw Error.error(ErrorCode.DATA_FILE_ERROR, e);
        } finally {
            writeLock.unlock();
        }
    }
    public String getHeader() {
        return header;
    }
    public void setHeaderInitialise(String header) {
        this.header = header;
    }
    public void setHeader(String header) {
        if (textFileSettings.ignoreFirst && fileFreePosition == 0) {
            try {
                writeHeader(header);
                this.header = header;
            } catch (HsqlException e) {
                throw new HsqlException(
                    e, Error.getMessage(ErrorCode.GENERAL_IO_ERROR),
                    ErrorCode.GENERAL_IO_ERROR);
            }
            return;
        }
        throw Error.error(ErrorCode.TEXT_TABLE_HEADER);
    }
    private void writeHeader(String header) {
        try {
            byte[] buf       = null;
            String firstLine = header + TextFileSettings.NL;
            try {
                buf = firstLine.getBytes(textFileSettings.stringEncoding);
            } catch (UnsupportedEncodingException e) {
                buf = firstLine.getBytes();
            }
            dataFile.seek(0);
            dataFile.write(buf, 0, buf.length);
            fileFreePosition = buf.length;
        } catch (IOException e) {
            throw Error.error(ErrorCode.TEXT_FILE_IO, e);
        }
    }
    public int getLineNumber() {
        return ((RowInputText) rowIn).getLineNumber();
    }
    public TextFileSettings getTextFileSettings() {
        return textFileSettings;
    }
    public boolean isIgnoreFirstLine() {
        return textFileSettings.ignoreFirst;
    }
    protected void setFileModified() {
        fileModified = true;
    }
    public TextFileReader getTextFileReader() {
        return new TextFileReader(dataFile, textFileSettings, rowIn,
                                  cacheReadonly);
    }
}