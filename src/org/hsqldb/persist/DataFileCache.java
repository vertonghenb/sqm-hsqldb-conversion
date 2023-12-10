package org.hsqldb.persist;
import java.io.IOException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.hsqldb.Database;
import org.hsqldb.HsqlException;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.FileAccess;
import org.hsqldb.lib.FileArchiver;
import org.hsqldb.lib.FileUtil;
import org.hsqldb.rowio.RowInputBinary180;
import org.hsqldb.rowio.RowInputBinaryDecode;
import org.hsqldb.rowio.RowInputInterface;
import org.hsqldb.rowio.RowOutputBinary180;
import org.hsqldb.rowio.RowOutputBinaryEncode;
import org.hsqldb.rowio.RowOutputInterface;
import org.hsqldb.store.BitMap;
public class DataFileCache {
    protected FileAccess fa;
    public static final int FLAG_ISSHADOWED = 1;
    public static final int FLAG_ISSAVED    = 2;
    public static final int FLAG_ROWINFO    = 3;
    public static final int FLAG_190        = 4;
    public static final int FLAG_HX         = 5;
    static final int LONG_EMPTY_SIZE      = 4;        
    static final int LONG_FREE_POS_POS    = 12;       
    static final int LONG_EMPTY_INDEX_POS = 20;       
    static final int FLAGS_POS            = 28;
    static final int MIN_INITIAL_FREE_POS = 32;
    DataFileBlockManager     freeBlocks;
    private static final int initIOBufferSize = 256;
    protected String   dataFileName;
    protected String   backupFileName;
    protected Database database;
    protected boolean fileModified;
    protected boolean cacheModified;
    protected int     cacheFileScale;
    protected boolean cacheReadonly;
    protected int     cachedRowPadding;
    protected int     initialFreePos;
    protected long    fileStartFreePosition;
    protected boolean hasRowInfo = false;
    protected int     storeCount;
    protected RowInputInterface rowIn;
    public RowOutputInterface   rowOut;
    public long maxDataFileSize;
    boolean is180;
    protected RandomAccessInterface dataFile;
    protected volatile long         fileFreePosition;
    protected int                   maxCacheRows;     
    protected long                  maxCacheBytes;    
    protected Cache                 cache;
    private RAShadowFile shadowFile;
    ReadWriteLock lock      = new ReentrantReadWriteLock();
    Lock          readLock  = lock.readLock();
    Lock          writeLock = lock.writeLock();
    public DataFileCache(Database db, String baseFileName) {
        initParams(db, baseFileName);
        cache = new Cache(this);
    }
    protected void initParams(Database database, String baseFileName) {
        this.dataFileName   = baseFileName + Logger.dataFileExtension;
        this.backupFileName = baseFileName + Logger.backupFileExtension;
        this.database       = database;
        fa                  = database.logger.getFileAccess();
        cacheFileScale      = database.logger.getCacheFileScale();
        cachedRowPadding    = 8;
        if (cacheFileScale > 8) {
            cachedRowPadding = cacheFileScale;
        }
        initialFreePos = MIN_INITIAL_FREE_POS;
        if (initialFreePos < cacheFileScale) {
            initialFreePos = cacheFileScale;
        }
        cacheReadonly   = database.logger.propFilesReadOnly;
        maxCacheRows    = database.logger.propCacheMaxRows;
        maxCacheBytes   = database.logger.propCacheMaxSize;
        maxDataFileSize = (long) Integer.MAX_VALUE * cacheFileScale;
        dataFile        = null;
        shadowFile      = null;
    }
    public void open(boolean readonly) {
        fileFreePosition = initialFreePos;
        database.logger.logInfoEvent("dataFileCache open start");
        try {
            boolean isNio = database.logger.propNioDataFile;
            int     fileType;
            if (database.logger.isStoredFileAccess()) {
                fileType = ScaledRAFile.DATA_FILE_STORED;
            } else if (database.isFilesInJar()) {
                fileType = ScaledRAFile.DATA_FILE_JAR;
            } else if (isNio) {
                fileType = ScaledRAFile.DATA_FILE_NIO;
            } else {
                fileType = ScaledRAFile.DATA_FILE_RAF;
            }
            if (readonly || database.isFilesInJar()) {
                dataFile = ScaledRAFile.newScaledRAFile(database,
                        dataFileName, readonly, fileType);
                dataFile.seek(FLAGS_POS);
                int flags = dataFile.readInt();
                is180 = !BitMap.isSet(flags, FLAG_190);
                if (BitMap.isSet(flags, FLAG_HX)) {
                    throw Error.error(ErrorCode.WRONG_DATABASE_FILE_VERSION);
                }
                dataFile.seek(LONG_FREE_POS_POS);
                fileFreePosition = dataFile.readLong();
                initBuffers();
                return;
            }
            long    freesize      = 0;
            boolean preexists     = fa.isStreamElement(dataFileName);
            boolean isIncremental = database.logger.propIncrementBackup;
            boolean isSaved       = false;
            if (preexists) {
                if (database.logger.isStoredFileAccess()) {
                    dataFile = ScaledRAFile.newScaledRAFile(database,
                            dataFileName, true, ScaledRAFile.DATA_FILE_STORED);
                } else {
                    dataFile = new ScaledRAFileSimple(database, dataFileName,
                                                      "r");
                }
                long    length       = dataFile.length();
                boolean wrongVersion = false;
                if (length > initialFreePos) {
                    dataFile.seek(FLAGS_POS);
                    int flags = dataFile.readInt();
                    isSaved       = BitMap.isSet(flags, FLAG_ISSAVED);
                    isIncremental = BitMap.isSet(flags, FLAG_ISSHADOWED);
                    is180         = !BitMap.isSet(flags, FLAG_190);
                    if (BitMap.isSet(flags, FLAG_HX)) {
                        wrongVersion = true;
                    }
                }
                dataFile.close();
                if (length > maxDataFileSize) {
                    throw Error.error(ErrorCode.WRONG_DATABASE_FILE_VERSION,
                                      "requires large database support");
                }
                if (wrongVersion) {
                    throw Error.error(ErrorCode.WRONG_DATABASE_FILE_VERSION);
                }
            }
            if (isSaved) {
                if (isIncremental) {
                    deleteBackup();
                } else {
                    boolean existsBackup = fa.isStreamElement(backupFileName);
                    if (!existsBackup) {
                        backupFile(false);
                    }
                }
            } else {
                if (isIncremental) {
                    preexists = restoreBackupIncremental();
                } else {
                    preexists = restoreBackup();
                }
            }
            dataFile = ScaledRAFile.newScaledRAFile(database, dataFileName,
                    readonly, fileType);
            if (preexists) {
                dataFile.seek(FLAGS_POS);
                int flags = dataFile.readInt();
                is180 = !BitMap.isSet(flags, FLAG_190);
                dataFile.seek(LONG_EMPTY_SIZE);
                freesize = dataFile.readLong();
                dataFile.seek(LONG_FREE_POS_POS);
                fileFreePosition      = dataFile.readLong();
                fileStartFreePosition = fileFreePosition;
                openShadowFile();
            } else {
                initNewFile();
            }
            initBuffers();
            fileModified  = false;
            cacheModified = false;
            freeBlocks =
                new DataFileBlockManager(database.logger.propMaxFreeBlocks,
                                         cacheFileScale, 0, freesize);
            database.logger.logInfoEvent("dataFileCache open end");
        } catch (Throwable t) {
            database.logger.logSevereEvent("dataFileCache open failed", t);
            close(false);
            throw Error.error(t, ErrorCode.FILE_IO_ERROR,
                              ErrorCode.M_DataFileCache_open, new Object[] {
                t.toString(), dataFileName
            });
        }
    }
    void initNewFile() throws IOException {
        fileFreePosition      = initialFreePos;
        fileStartFreePosition = initialFreePos;
        dataFile.seek(LONG_FREE_POS_POS);
        dataFile.writeLong(fileFreePosition);
        int flags = 0;
        if (database.logger.propIncrementBackup) {
            flags = BitMap.set(flags, FLAG_ISSHADOWED);
        }
        flags = BitMap.set(flags, FLAG_ISSAVED);
        flags = BitMap.set(flags, FLAG_190);
        dataFile.seek(FLAGS_POS);
        dataFile.writeInt(flags);
        dataFile.synch();
        is180 = false;
    }
    void openShadowFile() {
        if (database.logger.propIncrementBackup
                && fileFreePosition != initialFreePos) {
            shadowFile = new RAShadowFile(database, dataFile, backupFileName,
                                          fileFreePosition, 1 << 14);
        }
    }
    void setIncrementBackup(boolean value) {
        writeLock.lock();
        try {
            dataFile.seek(FLAGS_POS);
            int flags = dataFile.readInt();
            if (value) {
                flags = BitMap.set(flags, FLAG_ISSHADOWED);
            } else {
                flags = BitMap.unset(flags, FLAG_ISSHADOWED);
            }
            dataFile.seek(FLAGS_POS);
            dataFile.writeInt(flags);
            dataFile.synch();
            fileModified = true;
        } catch (Throwable t) {
            database.logger.logSevereEvent("backupFile failed", t);
        } finally {
            writeLock.unlock();
        }
    }
    private boolean restoreBackup() {
        deleteFile();
        try {
            FileAccess fa = database.logger.getFileAccess();
            if (fa.isStreamElement(backupFileName)) {
                FileArchiver.unarchive(backupFileName, dataFileName, fa,
                                       FileArchiver.COMPRESSION_ZIP);
                return true;
            }
            return false;
        } catch (Throwable t) {
            throw Error.error(t, ErrorCode.FILE_IO_ERROR,
                              ErrorCode.M_Message_Pair, new Object[] {
                t.toString(), backupFileName
            });
        }
    }
    private boolean restoreBackupIncremental() {
        try {
            if (fa.isStreamElement(backupFileName)) {
                RAShadowFile.restoreFile(database, backupFileName,
                                         dataFileName);
                deleteBackup();
                return true;
            }
            return false;
        } catch (IOException e) {
            throw Error.error(ErrorCode.FILE_IO_ERROR, e);
        }
    }
    public void close(boolean write) {
        writeLock.lock();
        try {
            if (dataFile == null) {
                return;
            }
            if (write) {
                commitChanges();
            } else {
                if (shadowFile != null) {
                    shadowFile.close();
                    shadowFile = null;
                }
            }
            dataFile.close();
            database.logger.logDetailEvent("dataFileCache file close");
            dataFile = null;
            if (!write) {
                return;
            }
            boolean empty = fileFreePosition == initialFreePos;
            if (empty) {
                deleteFile();
                deleteBackup();
            }
        } catch (HsqlException e) {
            throw e;
        } catch (Throwable t) {
            database.logger.logSevereEvent("dataFileCache close failed", t);
            throw Error.error(t, ErrorCode.FILE_IO_ERROR,
                              ErrorCode.M_DataFileCache_close, new Object[] {
                t.toString(), dataFileName
            });
        } finally {
            writeLock.unlock();
        }
    }
    public void clear() {
        writeLock.lock();
        try {
            cache.clear();
            fileFreePosition = initialFreePos;
            freeBlocks.clear();
            initBuffers();
        } finally {
            writeLock.unlock();
        }
    }
    public void adjustStoreCount(int adjust) {
        writeLock.lock();
        try {
            storeCount += adjust;
            if (storeCount == 0) {
                if (shadowFile == null) {
                    clear();
                } else {
                    cache.clear();
                }
            }
        } finally {
            writeLock.unlock();
        }
    }
    public void commitChanges() {
        writeLock.lock();
        try {
            if (cacheReadonly) {
                return;
            }
            database.logger.logInfoEvent("dataFileCache commit start");
            cache.saveAll();
            database.logger.logDetailEvent("dataFileCache save data");
            if (fileModified || freeBlocks.isModified()) {
                dataFile.seek(LONG_EMPTY_SIZE);
                dataFile.writeLong(freeBlocks.getLostBlocksSize());
                dataFile.seek(LONG_FREE_POS_POS);
                dataFile.writeLong(fileFreePosition);
                dataFile.seek(FLAGS_POS);
                int flags = dataFile.readInt();
                flags = BitMap.set(flags, FLAG_ISSAVED);
                dataFile.seek(FLAGS_POS);
                dataFile.writeInt(flags);
            }
            dataFile.synch();
            fileModified  = false;
            cacheModified = false;
            if (shadowFile != null) {
                shadowFile.close();
                shadowFile = null;
            }
            database.logger.logDetailEvent("dataFileCache commit end");
        } catch (Throwable t) {
            database.logger.logSevereEvent("dataFileCache commit failed", t);
            throw Error.error(t, ErrorCode.FILE_IO_ERROR,
                              ErrorCode.M_DataFileCache_close, new Object[] {
                t.toString(), dataFileName
            });
        } finally {
            writeLock.unlock();
        }
    }
    protected void initBuffers() {
        if (rowOut == null
                || rowOut.getOutputStream().getBuffer().length
                   > initIOBufferSize) {
            if (is180) {
                rowOut = new RowOutputBinary180(256, cachedRowPadding);
            } else {
                rowOut = new RowOutputBinaryEncode(database.logger.getCrypto(),
                                                   256, cachedRowPadding);
            }
        }
        if (rowIn == null || rowIn.getBuffer().length > initIOBufferSize) {
            if (is180) {
                rowIn = new RowInputBinary180(new byte[256]);
            } else {
                rowIn = new RowInputBinaryDecode(database.logger.getCrypto(),
                                                 new byte[256]);
            }
        }
    }
    DataFileDefrag defrag() {
        writeLock.lock();
        try {
            cache.saveAll();
            DataFileDefrag dfd = new DataFileDefrag(database, this,
                dataFileName);
            dfd.process();
            close(true);
            cache.clear();
            if (!database.logger.propIncrementBackup) {
                backupFile(true);
            }
            database.schemaManager.setTempIndexRoots(dfd.getIndexRoots());
            try {
                database.logger.log.writeScript(false);
            } finally {
                database.schemaManager.setTempIndexRoots(null);
            }
            database.getProperties().setProperty(
                HsqlDatabaseProperties.hsqldb_script_format,
                database.logger.propScriptFormat);
            database.getProperties().setDBModified(
                HsqlDatabaseProperties.FILES_MODIFIED_NEW);
            database.logger.log.closeLog();
            database.logger.log.deleteLog();
            database.logger.log.renameNewScript();
            renameBackupFile();
            renameDataFile();
            database.getProperties().setDBModified(
                HsqlDatabaseProperties.FILES_NOT_MODIFIED);
            open(false);
            database.schemaManager.setIndexRoots(dfd.getIndexRoots());
            if (database.logger.log.dbLogWriter != null) {
                database.logger.log.openLog();
            }
            database.getProperties().setDBModified(
                HsqlDatabaseProperties.FILES_MODIFIED);
            return dfd;
        } finally {
            writeLock.unlock();
        }
    }
    public void remove(int i, PersistentStore store) {
        writeLock.lock();
        try {
            CachedObject r = release(i);
            if (r != null) {
                int size = r.getStorageSize();
                freeBlocks.add(i, size);
            }
        } finally {
            writeLock.unlock();
        }
    }
    public void removePersistence(CachedObject object) {}
    int setFilePos(CachedObject r) {
        int  rowSize = r.getStorageSize();
        int  i       = freeBlocks.get(rowSize);
        long newFreePosition;
        if (i == -1) {
            i               = (int) (fileFreePosition / cacheFileScale);
            newFreePosition = fileFreePosition + rowSize;
            if (newFreePosition > maxDataFileSize) {
                database.logger.logSevereEvent(
                    "data file reached maximum size " + this.dataFileName,
                    null);
                throw Error.error(ErrorCode.DATA_FILE_IS_FULL);
            }
            boolean result = dataFile.ensureLength(newFreePosition);
            if (!result) {
                database.logger.logSevereEvent(
                    "data file cannot be enlarged - disk spacee "
                    + this.dataFileName, null);
                throw Error.error(ErrorCode.DATA_FILE_IS_FULL);
            }
            fileFreePosition = newFreePosition;
        }
        r.setPos(i);
        return i;
    }
    public void add(CachedObject object) {
        writeLock.lock();
        try {
            cacheModified = true;
            int i = setFilePos(object);
            cache.put(i, object);
        } finally {
            writeLock.unlock();
        }
    }
    public int getStorageSize(int i) {
        readLock.lock();
        try {
            CachedObject value = cache.get(i);
            if (value != null) {
                return value.getStorageSize();
            }
        } finally {
            readLock.unlock();
        }
        return readSize(i);
    }
    public void replace(CachedObject object) {
        writeLock.lock();
        try {
            int pos = object.getPos();
            cache.replace(pos, object);
        } finally {
            writeLock.unlock();
        }
    }
    public CachedObject get(CachedObject object, PersistentStore store,
                            boolean keep) {
        readLock.lock();
        int pos;
        try {
            if (object.isInMemory()) {
                if (keep) {
                    object.keepInMemory(true);
                }
                return object;
            }
            pos = object.getPos();
            if (pos < 0) {
                return null;
            }
            object = cache.get(pos);
            if (object != null) {
                if (keep) {
                    object.keepInMemory(true);
                }
                return object;
            }
        } finally {
            readLock.unlock();
        }
        return getFromFile(pos, store, keep);
    }
    public CachedObject get(int pos, PersistentStore store, boolean keep) {
        CachedObject object;
        if (pos < 0) {
            return null;
        }
        readLock.lock();
        try {
            object = cache.get(pos);
            if (object != null) {
                if (keep) {
                    object.keepInMemory(true);
                }
                return object;
            }
        } finally {
            readLock.unlock();
        }
        return getFromFile(pos, store, keep);
    }
    private CachedObject getFromFile(int pos, PersistentStore store,
                                     boolean keep) {
        CachedObject object = null;
        writeLock.lock();
        try {
            object = cache.get(pos);
            if (object != null) {
                if (keep) {
                    object.keepInMemory(true);
                }
                return object;
            }
            for (int j = 0; j < 2; j++) {
                try {
                    RowInputInterface rowInput = readObject(pos);
                    if (rowInput == null) {
                        return null;
                    }
                    object = store.get(rowInput);
                    break;
                } catch (OutOfMemoryError err) {
                    cache.forceCleanUp();
                    System.gc();
                    database.logger.logSevereEvent(dataFileName
                                                   + " getFromFile out of mem "
                                                   + pos, err);
                    if (j > 0) {
                        throw err;
                    }
                }
            }
            pos = object.getPos();
            cache.put(pos, object);
            if (keep) {
                object.keepInMemory(true);
            }
            store.set(object);
            return object;
        } catch (HsqlException e) {
            database.logger.logSevereEvent(dataFileName + " getFromFile "
                                           + pos, e);
            throw e;
        } finally {
            writeLock.unlock();
        }
    }
    RowInputInterface getRaw(int i) {
        writeLock.lock();
        try {
            return readObject(i);
        } finally {
            writeLock.unlock();
        }
    }
    protected int readSize(int pos) {
        writeLock.lock();
        try {
            dataFile.seek((long) pos * cacheFileScale);
            return dataFile.readInt();
        } catch (IOException e) {
            throw Error.error(ErrorCode.DATA_FILE_ERROR, e);
        } finally {
            writeLock.unlock();
        }
    }
    protected RowInputInterface readObject(int pos) {
        try {
            dataFile.seek((long) pos * cacheFileScale);
            int size = dataFile.readInt();
            rowIn.resetRow(pos, size);
            dataFile.read(rowIn.getBuffer(), 4, size - 4);
            return rowIn;
        } catch (IOException e) {
            throw Error.error(ErrorCode.DATA_FILE_ERROR, e);
        }
    }
    public CachedObject release(int pos) {
        writeLock.lock();
        try {
            return cache.release(pos);
        } finally {
            writeLock.unlock();
        }
    }
    protected void saveRows(CachedObject[] rows, int offset, int count) {
        if (count == 0) {
            return;
        }
        try {
            copyShadow(rows, offset, count);
            setFileModified();
            for (int i = offset; i < offset + count; i++) {
                CachedObject r = rows[i];
                saveRowNoLock(r);
                rows[i] = null;
            }
        } catch (HsqlException e) {
            database.logger.logSevereEvent("saveRows failed", e);
            throw e;
        } catch (Throwable e) {
            database.logger.logSevereEvent("saveRows failed", e);
            throw Error.error(ErrorCode.DATA_FILE_ERROR, e);
        } finally {
            initBuffers();
        }
    }
    public void saveRow(CachedObject row) {
        writeLock.lock();
        try {
            copyShadow(row);
            setFileModified();
            saveRowNoLock(row);
        } catch (Throwable e) {
            database.logger.logSevereEvent("saveRow failed", e);
            throw Error.error(ErrorCode.DATA_FILE_ERROR, e);
        } finally {
            writeLock.unlock();
        }
    }
    protected void saveRowNoLock(CachedObject row) {
        try {
            rowOut.reset();
            row.write(rowOut);
            dataFile.seek((long) row.getPos() * cacheFileScale);
            dataFile.write(rowOut.getOutputStream().getBuffer(), 0,
                           rowOut.getOutputStream().size());
        } catch (IOException e) {
            throw Error.error(ErrorCode.DATA_FILE_ERROR, e);
        }
    }
    protected void copyShadow(CachedObject[] rows, int offset,
                              int count) throws IOException {
        if (shadowFile != null) {
            long time = cache.saveAllTimer.elapsedTime();
            for (int i = offset; i < offset + count; i++) {
                CachedObject row     = rows[i];
                long         seekpos = (long) row.getPos() * cacheFileScale;
                shadowFile.copy(seekpos, row.getStorageSize());
            }
            shadowFile.synch();
            time = cache.saveAllTimer.elapsedTime() - time;
            database.logger.logDetailEvent("shadow copy " + time);
        }
    }
    protected void copyShadow(CachedObject row) throws IOException {
        if (shadowFile != null) {
            long seekpos = (long) row.getPos() * cacheFileScale;
            shadowFile.copy(seekpos, row.getStorageSize());
            shadowFile.synch();
        }
    }
    void backupFile(boolean newFile) {
        writeLock.lock();
        try {
            if (database.logger.propIncrementBackup) {
                if (fa.isStreamElement(backupFileName)) {
                    deleteBackup();
                }
                return;
            }
            if (fa.isStreamElement(dataFileName)) {
                String filename = newFile
                                  ? dataFileName + Logger.newFileExtension
                                  : dataFileName;
                FileArchiver.archive(filename,
                                     backupFileName + Logger.newFileExtension,
                                     database.logger.getFileAccess(),
                                     FileArchiver.COMPRESSION_ZIP);
            }
        } catch (IOException e) {
            database.logger.logSevereEvent("backupFile failed", e);
            throw Error.error(ErrorCode.DATA_FILE_ERROR, e);
        } finally {
            writeLock.unlock();
        }
    }
    void renameBackupFile() {
        writeLock.lock();
        try {
            if (database.logger.propIncrementBackup) {
                deleteBackup();
                return;
            }
            if (fa.isStreamElement(backupFileName + Logger.newFileExtension)) {
                deleteBackup();
                fa.renameElement(backupFileName + Logger.newFileExtension,
                                 backupFileName);
            }
        } finally {
            writeLock.unlock();
        }
    }
    void renameDataFile() {
        writeLock.lock();
        try {
            if (fa.isStreamElement(dataFileName + Logger.newFileExtension)) {
                deleteFile();
                fa.renameElement(dataFileName + Logger.newFileExtension,
                                 dataFileName);
            }
        } finally {
            writeLock.unlock();
        }
    }
    void deleteFile() {
        writeLock.lock();
        try {
            fa.removeElement(dataFileName);
            if (database.logger.isStoredFileAccess()) {
                return;
            }
            if (fa.isStreamElement(dataFileName)) {
                this.database.logger.log.deleteOldDataFiles();
                fa.removeElement(dataFileName);
                if (fa.isStreamElement(dataFileName)) {
                    String discardName =
                        FileUtil.newDiscardFileName(dataFileName);
                    fa.renameElement(dataFileName, discardName);
                }
            }
        } finally {
            writeLock.unlock();
        }
    }
    void deleteBackup() {
        writeLock.lock();
        try {
            if (fa.isStreamElement(backupFileName)) {
                fa.removeElement(backupFileName);
            }
        } finally {
            writeLock.unlock();
        }
    }
    public int capacity() {
        return maxCacheRows;
    }
    public long bytesCapacity() {
        return maxCacheBytes;
    }
    public long getTotalCachedBlockSize() {
        return cache.getTotalCachedBlockSize();
    }
    public int getFreeBlockCount() {
        return freeBlocks.size();
    }
    public int getTotalFreeBlockSize() {
        return 0;
    }
    public long getFileFreePos() {
        return fileFreePosition;
    }
    public int getCachedObjectCount() {
        return cache.size();
    }
    public int getAccessCount() {
        return cache.incrementAccessCount();
    }
    public String getFileName() {
        return dataFileName;
    }
    public boolean hasRowInfo() {
        return hasRowInfo;
    }
    public boolean isFileModified() {
        return fileModified;
    }
    public boolean isModified() {
        return cacheModified;
    }
    public boolean isFileOpen() {
        return dataFile != null;
    }
    protected void setFileModified() {
        writeLock.lock();
        try {
            if (!fileModified) {
                long start = cache.saveAllTimer.elapsedTime();
                cache.saveAllTimer.start();
                dataFile.seek(FLAGS_POS);
                int flags = dataFile.readInt();
                flags = BitMap.unset(flags, FLAG_ISSAVED);
                dataFile.seek(FLAGS_POS);
                dataFile.writeInt(flags);
                dataFile.synch();
                cache.saveAllTimer.stop();
                database.logger.logDetailEvent(
                    "flags set " + cache.saveAllTimer.elapsedTime());
                fileModified = true;
            }
        } catch (Throwable t) {}
        finally {
            writeLock.unlock();
        }
    }
    public int getFlags() {
        try {
            dataFile.seek(FLAGS_POS);
            int flags = dataFile.readInt();
            return flags;
        } catch (Throwable t) {}
        return 0;
    }
    public boolean isDataReadOnly() {
        return this.cacheReadonly;
    }
    public RAShadowFile getShadowFile() {
        return shadowFile;
    }
}