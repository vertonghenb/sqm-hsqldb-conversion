package org.hsqldb.persist;
import java.io.IOException;
import org.hsqldb.Database;
import org.hsqldb.Session;
import org.hsqldb.Table;
import org.hsqldb.TableBase;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.lib.DoubleIntIndex;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.StopWatch;
import org.hsqldb.navigator.RowIterator;
import org.hsqldb.rowio.RowOutputInterface;
import org.hsqldb.store.BitMap;
final class DataFileDefrag {
    RandomAccessInterface randomAccessOut;
    long                  fileOffset;
    StopWatch             stopw = new StopWatch();
    String                dataFileName;
    long[][]              rootsList;
    Database              database;
    DataFileCache         dataCache;
    int                   scale;
    DoubleIntIndex        pointerLookup;
    DataFileDefrag(Database db, DataFileCache cache, String dataFileName) {
        this.database     = db;
        this.dataCache    = cache;
        this.scale        = cache.cacheFileScale;
        this.dataFileName = dataFileName;
    }
    void process() {
        Throwable error = null;
        database.logger.logDetailEvent("Defrag process begins");
        HsqlArrayList allTables = database.schemaManager.getAllTables(true);
        rootsList = new long[allTables.size()][];
        long maxSize = 0;
        for (int i = 0, tSize = allTables.size(); i < tSize; i++) {
            Table table = (Table) allTables.get(i);
            if (table.getTableType() == TableBase.CACHED_TABLE) {
                PersistentStore store =
                    database.persistentStoreCollection.getStore(table);
                long size = store.elementCount();
                if (size > maxSize) {
                    maxSize = size;
                }
            }
        }
        if (maxSize > Integer.MAX_VALUE / 2) {
            throw Error.error(ErrorCode.X_2200T);
        }
        try {
            pointerLookup = new DoubleIntIndex((int) maxSize, false);
            if (database.logger.isStoredFileAccess()) {
                randomAccessOut = ScaledRAFile.newScaledRAFile(database,
                        dataFileName + Logger.newFileExtension, false,
                        ScaledRAFile.DATA_FILE_STORED);
            } else {
                randomAccessOut = new ScaledRAFileSimple(database,
                        dataFileName + Logger.newFileExtension, "rw");
            }
            randomAccessOut.write(new byte[dataCache.initialFreePos], 0,
                                  dataCache.initialFreePos);
            fileOffset = dataCache.initialFreePos;
            for (int i = 0, tSize = allTables.size(); i < tSize; i++) {
                Table t = (Table) allTables.get(i);
                if (t.getTableType() == TableBase.CACHED_TABLE) {
                    long[] rootsArray = writeTableToDataFile(t);
                    rootsList[i] = rootsArray;
                    randomAccessOut.synch();
                } else {
                    rootsList[i] = null;
                }
                database.logger.logDetailEvent("table complete "
                                               + t.getName().name);
            }
            int padding =
                (int) (ArrayUtil.getBinaryNormalisedCeiling(fileOffset, ScaledRAFile.bufferSize)
                       - fileOffset);
            byte[] bytes = new byte[padding];
            randomAccessOut.write(bytes, 0, padding);
            randomAccessOut.seek(DataFileCache.LONG_FREE_POS_POS);
            randomAccessOut.writeLong(fileOffset);
            int flags = 0;
            if (database.logger.propIncrementBackup) {
                flags = BitMap.set(flags, DataFileCache.FLAG_ISSHADOWED);
            }
            flags = BitMap.set(flags, DataFileCache.FLAG_190);
            flags = BitMap.set(flags, DataFileCache.FLAG_ISSAVED);
            randomAccessOut.seek(DataFileCache.FLAGS_POS);
            randomAccessOut.writeInt(flags);
            randomAccessOut.synch();
            randomAccessOut.close();
            randomAccessOut = null;
            for (int i = 0, size = rootsList.length; i < size; i++) {
                long[] roots = rootsList[i];
                if (roots != null) {
                    database.logger.logDetailEvent(
                        "roots: "
                        + org.hsqldb.lib.StringUtil.getList(roots, ",", ""));
                }
            }
        } catch (IOException e) {
            error = e;
            throw Error.error(ErrorCode.FILE_IO_ERROR, e);
        } catch (OutOfMemoryError e) {
            error = e;
            throw Error.error(ErrorCode.OUT_OF_MEMORY, e);
        } catch (Throwable t) {
            error = t;
            throw Error.error(ErrorCode.GENERAL_ERROR, t);
        } finally {
            try {
                if (randomAccessOut != null) {
                    randomAccessOut.close();
                }
            } catch (Throwable t) {}
            if (error instanceof OutOfMemoryError) {
                database.logger.logInfoEvent(
                    "defrag failed - out of memory - required: "
                    + maxSize * 8);
            }
            if (error == null) {
                database.logger.logDetailEvent("Defrag transfer complete: "
                                               + stopw.elapsedTime());
            } else {
                database.logger.logSevereEvent("defrag failed ", error);
                database.logger.getFileAccess().removeElement(dataFileName
                        + Logger.newFileExtension);
            }
        }
    }
    void updateTableIndexRoots() {
        HsqlArrayList allTables = database.schemaManager.getAllTables(true);
        for (int i = 0, size = allTables.size(); i < size; i++) {
            Table t = (Table) allTables.get(i);
            if (t.getTableType() == TableBase.CACHED_TABLE) {
                long[] rootsArray = rootsList[i];
                t.setIndexRoots(rootsArray);
            }
        }
    }
    long[] writeTableToDataFile(Table table) throws IOException {
        Session session = database.getSessionManager().getSysSession();
        PersistentStore    store      = table.getRowStore(session);
        RowOutputInterface rowOut     = dataCache.rowOut.duplicate();
        long[]             rootsArray = table.getIndexRootsArray();
        long               pos        = fileOffset;
        long               count      = 0;
        pointerLookup.removeAll();
        pointerLookup.setKeysSearchTarget();
        database.logger.logDetailEvent("lookup begins " + table.getName().name
                                       + " " + stopw.elapsedTime());
        RowIterator it = table.rowIteratorClustered(store);
        for (; it.hasNext(); count++) {
            CachedObject row = it.getNextRow();
            pointerLookup.addUnsorted((int) row.getPos(), (int) (pos / scale));
            if (count != 0 && count % 100000 == 0) {
                database.logger.logDetailEvent("pointer pair for row " + count
                                               + " " + row.getPos() + " "
                                               + pos);
            }
            pos += row.getStorageSize();
        }
        database.logger.logDetailEvent("table read " + table.getName().name
                                       + " " + stopw.elapsedTime());
        count = 0;
        it    = table.rowIteratorClustered(store);
        for (; it.hasNext(); count++) {
            CachedObject row = it.getNextRow();
            rowOut.reset();
            row.write(rowOut, pointerLookup);
            randomAccessOut.write(rowOut.getOutputStream().getBuffer(), 0,
                                  rowOut.size());
            fileOffset += row.getStorageSize();
            if (count != 0 && count % 100000 == 0) {
                database.logger.logDetailEvent("rows count " + count + " "
                                               + stopw.elapsedTime());
            }
        }
        for (int i = 0; i < table.getIndexCount(); i++) {
            if (rootsArray[i] == -1) {
                continue;
            }
            int lookupIndex =
                pointerLookup.findFirstEqualKeyIndex((int) rootsArray[i]);
            if (lookupIndex == -1) {
                throw Error.error(ErrorCode.DATA_FILE_ERROR);
            }
            rootsArray[i] = pointerLookup.getValue(lookupIndex);
        }
        database.logger.logDetailEvent("table written "
                                       + table.getName().name);
        return rootsArray;
    }
    public long[][] getIndexRoots() {
        return rootsList;
    }
    static boolean checkAllTables(Database database) {
        Session       session   = database.getSessionManager().getSysSession();
        HsqlArrayList allTables = database.schemaManager.getAllTables(true);
        for (int i = 0, tSize = allTables.size(); i < tSize; i++) {
            Table t     = (Table) allTables.get(i);
            int   count = 0;
            if (t.getTableType() == TableBase.CACHED_TABLE) {
                RowIterator it = t.rowIterator(session);
                for (; it.hasNext(); count++) {
                    CachedObject row = it.getNextRow();
                }
                System.out.println("table " + t.getName().name + " " + count);
            }
        }
        return true;
    }
}