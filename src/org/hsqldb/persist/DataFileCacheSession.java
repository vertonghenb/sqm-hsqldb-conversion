


package org.hsqldb.persist;

import org.hsqldb.Database;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.FileUtil;


public class DataFileCacheSession extends DataFileCache {

    public DataFileCacheSession(Database db, String baseFileName) {
        super(db, baseFileName);
    }

    
    protected void initParams(Database database, String baseFileName) {

        this.dataFileName = baseFileName + ".data.tmp";
        this.database     = database;
        fa                = FileUtil.getFileUtil();
        cacheFileScale    = 64;
        cachedRowPadding  = cacheFileScale;
        initialFreePos    = cacheFileScale;
        maxCacheRows      = 2048;
        maxCacheBytes     = maxCacheRows * 1024;
        maxDataFileSize   = (long) Integer.MAX_VALUE * cacheFileScale;
        dataFile          = null;
    }

    
    public void open(boolean readonly) {

        try {
            dataFile = new ScaledRAFile(database, dataFileName, false, false);
            fileFreePosition = initialFreePos;

            initBuffers();

            freeBlocks = new DataFileBlockManager(0, cacheFileScale, 0, 0);
        } catch (Throwable t) {
            database.logger.logWarningEvent("Failed to open Session RA file",
                                            t);
            close(false);

            throw Error.error(t, ErrorCode.FILE_IO_ERROR,
                              ErrorCode.M_DataFileCache_open, new Object[] {
                t.toString(), dataFileName
            });
        }
    }

    
    public void close(boolean write) {

        writeLock.lock();

        try {
            cache.clear();

            if (dataFile != null) {
                dataFile.close();

                dataFile = null;

                fa.removeElement(dataFileName);
            }
        } catch (Throwable t) {
            database.logger.logWarningEvent("Failed to close Session RA file",
                                            t);

            throw Error.error(t, ErrorCode.FILE_IO_ERROR,
                              ErrorCode.M_DataFileCache_close, new Object[] {
                t.toString(), dataFileName
            });
        } finally {
            writeLock.unlock();
        }
    }
}
