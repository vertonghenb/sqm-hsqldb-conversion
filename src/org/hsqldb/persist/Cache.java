package org.hsqldb.persist;
import java.util.Comparator;
import org.hsqldb.lib.ArraySort;
import org.hsqldb.lib.Iterator;
import org.hsqldb.lib.StopWatch;
import org.hsqldb.store.BaseHashMap;
public class Cache extends BaseHashMap {
    final DataFileCache                  dataFileCache;
    private int                          capacity;         
    private long                         bytesCapacity;    
    private final CachedObjectComparator rowComparator;
    private CachedObject[] rowTable;
    long                   cacheBytesLength;
    StopWatch saveAllTimer = new StopWatch(false);
    StopWatch sortTimer    = new StopWatch(false);
    int       saveRowCount = 0;
    Cache(DataFileCache dfc) {
        super(dfc.capacity(), BaseHashMap.intKeyOrValue,
              BaseHashMap.objectKeyOrValue, true);
        maxCapacity      = dfc.capacity();
        dataFileCache    = dfc;
        capacity         = dfc.capacity();
        bytesCapacity    = dfc.bytesCapacity();
        rowComparator    = new CachedObjectComparator();
        rowTable         = new CachedObject[capacity];
        cacheBytesLength = 0;
    }
    void init(int capacity, long bytesCapacity) {}
    long getTotalCachedBlockSize() {
        return cacheBytesLength;
    }
    public synchronized CachedObject get(int pos) {
        if (accessCount > ACCESS_MAX) {
            updateAccessCounts();
            resetAccessCount();
            updateObjectAccessCounts();
        }
        int lookup = getLookup(pos);
        if (lookup == -1) {
            return null;
        }
        accessTable[lookup] = ++accessCount;
        CachedObject object = (CachedObject) objectValueTable[lookup];
        return object;
    }
    synchronized void put(int key, CachedObject row) {
        int storageSize = row.getStorageSize();
        if (size() >= capacity
                || storageSize + cacheBytesLength > bytesCapacity) {
            cleanUp();
            if (size() >= capacity) {
                forceCleanUp();
            }
        }
        if (accessCount > ACCESS_MAX) {
            updateAccessCounts();
            resetAccessCount();
            updateObjectAccessCounts();
        }
        super.addOrRemove(key, row, null, false);
        row.setInMemory(true);
        cacheBytesLength += storageSize;
    }
    synchronized CachedObject release(int i) {
        CachedObject r = (CachedObject) super.addOrRemove(i, null, null, true);
        if (r == null) {
            return null;
        }
        cacheBytesLength -= r.getStorageSize();
        r.setInMemory(false);
        return r;
    }
    synchronized void replace(int key, CachedObject row) {
        int lookup = super.getLookup(key);
        super.objectValueTable[lookup] = row;
    }
    private void updateAccessCounts() {
        CachedObject r;
        int          count;
        for (int i = 0; i < objectValueTable.length; i++) {
            r = (CachedObject) objectValueTable[i];
            if (r != null) {
                count = r.getAccessCount();
                if (count > accessTable[i]) {
                    accessTable[i] = count;
                }
            }
        }
    }
    private void updateObjectAccessCounts() {
        CachedObject r;
        int          count;
        for (int i = 0; i < objectValueTable.length; i++) {
            r = (CachedObject) objectValueTable[i];
            if (r != null) {
                count = accessTable[i];
                r.updateAccessCount(count);
            }
        }
    }
    private synchronized void cleanUp() {
        updateAccessCounts();
        int                          removeCount = size() / 2;
        int accessTarget = getAccessCountCeiling(removeCount, removeCount / 8);
        BaseHashMap.BaseHashIterator it          = new BaseHashIterator();
        int                          savecount   = 0;
        for (; it.hasNext(); ) {
            CachedObject row                = (CachedObject) it.next();
            int          currentAccessCount = it.getAccessCount();
            boolean      oldRow = currentAccessCount <= accessTarget;
            if (oldRow) {
                synchronized (row) {
                    if (row.isKeepInMemory()) {
                        it.setAccessCount(accessTarget + 1);
                    } else {
                        if (row.hasChanged()) {
                            rowTable[savecount++] = row;
                        }
                        row.setInMemory(false);
                        it.remove();
                        cacheBytesLength -= row.getStorageSize();
                        removeCount--;
                    }
                }
            }
            if (savecount == rowTable.length) {
                saveRows(savecount);
                savecount = 0;
            }
        }
        super.setAccessCountFloor(accessTarget);
        saveRows(savecount);
    }
    synchronized void forceCleanUp() {
        BaseHashMap.BaseHashIterator it = new BaseHashIterator();
        for (; it.hasNext(); ) {
            CachedObject row = (CachedObject) it.next();
            synchronized (row) {
                if (!row.hasChanged() && !row.isKeepInMemory()) {
                    row.setInMemory(false);
                    it.remove();
                    cacheBytesLength -= row.getStorageSize();
                }
            }
        }
    }
    private synchronized void saveRows(int count) {
        if (count == 0) {
            return;
        }
        long startTime = saveAllTimer.elapsedTime();
        rowComparator.setType(CachedObjectComparator.COMPARE_POSITION);
        sortTimer.zero();
        sortTimer.start();
        ArraySort.sort(rowTable, 0, count, rowComparator);
        sortTimer.stop();
        saveAllTimer.start();
        dataFileCache.saveRows(rowTable, 0, count);
        saveRowCount += count;
        saveAllTimer.stop();
        logSaveRowsEvent(count, startTime);
    }
    synchronized void saveAll() {
        Iterator it        = new BaseHashIterator();
        int      savecount = 0;
        for (; it.hasNext(); ) {
            if (savecount == rowTable.length) {
                saveRows(savecount);
                savecount = 0;
            }
            CachedObject r = (CachedObject) it.next();
            if (r.hasChanged()) {
                rowTable[savecount] = r;
                savecount++;
            }
        }
        saveRows(savecount);
    }
    void logSaveRowsEvent(int saveCount, long startTime) {
        StringBuffer sb = new StringBuffer();
        sb.append("cache save rows [count,time] totals ");
        sb.append(saveRowCount);
        sb.append(',').append(saveAllTimer.elapsedTime()).append(' ');
        sb.append("operation ").append(saveCount).append(',');
        sb.append(saveAllTimer.elapsedTime() - startTime).append(' ');
        sb.append("txts ");
        sb.append(dataFileCache.database.txManager.getGlobalChangeTimestamp());
        dataFileCache.database.logger.logDetailEvent(sb.toString());
    }
    synchronized public void clear() {
        super.clear();
        cacheBytesLength = 0;
    }
    static final class CachedObjectComparator implements Comparator {
        static final int COMPARE_LAST_ACCESS = 0;
        static final int COMPARE_POSITION    = 1;
        static final int COMPARE_SIZE        = 2;
        private int      compareType;
        CachedObjectComparator() {}
        void setType(int type) {
            compareType = type;
        }
        public int compare(Object a, Object b) {
            int diff;
            switch (compareType) {
                case COMPARE_POSITION :
                    diff = ((CachedObject) a).getPos()
                           - ((CachedObject) b).getPos();
                    break;
                case COMPARE_SIZE :
                    diff = ((CachedObject) a).getStorageSize()
                           - ((CachedObject) b).getStorageSize();
                    break;
                default :
                    return 0;
            }
            return diff == 0 ? 0
                             : diff > 0 ? 1
                                        : -1;
        }
    }
}