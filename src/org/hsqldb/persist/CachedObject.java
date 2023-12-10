package org.hsqldb.persist;
import org.hsqldb.lib.IntLookup;
import org.hsqldb.rowio.RowOutputInterface;
public interface CachedObject {
    CachedObject[] emptyArray = new CachedObject[]{};
    boolean isMemory();
    void updateAccessCount(int count);
    int getAccessCount();
    void setStorageSize(int size);
    int getStorageSize();
    int getPos();
    void setPos(int pos);
    boolean hasChanged();
    boolean isKeepInMemory();
    boolean keepInMemory(boolean keep);
    boolean isInMemory();
    void setInMemory(boolean in);
    void restore();
    void destroy();
    int getRealSize(RowOutputInterface out);
    void write(RowOutputInterface out);
    void write(RowOutputInterface out, IntLookup lookup);
}