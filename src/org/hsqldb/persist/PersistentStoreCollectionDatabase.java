


package org.hsqldb.persist;

import org.hsqldb.TableBase;
import org.hsqldb.lib.LongKeyHashMap;

public class PersistentStoreCollectionDatabase
implements PersistentStoreCollection {

    private long                 persistentStoreIdSequence;
    private final LongKeyHashMap rowStoreMap = new LongKeyHashMap();

    public void setStore(Object key, PersistentStore store) {

        long persistenceId = ((TableBase) key).getPersistenceId();

        if (store == null) {
            rowStoreMap.remove(persistenceId);
        } else {
            rowStoreMap.put(persistenceId, store);
        }
    }

    public PersistentStore getStore(Object key) {

        long persistenceId = ((TableBase) key).getPersistenceId();
        PersistentStore store =
            (PersistentStore) rowStoreMap.get(persistenceId);

        return store;
    }

    public void releaseStore(TableBase table) {

        PersistentStore store =
            (PersistentStore) rowStoreMap.get(table.getPersistenceId());

        if (store != null) {
            store.release();
            rowStoreMap.remove(table.getPersistenceId());
        }
    }

    public long getNextId() {
        return persistentStoreIdSequence++;
    }
}
