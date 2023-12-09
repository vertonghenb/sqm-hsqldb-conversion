


package org.hsqldb.persist;

public interface PersistentStoreCollection {

    PersistentStore getStore(Object key);

    void setStore(Object key, PersistentStore store);
}
