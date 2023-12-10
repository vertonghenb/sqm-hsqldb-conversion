package org.hsqldb.store;
import org.hsqldb.lib.HashMappedList;
import org.hsqldb.lib.HashSet;
public class ReusableObjectCache {
    public ReusableObjectCache() {
        try {
            jbInit();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
    public static HashMappedList getHashMappedList() {
        return new HashMappedList();
    }
    public static void putHashMappedList(HashMappedList object) {}
    public static HashSet getHashSet() {
        return new HashSet();
    }
    public static void putHashSet(HashSet object) {}
    private void jbInit() throws Exception {}
}