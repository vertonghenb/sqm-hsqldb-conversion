package org.hsqldb;
import java.util.Vector;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.FileUtil;
import org.hsqldb.lib.HashMap;
import org.hsqldb.lib.HashSet;
import org.hsqldb.lib.HsqlTimer;
import org.hsqldb.lib.IntKeyHashMap;
import org.hsqldb.lib.Iterator;
import org.hsqldb.persist.HsqlProperties;
import org.hsqldb.server.Server;
import org.hsqldb.server.ServerConstants;
import org.hsqldb.store.ValuePool;
public class DatabaseManager {
    private static int dbIDCounter;
    static final HashMap memDatabaseMap = new HashMap();
    static final HashMap fileDatabaseMap = new HashMap();
    static final HashMap resDatabaseMap = new HashMap();
    static final IntKeyHashMap databaseIDMap = new IntKeyHashMap();
    public static Vector getDatabaseURIs() {
        Vector v = new Vector();
        synchronized (databaseIDMap) {
            Iterator it = databaseIDMap.values().iterator();
            while (it.hasNext()) {
                Database db = (Database) it.next();
                v.addElement(db.getURI());
            }
        }
        return v;
    }
    public static void closeDatabases(int mode) {
        synchronized (databaseIDMap) {
            Iterator it = databaseIDMap.values().iterator();
            while (it.hasNext()) {
                Database db = (Database) it.next();
                try {
                    db.close(mode);
                } catch (HsqlException e) {}
            }
        }
    }
    public static Session newSession(int dbID, String user, String password,
                                     String zoneString, int timeZoneSeconds) {
        Database db = null;
        synchronized (databaseIDMap) {
            db = (Database) databaseIDMap.get(dbID);
        }
        if (db == null) {
            return null;
        }
        Session session = db.connect(user, password, zoneString,
                                     timeZoneSeconds);
        session.isNetwork = true;
        return session;
    }
    public static Session newSession(String type, String path, String user,
                                     String password, HsqlProperties props,
                                     String zoneString, int timeZoneSeconds) {
        Database db = getDatabase(type, path, props);
        if (db == null) {
            return null;
        }
        return db.connect(user, password, zoneString, timeZoneSeconds);
    }
    public static Session getSession(int dbId, long sessionId) {
        Database db = null;
        synchronized (databaseIDMap) {
            db = (Database) databaseIDMap.get(dbId);
        }
        return db == null ? null
                          : db.sessionManager.getSession(sessionId);
    }
    public static int getDatabase(String type, String path, Server server,
                                  HsqlProperties props) {
        Database db = getDatabase(type, path, props);
        registerServer(server, db);
        return db.databaseID;
    }
    public static Database getDatabase(int id) {
        return (Database) databaseIDMap.get(id);
    }
    public static void shutdownDatabases(Server server, int shutdownMode) {
        HashSet    databases = (HashSet) serverMap.get(server);
        Database[] dbArray   = new Database[databases.size()];
        databases.toArray(dbArray);
        for (int i = 0; i < dbArray.length; i++) {
            dbArray[i].close(shutdownMode);
        }
    }
    public static Database getDatabase(String type, String path,
                                       HsqlProperties props) {
        Database db = getDatabaseObject(type, path, props);
        synchronized (db) {
            switch (db.getState()) {
                case Database.DATABASE_ONLINE :
                    break;
                case Database.DATABASE_SHUTDOWN :
                    if (lookupDatabaseObject(type, path) == null) {
                        addDatabaseObject(type, path, db);
                    }
                    db.open();
                    break;
                case Database.DATABASE_CLOSING :
                case Database.DATABASE_OPENING :
                    throw Error.error(ErrorCode.LOCK_FILE_ACQUISITION_FAILURE,
                                      ErrorCode.M_DatabaseManager_getDatabase);
            }
        }
        return db;
    }
    private static synchronized Database getDatabaseObject(String type,
            String path, HsqlProperties props) {
        Database db;
        String   key = path;
        HashMap  databaseMap;
        if (type == DatabaseURL.S_FILE) {
            databaseMap = fileDatabaseMap;
            key         = filePathToKey(path);
            db          = (Database) databaseMap.get(key);
            if (db == null) {
                if (databaseMap.size() > 0) {
                    Iterator it = databaseMap.keySet().iterator();
                    while (it.hasNext()) {
                        String current = (String) it.next();
                        if (key.equalsIgnoreCase(current)) {
                            key = current;
                            break;
                        }
                    }
                }
            }
        } else if (type == DatabaseURL.S_RES) {
            databaseMap = resDatabaseMap;
        } else if (type == DatabaseURL.S_MEM) {
            databaseMap = memDatabaseMap;
        } else {
            throw Error.runtimeError(ErrorCode.U_S0500, "DatabaseManager");
        }
        db = (Database) databaseMap.get(key);
        if (db == null) {
            db            = new Database(type, path, key, props);
            db.databaseID = dbIDCounter;
            synchronized (databaseIDMap) {
                databaseIDMap.put(dbIDCounter, db);
                dbIDCounter++;
            }
            databaseMap.put(key, db);
        }
        return db;
    }
    public static synchronized Database lookupDatabaseObject(String type,
            String path) {
        Object  key = path;
        HashMap databaseMap;
        if (type == DatabaseURL.S_FILE) {
            databaseMap = fileDatabaseMap;
            key         = filePathToKey(path);
        } else if (type == DatabaseURL.S_RES) {
            databaseMap = resDatabaseMap;
        } else if (type == DatabaseURL.S_MEM) {
            databaseMap = memDatabaseMap;
        } else {
            throw (Error.runtimeError(ErrorCode.U_S0500, "DatabaseManager"));
        }
        return (Database) databaseMap.get(key);
    }
    private static synchronized void addDatabaseObject(String type,
            String path, Database db) {
        Object  key = path;
        HashMap databaseMap;
        if (type == DatabaseURL.S_FILE) {
            databaseMap = fileDatabaseMap;
            key         = filePathToKey(path);
        } else if (type == DatabaseURL.S_RES) {
            databaseMap = resDatabaseMap;
        } else if (type == DatabaseURL.S_MEM) {
            databaseMap = memDatabaseMap;
        } else {
            throw Error.runtimeError(ErrorCode.U_S0500, "DatabaseManager");
        }
        synchronized (databaseIDMap) {
            databaseIDMap.put(db.databaseID, db);
        }
        databaseMap.put(key, db);
    }
    static void removeDatabase(Database database) {
        int     dbID = database.databaseID;
        String  type = database.getType();
        String  path = database.getPath();
        Object  key  = path;
        HashMap databaseMap;
        notifyServers(database);
        if (type == DatabaseURL.S_FILE) {
            databaseMap = fileDatabaseMap;
            key         = filePathToKey(path);
        } else if (type == DatabaseURL.S_RES) {
            databaseMap = resDatabaseMap;
        } else if (type == DatabaseURL.S_MEM) {
            databaseMap = memDatabaseMap;
        } else {
            throw (Error.runtimeError(ErrorCode.U_S0500, "DatabaseManager"));
        }
        boolean isEmpty = false;
        synchronized (databaseIDMap) {
            databaseIDMap.remove(dbID);
            isEmpty = databaseIDMap.isEmpty();
        }
        synchronized (databaseMap) {
            databaseMap.remove(key);
        }
        if (isEmpty) {
            ValuePool.resetPool();
        }
    }
    static HashMap serverMap = new HashMap();
    public static void deRegisterServer(Server server) {
        serverMap.remove(server);
    }
    private static void deRegisterServer(Server server, Database db) {
        Iterator it = serverMap.values().iterator();
        for (; it.hasNext(); ) {
            HashSet databases = (HashSet) it.next();
            databases.remove(db);
            if (databases.isEmpty()) {
                it.remove();
            }
        }
    }
    private static void registerServer(Server server, Database db) {
        if (!serverMap.containsKey(server)) {
            serverMap.put(server, new HashSet());
        }
        HashSet databases = (HashSet) serverMap.get(server);
        databases.add(db);
    }
    private static void notifyServers(Database db) {
        Iterator it = serverMap.keySet().iterator();
        for (; it.hasNext(); ) {
            Server  server    = (Server) it.next();
            HashSet databases = (HashSet) serverMap.get(server);
            if (databases.contains(db)) {
                server.notify(ServerConstants.SC_DATABASE_SHUTDOWN,
                              db.databaseID);
            }
        }
    }
    static boolean isServerDB(Database db) {
        Iterator it = serverMap.keySet().iterator();
        for (; it.hasNext(); ) {
            Server  server    = (Server) it.next();
            HashSet databases = (HashSet) serverMap.get(server);
            if (databases.contains(db)) {
                return true;
            }
        }
        return false;
    }
    private static final HsqlTimer timer = new HsqlTimer();
    public static HsqlTimer getTimer() {
        return timer;
    }
    private static String filePathToKey(String path) {
        try {
            return FileUtil.getFileUtil().canonicalPath(path);
        } catch (Exception e) {
            return path;
        }
    }
}