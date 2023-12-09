


package org.hsqldb;

import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.lib.Iterator;
import org.hsqldb.lib.LongKeyHashMap;
import org.hsqldb.rights.User;


public class SessionManager {

    long                   sessionIdCount = 0;
    private LongKeyHashMap sessionMap     = new LongKeyHashMap();
    private Session        sysSession;
    private Session        sysLobSession;

    

    
    public SessionManager(Database db) {

        User sysUser = db.getUserManager().getSysUser();

        sysSession = new Session(db, sysUser, false, false, sessionIdCount++,
                                 null, 0);
        sysLobSession = new Session(db, sysUser, true, false,
                                    sessionIdCount++, null, 0);
    }

    

    
    public synchronized Session newSession(Database db, User user,
                                           boolean readonly,
                                           boolean autoCommit,
                                           String zoneString,
                                           int timeZoneSeconds) {

        Session s = new Session(db, user, autoCommit, readonly,
                                sessionIdCount, zoneString, timeZoneSeconds);

        sessionMap.put(sessionIdCount, s);

        sessionIdCount++;

        return s;
    }

    public synchronized Session newSessionForLog(Database db) {

        boolean autoCommit = db.databaseProperties.isVersion18();
        Session s = new Session(db, db.getUserManager().getSysUser(),
                                autoCommit, false, sessionIdCount, null, 0);

        s.isProcessingLog = true;

        sessionMap.put(sessionIdCount, s);

        sessionIdCount++;

        return s;
    }

    
    public Session getSysSessionForScript(Database db) {

        Session session = new Session(db, db.getUserManager().getSysUser(),
                                      false, false, 0, null, 0);

        session.isProcessingScript = true;

        return session;
    }

    public Session getSysLobSession() {
        return sysLobSession;
    }

    
    public Session getSysSession() {

        sysSession.currentSchema =
            sysSession.database.schemaManager.getDefaultSchemaHsqlName();
        sysSession.isProcessingScript = false;
        sysSession.isProcessingLog    = false;

        sysSession.setUser(sysSession.database.getUserManager().getSysUser());

        return sysSession;
    }

    
    public Session newSysSession() {

        Session session = new Session(sysSession.database,
                                      sysSession.getUser(), false, false,
                                      sessionIdCount, null, 0);

        session.currentSchema =
            sysSession.database.schemaManager.getDefaultSchemaHsqlName();

        sessionMap.put(sessionIdCount, session);

        sessionIdCount++;

        return session;
    }

    public Session newSysSession(HsqlName schema, User user) {

        Session session = new Session(sysSession.database, user, false, false,
                                      0, null, 0);

        session.currentSchema = schema;

        return session;
    }

    
    public void closeAllSessions() {

        
        Session[] sessions = getAllSessions();

        for (int i = 0; i < sessions.length; i++) {
            sessions[i].close();
        }
    }

    
    synchronized void removeSession(Session session) {
        sessionMap.remove(session.getId());
    }

    
    synchronized void close() {

        closeAllSessions();
        sysSession.close();
        sysLobSession.close();
    }

    
    synchronized boolean isEmpty() {
        return sessionMap.isEmpty();
    }

    
    public synchronized Session[] getVisibleSessions(Session session) {
        return session.isAdmin() ? getAllSessions()
                                 : new Session[]{ session };
    }

    
    synchronized Session getSession(long id) {
        return (Session) sessionMap.get(id);
    }

    public synchronized Session[] getAllSessions() {

        Session[] sessions = new Session[sessionMap.size()];
        Iterator  it       = sessionMap.values().iterator();

        for (int i = 0; it.hasNext(); i++) {
            sessions[i] = (Session) it.next();
        }

        return sessions;
    }

    public synchronized boolean isUserActive(String userName) {

        Iterator it = sessionMap.values().iterator();

        for (int i = 0; it.hasNext(); i++) {
            Session session = (Session) it.next();

            if (userName.equals(session.getUser().getName().getNameString())) {
                return true;
            }
        }

        return false;
    }

    public synchronized void removeSchemaReference(Schema schema) {

        Iterator it = sessionMap.values().iterator();

        for (int i = 0; it.hasNext(); i++) {
            Session session = (Session) it.next();

            if (session.getCurrentSchemaHsqlName() == schema.getName()) {
                session.resetSchema();
            }
        }
    }

    public synchronized void resetLoggedSchemas() {

        Iterator it = sessionMap.values().iterator();

        for (int i = 0; it.hasNext(); i++) {
            Session session = (Session) it.next();

            session.loggedSchema = null;
        }

        this.sysLobSession.loggedSchema = null;
    }
}
