


package org.hsqldb;

import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.lib.IntKeyHashMap;
import org.hsqldb.lib.LongKeyHashMap;
import org.hsqldb.lib.LongKeyIntValueHashMap;
import org.hsqldb.lib.LongValueHashMap;
import org.hsqldb.result.Result;


public final class StatementManager {

    
    private Database database;

    
    private IntKeyHashMap schemaMap;

    
    private LongKeyHashMap csidMap;

    
    private LongKeyIntValueHashMap useMap;

    
    private long next_cs_id;

    
    StatementManager(Database database) {

        this.database = database;
        schemaMap     = new IntKeyHashMap();
        csidMap       = new LongKeyHashMap();
        useMap        = new LongKeyIntValueHashMap();
        next_cs_id    = 0;
    }

    
    synchronized void reset() {

        schemaMap.clear();
        csidMap.clear();
        useMap.clear();

        next_cs_id = 0;
    }

    
    private long nextID() {

        next_cs_id++;

        return next_cs_id;
    }

    
    private long getStatementID(HsqlName schema, String sql) {

        LongValueHashMap sqlMap =
            (LongValueHashMap) schemaMap.get(schema.hashCode());

        if (sqlMap == null) {
            return -1;
        }

        return sqlMap.get(sql, -1);
    }

    
    public synchronized Statement getStatement(Session session, long csid) {

        Statement cs = (Statement) csidMap.get(csid);

        if (cs == null) {
            return null;
        }

        if (cs.getCompileTimestamp()
                < database.schemaManager.getSchemaChangeTimestamp()) {
            cs = recompileStatement(session, cs);

            if (cs == null) {
                freeStatement(csid);

                return null;
            }

            csidMap.put(csid, cs);
        }

        return cs;
    }

    
    public synchronized Statement getStatement(Session session,
            Statement statement) {

        long      csid = statement.getID();
        Statement cs   = (Statement) csidMap.get(csid);

        if (cs != null) {
            return getStatement(session, csid);
        }

        if (statement.getCompileTimestamp()
                < database.schemaManager.getSchemaChangeTimestamp()) {
            cs = recompileStatement(session, statement);

            if (cs == null) {
                freeStatement(csid);

                return null;
            }
        }

        return cs;
    }

    private Statement recompileStatement(Session session, Statement cs) {

        HsqlName  oldSchema = session.getCurrentSchemaHsqlName();
        Statement newStatement;

        
        try {
            HsqlName schema = cs.getSchemaName();
            int      props  = cs.getCursorPropertiesRequest();

            if (schema != null) {

                
                session.setSchema(schema.name);
            }

            boolean setGenerated = cs.generatedResultMetaData() != null;

            newStatement = session.compileStatement(cs.getSQL(), props);

            newStatement.setCursorPropertiesRequest(props);

            if (!cs.getResultMetaData().areTypesCompatible(
                    newStatement.getResultMetaData())) {
                return null;
            }

            if (!cs.getParametersMetaData().areTypesCompatible(
                    newStatement.getParametersMetaData())) {
                return null;
            }

            newStatement.setCompileTimestamp(
                database.txManager.getGlobalChangeTimestamp());

            if (setGenerated) {
                StatementDML si = (StatementDML) cs;

                newStatement.setGeneratedColumnInfo(si.generatedType,
                                                    si.generatedInputMetaData);
            }
        } catch (Throwable t) {
            return null;
        } finally {
            session.setCurrentSchemaHsqlName(oldSchema);
        }

        return newStatement;
    }

    
    private long registerStatement(long csid, Statement cs) {

        if (csid < 0) {
            csid = nextID();

            int schemaid = cs.getSchemaName().hashCode();
            LongValueHashMap sqlMap =
                (LongValueHashMap) schemaMap.get(schemaid);

            if (sqlMap == null) {
                sqlMap = new LongValueHashMap();

                schemaMap.put(schemaid, sqlMap);
            }

            sqlMap.put(cs.getSQL(), csid);
        }

        cs.setID(csid);
        cs.setCompileTimestamp(database.txManager.getGlobalChangeTimestamp());
        csidMap.put(csid, cs);

        return csid;
    }

    
    synchronized void freeStatement(long csid) {

        if (csid == -1) {

            
            return;
        }

        int useCount = useMap.get(csid, 1);

        if (useCount > 1) {
            useMap.put(csid, useCount - 1);

            return;
        }

        Statement cs = (Statement) csidMap.remove(csid);

        if (cs != null) {
            int schemaid = cs.getSchemaName().hashCode();
            LongValueHashMap sqlMap =
                (LongValueHashMap) schemaMap.get(schemaid);
            String sql = (String) cs.getSQL();

            sqlMap.remove(sql);
        }

        useMap.remove(csid);
    }

    
    synchronized Statement compile(Session session,
                                   Result cmd) throws Throwable {

        int       props = cmd.getExecuteProperties();
        Statement cs    = null;
        String    sql   = cmd.getMainString();
        long      csid  = getStatementID(session.currentSchema, sql);

        if (csid >= 0) {
            cs = (Statement) csidMap.get(csid);

            if (cs != null) {
                if (cs.getCursorPropertiesRequest() != props) {
                    cs   = null;
                    csid = -1;
                }

                
            }
        }

        if (cs == null || !cs.isValid()
                || cs.getCompileTimestamp()
                   < database.schemaManager.getSchemaChangeTimestamp()) {
            cs = session.compileStatement(sql, props);

            cs.setCursorPropertiesRequest(props);

            csid = registerStatement(csid, cs);
        }

        int useCount = useMap.get(csid, 0) + 1;

        useMap.put(csid, useCount);
        cs.setGeneratedColumnInfo(cmd.getGeneratedResultType(),
                                  cmd.getGeneratedResultMetaData());

        return cs;
    }
}
