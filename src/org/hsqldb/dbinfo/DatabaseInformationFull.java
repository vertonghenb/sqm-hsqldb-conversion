package org.hsqldb.dbinfo;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.security.AccessController;
import java.security.PrivilegedAction;
import org.hsqldb.ColumnSchema;
import org.hsqldb.Constraint;
import org.hsqldb.Database;
import org.hsqldb.Expression;
import org.hsqldb.ExpressionColumn;
import org.hsqldb.HsqlException;
import org.hsqldb.HsqlNameManager;
import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.NumberSequence;
import org.hsqldb.Routine;
import org.hsqldb.RoutineSchema;
import org.hsqldb.Schema;
import org.hsqldb.SchemaObject;
import org.hsqldb.SchemaObjectSet;
import org.hsqldb.Session;
import org.hsqldb.SqlInvariants;
import org.hsqldb.Statement;
import org.hsqldb.Table;
import org.hsqldb.TextTable;
import org.hsqldb.Tokens;
import org.hsqldb.TriggerDef;
import org.hsqldb.View;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.lib.FileUtil;
import org.hsqldb.lib.HashMappedList;
import org.hsqldb.lib.HashSet;
import org.hsqldb.lib.Iterator;
import org.hsqldb.lib.LineGroupReader;
import org.hsqldb.lib.OrderedHashSet;
import org.hsqldb.lib.Set;
import org.hsqldb.lib.WrapperIterator;
import org.hsqldb.persist.DataFileCache;
import org.hsqldb.persist.HsqlDatabaseProperties;
import org.hsqldb.persist.HsqlProperties;
import org.hsqldb.persist.PersistentStore;
import org.hsqldb.persist.TextCache;
import org.hsqldb.persist.TextFileSettings;
import org.hsqldb.result.Result;
import org.hsqldb.rights.GrantConstants;
import org.hsqldb.rights.Grantee;
import org.hsqldb.rights.Right;
import org.hsqldb.store.ValuePool;
import org.hsqldb.types.ArrayType;
import org.hsqldb.types.CharacterType;
import org.hsqldb.types.Charset;
import org.hsqldb.types.Collation;
import org.hsqldb.types.IntervalType;
import org.hsqldb.types.NumberType;
import org.hsqldb.types.TimestampData;
import org.hsqldb.types.Type;
final class DatabaseInformationFull
extends org.hsqldb.dbinfo.DatabaseInformationMain {
    static final HashMappedList statementMap;
    static {
        synchronized (DatabaseInformationFull.class) {
            final String resourceFileName =
                "/org/hsqldb/resources/information-schema.sql";
            final String[] starters = new String[]{ "/*" };
            InputStream fis = (InputStream) AccessController.doPrivileged(
                new PrivilegedAction() {
                public InputStream run() {
                    return getClass().getResourceAsStream(resourceFileName);
                }
            });
            InputStreamReader reader = null;
            try {
                reader = new InputStreamReader(fis, "ISO-8859-1");
            } catch (Exception e) {}
            LineNumberReader lineReader = new LineNumberReader(reader);
            LineGroupReader  lg = new LineGroupReader(lineReader, starters);
            statementMap = lg.getAsMap();
            lg.close();
        }
    }
    DatabaseInformationFull(Database db) {
        super(db);
    }
    protected Table generateTable(Session session, PersistentStore store,
                                  int tableIndex) {
        switch (tableIndex) {
            case SYSTEM_CACHEINFO :
                return SYSTEM_CACHEINFO(session, store);
            case SYSTEM_COLUMN_SEQUENCE_USAGE :
                return SYSTEM_COLUMN_SEQUENCE_USAGE(session, store);
            case SYSTEM_COMMENTS :
                return SYSTEM_COMMENTS(session, store);
            case SYSTEM_SESSIONINFO :
                return SYSTEM_SESSIONINFO(session, store);
            case SYSTEM_PROPERTIES :
                return SYSTEM_PROPERTIES(session, store);
            case SYSTEM_SESSIONS :
                return SYSTEM_SESSIONS(session, store);
            case SYSTEM_TEXTTABLES :
                return SYSTEM_TEXTTABLES(session, store);
            case ADMINISTRABLE_ROLE_AUTHORIZATIONS :
                return ADMINISTRABLE_ROLE_AUTHORIZATIONS(session, store);
            case APPLICABLE_ROLES :
                return APPLICABLE_ROLES(session, store);
            case ASSERTIONS :
                return ASSERTIONS(session, store);
            case AUTHORIZATIONS :
                return AUTHORIZATIONS(session, store);
            case CHARACTER_SETS :
                return CHARACTER_SETS(session, store);
            case CHECK_CONSTRAINT_ROUTINE_USAGE :
                return CHECK_CONSTRAINT_ROUTINE_USAGE(session, store);
            case CHECK_CONSTRAINTS :
                return CHECK_CONSTRAINTS(session, store);
            case COLLATIONS :
                return COLLATIONS(session, store);
            case COLUMN_COLUMN_USAGE :
                return COLUMN_COLUMN_USAGE(session, store);
            case COLUMN_DOMAIN_USAGE :
                return COLUMN_DOMAIN_USAGE(session, store);
            case COLUMN_UDT_USAGE :
                return COLUMN_UDT_USAGE(session, store);
            case CONSTRAINT_COLUMN_USAGE :
                return CONSTRAINT_COLUMN_USAGE(session, store);
            case CONSTRAINT_TABLE_USAGE :
                return CONSTRAINT_TABLE_USAGE(session, store);
            case COLUMNS :
                return COLUMNS(session, store);
            case DATA_TYPE_PRIVILEGES :
                return DATA_TYPE_PRIVILEGES(session, store);
            case DOMAIN_CONSTRAINTS :
                return DOMAIN_CONSTRAINTS(session, store);
            case DOMAINS :
                return DOMAINS(session, store);
            case ELEMENT_TYPES :
                return ELEMENT_TYPES(session, store);
            case ENABLED_ROLES :
                return ENABLED_ROLES(session, store);
            case JAR_JAR_USAGE :
                return JAR_JAR_USAGE(session, store);
            case JARS :
                return JARS(session, store);
            case KEY_COLUMN_USAGE :
                return KEY_COLUMN_USAGE(session, store);
            case METHOD_SPECIFICATIONS :
                return METHOD_SPECIFICATIONS(session, store);
            case MODULE_COLUMN_USAGE :
                return MODULE_COLUMN_USAGE(session, store);
            case MODULE_PRIVILEGES :
                return MODULE_PRIVILEGES(session, store);
            case MODULE_TABLE_USAGE :
                return MODULE_TABLE_USAGE(session, store);
            case MODULES :
                return MODULES(session, store);
            case PARAMETERS :
                return PARAMETERS(session, store);
            case REFERENTIAL_CONSTRAINTS :
                return REFERENTIAL_CONSTRAINTS(session, store);
            case ROLE_AUTHORIZATION_DESCRIPTORS :
                return ROLE_AUTHORIZATION_DESCRIPTORS(session, store);
            case ROLE_COLUMN_GRANTS :
                return ROLE_COLUMN_GRANTS(session, store);
            case ROLE_ROUTINE_GRANTS :
                return ROLE_ROUTINE_GRANTS(session, store);
            case ROLE_TABLE_GRANTS :
                return ROLE_TABLE_GRANTS(session, store);
            case ROLE_USAGE_GRANTS :
                return ROLE_USAGE_GRANTS(session, store);
            case ROLE_UDT_GRANTS :
                return ROLE_UDT_GRANTS(session, store);
            case ROUTINE_COLUMN_USAGE :
                return ROUTINE_COLUMN_USAGE(session, store);
            case ROUTINE_JAR_USAGE :
                return ROUTINE_JAR_USAGE(session, store);
            case ROUTINE_PRIVILEGES :
                return ROUTINE_PRIVILEGES(session, store);
            case ROUTINE_ROUTINE_USAGE :
                return ROUTINE_ROUTINE_USAGE(session, store);
            case ROUTINE_SEQUENCE_USAGE :
                return ROUTINE_SEQUENCE_USAGE(session, store);
            case ROUTINE_TABLE_USAGE :
                return ROUTINE_TABLE_USAGE(session, store);
            case ROUTINES :
                return ROUTINES(session, store);
            case SCHEMATA :
                return SCHEMATA(session, store);
            case SEQUENCES :
                return SEQUENCES(session, store);
            case SQL_FEATURES :
                return SQL_FEATURES(session, store);
            case SQL_IMPLEMENTATION_INFO :
                return SQL_IMPLEMENTATION_INFO(session, store);
            case SQL_PACKAGES :
                return SQL_PACKAGES(session, store);
            case SQL_PARTS :
                return SQL_PARTS(session, store);
            case SQL_SIZING :
                return SQL_SIZING(session, store);
            case SQL_SIZING_PROFILES :
                return SQL_SIZING_PROFILES(session, store);
            case TABLE_CONSTRAINTS :
                return TABLE_CONSTRAINTS(session, store);
            case TABLES :
                return TABLES(session, store);
            case TRANSLATIONS :
                return TRANSLATIONS(session, store);
            case TRIGGERED_UPDATE_COLUMNS :
                return TRIGGERED_UPDATE_COLUMNS(session, store);
            case TRIGGER_COLUMN_USAGE :
                return TRIGGER_COLUMN_USAGE(session, store);
            case TRIGGER_ROUTINE_USAGE :
                return TRIGGER_ROUTINE_USAGE(session, store);
            case TRIGGER_SEQUENCE_USAGE :
                return TRIGGER_SEQUENCE_USAGE(session, store);
            case TRIGGER_TABLE_USAGE :
                return TRIGGER_TABLE_USAGE(session, store);
            case TRIGGERS :
                return TRIGGERS(session, store);
            case UDT_PRIVILEGES :
                return UDT_PRIVILEGES(session, store);
            case USAGE_PRIVILEGES :
                return USAGE_PRIVILEGES(session, store);
            case USER_DEFINED_TYPES :
                return USER_DEFINED_TYPES(session, store);
            case VIEW_COLUMN_USAGE :
                return VIEW_COLUMN_USAGE(session, store);
            case VIEW_ROUTINE_USAGE :
                return VIEW_ROUTINE_USAGE(session, store);
            case VIEW_TABLE_USAGE :
                return VIEW_TABLE_USAGE(session, store);
            case VIEWS :
                return VIEWS(session, store);
            default :
                return super.generateTable(session, store, tableIndex);
        }
    }
    Table SYSTEM_CACHEINFO(Session session, PersistentStore store) {
        Table t = sysTables[SYSTEM_CACHEINFO];
        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SYSTEM_CACHEINFO]);
            addColumn(t, "CACHE_FILE", CHARACTER_DATA);          
            addColumn(t, "MAX_CACHE_COUNT", CARDINAL_NUMBER);    
            addColumn(t, "MAX_CACHE_BYTES", CARDINAL_NUMBER);    
            addColumn(t, "CACHE_SIZE", CARDINAL_NUMBER);         
            addColumn(t, "CACHE_BYTES", CARDINAL_NUMBER);        
            addColumn(t, "FILE_FREE_BYTES", CARDINAL_NUMBER);    
            addColumn(t, "FILE_FREE_COUNT", CARDINAL_NUMBER);    
            addColumn(t, "FILE_FREE_POS", CARDINAL_NUMBER);      
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[SYSTEM_CACHEINFO].name, false,
                SchemaObject.INDEX);
            t.createPrimaryKeyConstraint(name, new int[]{ 0 }, true);
            return t;
        }
        final int icache_file      = 0;
        final int imax_cache_sz    = 1;
        final int imax_cache_bytes = 2;
        final int icache_size      = 3;
        final int icache_length    = 4;
        final int ifree_bytes      = 5;
        final int ifree_count      = 6;
        final int ifree_pos        = 7;
        DataFileCache cache = null;
        Object[]      row;
        HashSet       cacheSet;
        Iterator      caches;
        Iterator      tables;
        Table         table;
        int           iFreeBytes;
        int           iLargestFreeItem;
        long          lSmallestFreeItem;
        cacheSet = new HashSet();
        tables =
            database.schemaManager.databaseObjectIterator(SchemaObject.TABLE);
        while (tables.hasNext()) {
            table = (Table) tables.next();
            PersistentStore currentStore = table.getRowStore(session);
            if (session.getGrantee().isFullyAccessibleByRole(
                    table.getName())) {
                if (currentStore != null) {
                    cache = currentStore.getCache();
                }
                if (cache != null) {
                    cacheSet.add(cache);
                }
            }
        }
        caches = cacheSet.iterator();
        while (caches.hasNext()) {
            cache = (DataFileCache) caches.next();
            row   = t.getEmptyRowData();
            row[icache_file] = FileUtil.getFileUtil().canonicalOrAbsolutePath(
                cache.getFileName());
            row[imax_cache_sz]    = ValuePool.getLong(cache.capacity());
            row[imax_cache_bytes] = ValuePool.getLong(cache.bytesCapacity());
            row[icache_size] = ValuePool.getLong(cache.getCachedObjectCount());
            row[icache_length] =
                ValuePool.getLong(cache.getTotalCachedBlockSize());
            row[ifree_bytes] =
                ValuePool.getLong(cache.getTotalFreeBlockSize());
            row[ifree_count] = ValuePool.getLong(cache.getFreeBlockCount());
            row[ifree_pos]   = ValuePool.getLong(cache.getFileFreePos());
            t.insertSys(session, store, row);
        }
        return t;
    }
    Table SYSTEM_COLUMN_SEQUENCE_USAGE(Session session,
                                       PersistentStore store) {
        Table t = sysTables[SYSTEM_COLUMN_SEQUENCE_USAGE];
        if (t == null) {
            t = createBlankTable(
                sysTableHsqlNames[SYSTEM_COLUMN_SEQUENCE_USAGE]);
            addColumn(t, "TABLE_CATALOG", SQL_IDENTIFIER);    
            addColumn(t, "TABLE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "TABLE_NAME", SQL_IDENTIFIER);
            addColumn(t, "COLUMN_NAME", SQL_IDENTIFIER);
            addColumn(t, "SEQUENCE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "SEQUENCE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "SEQUENCE_NAME", SQL_IDENTIFIER);
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[SYSTEM_COLUMN_SEQUENCE_USAGE].name, false,
                SchemaObject.INDEX);
            t.createPrimaryKeyConstraint(name, new int[] {
                0, 1, 2, 3, 4
            }, false);
            return t;
        }
        final int table_cat        = 0;
        final int table_schem      = 1;
        final int table_name       = 2;
        final int column_name      = 3;
        final int sequence_catalog = 4;
        final int sequence_schema  = 5;
        final int sequence_name    = 6;
        int            columnCount;
        Iterator       tables;
        Table          table;
        Object[]       row;
        OrderedHashSet columnList;
        NumberSequence sequence;
        tables = allTables();
        while (tables.hasNext()) {
            table = (Table) tables.next();
            if (!table.hasIdentityColumn()) {
                continue;
            }
            columnList =
                session.getGrantee().getColumnsForAllPrivileges(table);
            if (columnList.isEmpty()) {
                continue;
            }
            columnCount = table.getColumnCount();
            for (int i = 0; i < columnCount; i++) {
                ColumnSchema column = table.getColumn(i);
                if (!column.isIdentity()) {
                    continue;
                }
                sequence = column.getIdentitySequence();
                if (sequence.getName() == null) {
                    continue;
                }
                if (!columnList.contains(column.getName())) {
                    continue;
                }
                row                   = t.getEmptyRowData();
                row[table_cat]        = database.getCatalogName().name;
                row[table_schem]      = table.getSchemaName().name;
                row[table_name]       = table.getName().name;
                row[column_name]      = column.getName().name;
                row[sequence_catalog] = database.getCatalogName().name;
                row[sequence_schema]  = sequence.getSchemaName().name;
                row[sequence_name]    = sequence.getName().name;
                t.insertSys(session, store, row);
            }
        }
        return t;
    }
    Table SYSTEM_COMMENTS(Session session, PersistentStore store) {
        Table t = sysTables[SYSTEM_COMMENTS];
        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SYSTEM_COMMENTS]);
            addColumn(t, "OBJECT_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "OBJECT_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "OBJECT_NAME", SQL_IDENTIFIER);    
            addColumn(t, "OBJECT_TYPE", SQL_IDENTIFIER);
            addColumn(t, "COLUMN_NAME", SQL_IDENTIFIER);
            addColumn(t, "COMMENT", CHARACTER_DATA);
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[SYSTEM_COMMENTS].name, false,
                SchemaObject.INDEX);
            t.createPrimaryKeyConstraint(name, new int[] {
                0, 1, 2, 3, 4
            }, false);
            return t;
        }
        final int catalog     = 0;
        final int schema      = 1;
        final int name        = 2;
        final int type        = 3;
        final int column_name = 4;
        final int remark      = 5;
        Iterator it;
        Object[] row;
        DITableInfo ti = new DITableInfo();
        it = allTables();
        while (it.hasNext()) {
            Table table = (Table) it.next();
            if (!session.getGrantee().isAccessible(table)) {
                continue;
            }
            ti.setTable(table);
            int colCount = table.getColumnCount();
            for (int i = 0; i < colCount; i++) {
                ColumnSchema column = table.getColumn(i);
                if (column.getName().comment == null) {
                    continue;
                }
                row              = t.getEmptyRowData();
                row[catalog]     = database.getCatalogName().name;
                row[schema]      = table.getSchemaName().name;
                row[name]        = table.getName().name;
                row[type]        = "COLUMN";
                row[column_name] = column.getName().name;
                row[remark]      = column.getName().comment;
                t.insertSys(session, store, row);
            }
            if (table.getTableType() != Table.INFO_SCHEMA_TABLE
                    && table.getName().comment == null) {
                continue;
            }
            row          = t.getEmptyRowData();
            row[catalog] = database.getCatalogName().name;
            row[schema]  = table.getSchemaName().name;
            row[name]    = table.getName().name;
            row[type] =
                table.isView()
                || table.getTableType() == Table.INFO_SCHEMA_TABLE ? "VIEW"
                                                                   : "TABLE";
            row[column_name] = null;
            row[remark]      = ti.getRemark();
            t.insertSys(session, store, row);
        }
        it = database.schemaManager.databaseObjectIterator(
            SchemaObject.ROUTINE);
        while (it.hasNext()) {
            SchemaObject object = (SchemaObject) it.next();
            if (!session.getGrantee().isAccessible(object)) {
                continue;
            }
            if (object.getName().comment == null) {
                continue;
            }
            row              = t.getEmptyRowData();
            row[catalog]     = database.getCatalogName().name;
            row[schema]      = object.getSchemaName().name;
            row[name]        = object.getName().name;
            row[type]        = "ROUTINE";
            row[column_name] = null;
            row[remark]      = object.getName().comment;
            t.insertSys(session, store, row);
        }
        return t;
    }
    Table SYSTEM_PROPERTIES(Session session, PersistentStore store) {
        Table t = sysTables[SYSTEM_PROPERTIES];
        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SYSTEM_PROPERTIES]);
            addColumn(t, "PROPERTY_SCOPE", CHARACTER_DATA);
            addColumn(t, "PROPERTY_NAMESPACE", CHARACTER_DATA);
            addColumn(t, "PROPERTY_NAME", CHARACTER_DATA);
            addColumn(t, "PROPERTY_VALUE", CHARACTER_DATA);
            addColumn(t, "PROPERTY_CLASS", CHARACTER_DATA);
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[SYSTEM_PROPERTIES].name, false,
                SchemaObject.INDEX);
            t.createPrimaryKeyConstraint(name, new int[] {
                0, 1, 2
            }, true);
            return t;
        }
        final int iscope = 0;
        final int ins    = 1;
        final int iname  = 2;
        final int ivalue = 3;
        final int iclass = 4;
        String scope;
        String nameSpace;
        Object[]               row;
        HsqlDatabaseProperties props;
        scope     = "SESSION";
        props     = database.getProperties();
        nameSpace = "database.properties";
        Iterator it = props.getUserDefinedPropertyData().iterator();
        while (it.hasNext()) {
            Object[] metaData = (Object[]) it.next();
            row         = t.getEmptyRowData();
            row[iscope] = scope;
            row[ins]    = nameSpace;
            row[iname]  = metaData[HsqlProperties.indexName];
            row[ivalue] =
                database.logger.getValueStringForProperty((String) row[iname]);
            if (row[ivalue] == null) {
                row[ivalue] = props.getPropertyString((String) row[iname]);
            }
            row[iclass] = metaData[HsqlProperties.indexClass];
            t.insertSys(session, store, row);
        }
        return t;
    }
    Table SYSTEM_SESSIONINFO(Session session, PersistentStore store) {
        Table t = sysTables[SYSTEM_SESSIONINFO];
        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SYSTEM_SESSIONINFO]);
            addColumn(t, "KEY", CHARACTER_DATA);      
            addColumn(t, "VALUE", CHARACTER_DATA);    
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[SYSTEM_SESSIONINFO].name, false,
                SchemaObject.INDEX);
            t.createPrimaryKeyConstraint(name, new int[]{ 0 }, true);
            return t;
        }
        Object[] row;
        row    = t.getEmptyRowData();
        row[0] = "SESSION ID";
        row[1] = String.valueOf(session.getId());
        t.insertSys(session, store, row);
        row    = t.getEmptyRowData();
        row[0] = "AUTOCOMMIT";
        row[1] = session.isAutoCommit() ? Tokens.T_TRUE
                                        : Tokens.T_FALSE;
        t.insertSys(session, store, row);
        row    = t.getEmptyRowData();
        row[0] = "USER";
        row[1] = session.getUsername();
        t.insertSys(session, store, row);
        row    = t.getEmptyRowData();
        row[0] = "SESSION READONLY";
        row[1] = session.isReadOnlyDefault() ? Tokens.T_TRUE
                                             : Tokens.T_FALSE;
        t.insertSys(session, store, row);
        row    = t.getEmptyRowData();
        row[0] = "DATABASE READONLY";
        row[1] = database.isReadOnly() ? Tokens.T_TRUE
                                       : Tokens.T_FALSE;
        t.insertSys(session, store, row);
        row    = t.getEmptyRowData();
        row[0] = "DATABASE";
        row[1] = database.getURI();
        t.insertSys(session, store, row);
        row    = t.getEmptyRowData();
        row[0] = "IDENTITY";
        row[1] = String.valueOf(session.getLastIdentity());
        t.insertSys(session, store, row);
        row    = t.getEmptyRowData();
        row[0] = "CURRENT SCHEMA";
        row[1] = String.valueOf(session.getSchemaName(null));
        t.insertSys(session, store, row);
        row    = t.getEmptyRowData();
        row[0] = "ISOLATION LEVEL";
        row[1] = String.valueOf(session.getIsolation());
        t.insertSys(session, store, row);
        row    = t.getEmptyRowData();
        row[0] = "IGNORECASE";
        row[1] = session.isIgnorecase() ? Tokens.T_TRUE
                                        : Tokens.T_FALSE;
        t.insertSys(session, store, row);
        row    = t.getEmptyRowData();
        row[0] = "CURRENT STATEMENT";
        row[1] = "";
        Statement st = session.sessionContext.currentStatement;
        if (st != null) {
            row[1] = st.getSQL();
        }
        t.insertSys(session, store, row);
        return t;
    }
    Table SYSTEM_SESSIONS(Session session, PersistentStore store) {
        Table t = sysTables[SYSTEM_SESSIONS];
        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SYSTEM_SESSIONS]);
            addColumn(t, "SESSION_ID", CARDINAL_NUMBER);
            addColumn(t, "CONNECTED", TIME_STAMP);
            addColumn(t, "USER_NAME", SQL_IDENTIFIER);
            addColumn(t, "IS_ADMIN", Type.SQL_BOOLEAN);
            addColumn(t, "AUTOCOMMIT", Type.SQL_BOOLEAN);
            addColumn(t, "READONLY", Type.SQL_BOOLEAN);
            addColumn(t, "LAST_IDENTITY", CARDINAL_NUMBER);
            addColumn(t, "SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "TRANSACTION", Type.SQL_BOOLEAN);
            addColumn(t, "TRANSACTION_SIZE", CARDINAL_NUMBER);
            addColumn(t, "WAITING_FOR_THIS", CHARACTER_DATA);
            addColumn(t, "THIS_WAITING_FOR", CHARACTER_DATA);
            addColumn(t, "CURRENT_STATEMENT", CHARACTER_DATA);
            addColumn(t, "LATCH_COUNT", CARDINAL_NUMBER);
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[SYSTEM_SESSIONS].name, false,
                SchemaObject.INDEX);
            t.createPrimaryKeyConstraint(name, new int[]{ 0 }, true);
            return t;
        }
        final int isid           = 0;
        final int ict            = 1;
        final int iuname         = 2;
        final int iis_admin      = 3;
        final int iautocmt       = 4;
        final int ireadonly      = 5;
        final int ilast_id       = 6;
        final int it_schema      = 7;
        final int it_tx          = 8;
        final int it_size        = 9;
        final int it_waiting     = 10;
        final int it_waited      = 11;
        final int it_statement   = 12;
        final int it_latch_count = 13;
        Session[] sessions;
        Session   s;
        Object[]  row;
        sessions = database.sessionManager.getVisibleSessions(session);
        for (int i = 0; i < sessions.length; i++) {
            if (sessions[i].isClosed()) {
                continue;
            }
            s              = sessions[i];
            row            = t.getEmptyRowData();
            row[isid]      = ValuePool.getLong(s.getId());
            row[ict]       = new TimestampData(s.getConnectTime() / 1000);
            row[iuname]    = s.getUsername();
            row[iis_admin] = ValuePool.getBoolean(s.isAdmin());
            row[iautocmt]  = s.sessionContext.isAutoCommit;
            row[ireadonly] = s.isReadOnlyDefault;
            Number lastId = s.getLastIdentity();
            if (lastId != null) {
                row[ilast_id] = ValuePool.getLong(lastId.longValue());
            }
            row[it_tx]   = Boolean.valueOf(s.isInMidTransaction());
            row[it_size] = ValuePool.getLong(s.getTransactionSize());
            HsqlName name = s.getCurrentSchemaHsqlName();
            if (name != null) {
                row[it_schema] = name.name;
            }
            row[it_waiting] = "";
            row[it_waited]  = "";
            if (s.waitingSessions.size() > 0) {
                StringBuffer sb    = new StringBuffer();
                Session[]    array = new Session[s.waitingSessions.size()];
                s.waitingSessions.toArray(array);
                for (int j = 0; j < array.length; j++) {
                    if (j > 0) {
                        sb.append(',');
                    }
                    sb.append(array[j].getId());
                }
                row[it_waiting] = sb.toString();
            }
            if (s.waitedSessions.size() > 0) {
                StringBuffer sb    = new StringBuffer();
                Session[]    array = new Session[s.waitedSessions.size()];
                s.waitedSessions.toArray(array);
                for (int j = 0; j < array.length; j++) {
                    if (j > 0) {
                        sb.append(',');
                    }
                    sb.append(array[j].getId());
                }
                row[it_waited] = sb.toString();
            }
            Statement st = s.sessionContext.currentStatement;
            row[it_statement]   = st == null ? ""
                                             : st.getSQL();
            row[it_latch_count] = new Long(s.latch.getCount());
            t.insertSys(session, store, row);
        }
        return t;
    }
    Table SYSTEM_TEXTTABLES(Session session, PersistentStore store) {
        Table t = sysTables[SYSTEM_TEXTTABLES];
        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SYSTEM_TEXTTABLES]);
            addColumn(t, "TABLE_CAT", SQL_IDENTIFIER);
            addColumn(t, "TABLE_SCHEM", SQL_IDENTIFIER);
            addColumn(t, "TABLE_NAME", SQL_IDENTIFIER);    
            addColumn(t, "DATA_SOURCE_DEFINTION", CHARACTER_DATA);
            addColumn(t, "FILE_PATH", CHARACTER_DATA);
            addColumn(t, "FILE_ENCODING", CHARACTER_DATA);
            addColumn(t, "FIELD_SEPARATOR", CHARACTER_DATA);
            addColumn(t, "VARCHAR_SEPARATOR", CHARACTER_DATA);
            addColumn(t, "LONGVARCHAR_SEPARATOR", CHARACTER_DATA);
            addColumn(t, "IS_IGNORE_FIRST", Type.SQL_BOOLEAN);
            addColumn(t, "IS_ALL_QUOTED", Type.SQL_BOOLEAN);
            addColumn(t, "IS_QUOTED", Type.SQL_BOOLEAN);
            addColumn(t, "IS_DESC", Type.SQL_BOOLEAN);
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[SYSTEM_TEXTTABLES].name, false,
                SchemaObject.INDEX);
            t.createPrimaryKeyConstraint(name, new int[] {
                0, 1, 2,
            }, false);
            return t;
        }
        final int itable_cat   = 0;
        final int itable_schem = 1;
        final int itable_name  = 2;
        final int idsd         = 3;
        final int ifile_path   = 4;
        final int ifile_enc    = 5;
        final int ifs          = 6;
        final int ivfs         = 7;
        final int ilvfs        = 8;
        final int iif          = 9;
        final int iiq          = 10;
        final int iiaq         = 11;
        final int iid          = 12;
        Iterator tables;
        Table    table;
        Object[] row;
        tables =
            database.schemaManager.databaseObjectIterator(SchemaObject.TABLE);
        while (tables.hasNext()) {
            table = (Table) tables.next();
            PersistentStore currentStore = table.getRowStore(session);
            if (!table.isText() || !isAccessibleTable(session, table)) {
                continue;
            }
            row               = t.getEmptyRowData();
            row[itable_cat]   = database.getCatalogName().name;
            row[itable_schem] = table.getSchemaName().name;
            row[itable_name]  = table.getName().name;
            row[idsd]         = ((TextTable) table).getDataSource();
            TextCache cache = (TextCache) currentStore.getCache();
            if (cache != null) {
                TextFileSettings textFileSettings =
                    cache.getTextFileSettings();
                row[ifile_path] =
                    FileUtil.getFileUtil().canonicalOrAbsolutePath(
                        cache.getFileName());
                row[ifile_enc] = textFileSettings.stringEncoding;
                row[ifs]       = textFileSettings.fs;
                row[ivfs]      = textFileSettings.vs;
                row[ilvfs]     = textFileSettings.lvs;
                row[iif] = ValuePool.getBoolean(textFileSettings.ignoreFirst);
                row[iiq] = ValuePool.getBoolean(textFileSettings.isQuoted);
                row[iiaq] = ValuePool.getBoolean(textFileSettings.isAllQuoted);
                row[iid] = ((TextTable) table).isDescDataSource()
                           ? Boolean.TRUE
                           : Boolean.FALSE;
            }
            t.insertSys(session, store, row);
        }
        return t;
    }
    Table ADMINISTRABLE_ROLE_AUTHORIZATIONS(Session session,
            PersistentStore store) {
        Table t = sysTables[ADMINISTRABLE_ROLE_AUTHORIZATIONS];
        if (t == null) {
            t = createBlankTable(
                sysTableHsqlNames[ADMINISTRABLE_ROLE_AUTHORIZATIONS]);
            addColumn(t, "GRANTEE", SQL_IDENTIFIER);
            addColumn(t, "ROLE_NAME", SQL_IDENTIFIER);
            addColumn(t, "IS_GRANTABLE", SQL_IDENTIFIER);
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[ADMINISTRABLE_ROLE_AUTHORIZATIONS].name,
                false, SchemaObject.INDEX);
            t.createPrimaryKeyConstraint(name, new int[] {
                0, 1, 2
            }, false);
            return t;
        }
        if (session.isAdmin()) {
            insertRoles(session, t, session.getGrantee(), true);
        }
        return t;
    }
    Table APPLICABLE_ROLES(Session session, PersistentStore store) {
        Table t = sysTables[APPLICABLE_ROLES];
        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[APPLICABLE_ROLES]);
            addColumn(t, "GRANTEE", SQL_IDENTIFIER);
            addColumn(t, "ROLE_NAME", SQL_IDENTIFIER);
            addColumn(t, "IS_GRANTABLE", SQL_IDENTIFIER);
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[APPLICABLE_ROLES].name, false,
                SchemaObject.INDEX);
            t.createPrimaryKeyConstraint(name, new int[] {
                0, 1, 2
            }, false);
            return t;
        }
        insertRoles(session, t, session.getGrantee(), session.isAdmin());
        return t;
    }
    private void insertRoles(Session session, Table t, Grantee role,
                             boolean isGrantable) {
        final int       grantee      = 0;
        final int       role_name    = 1;
        final int       is_grantable = 2;
        PersistentStore store        = t.getRowStore(session);
        if (isGrantable) {
            Set      roles = database.getGranteeManager().getRoleNames();
            Iterator it    = roles.iterator();
            while (it.hasNext()) {
                String   roleName = (String) it.next();
                Object[] row      = t.getEmptyRowData();
                row[grantee]      = role.getName().getNameString();
                row[role_name]    = roleName;
                row[is_grantable] = Tokens.T_YES;
                t.insertSys(session, store, row);
            }
        } else {
            OrderedHashSet roles = role.getDirectRoles();
            for (int i = 0; i < roles.size(); i++) {
                String   roleName = (String) roles.get(i);
                Object[] row      = t.getEmptyRowData();
                row[grantee]      = role.getName().getNameString();
                row[role_name]    = roleName;
                row[is_grantable] = Tokens.T_NO;
                t.insertSys(session, store, row);
                role = database.getGranteeManager().getRole(roleName);
                insertRoles(session, t, role, isGrantable);
            }
        }
    }
    Table ASSERTIONS(Session session, PersistentStore store) {
        Table t = sysTables[ASSERTIONS];
        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[ASSERTIONS]);
            addColumn(t, "CONSTRAINT_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "CONSTRAINT_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "CONSTRAINT_NAME", SQL_IDENTIFIER);    
            addColumn(t, "IS_DEFERRABLE", YES_OR_NO);
            addColumn(t, "INITIALLY_DEFERRED", YES_OR_NO);
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[ASSERTIONS].name, false, SchemaObject.INDEX);
            t.createPrimaryKeyConstraint(name, new int[] {
                0, 1, 2
            }, false);
            return t;
        }
        final int constraint_catalog = 0;
        final int constraint_schema  = 1;
        final int constraint_name    = 2;
        final int is_deferrable      = 3;
        final int initially_deferred = 4;
        return t;
    }
    Table AUTHORIZATIONS(Session session, PersistentStore store) {
        Table t = sysTables[AUTHORIZATIONS];
        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[AUTHORIZATIONS]);
            addColumn(t, "AUTHORIZATION_NAME", SQL_IDENTIFIER);    
            addColumn(t, "AUTHORIZATION_TYPE", SQL_IDENTIFIER);    
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[AUTHORIZATIONS].name, false,
                SchemaObject.INDEX);
            t.createPrimaryKeyConstraint(name, new int[]{ 0 }, true);
            return t;
        }
        Iterator grantees;
        Grantee  grantee;
        Object[] row;
        grantees = session.getGrantee().visibleGrantees().iterator();
        while (grantees.hasNext()) {
            grantee = (Grantee) grantees.next();
            row     = t.getEmptyRowData();
            row[0]  = grantee.getName().getNameString();
            row[1]  = grantee.isRole() ? "ROLE"
                                       : "USER";
            t.insertSys(session, store, row);
        }
        return t;
    }
    Table CHARACTER_SETS(Session session, PersistentStore store) {
        Table t = sysTables[CHARACTER_SETS];
        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[CHARACTER_SETS]);
            addColumn(t, "CHARACTER_SET_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "CHARACTER_SET_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "CHARACTER_SET_NAME", SQL_IDENTIFIER);
            addColumn(t, "CHARACTER_REPERTOIRE", SQL_IDENTIFIER);
            addColumn(t, "FORM_OF_USE", SQL_IDENTIFIER);
            addColumn(t, "DEFAULT_COLLATE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "DEFAULT_COLLATE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "DEFAULT_COLLATE_NAME", SQL_IDENTIFIER);
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[CHARACTER_SETS].name, false,
                SchemaObject.INDEX);
            t.createPrimaryKeyConstraint(name, new int[] {
                0, 1, 2
            }, false);
            return t;
        }
        final int character_set_catalog   = 0;
        final int character_set_schema    = 1;
        final int character_set_name      = 2;
        final int character_repertoire    = 3;
        final int form_of_use             = 4;
        final int default_collate_catalog = 5;
        final int default_collate_schema  = 6;
        final int default_collate_name    = 7;
        Iterator it = database.schemaManager.databaseObjectIterator(
            SchemaObject.CHARSET);
        while (it.hasNext()) {
            Charset charset = (Charset) it.next();
            if (!session.getGrantee().isAccessible(charset)) {
                continue;
            }
            Object[] data = t.getEmptyRowData();
            data[character_set_catalog]   = database.getCatalogName().name;
            data[character_set_schema]    = charset.getSchemaName().name;
            data[character_set_name]      = charset.getName().name;
            data[character_repertoire]    = "UCS";
            data[form_of_use]             = "UTF16";
            data[default_collate_catalog] = data[character_set_catalog];
            if (charset.base == null) {
                data[default_collate_schema] = data[character_set_schema];
                data[default_collate_name]   = data[character_set_name];
            } else {
                data[default_collate_schema] = charset.base.schema.name;
                data[default_collate_name]   = charset.base.name;
            }
            t.insertSys(session, store, data);
        }
        return t;
    }
    Table CHECK_CONSTRAINT_ROUTINE_USAGE(Session session,
                                         PersistentStore store) {
        Table t = sysTables[CHECK_CONSTRAINT_ROUTINE_USAGE];
        if (t == null) {
            t = createBlankTable(
                sysTableHsqlNames[CHECK_CONSTRAINT_ROUTINE_USAGE]);
            addColumn(t, "CONSTRAINT_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "CONSTRAINT_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "CONSTRAINT_NAME", SQL_IDENTIFIER);    
            addColumn(t, "SPECIFIC_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "SPECIFIC_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "SPECIFIC_NAME", SQL_IDENTIFIER);      
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[CHECK_CONSTRAINT_ROUTINE_USAGE].name, false,
                SchemaObject.INDEX);
            t.createPrimaryKeyConstraint(name, new int[] {
                0, 1, 2, 3, 4, 5
            }, false);
            return t;
        }
        final int constraint_catalog = 0;
        final int constraint_schema  = 1;
        final int constraint_name    = 2;
        final int specific_catalog   = 3;
        final int specific_schema    = 4;
        final int specific_name      = 5;
        Iterator       constraints;
        Constraint     constraint;
        OrderedHashSet references;
        Object[]       row;
        constraints = database.schemaManager.databaseObjectIterator(
            SchemaObject.CONSTRAINT);
        while (constraints.hasNext()) {
            HsqlName constraintName = (HsqlName) constraints.next();
            if (constraintName.parent == null) {
                continue;
            }
            switch (constraintName.parent.type) {
                case SchemaObject.TABLE : {
                    Table table;
                    try {
                        table = (Table) database.schemaManager.getSchemaObject(
                            constraintName.parent.name,
                            constraintName.parent.schema.name,
                            SchemaObject.TABLE);
                    } catch (Exception e) {
                        continue;
                    }
                    constraint = table.getConstraint(constraintName.name);
                    if (constraint.getConstraintType()
                            != SchemaObject.ConstraintTypes.CHECK) {
                        continue;
                    }
                    break;
                }
                case SchemaObject.DOMAIN : {
                    Type domain;
                    try {
                        domain = (Type) database.schemaManager.getSchemaObject(
                            constraintName.parent.name,
                            constraintName.parent.schema.name,
                            SchemaObject.DOMAIN);
                    } catch (Exception e) {
                        continue;
                    }
                    constraint = domain.userTypeModifier.getConstraint(
                        constraintName.name);
                }
                default :
                    continue;
            }
            references = constraint.getReferences();
            for (int i = 0; i < references.size(); i++) {
                HsqlName name = (HsqlName) references.get(i);
                if (name.type != SchemaObject.SPECIFIC_ROUTINE) {
                    continue;
                }
                if (!session.getGrantee().isFullyAccessibleByRole(name)) {
                    continue;
                }
                row                     = t.getEmptyRowData();
                row[constraint_catalog] = database.getCatalogName().name;
                row[constraint_schema]  = constraint.getSchemaName().name;
                row[constraint_name]    = constraint.getName().name;
                row[specific_catalog]   = database.getCatalogName().name;
                row[specific_schema]    = name.schema.name;
                row[specific_name]      = name.name;
                t.insertSys(session, store, row);
            }
        }
        return t;
    }
    Table CHECK_CONSTRAINTS(Session session, PersistentStore store) {
        Table t = sysTables[CHECK_CONSTRAINTS];
        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[CHECK_CONSTRAINTS]);
            addColumn(t, "CONSTRAINT_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "CONSTRAINT_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "CONSTRAINT_NAME", SQL_IDENTIFIER);    
            addColumn(t, "CHECK_CLAUSE", CHARACTER_DATA);       
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[CHECK_CONSTRAINTS].name, false,
                SchemaObject.INDEX);
            t.createPrimaryKeyConstraint(name, new int[] {
                2, 1, 0
            }, false);
            return t;
        }
        final int constraint_catalog = 0;
        final int constraint_schema  = 1;
        final int constraint_name    = 2;
        final int check_clause       = 3;
        Iterator     tables;
        Table        table;
        Constraint[] tableConstraints;
        int          constraintCount;
        Constraint   constraint;
        Object[]     row;
        tables =
            database.schemaManager.databaseObjectIterator(SchemaObject.TABLE);
        while (tables.hasNext()) {
            table = (Table) tables.next();
            if (table.isView()
                    || !session.getGrantee().isFullyAccessibleByRole(
                        table.getName())) {
                continue;
            }
            tableConstraints = table.getConstraints();
            constraintCount  = tableConstraints.length;
            for (int i = 0; i < constraintCount; i++) {
                constraint = tableConstraints[i];
                if (constraint.getConstraintType()
                        != SchemaObject.ConstraintTypes.CHECK) {
                    continue;
                }
                row                     = t.getEmptyRowData();
                row[constraint_catalog] = database.getCatalogName().name;
                row[constraint_schema]  = table.getSchemaName().name;
                row[constraint_name]    = constraint.getName().name;
                try {
                    row[check_clause] = constraint.getCheckSQL();
                } catch (Exception e) {}
                t.insertSys(session, store, row);
            }
        }
        Iterator it =
            database.schemaManager.databaseObjectIterator(SchemaObject.DOMAIN);
        while (it.hasNext()) {
            Type domain = (Type) it.next();
            if (!domain.isDomainType()) {
                continue;
            }
            if (!session.getGrantee().isFullyAccessibleByRole(
                    domain.getName())) {
                continue;
            }
            tableConstraints = domain.userTypeModifier.getConstraints();
            constraintCount  = tableConstraints.length;
            for (int i = 0; i < constraintCount; i++) {
                constraint              = tableConstraints[i];
                row                     = t.getEmptyRowData();
                row[constraint_catalog] = database.getCatalogName().name;
                row[constraint_schema]  = domain.getSchemaName().name;
                row[constraint_name]    = constraint.getName().name;
                try {
                    row[check_clause] = constraint.getCheckSQL();
                } catch (Exception e) {}
                t.insertSys(session, store, row);
            }
        }
        return t;
    }
    Table COLLATIONS(Session session, PersistentStore store) {
        Table t = sysTables[COLLATIONS];
        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[COLLATIONS]);
            addColumn(t, "COLLATION_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "COLLATION_SCHEMA", SQL_IDENTIFIER);    
            addColumn(t, "COLLATION_NAME", SQL_IDENTIFIER);      
            addColumn(t, "PAD_ATTRIBUTE", CHARACTER_DATA);
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[COLLATIONS].name, false, SchemaObject.INDEX);
            t.createPrimaryKeyConstraint(name, new int[] {
                0, 1, 2
            }, false);
            return t;
        }
        final int collation_catalog = 0;
        final int collation_schema  = 1;
        final int collation_name    = 2;
        final int pad_attribute     = 3;
        Iterator collations;
        String   collation;
        String   collationSchema = SqlInvariants.PUBLIC_SCHEMA;
        String   padAttribute    = "NO PAD";
        Object[] row;
        collations = Collation.nameToJavaName.keySet().iterator();
        while (collations.hasNext()) {
            row                    = t.getEmptyRowData();
            collation              = (String) collations.next();
            row[collation_catalog] = database.getCatalogName().name;
            row[collation_schema]  = collationSchema;
            row[collation_name]    = collation;
            row[pad_attribute]     = padAttribute;
            t.insertSys(session, store, row);
        }
        return t;
    }
    Table COLUMN_COLUMN_USAGE(Session session, PersistentStore store) {
        Table t = sysTables[COLUMN_COLUMN_USAGE];
        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[COLUMN_COLUMN_USAGE]);
            addColumn(t, "TABLE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "TABLE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "TABLE_NAME", SQL_IDENTIFIER);
            addColumn(t, "COLUMN_NAME", SQL_IDENTIFIER);
            addColumn(t, "DEPENDENT_COLUMN", SQL_IDENTIFIER);
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[COLUMN_COLUMN_USAGE].name, false,
                SchemaObject.INDEX);
            t.createPrimaryKeyConstraint(name, new int[] {
                0, 1, 2, 3, 4
            }, false);
            return t;
        }
        final int table_catalog    = 0;
        final int table_schema     = 1;
        final int table_name       = 2;
        final int column_name      = 3;
        final int dependent_column = 4;
        Iterator tables;
        Table    table;
        Object[] row;
        tables =
            database.schemaManager.databaseObjectIterator(SchemaObject.TABLE);
        while (tables.hasNext()) {
            table = (Table) tables.next();
            if (table.isView()
                    || !session.getGrantee().isFullyAccessibleByRole(
                        table.getName())) {
                continue;
            }
            if (!table.hasGeneratedColumn()) {
                continue;
            }
            HsqlName name = table.getName();
            for (int i = 0; i < table.getColumnCount(); i++) {
                ColumnSchema column = table.getColumn(i);
                if (!column.isGenerated()) {
                    continue;
                }
                OrderedHashSet set = column.getGeneratedColumnReferences();
                if (set != null) {
                    for (int j = 0; j < set.size(); j++) {
                        row                   = t.getEmptyRowData();
                        row[table_catalog]    = database.getCatalogName().name;
                        row[table_schema]     = name.schema.name;
                        row[table_name]       = name.name;
                        row[column_name]      = ((HsqlName) set.get(j)).name;
                        row[dependent_column] = column.getName().name;
                        t.insertSys(session, store, row);
                    }
                }
            }
        }
        return t;
    }
    Table COLUMN_DOMAIN_USAGE(Session session, PersistentStore store) {
        Table t = sysTables[COLUMN_DOMAIN_USAGE];
        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[COLUMN_DOMAIN_USAGE]);
            addColumn(t, "DOMAIN_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "DOMAIN_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "DOMAIN_NAME", SQL_IDENTIFIER);
            addColumn(t, "TABLE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "TABLE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "TABLE_NAME", SQL_IDENTIFIER);     
            addColumn(t, "COLUMN_NAME", SQL_IDENTIFIER);    
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[COLUMN_DOMAIN_USAGE].name, false,
                SchemaObject.INDEX);
            t.createPrimaryKeyConstraint(name, new int[] {
                0, 1, 2, 3, 4, 5, 6
            }, false);
            return t;
        }
        final int domain_catalog = 0;
        final int domain_schema  = 1;
        final int domain_name    = 2;
        final int table_catalog  = 3;
        final int table_schema   = 4;
        final int table_name     = 5;
        final int column_name    = 6;
        int      columnCount;
        Iterator tables;
        Table    table;
        Object[] row;
        Type     type;
        HsqlName tableName;
        tables = allTables();
        Grantee grantee = session.getGrantee();
        while (tables.hasNext()) {
            table       = (Table) tables.next();
            columnCount = table.getColumnCount();
            tableName   = table.getName();
            for (int i = 0; i < columnCount; i++) {
                ColumnSchema column = table.getColumn(i);
                type = column.getDataType();
                if (!type.isDomainType()) {
                    continue;
                }
                if (!grantee.isFullyAccessibleByRole(type.getName())) {
                    continue;
                }
                row                 = t.getEmptyRowData();
                row[domain_catalog] = database.getCatalogName().name;
                row[domain_schema]  = type.getSchemaName().name;
                row[domain_name]    = type.getName().name;
                row[table_catalog]  = database.getCatalogName().name;
                row[table_schema]   = tableName.schema.name;
                row[table_name]     = tableName.name;
                row[column_name]    = column.getNameString();
                t.insertSys(session, store, row);
            }
        }
        return t;
    }
    Table COLUMN_UDT_USAGE(Session session, PersistentStore store) {
        Table t = sysTables[COLUMN_UDT_USAGE];
        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[COLUMN_UDT_USAGE]);
            addColumn(t, "UDT_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "UDT_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "UDT_NAME", SQL_IDENTIFIER);
            addColumn(t, "TABLE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "TABLE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "TABLE_NAME", SQL_IDENTIFIER);     
            addColumn(t, "COLUMN_NAME", SQL_IDENTIFIER);    
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[COLUMN_UDT_USAGE].name, false,
                SchemaObject.INDEX);
            t.createPrimaryKeyConstraint(name, new int[] {
                0, 1, 2, 3, 4, 5, 6
            }, false);
            return t;
        }
        final int udt_catalog   = 0;
        final int udt_schema    = 1;
        final int udt_name      = 2;
        final int table_catalog = 3;
        final int table_schema  = 4;
        final int table_name    = 5;
        final int column_name   = 6;
        int      columnCount;
        Iterator tables;
        Table    table;
        Object[] row;
        Type     type;
        HsqlName tableName;
        tables = allTables();
        Grantee grantee = session.getGrantee();
        while (tables.hasNext()) {
            table       = (Table) tables.next();
            columnCount = table.getColumnCount();
            tableName   = table.getName();
            for (int i = 0; i < columnCount; i++) {
                ColumnSchema column = table.getColumn(i);
                type = column.getDataType();
                if (!type.isDistinctType()) {
                    continue;
                }
                if (!grantee.isFullyAccessibleByRole(type.getName())) {
                    continue;
                }
                row                = t.getEmptyRowData();
                row[udt_catalog]   = database.getCatalogName().name;
                row[udt_schema]    = type.getSchemaName().name;
                row[udt_name]      = type.getName().name;
                row[table_catalog] = database.getCatalogName().name;
                row[table_schema]  = tableName.schema.name;
                row[table_name]    = tableName.name;
                row[column_name]   = column.getNameString();
                t.insertSys(session, store, row);
            }
        }
        return t;
    }
    Table COLUMNS(Session session, PersistentStore store) {
        Table t = sysTables[COLUMNS];
        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[COLUMNS]);
            addColumn(t, "TABLE_CATALOG", SQL_IDENTIFIER);           
            addColumn(t, "TABLE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "TABLE_NAME", SQL_IDENTIFIER);
            addColumn(t, "COLUMN_NAME", SQL_IDENTIFIER);
            addColumn(t, "ORDINAL_POSITION", CARDINAL_NUMBER);
            addColumn(t, "COLUMN_DEFAULT", CHARACTER_DATA);
            addColumn(t, "IS_NULLABLE", YES_OR_NO);
            addColumn(t, "DATA_TYPE", CHARACTER_DATA);
            addColumn(t, "CHARACTER_MAXIMUM_LENGTH", CARDINAL_NUMBER);
            addColumn(t, "CHARACTER_OCTET_LENGTH", CARDINAL_NUMBER);
            addColumn(t, "NUMERIC_PRECISION", CARDINAL_NUMBER);      
            addColumn(t, "NUMERIC_PRECISION_RADIX", CARDINAL_NUMBER);
            addColumn(t, "NUMERIC_SCALE", CARDINAL_NUMBER);
            addColumn(t, "DATETIME_PRECISION", CARDINAL_NUMBER);
            addColumn(t, "INTERVAL_TYPE", CHARACTER_DATA);
            addColumn(t, "INTERVAL_PRECISION", CARDINAL_NUMBER);
            addColumn(t, "CHARACTER_SET_CATALOG", CHARACTER_DATA);
            addColumn(t, "CHARACTER_SET_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "CHARACTER_SET_NAME", SQL_IDENTIFIER);
            addColumn(t, "COLLATION_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "COLLATION_SCHEMA", SQL_IDENTIFIER);        
            addColumn(t, "COLLATION_NAME", SQL_IDENTIFIER);
            addColumn(t, "DOMAIN_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "DOMAIN_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "DOMAIN_NAME", SQL_IDENTIFIER);
            addColumn(t, "UDT_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "UDT_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "UDT_NAME", SQL_IDENTIFIER);
            addColumn(t, "SCOPE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "SCOPE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "SCOPE_NAME", SQL_IDENTIFIER);              
            addColumn(t, "MAXIMUM_CARDINALITY", CARDINAL_NUMBER);    
            addColumn(t, "DTD_IDENTIFIER", SQL_IDENTIFIER);
            addColumn(t, "IS_SELF_REFERENCING", YES_OR_NO);
            addColumn(t, "IS_IDENTITY", YES_OR_NO);
            addColumn(t, "IDENTITY_GENERATION", CHARACTER_DATA);     
            addColumn(t, "IDENTITY_START", CHARACTER_DATA);
            addColumn(t, "IDENTITY_INCREMENT", CHARACTER_DATA);
            addColumn(t, "IDENTITY_MAXIMUM", CHARACTER_DATA);
            addColumn(t, "IDENTITY_MINIMUM", CHARACTER_DATA);
            addColumn(t, "IDENTITY_CYCLE", YES_OR_NO);               
            addColumn(t, "IS_GENERATED", CHARACTER_DATA);            
            addColumn(t, "GENERATION_EXPRESSION", CHARACTER_DATA);
            addColumn(t, "IS_UPDATABLE", YES_OR_NO);
            addColumn(t, "DECLARED_DATA_TYPE", CHARACTER_DATA);
            addColumn(t, "DECLARED_NUMERIC_PRECISION", CARDINAL_NUMBER);
            addColumn(t, "DECLARED_NUMERIC_SCALE", CARDINAL_NUMBER);
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[COLUMNS].name, false, SchemaObject.INDEX);
            t.createPrimaryKeyConstraint(name, new int[] {
                3, 2, 1, 4
            }, false);
            return t;
        }
        final int table_cat                  = 0;
        final int table_schem                = 1;
        final int table_name                 = 2;
        final int column_name                = 3;
        final int ordinal_position           = 4;
        final int column_default             = 5;
        final int is_nullable                = 6;
        final int data_type                  = 7;
        final int character_maximum_length   = 8;
        final int character_octet_length     = 9;
        final int numeric_precision          = 10;
        final int numeric_precision_radix    = 11;
        final int numeric_scale              = 12;
        final int datetime_precision         = 13;
        final int interval_type              = 14;
        final int interval_precision         = 15;
        final int character_set_catalog      = 16;
        final int character_set_schema       = 17;
        final int character_set_name         = 18;
        final int collation_catalog          = 19;
        final int collation_schema           = 20;
        final int collation_name             = 21;
        final int domain_catalog             = 22;
        final int domain_schema              = 23;
        final int domain_name                = 24;
        final int udt_catalog                = 25;
        final int udt_schema                 = 26;
        final int udt_name                   = 27;
        final int scope_catalog              = 28;
        final int scope_schema               = 29;
        final int scope_name                 = 30;
        final int maximum_cardinality        = 31;
        final int dtd_identifier             = 32;
        final int is_self_referencing        = 33;
        final int is_identity                = 34;
        final int identity_generation        = 35;
        final int identity_start             = 36;
        final int identity_increment         = 37;
        final int identity_maximum           = 38;
        final int identity_minimum           = 39;
        final int identity_cycle             = 40;
        final int is_generated               = 41;
        final int generation_expression      = 42;
        final int is_updatable               = 43;
        final int declared_data_type         = 44;
        final int declared_numeric_precision = 45;
        final int declared_numeric_scale     = 46;
        int            columnCount;
        Iterator       tables;
        Table          table;
        Object[]       row;
        OrderedHashSet columnList;
        Type           type;
        tables = allTables();
        while (tables.hasNext()) {
            table = (Table) tables.next();
            columnList =
                session.getGrantee().getColumnsForAllPrivileges(table);
            if (columnList.isEmpty()) {
                continue;
            }
            columnCount = table.getColumnCount();
            for (int i = 0; i < columnCount; i++) {
                ColumnSchema column = table.getColumn(i);
                type = column.getDataType();
                if (!columnList.contains(column.getName())) {
                    continue;
                }
                row                   = t.getEmptyRowData();
                row[table_cat]        = database.getCatalogName().name;
                row[table_schem]      = table.getSchemaName().name;
                row[table_name]       = table.getName().name;
                row[column_name]      = column.getName().name;
                row[ordinal_position] = ValuePool.getLong(i + 1);
                row[column_default]   = column.getDefaultSQL();
                row[is_nullable]      = column.isNullable() ? "YES"
                                                            : "NO";
                row[data_type]        = type.getFullNameString();
                if (type.isCharacterType()) {
                    row[character_maximum_length] =
                        ValuePool.getLong(type.precision);
                    row[character_octet_length] =
                        ValuePool.getLong(type.precision * 2);
                    row[character_set_catalog] =
                        database.getCatalogName().name;
                    row[character_set_schema] =
                        ((CharacterType) type).getCharacterSet()
                            .getSchemaName().name;
                    row[character_set_name] =
                        ((CharacterType) type).getCharacterSet().getName()
                            .name;
                    row[collation_catalog] = database.getCatalogName().name;
                    row[collation_schema] =
                        ((CharacterType) type).getCollation().getSchemaName()
                            .name;
                    row[collation_name] =
                        ((CharacterType) type).getCollation().getName().name;
                } else if (type.isNumberType()) {
                    row[numeric_precision] = ValuePool.getLong(
                        ((NumberType) type).getNumericPrecisionInRadix());
                    row[declared_numeric_precision] = ValuePool.getLong(
                        ((NumberType) type).getNumericPrecisionInRadix());
                    if (type.isExactNumberType()) {
                        row[numeric_scale] = row[declared_numeric_scale] =
                            ValuePool.getLong(type.scale);
                    }
                    row[numeric_precision_radix] =
                        ValuePool.getLong(type.getPrecisionRadix());
                } else if (type.isBooleanType()) {
                } else if (type.isDateTimeType()) {
                    row[datetime_precision] = ValuePool.getLong(type.scale);
                } else if (type.isIntervalType()) {
                    row[data_type] = "INTERVAL";
                    row[interval_type] =
                        ((IntervalType) type).getQualifier(type.typeCode);
                    row[interval_precision] =
                        ValuePool.getLong(type.precision);
                    row[datetime_precision] = ValuePool.getLong(type.scale);
                } else if (type.isBinaryType()) {
                    row[character_maximum_length] =
                        ValuePool.getLong(type.precision);
                    row[character_octet_length] =
                        ValuePool.getLong(type.precision);
                } else if (type.isBitType()) {
                    row[character_maximum_length] =
                        ValuePool.getLong(type.precision);
                    row[character_octet_length] =
                        ValuePool.getLong(type.precision);
                } else if (type.isArrayType()) {
                    row[maximum_cardinality] =
                        ValuePool.getLong(type.arrayLimitCardinality());
                    row[data_type] = "ARRAY";
                }
                if (type.isDomainType()) {
                    row[domain_catalog] = database.getCatalogName().name;
                    row[domain_schema]  = type.getSchemaName().name;
                    row[domain_name]    = type.getName().name;
                }
                if (type.isDistinctType()) {
                    row[udt_catalog] = database.getCatalogName().name;
                    row[udt_schema]  = type.getSchemaName().name;
                    row[udt_name]    = type.getName().name;
                }
                row[scope_catalog]       = null;
                row[scope_schema]        = null;
                row[scope_name]          = null;
                row[dtd_identifier]      = type.getDefinition();
                row[is_self_referencing] = null;
                row[is_identity]         = column.isIdentity() ? "YES"
                                                               : "NO";
                if (column.isIdentity()) {
                    NumberSequence sequence = column.getIdentitySequence();
                    row[identity_generation] = sequence.isAlways() ? "ALWAYS"
                                                                   : "BY DEFAULT";
                    row[identity_start] =
                        Long.toString(sequence.getStartValue());
                    row[identity_increment] =
                        Long.toString(sequence.getIncrement());
                    row[identity_maximum] =
                        Long.toString(sequence.getMaxValue());
                    row[identity_minimum] =
                        Long.toString(sequence.getMinValue());
                    row[identity_cycle] = sequence.isCycle() ? "YES"
                                                             : "NO";
                }
                row[is_generated] = "NEVER";
                if (column.isGenerated()) {
                    row[is_generated] = "ALWAYS";
                    row[generation_expression] =
                        column.getGeneratingExpression().getSQL();
                }
                row[is_updatable]       = table.isWritable() ? "YES"
                                                             : "NO";
                row[declared_data_type] = row[data_type];
                if (type.isNumberType()) {
                    row[declared_numeric_precision] = row[numeric_precision];
                    row[declared_numeric_scale]     = row[numeric_scale];
                }
                t.insertSys(session, store, row);
            }
        }
        return t;
    }
    Table CONSTRAINT_COLUMN_USAGE(Session session, PersistentStore store) {
        Table t = sysTables[CONSTRAINT_COLUMN_USAGE];
        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[CONSTRAINT_COLUMN_USAGE]);
            addColumn(t, "TABLE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "TABLE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "TABLE_NAME", SQL_IDENTIFIER);         
            addColumn(t, "COLUMN_NAME", SQL_IDENTIFIER);        
            addColumn(t, "CONSTRAINT_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "CONSTRAINT_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "CONSTRAINT_NAME", SQL_IDENTIFIER);    
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[CONSTRAINT_COLUMN_USAGE].name, false,
                SchemaObject.INDEX);
            t.createPrimaryKeyConstraint(name, new int[] {
                0, 1, 2, 3, 4, 5, 6
            }, false);
            return t;
        }
        final int table_catalog      = 0;
        final int table_schema       = 1;
        final int table_name         = 2;
        final int column_name        = 3;
        final int constraint_catalog = 4;
        final int constraint_schema  = 5;
        final int constraint_name    = 6;
        String constraintCatalog;
        String constraintSchema;
        String constraintName;
        Iterator     tables;
        Table        table;
        Constraint[] constraints;
        int          constraintCount;
        Constraint   constraint;
        Iterator     iterator;
        Object[]     row;
        tables =
            database.schemaManager.databaseObjectIterator(SchemaObject.TABLE);
        while (tables.hasNext()) {
            table = (Table) tables.next();
            if (table.isView()
                    || !session.getGrantee().isFullyAccessibleByRole(
                        table.getName())) {
                continue;
            }
            constraints       = table.getConstraints();
            constraintCount   = constraints.length;
            constraintCatalog = database.getCatalogName().name;
            constraintSchema  = table.getSchemaName().name;
            for (int i = 0; i < constraintCount; i++) {
                constraint     = constraints[i];
                constraintName = constraint.getName().name;
                switch (constraint.getConstraintType()) {
                    case SchemaObject.ConstraintTypes.CHECK : {
                        OrderedHashSet expressions =
                            constraint.getCheckColumnExpressions();
                        if (expressions == null) {
                            break;
                        }
                        iterator = expressions.iterator();
                        while (iterator.hasNext()) {
                            ExpressionColumn expr =
                                (ExpressionColumn) iterator.next();
                            HsqlName name = expr.getBaseColumnHsqlName();
                            if (name.type != SchemaObject.COLUMN) {
                                continue;
                            }
                            row = t.getEmptyRowData();
                            row[table_catalog] =
                                database.getCatalogName().name;
                            row[table_schema]       = name.schema.name;
                            row[table_name]         = name.parent.name;
                            row[column_name]        = name.name;
                            row[constraint_catalog] = constraintCatalog;
                            row[constraint_schema]  = constraintSchema;
                            row[constraint_name]    = constraintName;
                            try {
                                t.insertSys(session, store, row);
                            } catch (HsqlException e) {}
                        }
                        break;
                    }
                    case SchemaObject.ConstraintTypes.UNIQUE :
                    case SchemaObject.ConstraintTypes.PRIMARY_KEY :
                    case SchemaObject.ConstraintTypes.FOREIGN_KEY : {
                        Table target = table;
                        int[] cols   = constraint.getMainColumns();
                        if (constraint.getConstraintType()
                                == SchemaObject.ConstraintTypes.FOREIGN_KEY) {
                            cols = constraint.getRefColumns();
                        }
                        for (int j = 0; j < cols.length; j++) {
                            row = t.getEmptyRowData();
                            row[table_catalog] =
                                database.getCatalogName().name;
                            row[table_schema] = constraintSchema;
                            row[table_name]   = target.getName().name;
                            row[column_name] =
                                target.getColumn(cols[j]).getName().name;
                            row[constraint_catalog] = constraintCatalog;
                            row[constraint_schema]  = constraintSchema;
                            row[constraint_name]    = constraintName;
                            try {
                                t.insertSys(session, store, row);
                            } catch (HsqlException e) {}
                        }
                    }
                }
            }
        }
        return t;
    }
    Table CONSTRAINT_TABLE_USAGE(Session session, PersistentStore store) {
        Table t = sysTables[CONSTRAINT_TABLE_USAGE];
        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[CONSTRAINT_TABLE_USAGE]);
            addColumn(t, "TABLE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "TABLE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "TABLE_NAME", SQL_IDENTIFIER);         
            addColumn(t, "CONSTRAINT_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "CONSTRAINT_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "CONSTRAINT_NAME", SQL_IDENTIFIER);    
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[CONSTRAINT_TABLE_USAGE].name, false,
                SchemaObject.INDEX);
            t.createPrimaryKeyConstraint(name, new int[] {
                0, 1, 2, 3, 4, 5
            }, false);
            return t;
        }
        Session sys = database.sessionManager.newSysSession(
            SqlInvariants.INFORMATION_SCHEMA_HSQLNAME, session.getUser());
        Result rs = sys.executeDirectStatement(
            "select DISTINCT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, "
            + "CONSTRAINT_CATALOG, CONSTRAINT_SCHEMA, CONSTRAINT_NAME "
            + "from INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE");
        t.insertSys(session, store, rs);
        sys.close();
        return t;
    }
    Table DATA_TYPE_PRIVILEGES(Session session, PersistentStore store) {
        Table t = sysTables[DATA_TYPE_PRIVILEGES];
        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[DATA_TYPE_PRIVILEGES]);
            addColumn(t, "OBJECT_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "OBJECT_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "OBJECT_NAME", SQL_IDENTIFIER);    
            addColumn(t, "OBJECT_TYPE", SQL_IDENTIFIER);
            addColumn(t, "DTD_IDENTIFIER", SQL_IDENTIFIER);
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[DATA_TYPE_PRIVILEGES].name, false,
                SchemaObject.INDEX);
            t.createPrimaryKeyConstraint(name, new int[] {
                0, 1, 2, 3, 4
            }, false);
            return t;
        }
        Session sys = database.sessionManager.newSysSession(
            SqlInvariants.INFORMATION_SCHEMA_HSQLNAME, session.getUser());
        String sql = (String) statementMap.get("/*data_type_privileges*/");
        Result rs  = sys.executeDirectStatement(sql);
        t.insertSys(session, store, rs);
        sys.close();
        return t;
    }
    Table DOMAIN_CONSTRAINTS(Session session, PersistentStore store) {
        Table t = sysTables[DOMAIN_CONSTRAINTS];
        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[DOMAIN_CONSTRAINTS]);
            addColumn(t, "CONSTRAINT_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "CONSTRAINT_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "CONSTRAINT_NAME", SQL_IDENTIFIER);    
            addColumn(t, "DOMAIN_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "DOMAIN_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "DOMAIN_NAME", SQL_IDENTIFIER);
            addColumn(t, "IS_DEFERRABLE", YES_OR_NO);
            addColumn(t, "INITIALLY_DEFERRED", YES_OR_NO);
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[DOMAIN_CONSTRAINTS].name, false,
                SchemaObject.INDEX);
            t.createPrimaryKeyConstraint(name, new int[] {
                0, 1, 2, 4, 5, 6
            }, false);
            return t;
        }
        final int constraint_catalog = 0;
        final int constraint_schema  = 1;
        final int constraint_name    = 2;
        final int domain_catalog     = 3;
        final int domain_schema      = 4;
        final int domain_name        = 5;
        final int is_deferrable      = 6;
        final int initially_deferred = 7;
        Iterator it =
            database.schemaManager.databaseObjectIterator(SchemaObject.DOMAIN);
        while (it.hasNext()) {
            Type domain = (Type) it.next();
            if (!domain.isDomainType()) {
                continue;
            }
            if (!session.getGrantee().isFullyAccessibleByRole(
                    domain.getName())) {
                continue;
            }
            Constraint[] constraints =
                domain.userTypeModifier.getConstraints();
            for (int i = 0; i < constraints.length; i++) {
                Object[] data = t.getEmptyRowData();
                data[constraint_catalog] = data[domain_catalog] =
                    database.getCatalogName().name;
                data[constraint_schema] = data[domain_schema] =
                    domain.getSchemaName().name;
                data[constraint_name]    = constraints[i].getName().name;
                data[domain_name]        = domain.getName().name;
                data[is_deferrable]      = Tokens.T_NO;
                data[initially_deferred] = Tokens.T_NO;
                t.insertSys(session, store, data);
            }
        }
        return t;
    }
    Table DOMAINS(Session session, PersistentStore store) {
        Table t = sysTables[DOMAINS];
        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[DOMAINS]);
            addColumn(t, "DOMAIN_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "DOMAIN_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "DOMAIN_NAME", SQL_IDENTIFIER);
            addColumn(t, "DATA_TYPE", SQL_IDENTIFIER);
            addColumn(t, "CHARACTER_MAXIMUM_LENGTH", CARDINAL_NUMBER);
            addColumn(t, "CHARACTER_OCTET_LENGTH", CARDINAL_NUMBER);
            addColumn(t, "CHARACTER_SET_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "CHARACTER_SET_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "CHARACTER_SET_NAME", SQL_IDENTIFIER);
            addColumn(t, "COLLATION_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "COLLATION_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "COLLATION_NAME", SQL_IDENTIFIER);
            addColumn(t, "NUMERIC_PRECISION", CARDINAL_NUMBER);
            addColumn(t, "NUMERIC_PRECISION_RADIX", CARDINAL_NUMBER);
            addColumn(t, "NUMERIC_SCALE", CARDINAL_NUMBER);
            addColumn(t, "DATETIME_PRECISION", CARDINAL_NUMBER);
            addColumn(t, "INTERVAL_TYPE", CHARACTER_DATA);
            addColumn(t, "INTERVAL_PRECISION", CARDINAL_NUMBER);
            addColumn(t, "DOMAIN_DEFAULT", CHARACTER_DATA);
            addColumn(t, "MAXIMUM_CARDINALITY", CARDINAL_NUMBER);
            addColumn(t, "DTD_IDENTIFIER", SQL_IDENTIFIER);
            addColumn(t, "DECLARED_DATA_TYPE", CHARACTER_DATA);
            addColumn(t, "DECLARED_NUMERIC_PRECISION", CARDINAL_NUMBER);
            addColumn(t, "DECLARED_NUMERIC_SCALE", CARDINAL_NUMBER);
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[DOMAINS].name, false, SchemaObject.INDEX);
            t.createPrimaryKeyConstraint(name, new int[] {
                0, 1, 2, 4, 5, 6
            }, false);
            return t;
        }
        final int domain_catalog             = 0;
        final int domain_schema              = 1;
        final int domain_name                = 2;
        final int data_type                  = 3;
        final int character_maximum_length   = 4;
        final int character_octet_length     = 5;
        final int character_set_catalog      = 6;
        final int character_set_schema       = 7;
        final int character_set_name         = 8;
        final int collation_catalog          = 9;
        final int collation_schema           = 10;
        final int collation_name             = 11;
        final int numeric_precision          = 12;
        final int numeric_precision_radix    = 13;
        final int numeric_scale              = 14;
        final int datetime_precision         = 15;
        final int interval_type              = 16;
        final int interval_precision         = 17;
        final int domain_default             = 18;
        final int maximum_cardinality        = 19;
        final int dtd_identifier             = 20;
        final int declared_data_type         = 21;
        final int declared_numeric_precision = 22;
        final int declared_numeric_scale     = 23;
        Iterator it =
            database.schemaManager.databaseObjectIterator(SchemaObject.DOMAIN);
        while (it.hasNext()) {
            Type type = (Type) it.next();
            if (!type.isDomainType()) {
                continue;
            }
            if (!session.getGrantee().isAccessible(type)) {
                continue;
            }
            Object[] row = t.getEmptyRowData();
            row[domain_catalog] = database.getCatalogName().name;
            row[domain_schema]  = type.getSchemaName().name;
            row[domain_name]    = type.getName().name;
            row[data_type]      = type.getFullNameString();
            if (type.isCharacterType()) {
                row[character_maximum_length] =
                    ValuePool.getLong(type.precision);
                row[character_octet_length] = ValuePool.getLong(type.precision
                        * 2);
                row[character_set_catalog] = database.getCatalogName().name;
                row[character_set_schema] =
                    ((CharacterType) type).getCharacterSet().getSchemaName()
                        .name;
                row[character_set_name] =
                    ((CharacterType) type).getCharacterSet().getName().name;
                row[collation_catalog] = database.getCatalogName().name;
                row[collation_schema] =
                    ((CharacterType) type).getCollation().getSchemaName().name;
                row[collation_name] =
                    ((CharacterType) type).getCollation().getName().name;
            } else if (type.isNumberType()) {
                row[numeric_precision] = ValuePool.getLong(
                    ((NumberType) type).getNumericPrecisionInRadix());
                row[declared_numeric_precision] = ValuePool.getLong(
                    ((NumberType) type).getNumericPrecisionInRadix());
                if (type.isExactNumberType()) {
                    row[numeric_scale] = row[declared_numeric_scale] =
                        ValuePool.getLong(type.scale);
                }
                row[numeric_precision_radix] =
                    ValuePool.getLong(type.getPrecisionRadix());
            } else if (type.isBooleanType()) {
            } else if (type.isDateTimeType()) {
                row[datetime_precision] = ValuePool.getLong(type.scale);
            } else if (type.isIntervalType()) {
                row[data_type] = "INTERVAL";
                row[interval_type] =
                    ((IntervalType) type).getQualifier(type.typeCode);
                row[interval_precision] = ValuePool.getLong(type.precision);
                row[datetime_precision] = ValuePool.getLong(type.scale);
            } else if (type.isBinaryType()) {
                row[character_maximum_length] =
                    ValuePool.getLong(type.precision);
                row[character_octet_length] =
                    ValuePool.getLong(type.precision);
            } else if (type.isBitType()) {
                row[character_maximum_length] =
                    ValuePool.getLong(type.precision);
                row[character_octet_length] =
                    ValuePool.getLong(type.precision);
            } else if (type.isArrayType()) {
                row[maximum_cardinality] =
                    ValuePool.getLong(type.arrayLimitCardinality());
                row[data_type] = "ARRAY";
            }
            row[dtd_identifier]     = type.getDefinition();
            row[declared_data_type] = row[data_type];
            Expression defaultExpression =
                type.userTypeModifier.getDefaultClause();
            if (defaultExpression != null) {
                row[domain_default] = defaultExpression.getSQL();
            }
            t.insertSys(session, store, row);
        }
        return t;
    }
    Table ELEMENT_TYPES(Session session, PersistentStore store) {
        Table t = sysTables[ELEMENT_TYPES];
        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[ELEMENT_TYPES]);
            addColumn(t, "OBJECT_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "OBJECT_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "OBJECT_NAME", SQL_IDENTIFIER);
            addColumn(t, "OBJECT_TYPE", SQL_IDENTIFIER);
            addColumn(t, "COLLECTION_TYPE_IDENTIFIER", SQL_IDENTIFIER);
            addColumn(t, "DATA_TYPE", SQL_IDENTIFIER);
            addColumn(t, "CHARACTER_MAXIMUM_LENGTH", CARDINAL_NUMBER);
            addColumn(t, "CHARACTER_OCTET_LENGTH", CARDINAL_NUMBER);
            addColumn(t, "CHARACTER_SET_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "CHARACTER_SET_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "CHARACTER_SET_NAME", SQL_IDENTIFIER);
            addColumn(t, "COLLATION_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "COLLATION_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "COLLATION_NAME", SQL_IDENTIFIER);
            addColumn(t, "NUMERIC_PRECISION", CARDINAL_NUMBER);
            addColumn(t, "NUMERIC_PRECISION_RADIX", CARDINAL_NUMBER);
            addColumn(t, "NUMERIC_SCALE", CARDINAL_NUMBER);
            addColumn(t, "DATETIME_PRECISION", CARDINAL_NUMBER);
            addColumn(t, "INTERVAL_TYPE", CHARACTER_DATA);
            addColumn(t, "INTERVAL_PRECISION", CARDINAL_NUMBER);
            addColumn(t, "UDT_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "UDT_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "UDT_NAME", SQL_IDENTIFIER);
            addColumn(t, "SCOPE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "SCOPE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "SCOPE_NAME", SQL_IDENTIFIER);
            addColumn(t, "MAXIMUM_CARDINALITY", CARDINAL_NUMBER);
            addColumn(t, "DTD_IDENTIFIER", SQL_IDENTIFIER);
            addColumn(t, "DECLARED_DATA_TYPE", CHARACTER_DATA);
            addColumn(t, "DECLARED_NUMERIC_PRECISION", CARDINAL_NUMBER);
            addColumn(t, "DECLARED_NUMERIC_SCALE", CARDINAL_NUMBER);
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[ELEMENT_TYPES].name, false,
                SchemaObject.INDEX);
            t.createPrimaryKeyConstraint(name, new int[] {
                0, 1, 2, 4, 5, 27
            }, true);
            return t;
        }
        final int object_catalog             = 0;
        final int object_schema              = 1;
        final int object_name                = 2;
        final int object_type                = 3;
        final int collection_type_identifier = 4;
        final int udt_catalog   = 20;
        final int udt_schema    = 21;
        final int udt_name      = 22;
        final int scope_catalog = 23;
        final int scope_schema  = 24;
        final int scope_name    = 25;
        int            columnCount;
        Iterator       tables;
        Table          table;
        Object[]       row;
        OrderedHashSet columnList;
        Type           type;
        tables = allTables();
        while (tables.hasNext()) {
            table = (Table) tables.next();
            columnList =
                session.getGrantee().getColumnsForAllPrivileges(table);
            if (columnList.isEmpty()) {
                continue;
            }
            columnCount = table.getColumnCount();
            for (int i = 0; i < columnCount; i++) {
                ColumnSchema column = table.getColumn(i);
                if (!columnList.contains(column.getName())) {
                    continue;
                }
                type = column.getDataType();
                if (type.isDistinctType() || type.isDomainType()
                        || !type.isArrayType()) {
                    continue;
                }
                row                             = t.getEmptyRowData();
                row[object_catalog] = database.getCatalogName().name;
                row[object_schema]              = table.getSchemaName().name;
                row[object_name]                = table.getName().name;
                row[object_type]                = "TABLE";
                row[collection_type_identifier] = type.getDefinition();
                addTypeInfo(row, ((ArrayType) type).collectionBaseType());
                try {
                    t.insertSys(session, store, row);
                } catch (HsqlException e) {}
            }
        }
        Iterator it =
            database.schemaManager.databaseObjectIterator(SchemaObject.DOMAIN);
        while (it.hasNext()) {
            type = (Type) it.next();
            if (!type.isDomainType() || !type.isArrayType()) {
                continue;
            }
            if (!session.getGrantee().isAccessible(type)) {
                continue;
            }
            row                             = t.getEmptyRowData();
            row[object_catalog]             = database.getCatalogName().name;
            row[object_schema]              = type.getSchemaName().name;
            row[object_name]                = type.getName().name;
            row[object_type]                = "DOMAIN";
            row[collection_type_identifier] = type.getDefinition();
            addTypeInfo(row, ((ArrayType) type).collectionBaseType());
            t.insertSys(session, store, row);
        }
        it = database.schemaManager.databaseObjectIterator(SchemaObject.TYPE);
        while (it.hasNext()) {
            type = (Type) it.next();
            if (!type.isDistinctType() || !type.isArrayType()) {
                continue;
            }
            if (!session.getGrantee().isAccessible(type)) {
                continue;
            }
            row                             = t.getEmptyRowData();
            row[object_catalog]             = database.getCatalogName().name;
            row[object_schema]              = type.getSchemaName().name;
            row[object_name]                = type.getName().name;
            row[object_type]                = "USER-DEFINED TYPE";
            row[collection_type_identifier] = type.getDefinition();
            addTypeInfo(row, ((ArrayType) type).collectionBaseType());
            try {
                t.insertSys(session, store, row);
            } catch (HsqlException e) {}
        }
        it = database.schemaManager.databaseObjectIterator(
            SchemaObject.SPECIFIC_ROUTINE);
        while (it.hasNext()) {
            Routine routine = (Routine) it.next();
            if (!session.getGrantee().isAccessible(routine)) {
                continue;
            }
            type = routine.isProcedure() ? null
                                         : routine.getReturnType();
            if (type == null || type.isDistinctType() || type.isDomainType()
                    || !type.isArrayType()) {
            } else {
                row                             = t.getEmptyRowData();
                row[object_catalog] = database.getCatalogName().name;
                row[object_schema]              = routine.getSchemaName().name;
                row[object_name]                = routine.getName().name;
                row[object_type]                = "ROUTINE";
                row[collection_type_identifier] = type.getDefinition();
                addTypeInfo(row, ((ArrayType) type).collectionBaseType());
                try {
                    t.insertSys(session, store, row);
                } catch (HsqlException e) {}
            }
            Type returnType = type;
            int  paramCount = routine.getParameterCount();
            for (int i = 0; i < paramCount; i++) {
                ColumnSchema param = routine.getParameter(i);
                type = param.getDataType();
                if (type.isDistinctType() || type.isDomainType()
                        || !type.isArrayType()) {
                    continue;
                }
                if (type.equals(returnType)) {
                    continue;
                }
                row                             = t.getEmptyRowData();
                row[object_catalog] = database.getCatalogName().name;
                row[object_schema]              = routine.getSchemaName().name;
                row[object_name]                = routine.getName().name;
                row[object_type]                = "ROUTINE";
                row[collection_type_identifier] = type.getDefinition();
                addTypeInfo(row, ((ArrayType) type).collectionBaseType());
                try {
                    t.insertSys(session, store, row);
                } catch (HsqlException e) {}
            }
        }
        return t;
    }
    void addTypeInfo(Object[] row, Type type) {
        final int data_type                = 5;
        final int character_maximum_length = 6;
        final int character_octet_length   = 7;
        final int character_set_catalog    = 8;
        final int character_set_schema     = 9;
        final int character_set_name       = 10;
        final int collation_catalog        = 11;
        final int collation_schema         = 12;
        final int collation_name           = 13;
        final int numeric_precision        = 14;
        final int numeric_precision_radix  = 15;
        final int numeric_scale            = 16;
        final int datetime_precision       = 17;
        final int interval_type            = 18;
        final int interval_precision       = 19;
        final int maximum_cardinality        = 26;
        final int dtd_identifier             = 27;
        final int declared_data_type         = 28;
        final int declared_numeric_precision = 29;
        final int declared_numeric_scale     = 30;
        row[data_type] = type.getFullNameString();
        if (type.isCharacterType()) {
            row[character_maximum_length] = ValuePool.getLong(type.precision);
            row[character_octet_length] = ValuePool.getLong(type.precision
                    * 2);
            row[character_set_catalog] = database.getCatalogName().name;
            row[character_set_schema] =
                ((CharacterType) type).getCharacterSet().getSchemaName().name;
            row[character_set_name] =
                ((CharacterType) type).getCharacterSet().getName().name;
            row[collation_catalog] = database.getCatalogName().name;
            row[collation_schema] =
                ((CharacterType) type).getCollation().getSchemaName().name;
            row[collation_name] =
                ((CharacterType) type).getCollation().getName().name;
        } else if (type.isNumberType()) {
            row[numeric_precision] = ValuePool.getLong(
                ((NumberType) type).getNumericPrecisionInRadix());
            row[declared_numeric_precision] = ValuePool.getLong(
                ((NumberType) type).getNumericPrecisionInRadix());
            if (type.isExactNumberType()) {
                row[numeric_scale] = row[declared_numeric_scale] =
                    ValuePool.getLong(type.scale);
            }
            row[numeric_precision_radix] =
                ValuePool.getLong(type.getPrecisionRadix());
        } else if (type.isBooleanType()) {
        } else if (type.isDateTimeType()) {
            row[datetime_precision] = ValuePool.getLong(type.scale);
        } else if (type.isIntervalType()) {
            row[data_type] = "INTERVAL";
            row[interval_type] =
                ((IntervalType) type).getQualifier(type.typeCode);
            row[interval_precision] = ValuePool.getLong(type.precision);
            row[datetime_precision] = ValuePool.getLong(type.scale);
        } else if (type.isBinaryType()) {
            row[character_maximum_length] = ValuePool.getLong(type.precision);
            row[character_octet_length]   = ValuePool.getLong(type.precision);
        } else if (type.isBitType()) {
            row[character_maximum_length] = ValuePool.getLong(type.precision);
            row[character_octet_length]   = ValuePool.getLong(type.precision);
        } else if (type.isArrayType()) {
            row[maximum_cardinality] =
                ValuePool.getLong(type.arrayLimitCardinality());
        }
        row[dtd_identifier]     = type.getDefinition();
        row[declared_data_type] = row[data_type];
    }
    Table ENABLED_ROLES(Session session, PersistentStore store) {
        Table t = sysTables[ENABLED_ROLES];
        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[ENABLED_ROLES]);
            addColumn(t, "ROLE_NAME", SQL_IDENTIFIER);
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[ENABLED_ROLES].name, false,
                SchemaObject.INDEX);
            t.createPrimaryKeyConstraint(name, new int[]{ 0 }, true);
            return t;
        }
        Iterator grantees;
        Grantee  grantee;
        Object[] row;
        grantees = session.getGrantee().getAllRoles().iterator();
        while (grantees.hasNext()) {
            grantee = (Grantee) grantees.next();
            row     = t.getEmptyRowData();
            row[0]  = grantee.getName().getNameString();
            t.insertSys(session, store, row);
        }
        return t;
    }
    Table JAR_JAR_USAGE(Session session, PersistentStore store) {
        Table t = sysTables[JAR_JAR_USAGE];
        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[JAR_JAR_USAGE]);
            addColumn(t, "PATH_JAR_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "PATH_JAR_SCHAMA", SQL_IDENTIFIER);
            addColumn(t, "PATH_JAR_NAME", SQL_IDENTIFIER);
            addColumn(t, "JAR_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "JAR_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "JAR_NAME", SQL_IDENTIFIER);
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[JAR_JAR_USAGE].name, false,
                SchemaObject.INDEX);
            t.createPrimaryKeyConstraint(name, new int[] {
                0, 1, 2, 3, 4, 5
            }, false);
            return t;
        }
        final int path_jar_catalog = 0;
        final int path_jar_schema  = 1;
        final int path_jar_name    = 2;
        final int jar_catalog      = 3;
        final int jar_schema       = 4;
        final int jar_name         = 5;
        Iterator it;
        Object[] row;
        return t;
    }
    Table JARS(Session session, PersistentStore store) {
        Table t = sysTables[JARS];
        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[JARS]);
            addColumn(t, "JAR_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "JAR_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "JAR_NAME", SQL_IDENTIFIER);
            addColumn(t, "JAR_PATH", CHARACTER_DATA);
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[JARS].name, false, SchemaObject.INDEX);
            t.createPrimaryKeyConstraint(name, new int[] {
                0, 1, 2, 3
            }, false);
            return t;
        }
        final int jar_catalog = 0;
        final int jar_schema  = 1;
        final int jar_name    = 2;
        final int jar_path    = 3;
        Iterator it;
        Object[] row;
        return t;
    }
    Table KEY_COLUMN_USAGE(Session session, PersistentStore store) {
        Table t = sysTables[KEY_COLUMN_USAGE];
        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[KEY_COLUMN_USAGE]);
            addColumn(t, "CONSTRAINT_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "CONSTRAINT_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "CONSTRAINT_NAME", SQL_IDENTIFIER);                   
            addColumn(t, "TABLE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "TABLE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "TABLE_NAME", SQL_IDENTIFIER);                        
            addColumn(t, "COLUMN_NAME", SQL_IDENTIFIER);                       
            addColumn(t, "ORDINAL_POSITION", CARDINAL_NUMBER);                 
            addColumn(t, "POSITION_IN_UNIQUE_CONSTRAINT", CARDINAL_NUMBER);    
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[KEY_COLUMN_USAGE].name, false,
                SchemaObject.INDEX);
            t.createPrimaryKeyConstraint(name, new int[] {
                2, 1, 0, 6, 7
            }, false);
            return t;
        }
        Iterator tables;
        Object[] row;
        final int constraint_catalog            = 0;
        final int constraint_schema             = 1;
        final int constraint_name               = 2;
        final int table_catalog                 = 3;
        final int table_schema                  = 4;
        final int table_name                    = 5;
        final int column_name                   = 6;
        final int ordinal_position              = 7;
        final int position_in_unique_constraint = 8;
        tables =
            database.schemaManager.databaseObjectIterator(SchemaObject.TABLE);
        while (tables.hasNext()) {
            Table    table        = (Table) tables.next();
            String   tableCatalog = database.getCatalogName().name;
            HsqlName tableName    = table.getName();
            if (table.isView()) {
                continue;
            }
            if (!session.getGrantee().isAccessible(tableName)) {
                continue;
            }
            Constraint[] constraints = table.getConstraints();
            for (int i = 0; i < constraints.length; i++) {
                Constraint constraint = constraints[i];
                if (constraint.getConstraintType() == SchemaObject
                        .ConstraintTypes.PRIMARY_KEY || constraint
                        .getConstraintType() == SchemaObject.ConstraintTypes
                        .UNIQUE || constraint
                        .getConstraintType() == SchemaObject.ConstraintTypes
                        .FOREIGN_KEY) {
                    String constraintName = constraint.getName().name;
                    int[]  cols           = constraint.getMainColumns();
                    int[]  uniqueColMap   = null;
                    if (constraint.getConstraintType()
                            == SchemaObject.ConstraintTypes.FOREIGN_KEY) {
                        Table uniqueConstTable = constraint.getMain();
                        Constraint uniqueConstraint =
                            uniqueConstTable.getConstraint(
                                constraint.getUniqueName().name);
                        int[] uniqueConstIndexes =
                            uniqueConstraint.getMainColumns();
                        uniqueColMap = new int[cols.length];
                        for (int j = 0; j < cols.length; j++) {
                            uniqueColMap[j] =
                                ArrayUtil.find(uniqueConstIndexes, cols[j]);
                        }
                        cols = constraint.getRefColumns();
                    }
                    if (!session.getGrantee().hasColumnRights(table, cols)) {
                        continue;
                    }
                    for (int j = 0; j < cols.length; j++) {
                        row                     = t.getEmptyRowData();
                        row[constraint_catalog] = tableCatalog;
                        row[constraint_schema]  = tableName.schema.name;
                        row[constraint_name]    = constraintName;
                        row[table_catalog]      = tableCatalog;
                        row[table_schema]       = tableName.schema.name;
                        row[table_name]         = tableName.name;
                        row[column_name] =
                            table.getColumn(cols[j]).getName().name;
                        row[ordinal_position] = ValuePool.getLong(j + 1);
                        if (constraint.getConstraintType()
                                == SchemaObject.ConstraintTypes.FOREIGN_KEY) {
                            row[position_in_unique_constraint] =
                                ValuePool.getInt(uniqueColMap[j] + 1);
                        }
                        t.insertSys(session, store, row);
                    }
                }
            }
        }
        return t;
    }
    Table METHOD_SPECIFICATION_PARAMETERS(Session session,
                                          PersistentStore store) {
        return null;
    }
    Table METHOD_SPECIFICATIONS(Session session, PersistentStore store) {
        return null;
    }
    Table MODULE_COLUMN_USAGE(Session session, PersistentStore store) {
        return null;
    }
    Table MODULE_PRIVILEGES(Session session, PersistentStore store) {
        return null;
    }
    Table MODULE_TABLE_USAGE(Session session, PersistentStore store) {
        return null;
    }
    Table MODULES(Session session, PersistentStore store) {
        return null;
    }
    Table PARAMETERS(Session session, PersistentStore store) {
        Table t = sysTables[PARAMETERS];
        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[PARAMETERS]);
            addColumn(t, "SPECIFIC_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "SPECIFIC_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "SPECIFIC_NAME", SQL_IDENTIFIER);
            addColumn(t, "ORDINAL_POSITION", CARDINAL_NUMBER);
            addColumn(t, "PARAMETER_MODE", CHARACTER_DATA);
            addColumn(t, "IS_RESULT", YES_OR_NO);
            addColumn(t, "AS_LOCATOR", YES_OR_NO);
            addColumn(t, "PARAMETER_NAME", SQL_IDENTIFIER);
            addColumn(t, "FROM_SQL_SPECIFIC_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "FROM_SQL_SPECIFIC_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "FROM_SQL_SPECIFIC_NAME", SQL_IDENTIFIER);
            addColumn(t, "TO_SQL_SPECIFIC_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "TO_SQL_SPECIFIC_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "TO_SQL_SPECIFIC_NAME", SQL_IDENTIFIER);
            addColumn(t, "DATA_TYPE", CHARACTER_DATA);
            addColumn(t, "CHARACTER_MAXIMUM_LENGTH", CARDINAL_NUMBER);
            addColumn(t, "CHARACTER_OCTET_LENGTH", CARDINAL_NUMBER);
            addColumn(t, "CHARACTER_SET_CATALOG", CHARACTER_DATA);
            addColumn(t, "CHARACTER_SET_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "CHARACTER_SET_NAME", SQL_IDENTIFIER);
            addColumn(t, "COLLATION_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "COLLATION_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "COLLATION_NAME", SQL_IDENTIFIER);
            addColumn(t, "NUMERIC_PRECISION", CARDINAL_NUMBER);
            addColumn(t, "NUMERIC_PRECISION_RADIX", CARDINAL_NUMBER);
            addColumn(t, "NUMERIC_SCALE", CARDINAL_NUMBER);
            addColumn(t, "DATETIME_PRECISION", CARDINAL_NUMBER);
            addColumn(t, "INTERVAL_TYPE", CHARACTER_DATA);
            addColumn(t, "INTERVAL_PRECISION", CARDINAL_NUMBER);
            addColumn(t, "UDT_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "UDT_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "UDT_NAME", SQL_IDENTIFIER);
            addColumn(t, "SCOPE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "SCOPE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "SCOPE_NAME", SQL_IDENTIFIER);
            addColumn(t, "MAXIMUM_CARDINALITY", CARDINAL_NUMBER);
            addColumn(t, "DTD_IDENTIFIER", SQL_IDENTIFIER);
            addColumn(t, "DECLARED_DATA_TYPE", CHARACTER_DATA);
            addColumn(t, "DECLARED_NUMERIC_PRECISION", CARDINAL_NUMBER);
            addColumn(t, "DECLARED_NUMERIC_SCALE", CARDINAL_NUMBER);
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[PARAMETERS].name, false, SchemaObject.INDEX);
            t.createPrimaryKeyConstraint(name, new int[] {
                0, 1, 2, 3
            }, false);
            return t;
        }
        final int specific_cat             = 0;
        final int specific_schem           = 1;
        final int specific_name            = 2;
        final int ordinal_position         = 3;
        final int parameter_mode           = 4;
        final int is_result                = 5;
        final int as_locator               = 6;
        final int parameter_name           = 7;
        final int from_specific_catalog    = 8;
        final int from_specific_schema     = 9;
        final int from_specific_name       = 10;
        final int to_specific_catalog      = 11;
        final int to_specific_schema       = 12;
        final int to_specific_name         = 13;
        final int data_type                = 14;
        final int character_maximum_length = 15;
        final int character_octet_length   = 16;
        final int character_set_catalog    = 17;
        final int character_set_schema     = 18;
        final int character_set_name       = 19;
        final int collation_catalog        = 20;
        final int collation_schema         = 21;
        final int collation_name           = 22;
        final int numeric_precision        = 23;
        final int numeric_precision_radix  = 24;
        final int numeric_scale            = 25;
        final int datetime_precision       = 26;
        final int interval_type            = 27;
        final int interval_precision       = 28;
        final int udt_catalog              = 29;
        final int udt_schema               = 30;
        final int udt_name                 = 31;
        final int scope_catalog            = 32;
        final int scope_schema             = 33;
        final int scope_name               = 34;
        final int maximum_cardinality      = 35;
        final int dtd_identifier           = 36;
        int           columnCount;
        Iterator      routines;
        RoutineSchema routineSchema;
        Routine       routine;
        Object[]      row;
        Type          type;
        routines = database.schemaManager.databaseObjectIterator(
            SchemaObject.ROUTINE);
        while (routines.hasNext()) {
            routineSchema = (RoutineSchema) routines.next();
            if (!session.getGrantee().isAccessible(routineSchema)) {
                continue;
            }
            Routine[] specifics = routineSchema.getSpecificRoutines();
            for (int i = 0; i < specifics.length; i++) {
                routine     = specifics[i];
                columnCount = routine.getParameterCount();
                for (int j = 0; j < columnCount; j++) {
                    ColumnSchema column = routine.getParameter(j);
                    type                  = column.getDataType();
                    row                   = t.getEmptyRowData();
                    row[specific_cat]     = database.getCatalogName().name;
                    row[specific_schem]   = routine.getSchemaName().name;
                    row[specific_name]    = routine.getSpecificName().name;
                    row[parameter_name]   = column.getName().name;
                    row[ordinal_position] = ValuePool.getLong(j + 1);
                    switch (column.getParameterMode()) {
                        case SchemaObject.ParameterModes.PARAM_IN :
                            row[parameter_mode] = "IN";
                            break;
                        case SchemaObject.ParameterModes.PARAM_OUT :
                            row[parameter_mode] = "OUT";
                            break;
                        case SchemaObject.ParameterModes.PARAM_INOUT :
                            row[parameter_mode] = "INOUT";
                            break;
                    }
                    row[is_result]  = "NO";
                    row[as_locator] = "NO";
                    row[data_type]  = type.getFullNameString();
                    if (type.isCharacterType()) {
                        row[character_maximum_length] =
                            ValuePool.getLong(type.precision);
                        row[character_octet_length] =
                            ValuePool.getLong(type.precision * 2);
                        row[character_set_catalog] =
                            database.getCatalogName().name;
                        row[character_set_schema] =
                            ((CharacterType) type).getCharacterSet()
                                .getSchemaName().name;
                        row[character_set_name] =
                            ((CharacterType) type).getCharacterSet().getName()
                                .name;
                        row[collation_catalog] =
                            database.getCatalogName().name;
                        row[collation_schema] =
                            ((CharacterType) type).getCollation()
                                .getSchemaName().name;
                        row[collation_name] =
                            ((CharacterType) type).getCollation().getName()
                                .name;
                    } else if (type.isNumberType()) {
                        row[numeric_precision] = ValuePool.getLong(
                            ((NumberType) type).getNumericPrecisionInRadix());
                        row[numeric_precision_radix] =
                            ValuePool.getLong(type.getPrecisionRadix());
                    } else if (type.isBooleanType()) {
                    } else if (type.isDateTimeType()) {
                        row[datetime_precision] =
                            ValuePool.getLong(type.scale);
                    } else if (type.isIntervalType()) {
                        row[data_type] = "INTERVAL";
                        row[interval_type] =
                            ((IntervalType) type).getQualifier(type.typeCode);
                        row[interval_precision] =
                            ValuePool.getLong(type.precision);
                        row[datetime_precision] =
                            ValuePool.getLong(type.scale);
                    } else if (type.isBinaryType()) {
                        row[character_maximum_length] =
                            ValuePool.getLong(type.precision);
                        row[character_octet_length] =
                            ValuePool.getLong(type.precision);
                    } else if (type.isBitType()) {
                        row[character_maximum_length] =
                            ValuePool.getLong(type.precision);
                        row[character_octet_length] =
                            ValuePool.getLong(type.precision);
                    } else if (type.isArrayType()) {
                        row[maximum_cardinality] =
                            ValuePool.getLong(type.arrayLimitCardinality());
                        row[data_type] = "ARRAY";
                    }
                    if (type.isDistinctType()) {
                        row[udt_catalog] = database.getCatalogName().name;
                        row[udt_schema]  = type.getSchemaName().name;
                        row[udt_name]    = type.getName().name;
                    }
                    row[dtd_identifier] = type.getDefinition();
                    t.insertSys(session, store, row);
                }
            }
        }
        return t;
    }
    Table REFERENTIAL_CONSTRAINTS(Session session, PersistentStore store) {
        Table t = sysTables[REFERENTIAL_CONSTRAINTS];
        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[REFERENTIAL_CONSTRAINTS]);
            addColumn(t, "CONSTRAINT_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "CONSTRAINT_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "CONSTRAINT_NAME", SQL_IDENTIFIER);              
            addColumn(t, "UNIQUE_CONSTRAINT_CATALOG", SQL_IDENTIFIER);    
            addColumn(t, "UNIQUE_CONSTRAINT_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "UNIQUE_CONSTRAINT_NAME", SQL_IDENTIFIER);
            addColumn(t, "MATCH_OPTION", CHARACTER_DATA);                 
            addColumn(t, "UPDATE_RULE", CHARACTER_DATA);                  
            addColumn(t, "DELETE_RULE", CHARACTER_DATA);                  
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[REFERENTIAL_CONSTRAINTS].name, false,
                SchemaObject.INDEX);
            t.createPrimaryKeyConstraint(name, new int[] {
                0, 1, 2,
            }, false);
            return t;
        }
        final int constraint_catalog        = 0;
        final int constraint_schema         = 1;
        final int constraint_name           = 2;
        final int unique_constraint_catalog = 3;
        final int unique_constraint_schema  = 4;
        final int unique_constraint_name    = 5;
        final int match_option              = 6;
        final int update_rule               = 7;
        final int delete_rule               = 8;
        Iterator     tables;
        Table        table;
        Constraint[] constraints;
        Constraint   constraint;
        Object[]     row;
        tables =
            database.schemaManager.databaseObjectIterator(SchemaObject.TABLE);
        while (tables.hasNext()) {
            table = (Table) tables.next();
            if (table.isView()
                    || !session.getGrantee().hasNonSelectTableRight(table)) {
                continue;
            }
            constraints = table.getConstraints();
            for (int i = 0; i < constraints.length; i++) {
                constraint = constraints[i];
                if (constraint.getConstraintType()
                        != SchemaObject.ConstraintTypes.FOREIGN_KEY) {
                    continue;
                }
                HsqlName uniqueName = constraint.getUniqueName();
                row                     = t.getEmptyRowData();
                row[constraint_catalog] = database.getCatalogName().name;
                row[constraint_schema]  = constraint.getSchemaName().name;
                row[constraint_name]    = constraint.getName().name;
                if (isAccessibleTable(session, constraint.getMain())) {
                    row[unique_constraint_catalog] =
                        database.getCatalogName().name;
                    row[unique_constraint_schema] = uniqueName.schema.name;
                    row[unique_constraint_name]   = uniqueName.name;
                }
                row[match_option] = Tokens.T_NONE;
                row[update_rule]  = constraint.getUpdateActionString();
                row[delete_rule]  = constraint.getDeleteActionString();
                t.insertSys(session, store, row);
            }
        }
        return t;
    }
    Table ROLE_COLUMN_GRANTS(Session session, PersistentStore store) {
        Table t = sysTables[ROLE_COLUMN_GRANTS];
        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[ROLE_COLUMN_GRANTS]);
            addColumn(t, "GRANTOR", SQL_IDENTIFIER);           
            addColumn(t, "GRANTEE", SQL_IDENTIFIER);           
            addColumn(t, "TABLE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "TABLE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "TABLE_NAME", SQL_IDENTIFIER);        
            addColumn(t, "COLUMN_NAME", SQL_IDENTIFIER);       
            addColumn(t, "PRIVILEGE_TYPE", CHARACTER_DATA);    
            addColumn(t, "IS_GRANTABLE", YES_OR_NO);           
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[ROLE_COLUMN_GRANTS].name, false,
                SchemaObject.INDEX);
            t.createPrimaryKeyConstraint(name, new int[] {
                5, 6, 1, 0, 4, 3, 2
            }, false);
            return t;
        }
        Session sys = database.sessionManager.newSysSession(
            SqlInvariants.INFORMATION_SCHEMA_HSQLNAME, session.getUser());
        Result rs = sys.executeDirectStatement(
            "SELECT GRANTOR, GRANTEE, TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, PRIVILEGE_TYPE, IS_GRANTABLE "
            + "FROM INFORMATION_SCHEMA.COLUMN_PRIVILEGES "
            + "JOIN INFORMATION_SCHEMA.APPLICABLE_ROLES ON GRANTEE = ROLE_NAME;");
        t.insertSys(session, store, rs);
        sys.close();
        return t;
    }
    Table ROLE_ROUTINE_GRANTS(Session session, PersistentStore store) {
        Table t = sysTables[ROLE_ROUTINE_GRANTS];
        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[ROLE_ROUTINE_GRANTS]);
            addColumn(t, "GRANTOR", SQL_IDENTIFIER);          
            addColumn(t, "GRANTEE", SQL_IDENTIFIER);          
            addColumn(t, "SPECIFIC_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "SPECIFIC_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "SPECIFIC_NAME", SQL_IDENTIFIER);    
            addColumn(t, "ROUTINE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "ROUTINE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "ROUTINE_NAME", SQL_IDENTIFIER);
            addColumn(t, "PRIVILEGE_TYPE", CHARACTER_DATA);
            addColumn(t, "IS_GRANTABLE", YES_OR_NO);
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[ROLE_ROUTINE_GRANTS].name, false,
                SchemaObject.INDEX);
            t.createPrimaryKeyConstraint(name, new int[] {
                0, 1, 2, 3, 4, 5, 6, 7, 8, 9
            }, false);
            return t;
        }
        Session sys = database.sessionManager.newSysSession(
            SqlInvariants.INFORMATION_SCHEMA_HSQLNAME, session.getUser());
        Result rs = sys.executeDirectStatement(
            "SELECT GRANTOR, GRANTEE, SPECIFIC_CATALOG, SPECIFIC_SCHEMA, "
            + "SPECIFIC_NAME, ROUTINE_CATALOG, ROUTINE_SCHEMA, ROUTINE_NAME, "
            + "PRIVILEGE_TYPE, IS_GRANTABLE "
            + "FROM INFORMATION_SCHEMA.ROUTINE_PRIVILEGES "
            + "JOIN INFORMATION_SCHEMA.APPLICABLE_ROLES ON GRANTEE = ROLE_NAME;");
        t.insertSys(session, store, rs);
        sys.close();
        final int grantor          = 0;
        final int grantee          = 1;
        final int table_name       = 2;
        final int specific_catalog = 3;
        final int specific_schema  = 4;
        final int specific_name    = 5;
        final int routine_catalog  = 6;
        final int routine_schema   = 7;
        final int routine_name     = 8;
        final int privilege_type   = 9;
        final int is_grantable     = 10;
        return t;
    }
    Table ROLE_TABLE_GRANTS(Session session, PersistentStore store) {
        Table t = sysTables[ROLE_TABLE_GRANTS];
        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[ROLE_TABLE_GRANTS]);
            addColumn(t, "GRANTOR", SQL_IDENTIFIER);           
            addColumn(t, "GRANTEE", SQL_IDENTIFIER);           
            addColumn(t, "TABLE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "TABLE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "TABLE_NAME", SQL_IDENTIFIER);        
            addColumn(t, "PRIVILEGE_TYPE", CHARACTER_DATA);    
            addColumn(t, "IS_GRANTABLE", YES_OR_NO);           
            addColumn(t, "WITH_HIERARCHY", YES_OR_NO);
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[ROLE_TABLE_GRANTS].name, false,
                SchemaObject.INDEX);
            t.createPrimaryKeyConstraint(name, new int[] {
                3, 4, 5, 0, 1
            }, false);
            return t;
        }
        Session sys = database.sessionManager.newSysSession(
            SqlInvariants.INFORMATION_SCHEMA_HSQLNAME, session.getUser());
        Result rs = sys.executeDirectStatement(
            "SELECT GRANTOR, GRANTEE, TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, "
            + "PRIVILEGE_TYPE, IS_GRANTABLE, 'NO' "
            + "FROM INFORMATION_SCHEMA.TABLE_PRIVILEGES "
            + "JOIN INFORMATION_SCHEMA.APPLICABLE_ROLES ON GRANTEE = ROLE_NAME;");
        t.insertSys(session, store, rs);
        sys.close();
        return t;
    }
    Table ROLE_UDT_GRANTS(Session session, PersistentStore store) {
        Table t = sysTables[ROLE_UDT_GRANTS];
        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[ROLE_UDT_GRANTS]);
            addColumn(t, "GRANTOR", SQL_IDENTIFIER);     
            addColumn(t, "GRANTEE", SQL_IDENTIFIER);     
            addColumn(t, "UDT_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "UDT_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "UDT_NAME", SQL_IDENTIFIER);    
            addColumn(t, "PRIVILEGE_TYPE", CHARACTER_DATA);
            addColumn(t, "IS_GRANTABLE", YES_OR_NO);     
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[ROLE_TABLE_GRANTS].name, false,
                SchemaObject.INDEX);
            t.createPrimaryKeyConstraint(name, null, false);
            return t;
        }
        Session sys = database.sessionManager.newSysSession(
            SqlInvariants.INFORMATION_SCHEMA_HSQLNAME, session.getUser());
        Result rs = sys.executeDirectStatement(
            "SELECT GRANTOR, GRANTEE, UDT_CATALOG, UDT_SCHEMA, UDT_NAME, "
            + "PRIVILEGE_TYPE, IS_GRANTABLE "
            + "FROM INFORMATION_SCHEMA.UDT_PRIVILEGES "
            + "JOIN INFORMATION_SCHEMA.APPLICABLE_ROLES ON GRANTEE = ROLE_NAME;");
        t.insertSys(session, store, rs);
        sys.close();
        return t;
    }
    Table ROLE_USAGE_GRANTS(Session session, PersistentStore store) {
        Table t = sysTables[ROLE_USAGE_GRANTS];
        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[ROLE_USAGE_GRANTS]);
            addColumn(t, "GRANTOR", SQL_IDENTIFIER);        
            addColumn(t, "GRANTEE", SQL_IDENTIFIER);        
            addColumn(t, "OBJECT_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "OBJECT_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "OBJECT_NAME", SQL_IDENTIFIER);    
            addColumn(t, "OBJECT_TYPE", CHARACTER_DATA);    
            addColumn(t, "PRIVILEGE_TYPE", CHARACTER_DATA);
            addColumn(t, "IS_GRANTABLE", YES_OR_NO);        
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[ROLE_USAGE_GRANTS].name, false,
                SchemaObject.INDEX);
            t.createPrimaryKeyConstraint(name, new int[] {
                0, 1, 2, 3, 4, 5, 6, 7
            }, false);
            return t;
        }
        Session sys = database.sessionManager.newSysSession(
            SqlInvariants.INFORMATION_SCHEMA_HSQLNAME, session.getUser());
        Result rs = sys.executeDirectStatement(
            "SELECT GRANTOR, GRANTEE, OBJECT_CATALOG, OBJECT_SCHEMA, OBJECT_NAME, "
            + "OBJECT_TYPE, PRIVILEGE_TYPE, IS_GRANTABLE "
            + "FROM INFORMATION_SCHEMA.USAGE_PRIVILEGES "
            + "JOIN INFORMATION_SCHEMA.APPLICABLE_ROLES ON GRANTEE = ROLE_NAME;");
        t.insertSys(session, store, rs);
        sys.close();
        return t;
    }
    Table ROUTINE_COLUMN_USAGE(Session session, PersistentStore store) {
        Table t = sysTables[ROUTINE_COLUMN_USAGE];
        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[ROUTINE_COLUMN_USAGE]);
            addColumn(t, "SPECIFIC_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "SPECIFIC_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "SPECIFIC_NAME", SQL_IDENTIFIER);
            addColumn(t, "ROUTINE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "ROUTINE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "ROUTINE_NAME", SQL_IDENTIFIER);
            addColumn(t, "TABLE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "TABLE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "TABLE_NAME", SQL_IDENTIFIER);
            addColumn(t, "COLUMN_NAME", SQL_IDENTIFIER);
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[ROUTINE_COLUMN_USAGE].name, false,
                SchemaObject.INDEX);
            t.createPrimaryKeyConstraint(name, new int[] {
                3, 4, 5, 0, 1, 2, 6, 7, 8, 9
            }, false);
            return t;
        }
        final int specific_catalog = 0;
        final int specific_schema  = 1;
        final int specific_name    = 2;
        final int routine_catalog  = 3;
        final int routine_schema   = 4;
        final int routine_name     = 5;
        final int table_catalog    = 6;
        final int table_schema     = 7;
        final int table_name       = 8;
        final int column_name      = 9;
        Iterator it;
        Object[] row;
        it = database.schemaManager.databaseObjectIterator(
            SchemaObject.SPECIFIC_ROUTINE);
        while (it.hasNext()) {
            Routine        routine = (Routine) it.next();
            OrderedHashSet set     = routine.getReferences();
            for (int i = 0; i < set.size(); i++) {
                HsqlName refName = (HsqlName) set.get(i);
                if (refName.type != SchemaObject.COLUMN) {
                    continue;
                }
                if (!session.getGrantee().isFullyAccessibleByRole(refName)) {
                    continue;
                }
                row = t.getEmptyRowData();
                row[specific_catalog] = database.getCatalogName().name;
                row[specific_schema]  = routine.getSchemaName().name;
                row[specific_name]    = routine.getSpecificName().name;
                row[routine_catalog]  = database.getCatalogName().name;
                row[routine_schema]   = routine.getSchemaName().name;
                row[routine_name]     = routine.getName().name;
                row[table_catalog]    = database.getCatalogName().name;
                row[table_schema]     = refName.parent.schema.name;
                row[table_name]       = refName.parent.name;
                row[column_name]      = refName.name;
                try {
                    t.insertSys(session, store, row);
                } catch (HsqlException e) {}
            }
        }
        return t;
    }
    Table ROUTINE_PRIVILEGES(Session session, PersistentStore store) {
        Table t = sysTables[ROUTINE_PRIVILEGES];
        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[ROUTINE_PRIVILEGES]);
            addColumn(t, "GRANTOR", SQL_IDENTIFIER);           
            addColumn(t, "GRANTEE", SQL_IDENTIFIER);           
            addColumn(t, "SPECIFIC_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "SPECIFIC_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "SPECIFIC_NAME", SQL_IDENTIFIER);     
            addColumn(t, "ROUTINE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "ROUTINE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "ROUTINE_NAME", SQL_IDENTIFIER);      
            addColumn(t, "PRIVILEGE_TYPE", CHARACTER_DATA);    
            addColumn(t, "IS_GRANTABLE", YES_OR_NO);           
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[ROUTINE_PRIVILEGES].name, false,
                SchemaObject.INDEX);
            t.createPrimaryKeyConstraint(name, new int[] {
                0, 1, 2, 3, 4, 5, 6, 7, 8, 9
            }, false);
            return t;
        }
        final int grantor          = 0;
        final int grantee          = 1;
        final int specific_catalog = 2;
        final int specific_schema  = 3;
        final int specific_name    = 4;
        final int routine_catalog  = 5;
        final int routine_schema   = 6;
        final int routine_name     = 7;
        final int privilege_type   = 8;
        final int is_grantable     = 9;
        Grantee granteeObject;
        String  privilege;
        Iterator       routines;
        Routine        routine;
        Object[]       row;
        OrderedHashSet grantees = session.getGrantee().visibleGrantees();
        routines = database.schemaManager.databaseObjectIterator(
            SchemaObject.SPECIFIC_ROUTINE);
        while (routines.hasNext()) {
            routine = (Routine) routines.next();
            for (int i = 0; i < grantees.size(); i++) {
                granteeObject = (Grantee) grantees.get(i);
                OrderedHashSet rights =
                    granteeObject.getAllDirectPrivileges(routine);
                OrderedHashSet grants =
                    granteeObject.getAllGrantedPrivileges(routine);
                if (!grants.isEmpty()) {
                    grants.addAll(rights);
                    rights = grants;
                }
                for (int j = 0; j < rights.size(); j++) {
                    Right right          = (Right) rights.get(j);
                    Right grantableRight = right.getGrantableRights();
                    if (!right.canAccessFully(GrantConstants.EXECUTE)) {
                        continue;
                    }
                    privilege = Tokens.T_EXECUTE;
                    row       = t.getEmptyRowData();
                    row[grantor]          = right.getGrantor().getName().name;
                    row[grantee]          = right.getGrantee().getName().name;
                    row[specific_catalog] = database.getCatalogName().name;
                    row[specific_schema]  = routine.getSchemaName().name;
                    row[specific_name]    = routine.getSpecificName().name;
                    row[routine_catalog]  = database.getCatalogName().name;
                    row[routine_schema]   = routine.getSchemaName().name;
                    row[routine_name]     = routine.getName().name;
                    row[privilege_type]   = privilege;
                    row[is_grantable] =
                        right.getGrantee() == routine.getOwner()
                        || grantableRight.canAccessFully(
                            GrantConstants.EXECUTE) ? "YES"
                                                    : "NO";
                    try {
                        t.insertSys(session, store, row);
                    } catch (HsqlException e) {}
                }
            }
        }
        return t;
    }
    Table ROUTINE_JAR_USAGE(Session session, PersistentStore store) {
        Table t = sysTables[ROUTINE_JAR_USAGE];
        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[ROUTINE_JAR_USAGE]);
            addColumn(t, "SPECIFIC_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "SPECIFIC_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "SPECIFIC_NAME", SQL_IDENTIFIER);
            addColumn(t, "JAR_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "JAR_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "JAR_NAME", SQL_IDENTIFIER);
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[ROUTINE_JAR_USAGE].name, false,
                SchemaObject.INDEX);
            t.createPrimaryKeyConstraint(name, new int[] {
                0, 1, 2, 3, 4, 5
            }, false);
            return t;
        }
        final int specific_catalog = 0;
        final int specific_schema  = 1;
        final int specific_name    = 2;
        final int jar_catalog      = 3;
        final int jar_schema       = 4;
        final int jar_name         = 5;
        Iterator it;
        Object[] row;
        if (!session.isAdmin()) {
            return t;
        }
        it = database.schemaManager.databaseObjectIterator(
            SchemaObject.SPECIFIC_ROUTINE);
        while (it.hasNext()) {
            Routine routine = (Routine) it.next();
            if (routine.getLanguage() != Routine.LANGUAGE_JAVA) {
                continue;
            }
            row                   = t.getEmptyRowData();
            row[specific_catalog] = database.getCatalogName().name;
            row[specific_schema]  = routine.getSchemaName().name;
            row[specific_name]    = routine.getSpecificName().name;
            row[jar_catalog]      = database.getCatalogName().name;
            row[jar_schema] = database.schemaManager.getSQLJSchemaHsqlName();
            row[jar_name]         = "CLASSPATH";
            t.insertSys(session, store, row);
        }
        return t;
    }
    Table ROUTINE_ROUTINE_USAGE(Session session, PersistentStore store) {
        Table t = sysTables[ROUTINE_ROUTINE_USAGE];
        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[ROUTINE_ROUTINE_USAGE]);
            addColumn(t, "SPECIFIC_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "SPECIFIC_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "SPECIFIC_NAME", SQL_IDENTIFIER);
            addColumn(t, "ROUTINE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "ROUTINE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "ROUTINE_NAME", SQL_IDENTIFIER);
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[ROUTINE_ROUTINE_USAGE].name, false,
                SchemaObject.INDEX);
            t.createPrimaryKeyConstraint(name, new int[] {
                0, 1, 2, 3, 4, 5
            }, false);
            return t;
        }
        final int specific_catalog = 0;
        final int specific_schema  = 1;
        final int specific_name    = 2;
        final int routine_catalog  = 3;
        final int routine_schema   = 4;
        final int routine_name     = 5;
        Iterator it;
        Object[] row;
        it = database.schemaManager.databaseObjectIterator(
            SchemaObject.SPECIFIC_ROUTINE);
        while (it.hasNext()) {
            Routine        routine = (Routine) it.next();
            OrderedHashSet set     = routine.getReferences();
            for (int i = 0; i < set.size(); i++) {
                HsqlName refName = (HsqlName) set.get(i);
                if (refName.type != SchemaObject.SPECIFIC_ROUTINE) {
                    continue;
                }
                if (!session.getGrantee().isFullyAccessibleByRole(refName)) {
                    continue;
                }
                row                   = t.getEmptyRowData();
                row[specific_catalog] = database.getCatalogName().name;
                row[specific_schema]  = routine.getSchemaName().name;
                row[specific_name]    = routine.getSpecificName().name;
                row[routine_catalog]  = database.getCatalogName().name;
                row[routine_schema]   = refName.schema.name;
                row[routine_name]     = refName.name;
                try {
                    t.insertSys(session, store, row);
                } catch (HsqlException e) {}
            }
        }
        return t;
    }
    Table ROUTINE_SEQUENCE_USAGE(Session session, PersistentStore store) {
        Table t = sysTables[ROUTINE_SEQUENCE_USAGE];
        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[ROUTINE_SEQUENCE_USAGE]);
            addColumn(t, "SPECIFIC_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "SPECIFIC_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "SPECIFIC_NAME", SQL_IDENTIFIER);
            addColumn(t, "SEQUENCE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "SEQUENCE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "SEQUENCE_NAME", SQL_IDENTIFIER);
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[ROUTINE_SEQUENCE_USAGE].name, false,
                SchemaObject.INDEX);
            t.createPrimaryKeyConstraint(name, new int[] {
                0, 1, 2, 3, 4, 5
            }, false);
            return t;
        }
        final int specific_catalog = 0;
        final int specific_schema  = 1;
        final int specific_name    = 2;
        final int sequence_catalog = 3;
        final int sequence_schema  = 4;
        final int sequence_name    = 5;
        Iterator it;
        Object[] row;
        it = database.schemaManager.databaseObjectIterator(
            SchemaObject.SPECIFIC_ROUTINE);
        while (it.hasNext()) {
            Routine        routine = (Routine) it.next();
            OrderedHashSet set     = routine.getReferences();
            for (int i = 0; i < set.size(); i++) {
                HsqlName refName = (HsqlName) set.get(i);
                if (refName.type != SchemaObject.SEQUENCE) {
                    continue;
                }
                if (!session.getGrantee().isFullyAccessibleByRole(refName)) {
                    continue;
                }
                row                   = t.getEmptyRowData();
                row[specific_catalog] = database.getCatalogName().name;
                row[specific_schema]  = routine.getSchemaName().name;
                row[specific_name]    = routine.getSpecificName().name;
                row[sequence_catalog] = database.getCatalogName().name;
                row[sequence_schema]  = refName.schema.name;
                row[sequence_name]    = refName.name;
                try {
                    t.insertSys(session, store, row);
                } catch (HsqlException e) {}
            }
        }
        return t;
    }
    Table ROUTINE_TABLE_USAGE(Session session, PersistentStore store) {
        Table t = sysTables[ROUTINE_TABLE_USAGE];
        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[ROUTINE_TABLE_USAGE]);
            addColumn(t, "SPECIFIC_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "SPECIFIC_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "SPECIFIC_NAME", SQL_IDENTIFIER);
            addColumn(t, "ROUTINE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "ROUTINE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "ROUTINE_NAME", SQL_IDENTIFIER);
            addColumn(t, "TABLE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "TABLE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "TABLE_NAME", SQL_IDENTIFIER);
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[ROUTINE_TABLE_USAGE].name, false,
                SchemaObject.INDEX);
            t.createPrimaryKeyConstraint(name, new int[] {
                3, 4, 5, 0, 1, 2, 6, 7, 8
            }, false);
            return t;
        }
        final int specific_catalog = 0;
        final int specific_schema  = 1;
        final int specific_name    = 2;
        final int routine_catalog  = 3;
        final int routine_schema   = 4;
        final int routine_name     = 5;
        final int table_catalog    = 6;
        final int table_schema     = 7;
        final int table_name       = 8;
        Iterator it;
        Object[] row;
        it = database.schemaManager.databaseObjectIterator(
            SchemaObject.SPECIFIC_ROUTINE);
        while (it.hasNext()) {
            Routine        routine = (Routine) it.next();
            OrderedHashSet set     = routine.getReferences();
            for (int i = 0; i < set.size(); i++) {
                HsqlName refName = (HsqlName) set.get(i);
                if (refName.type != SchemaObject.TABLE
                        && refName.type != SchemaObject.VIEW) {
                    continue;
                }
                if (!session.getGrantee().isFullyAccessibleByRole(refName)) {
                    continue;
                }
                row                   = t.getEmptyRowData();
                row[specific_catalog] = database.getCatalogName().name;
                row[specific_schema]  = routine.getSchemaName().name;
                row[specific_name]    = routine.getSpecificName().name;
                row[routine_catalog]  = database.getCatalogName().name;
                row[routine_schema]   = routine.getSchemaName().name;
                row[routine_name]     = routine.getName().name;
                row[table_catalog]    = database.getCatalogName().name;
                row[table_schema]     = refName.schema.name;
                row[table_name]       = refName.name;
                try {
                    t.insertSys(session, store, row);
                } catch (HsqlException e) {}
            }
        }
        return t;
    }
    Table ROUTINES(Session session, PersistentStore store) {
        Table t = sysTables[ROUTINES];
        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[ROUTINES]);
            addColumn(t, "SPECIFIC_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "SPECIFIC_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "SPECIFIC_NAME", SQL_IDENTIFIER);
            addColumn(t, "ROUTINE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "ROUTINE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "ROUTINE_NAME", SQL_IDENTIFIER);
            addColumn(t, "ROUTINE_TYPE", CHARACTER_DATA);
            addColumn(t, "MODULE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "MODULE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "MODULE_NAME", SQL_IDENTIFIER);
            addColumn(t, "UDT_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "UDT_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "UDT_NAME", SQL_IDENTIFIER);
            addColumn(t, "DATA_TYPE", CHARACTER_DATA);
            addColumn(t, "CHARACTER_MAXIMUM_LENGTH", CARDINAL_NUMBER);
            addColumn(t, "CHARACTER_OCTET_LENGTH", CARDINAL_NUMBER);
            addColumn(t, "CHARACTER_SET_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "CHARACTER_SET_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "CHARACTER_SET_NAME", SQL_IDENTIFIER);
            addColumn(t, "COLLATION_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "COLLATION_SCHEMA", SQL_IDENTIFIER);          
            addColumn(t, "COLLATION_NAME", SQL_IDENTIFIER);
            addColumn(t, "NUMERIC_PRECISION", CARDINAL_NUMBER);        
            addColumn(t, "NUMERIC_PRECISION_RADIX", CARDINAL_NUMBER);
            addColumn(t, "NUMERIC_SCALE", CARDINAL_NUMBER);
            addColumn(t, "DATETIME_PRECISION", CARDINAL_NUMBER);
            addColumn(t, "INTERVAL_TYPE", CHARACTER_DATA);
            addColumn(t, "INTERVAL_PRECISION", CARDINAL_NUMBER);
            addColumn(t, "TYPE_UDT_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "TYPE_UDT_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "TYPE_UDT_NAME", SQL_IDENTIFIER);
            addColumn(t, "SCOPE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "SCOPE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "SCOPE_NAME", SQL_IDENTIFIER);                
            addColumn(t, "MAXIMUM_CARDINALITY", CARDINAL_NUMBER);      
            addColumn(t, "DTD_IDENTIFIER", SQL_IDENTIFIER);
            addColumn(t, "ROUTINE_BODY", CHARACTER_DATA);
            addColumn(t, "ROUTINE_DEFINITION", CHARACTER_DATA);
            addColumn(t, "EXTERNAL_NAME", CHARACTER_DATA);
            addColumn(t, "EXTERNAL_LANGUAGE", CHARACTER_DATA);
            addColumn(t, "PARAMETER_STYLE", CHARACTER_DATA);
            addColumn(t, "IS_DETERMINISTIC", YES_OR_NO);
            addColumn(t, "SQL_DATA_ACCESS", CHARACTER_DATA);
            addColumn(t, "IS_NULL_CALL", YES_OR_NO);
            addColumn(t, "SQL_PATH", CHARACTER_DATA);
            addColumn(t, "SCHEMA_LEVEL_ROUTINE", YES_OR_NO);           
            addColumn(t, "MAX_DYNAMIC_RESULT_SETS", CARDINAL_NUMBER);
            addColumn(t, "IS_USER_DEFINED_CAST", YES_OR_NO);
            addColumn(t, "IS_IMPLICITLY_INVOCABLE", YES_OR_NO);
            addColumn(t, "SECURITY_TYPE", CHARACTER_DATA);
            addColumn(t, "TO_SQL_SPECIFIC_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "TO_SQL_SPECIFIC_SCHEMA", SQL_IDENTIFIER);    
            addColumn(t, "TO_SQL_SPECIFIC_NAME", SQL_IDENTIFIER);
            addColumn(t, "AS_LOCATOR", YES_OR_NO);
            addColumn(t, "CREATED", TIME_STAMP);
            addColumn(t, "LAST_ALTERED", TIME_STAMP);
            addColumn(t, "NEW_SAVEPOINT_LEVEL", YES_OR_NO);
            addColumn(t, "IS_UDT_DEPENDENT", YES_OR_NO);
            addColumn(t, "RESULT_CAST_FROM_DATA_TYPE", CHARACTER_DATA);
            addColumn(t, "RESULT_CAST_AS_LOCATOR", YES_OR_NO);
            addColumn(t, "RESULT_CAST_CHAR_MAX_LENGTH", CARDINAL_NUMBER);
            addColumn(t, "RESULT_CAST_CHAR_OCTET_LENGTH", CARDINAL_NUMBER);
            addColumn(t, "RESULT_CAST_CHAR_SET_CATALOG", CHARACTER_DATA);
            addColumn(t, "RESULT_CAST_CHAR_SET_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "RESULT_CAST_CHARACTER_SET_NAME", SQL_IDENTIFIER);
            addColumn(t, "RESULT_CAST_COLLATION_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "RESULT_CAST_COLLATION_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "RESULT_CAST_COLLATION_NAME", SQL_IDENTIFIER);
            addColumn(t, "RESULT_CAST_NUMERIC_PRECISION", CARDINAL_NUMBER);
            addColumn(t, "RESULT_CAST_NUMERIC_RADIX", CARDINAL_NUMBER);
            addColumn(t, "RESULT_CAST_NUMERIC_SCALE", CARDINAL_NUMBER);
            addColumn(t, "RESULT_CAST_DATETIME_PRECISION", CARDINAL_NUMBER);
            addColumn(t, "RESULT_CAST_INTERVAL_TYPE", CHARACTER_DATA);
            addColumn(t, "RESULT_CAST_INTERVAL_PRECISION", CARDINAL_NUMBER);
            addColumn(t, "RESULT_CAST_TYPE_UDT_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "RESULT_CAST_TYPE_UDT_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "RESULT_CAST_TYPE_UDT_NAME", SQL_IDENTIFIER);
            addColumn(t, "RESULT_CAST_SCOPE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "RESULT_CAST_SCOPE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "RESULT_CAST_SCOPE_NAME", SQL_IDENTIFIER);
            addColumn(t, "RESULT_CAST_MAX_CARDINALITY", CARDINAL_NUMBER);
            addColumn(t, "RESULT_CAST_DTD_IDENTIFIER", CHARACTER_DATA);
            addColumn(t, "DECLARED_DATA_TYPE", CHARACTER_DATA);
            addColumn(t, "DECLARED_NUMERIC_PRECISION", CARDINAL_NUMBER);
            addColumn(t, "DECLARED_NUMERIC_SCALE", CARDINAL_NUMBER);
            addColumn(t, "RESULT_CAST_FROM_DECLARED_DATA_TYPE",
                      CHARACTER_DATA);
            addColumn(t, "RESULT_CAST_DECLARED_NUMERIC_PRECISION",
                      CARDINAL_NUMBER);
            addColumn(t, "RESULT_CAST_DECLARED_NUMERIC_SCALE",
                      CARDINAL_NUMBER);
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[ROUTINES].name, false, SchemaObject.INDEX);
            t.createPrimaryKeyConstraint(name, new int[] {
                3, 4, 5, 0, 1, 2
            }, false);
            return t;
        }
        final int specific_catalog                       = 0;
        final int specific_schema                        = 1;
        final int specific_name                          = 2;
        final int routine_catalog                        = 3;
        final int routine_schema                         = 4;
        final int routine_name                           = 5;
        final int routine_type                           = 6;
        final int module_catalog                         = 7;
        final int module_schema                          = 8;
        final int module_name                            = 9;
        final int udt_catalog                            = 10;
        final int udt_schema                             = 11;
        final int udt_name                               = 12;
        final int data_type                              = 13;
        final int character_maximum_length               = 14;
        final int character_octet_length                 = 15;
        final int character_set_catalog                  = 16;
        final int character_set_schema                   = 17;
        final int character_set_name                     = 18;
        final int collation_catalog                      = 19;
        final int collation_schema                       = 20;
        final int collation_name                         = 21;
        final int numeric_precision                      = 22;
        final int numeric_precision_radix                = 23;
        final int numeric_scale                          = 24;
        final int datetime_precision                     = 25;
        final int interval_type                          = 26;
        final int interval_precision                     = 27;
        final int type_udt_catalog                       = 28;
        final int type_udt_schema                        = 29;
        final int type_udt_name                          = 30;
        final int scope_catalog                          = 31;
        final int scope_schema                           = 32;
        final int scope_name                             = 33;
        final int maximum_cardinality                    = 34;
        final int dtd_identifier                         = 35;
        final int routine_body                           = 36;
        final int routine_definition                     = 37;
        final int external_name                          = 38;
        final int external_language                      = 39;
        final int parameter_style                        = 40;
        final int is_deterministic                       = 41;
        final int sql_data_access                        = 42;
        final int is_null_call                           = 43;
        final int sql_path                               = 44;
        final int schema_level_routine                   = 45;
        final int max_dynamic_result_sets                = 46;
        final int is_user_defined_cast                   = 47;
        final int is_implicitly_invocable                = 48;
        final int security_type                          = 49;
        final int to_sql_specific_catalog                = 50;
        final int to_sql_specific_schema                 = 51;
        final int to_sql_specific_name                   = 52;
        final int as_locator                             = 53;
        final int created                                = 54;
        final int last_altered                           = 55;
        final int new_savepoint_level                    = 56;
        final int is_udt_dependent                       = 57;
        final int result_cast_from_data_type             = 58;
        final int result_cast_as_locator                 = 59;
        final int result_cast_char_max_length            = 60;
        final int result_cast_char_octet_length          = 61;
        final int result_cast_char_set_catalog           = 62;
        final int result_cast_char_set_schema            = 63;
        final int result_cast_character_set_name         = 64;
        final int result_cast_collation_catalog          = 65;
        final int result_cast_collation_schema           = 66;
        final int result_cast_collation_name             = 67;
        final int result_cast_numeric_precision          = 68;
        final int result_cast_numeric_radix              = 69;
        final int result_cast_numeric_scale              = 70;
        final int result_cast_datetime_precision         = 71;
        final int result_cast_interval_type              = 72;
        final int result_cast_interval_precision         = 73;
        final int result_cast_type_udt_catalog           = 74;
        final int result_cast_type_udt_schema            = 75;
        final int result_cast_type_udt_name              = 76;
        final int result_cast_scope_catalog              = 77;
        final int result_cast_scope_schema               = 78;
        final int result_cast_scope_name                 = 79;
        final int result_cast_max_cardinality            = 80;
        final int result_cast_dtd_identifier             = 81;
        final int declared_data_type                     = 82;
        final int declared_numeric_precision             = 83;
        final int declared_numeric_scale                 = 84;
        final int result_cast_from_declared_data_type    = 85;
        final int result_cast_declared_numeric_precision = 86;
        final int result_cast_declared_numeric_scale     = 87;
        Iterator it;
        Object[] row;
        it = database.schemaManager.databaseObjectIterator(
            SchemaObject.SPECIFIC_ROUTINE);
        while (it.hasNext()) {
            Routine routine = (Routine) it.next();
            boolean isFullyAccessible;
            if (!session.getGrantee().isAccessible(routine)) {
                continue;
            }
            isFullyAccessible = session.getGrantee().isFullyAccessibleByRole(
                routine.getName());
            row = t.getEmptyRowData();
            Type type = routine.isProcedure() ? null
                                              : routine.getReturnType();
            row[specific_catalog] = database.getCatalogName().name;
            row[specific_schema]  = routine.getSchemaName().name;
            row[specific_name]    = routine.getSpecificName().name;
            row[routine_catalog]  = database.getCatalogName().name;
            row[routine_schema]   = routine.getSchemaName().name;
            row[routine_name]     = routine.getName().name;
            row[routine_type]     = routine.isProcedure() ? Tokens.T_PROCEDURE
                                                          : Tokens.T_FUNCTION;
            row[module_catalog]   = null;
            row[module_schema]    = null;
            row[module_name]      = null;
            row[udt_catalog]      = null;
            row[udt_schema]       = null;
            row[udt_name]         = null;
            row[data_type]        = type == null ? null
                                                 : type.getNameString();
            if (type != null) {
                if (type.isCharacterType()) {
                    row[character_maximum_length] =
                        ValuePool.getLong(type.precision);
                    row[character_octet_length] =
                        ValuePool.getLong(type.precision * 2);
                    row[character_set_catalog] =
                        database.getCatalogName().name;
                    row[character_set_schema] =
                        ((CharacterType) type).getCharacterSet()
                            .getSchemaName().name;
                    row[character_set_name] =
                        ((CharacterType) type).getCharacterSet().getName()
                            .name;
                    row[collation_catalog] = database.getCatalogName().name;
                    row[collation_schema] =
                        ((CharacterType) type).getCollation().getSchemaName()
                            .name;
                    row[collation_name] =
                        ((CharacterType) type).getCollation().getName().name;
                } else if (type.isNumberType()) {
                    row[numeric_precision] = ValuePool.getLong(
                        ((NumberType) type).getNumericPrecisionInRadix());
                    row[declared_numeric_precision] = ValuePool.getLong(
                        ((NumberType) type).getNumericPrecisionInRadix());
                    if (type.isExactNumberType()) {
                        row[numeric_scale] = row[declared_numeric_scale] =
                            ValuePool.getLong(type.scale);
                    }
                    row[numeric_precision_radix] =
                        ValuePool.getLong(type.getPrecisionRadix());
                } else if (type.isBooleanType()) {
                } else if (type.isDateTimeType()) {
                    row[datetime_precision] = ValuePool.getLong(type.scale);
                } else if (type.isIntervalType()) {
                    row[data_type] = "INTERVAL";
                    row[interval_type] =
                        ((IntervalType) type).getQualifier(type.typeCode);
                    row[interval_precision] =
                        ValuePool.getLong(type.precision);
                    row[datetime_precision] = ValuePool.getLong(type.scale);
                } else if (type.isBinaryType()) {
                    row[character_maximum_length] =
                        ValuePool.getLong(type.precision);
                    row[character_octet_length] =
                        ValuePool.getLong(type.precision);
                } else if (type.isBitType()) {
                    row[character_maximum_length] =
                        ValuePool.getLong(type.precision);
                    row[character_octet_length] =
                        ValuePool.getLong(type.precision);
                } else if (type.isArrayType()) {
                    row[maximum_cardinality] =
                        ValuePool.getLong(type.arrayLimitCardinality());
                    row[data_type] = "ARRAY";
                }
                row[dtd_identifier]     = type.getDefinition();
                row[declared_data_type] = row[data_type];
            }
            row[type_udt_catalog] = null;
            row[type_udt_schema]  = null;
            row[type_udt_name]    = null;
            row[scope_catalog]    = null;
            row[scope_schema]     = null;
            row[scope_name]       = null;
            row[routine_body] = routine.getLanguage() == Routine.LANGUAGE_JAVA
                                ? "EXTERNAL"
                                : "SQL";
            row[routine_definition] = isFullyAccessible ? routine.getSQL()
                                                        : null;
            row[external_name]      = routine.getExternalName();
            row[external_language] = routine.getLanguage()
                                     == Routine.LANGUAGE_JAVA ? "JAVA"
                                                              : null;
            row[parameter_style] = routine.getLanguage()
                                   == Routine.LANGUAGE_JAVA ? "JAVA"
                                                            : null;
            row[is_deterministic] = routine.isDeterministic() ? "YES"
                                                              : "NO";
            row[sql_data_access]  = routine.getDataImpactString();
            row[is_null_call]     = type == null ? null
                                                 : routine.isNullInputOutput()
                                                   ? "YES"
                                                   : "NO";
            row[sql_path]                               = null;
            row[schema_level_routine]                   = "YES";
            row[max_dynamic_result_sets]                = ValuePool.getLong(0);
            row[is_user_defined_cast]                   = type == null ? null
                                                                       : "NO";
            row[is_implicitly_invocable]                = null;
            row[security_type]                          = "DEFINER";
            row[to_sql_specific_catalog]                = null;
            row[to_sql_specific_schema]                 = null;
            row[to_sql_specific_name]                   = null;
            row[as_locator]                             = type == null ? null
                                                                       : "NO";
            row[created]                                = null;
            row[last_altered]                           = null;
            row[new_savepoint_level]                    = "YES";
            row[is_udt_dependent]                       = null;
            row[result_cast_from_data_type]             = null;
            row[result_cast_as_locator]                 = null;
            row[result_cast_char_max_length]            = null;
            row[result_cast_char_octet_length]          = null;
            row[result_cast_char_set_catalog]           = null;
            row[result_cast_char_set_schema]            = null;
            row[result_cast_character_set_name]         = null;
            row[result_cast_collation_catalog]          = null;
            row[result_cast_collation_schema]           = null;
            row[result_cast_collation_name]             = null;
            row[result_cast_numeric_precision]          = null;
            row[result_cast_numeric_radix]              = null;
            row[result_cast_numeric_scale]              = null;
            row[result_cast_datetime_precision]         = null;
            row[result_cast_interval_type]              = null;
            row[result_cast_interval_precision]         = null;
            row[result_cast_type_udt_catalog]           = null;
            row[result_cast_type_udt_schema]            = null;
            row[result_cast_type_udt_name]              = null;
            row[result_cast_scope_catalog]              = null;
            row[result_cast_scope_schema]               = null;
            row[result_cast_scope_name]                 = null;
            row[result_cast_max_cardinality]            = null;
            row[result_cast_dtd_identifier]             = null;
            row[declared_data_type]                     = row[data_type];
            row[declared_numeric_precision] = row[numeric_precision];
            row[declared_numeric_scale]                 = row[numeric_scale];
            row[result_cast_from_declared_data_type]    = null;
            row[result_cast_declared_numeric_precision] = null;
            row[result_cast_declared_numeric_scale]     = null;
            t.insertSys(session, store, row);
        }
        return t;
    }
    Table SCHEMATA(Session session, PersistentStore store) {
        Table t = sysTables[SCHEMATA];
        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SCHEMATA]);
            addColumn(t, "CATALOG_NAME", SQL_IDENTIFIER);
            addColumn(t, "SCHEMA_NAME", SQL_IDENTIFIER);
            addColumn(t, "SCHEMA_OWNER", SQL_IDENTIFIER);
            addColumn(t, "DEFAULT_CHARACTER_SET_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "DEFAULT_CHARACTER_SET_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "DEFAULT_CHARACTER_SET_NAME", SQL_IDENTIFIER);
            addColumn(t, "SQL_PATH", CHARACTER_DATA);
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[SCHEMATA].name, false, SchemaObject.INDEX);
            t.createPrimaryKeyConstraint(name, new int[] {
                0, 1
            }, false);
            return t;
        }
        Schema[] schemas;
        Schema   schema;
        String   dcsSchema = SqlInvariants.INFORMATION_SCHEMA;
        String   dcsName   = ValuePool.getString(Tokens.T_UTF16);
        String   sqlPath   = null;
        Grantee  user      = session.getGrantee();
        Object[] row;
        final int schema_catalog                = 0;
        final int schema_name                   = 1;
        final int schema_owner                  = 2;
        final int default_character_set_catalog = 3;
        final int default_character_set_schema  = 4;
        final int default_character_set_name    = 5;
        final int sql_path                      = 6;
        schemas = database.schemaManager.getAllSchemas();
        for (int i = 0; i < schemas.length; i++) {
            schema = schemas[i];
            if (!user.hasSchemaUpdateOrGrantRights(
                    schema.getName().getNameString())) {
                continue;
            }
            row                 = t.getEmptyRowData();
            row[schema_catalog] = database.getCatalogName().name;
            row[schema_name]    = schema.getName().getNameString();
            row[schema_owner]   = schema.getOwner().getName().getNameString();
            row[default_character_set_catalog] =
                database.getCatalogName().name;
            row[default_character_set_schema] = dcsSchema;
            row[default_character_set_name]   = dcsName;
            row[sql_path]                     = sqlPath;
            t.insertSys(session, store, row);
        }
        return t;
    }
    Table SQL_FEATURES(Session session, PersistentStore store) {
        Table t = sysTables[SQL_FEATURES];
        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SQL_FEATURES]);
            addColumn(t, "FEATURE_ID", CHARACTER_DATA);
            addColumn(t, "FEATURE_NAME", CHARACTER_DATA);
            addColumn(t, "SUB_FEATURE_ID", CHARACTER_DATA);
            addColumn(t, "SUB_FEATURE_NAME", CHARACTER_DATA);
            addColumn(t, "IS_SUPPORTED", YES_OR_NO);
            addColumn(t, "IS_VERIFIED_BY", CHARACTER_DATA);
            addColumn(t, "COMMENTS", CHARACTER_DATA);
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[SQL_FEATURES].name, false,
                SchemaObject.INDEX);
            t.createPrimaryKeyConstraint(name, new int[] {
                0, 2
            }, false);
            return t;
        }
        Session sys = database.sessionManager.newSysSession(
            SqlInvariants.INFORMATION_SCHEMA_HSQLNAME, session.getUser());
        String sql = (String) statementMap.get("/*sql_features*/");
        Result rs  = sys.executeDirectStatement(sql);
        t.insertSys(session, store, rs);
        return t;
    }
    Table SQL_IMPLEMENTATION_INFO(Session session, PersistentStore store) {
        Table t = sysTables[SQL_IMPLEMENTATION_INFO];
        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SQL_IMPLEMENTATION_INFO]);
            addColumn(t, "IMPLEMENTATION_INFO_ID", CARDINAL_NUMBER);
            addColumn(t, "IMPLEMENTATION_INFO_NAME", CHARACTER_DATA);
            addColumn(t, "INTEGER_VALUE", CARDINAL_NUMBER);
            addColumn(t, "CHARACTER_VALUE", CHARACTER_DATA);
            addColumn(t, "COMMENTS", CHARACTER_DATA);
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[SQL_IMPLEMENTATION_INFO].name, false,
                SchemaObject.INDEX);
            t.createPrimaryKeyConstraint(name, new int[]{ 0 }, false);
            return t;
        }
        Session sys = database.sessionManager.newSysSession(
            SqlInvariants.INFORMATION_SCHEMA_HSQLNAME, session.getUser());
        String sql = (String) statementMap.get("/*sql_implementation_info*/");
        Result rs  = sys.executeDirectStatement(sql);
        t.insertSys(session, store, rs);
        return t;
    }
    Table SQL_PACKAGES(Session session, PersistentStore store) {
        Table t = sysTables[SQL_PACKAGES];
        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SQL_PACKAGES]);
            addColumn(t, "ID", CHARACTER_DATA);
            addColumn(t, "NAME", CHARACTER_DATA);
            addColumn(t, "IS_SUPPORTED", YES_OR_NO);
            addColumn(t, "IS_VERIFIED_BY", CHARACTER_DATA);
            addColumn(t, "COMMENTS", CHARACTER_DATA);
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[SQL_PACKAGES].name, false,
                SchemaObject.INDEX);
            t.createPrimaryKeyConstraint(name, new int[]{ 0 }, false);
            return t;
        }
        Session sys = database.sessionManager.newSysSession(
            SqlInvariants.INFORMATION_SCHEMA_HSQLNAME, session.getUser());
        String sql = (String) statementMap.get("/*sql_packages*/");
        Result rs  = sys.executeDirectStatement(sql);
        t.insertSys(session, store, rs);
        return t;
    }
    Table SQL_PARTS(Session session, PersistentStore store) {
        Table t = sysTables[SQL_PARTS];
        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SQL_PARTS]);
            addColumn(t, "PART", CHARACTER_DATA);
            addColumn(t, "NAME", CHARACTER_DATA);
            addColumn(t, "IS_SUPPORTED", YES_OR_NO);
            addColumn(t, "IS_VERIFIED_BY", CHARACTER_DATA);
            addColumn(t, "COMMENTS", CHARACTER_DATA);
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[SQL_PARTS].name, false, SchemaObject.INDEX);
            t.createPrimaryKeyConstraint(name, new int[]{ 0 }, false);
            return t;
        }
        Session sys = database.sessionManager.newSysSession(
            SqlInvariants.INFORMATION_SCHEMA_HSQLNAME, session.getUser());
        String sql = (String) statementMap.get("/*sql_parts*/");
        Result rs  = sys.executeDirectStatement(sql);
        t.insertSys(session, store, rs);
        return t;
    }
    Table SQL_SIZING(Session session, PersistentStore store) {
        Table t = sysTables[SQL_SIZING];
        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SQL_SIZING]);
            addColumn(t, "SIZING_ID", CARDINAL_NUMBER);
            addColumn(t, "SIZING_NAME", CHARACTER_DATA);
            addColumn(t, "SUPPORTED_VALUE", CARDINAL_NUMBER);
            addColumn(t, "COMMENTS", CHARACTER_DATA);
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[SQL_SIZING].name, false, SchemaObject.INDEX);
            t.createPrimaryKeyConstraint(name, new int[]{ 0 }, false);
            return t;
        }
        Session sys = database.sessionManager.newSysSession(
            SqlInvariants.INFORMATION_SCHEMA_HSQLNAME, session.getUser());
        String sql = (String) statementMap.get("/*sql_sizing*/");
        Result rs  = sys.executeDirectStatement(sql);
        t.insertSys(session, store, rs);
        return t;
    }
    Table SQL_SIZING_PROFILES(Session session, PersistentStore store) {
        Table t = sysTables[SQL_SIZING_PROFILES];
        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SQL_SIZING_PROFILES]);
            addColumn(t, "SIZING_ID", CARDINAL_NUMBER);
            addColumn(t, "SIZING_NAME", CHARACTER_DATA);
            addColumn(t, "PROFILE_ID", CARDINAL_NUMBER);
            addColumn(t, "PROFILE_NAME", CHARACTER_DATA);
            addColumn(t, "REQUIRED_VALUE", CARDINAL_NUMBER);
            addColumn(t, "COMMENTS", CHARACTER_DATA);
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[SQL_SIZING_PROFILES].name, false,
                SchemaObject.INDEX);
            t.createPrimaryKeyConstraint(name, new int[]{ 0 }, false);
            return t;
        }
        Session sys = database.sessionManager.newSysSession(
            SqlInvariants.INFORMATION_SCHEMA_HSQLNAME, session.getUser());
        return t;
    }
    Table TABLE_CONSTRAINTS(Session session, PersistentStore store) {
        Table t = sysTables[TABLE_CONSTRAINTS];
        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[TABLE_CONSTRAINTS]);
            addColumn(t, "CONSTRAINT_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "CONSTRAINT_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "CONSTRAINT_NAME", SQL_IDENTIFIER);    
            addColumn(t, "CONSTRAINT_TYPE", CHARACTER_DATA);    
            addColumn(t, "TABLE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "TABLE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "TABLE_NAME", SQL_IDENTIFIER);         
            addColumn(t, "IS_DEFERRABLE", YES_OR_NO);           
            addColumn(t, "INITIALLY_DEFERRED", YES_OR_NO);      
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[TABLE_CONSTRAINTS].name, false,
                SchemaObject.INDEX);
            t.createPrimaryKeyConstraint(name, new int[] {
                0, 1, 2, 4, 5, 6
            }, false);
            return t;
        }
        Iterator     tables;
        Table        table;
        Constraint[] constraints;
        int          constraintCount;
        Constraint   constraint;
        String       cat;
        String       schem;
        Object[]     row;
        final int constraint_catalog = 0;
        final int constraint_schema  = 1;
        final int constraint_name    = 2;
        final int constraint_type    = 3;
        final int table_catalog      = 4;
        final int table_schema       = 5;
        final int table_name         = 6;
        final int is_deferable       = 7;
        final int initially_deferred = 8;
        tables =
            database.schemaManager.databaseObjectIterator(SchemaObject.TABLE);
        table = null;    
        while (tables.hasNext()) {
            table = (Table) tables.next();
            if (table.isView()
                    || !session.getGrantee().hasNonSelectTableRight(table)) {
                continue;
            }
            constraints     = table.getConstraints();
            constraintCount = constraints.length;
            for (int i = 0; i < constraintCount; i++) {
                constraint = constraints[i];
                row        = t.getEmptyRowData();
                switch (constraint.getConstraintType()) {
                    case SchemaObject.ConstraintTypes.CHECK : {
                        row[constraint_type] = "CHECK";
                        break;
                    }
                    case SchemaObject.ConstraintTypes.UNIQUE : {
                        row[constraint_type] = "UNIQUE";
                        break;
                    }
                    case SchemaObject.ConstraintTypes.FOREIGN_KEY : {
                        row[constraint_type] = "FOREIGN KEY";
                        table                = constraint.getRef();
                        break;
                    }
                    case SchemaObject.ConstraintTypes.PRIMARY_KEY : {
                        row[constraint_type] = "PRIMARY KEY";
                        break;
                    }
                    case SchemaObject.ConstraintTypes.MAIN :
                    default : {
                        continue;
                    }
                }
                cat                     = database.getCatalogName().name;
                schem                   = table.getSchemaName().name;
                row[constraint_catalog] = cat;
                row[constraint_schema]  = schem;
                row[constraint_name]    = constraint.getName().name;
                row[table_catalog]      = cat;
                row[table_schema]       = schem;
                row[table_name]         = table.getName().name;
                row[is_deferable]       = Tokens.T_NO;
                row[initially_deferred] = Tokens.T_NO;
                t.insertSys(session, store, row);
            }
        }
        return t;
    }
    Table TRANSLATIONS(Session session, PersistentStore store) {
        Table t = sysTables[TRANSLATIONS];
        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[TRANSLATIONS]);
            addColumn(t, "TRANSLATION_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "TRANSLATION_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "TRANSLATION_NAME", SQL_IDENTIFIER);
            addColumn(t, "SOURCE_CHARACTER_SET_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "SOURCE_CHARACTER_SET_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "SOURCE_CHARACTER_SET_NAME", SQL_IDENTIFIER);
            addColumn(t, "TARGET_CHARACTER_SET_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "TARGET_CHARACTER_SET_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "TARGET_CHARACTER_SET_NAME", SQL_IDENTIFIER);
            addColumn(t, "TRANSLATION_SOURCE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "TRANSLATION_SOURCE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "TRANSLATION_SOURCE_NAME", SQL_IDENTIFIER);
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[TRANSLATIONS].name, false,
                SchemaObject.INDEX);
            t.createPrimaryKeyConstraint(name, new int[] {
                0, 1, 2
            }, false);
            return t;
        }
        return t;
    }
    Table TRIGGER_COLUMN_USAGE(Session session, PersistentStore store) {
        Table t = sysTables[TRIGGER_COLUMN_USAGE];
        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[TRIGGER_COLUMN_USAGE]);
            addColumn(t, "TRIGGER_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "TRIGGER_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "TRIGGER_NAME", SQL_IDENTIFIER);    
            addColumn(t, "TABLE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "TABLE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "TABLE_NAME", SQL_IDENTIFIER);      
            addColumn(t, "COLUMN_NAME", SQL_IDENTIFIER);     
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[TRIGGER_COLUMN_USAGE].name, false,
                SchemaObject.INDEX);
            t.createPrimaryKeyConstraint(name, new int[] {
                0, 1, 2, 3, 4, 5, 6
            }, false);
            return t;
        }
        final int trigger_catalog = 0;
        final int trigger_schema  = 1;
        final int trigger_name    = 2;
        final int table_catalog   = 3;
        final int table_schema    = 4;
        final int table_name      = 5;
        final int column_name     = 6;
        Iterator it;
        Object[] row;
        it = database.schemaManager.databaseObjectIterator(
            SchemaObject.TRIGGER);
        while (it.hasNext()) {
            TriggerDef trigger = (TriggerDef) it.next();
            if (!session.getGrantee().isFullyAccessibleByRole(
                    trigger.getName())) {
                continue;
            }
            OrderedHashSet set = trigger.getReferences();
            for (int i = 0; i < set.size(); i++) {
                HsqlName refName = (HsqlName) set.get(i);
                if (refName.type != SchemaObject.COLUMN) {
                    continue;
                }
                if (!session.getGrantee().isAccessible(refName)) {
                    continue;
                }
                row = t.getEmptyRowData();
                row[trigger_catalog] = database.getCatalogName().name;
                row[trigger_schema]  = trigger.getSchemaName().name;
                row[trigger_name]    = trigger.getName().name;
                row[table_catalog]   = database.getCatalogName().name;
                row[table_schema]    = refName.parent.schema.name;
                row[table_name]      = refName.parent.name;
                row[column_name]     = refName.name;
                try {
                    t.insertSys(session, store, row);
                } catch (HsqlException e) {}
            }
        }
        return t;
    }
    Table TRIGGER_ROUTINE_USAGE(Session session, PersistentStore store) {
        Table t = sysTables[TRIGGER_ROUTINE_USAGE];
        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[TRIGGER_ROUTINE_USAGE]);
            addColumn(t, "TRIGGER_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "TRIGGER_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "TRIGGER_NAME", SQL_IDENTIFIER);     
            addColumn(t, "SPECIFIC_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "SPECIFIC_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "SPECIFIC_NAME", SQL_IDENTIFIER);    
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[TRIGGER_ROUTINE_USAGE].name, false,
                SchemaObject.INDEX);
            t.createPrimaryKeyConstraint(name, new int[] {
                0, 1, 2, 3, 4, 5
            }, false);
            return t;
        }
        final int trigger_catalog  = 0;
        final int trigger_schema   = 1;
        final int trigger_name     = 2;
        final int specific_catalog = 3;
        final int specific_schema  = 4;
        final int specific_name    = 5;
        Iterator it;
        Object[] row;
        it = database.schemaManager.databaseObjectIterator(
            SchemaObject.TRIGGER);
        while (it.hasNext()) {
            TriggerDef trigger = (TriggerDef) it.next();
            if (!session.getGrantee().isFullyAccessibleByRole(
                    trigger.getName())) {
                continue;
            }
            OrderedHashSet set = trigger.getReferences();
            for (int i = 0; i < set.size(); i++) {
                HsqlName refName = (HsqlName) set.get(i);
                if (refName.type != SchemaObject.SPECIFIC_ROUTINE) {
                    continue;
                }
                row                   = t.getEmptyRowData();
                row[trigger_catalog]  = database.getCatalogName().name;
                row[trigger_schema]   = trigger.getSchemaName().name;
                row[trigger_name]     = trigger.getName().name;
                row[specific_catalog] = database.getCatalogName().name;
                row[specific_schema]  = refName.schema.name;
                row[specific_name]    = refName.name;
                try {
                    t.insertSys(session, store, row);
                } catch (HsqlException e) {}
            }
        }
        return t;
    }
    Table TRIGGER_SEQUENCE_USAGE(Session session, PersistentStore store) {
        Table t = sysTables[TRIGGER_SEQUENCE_USAGE];
        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[TRIGGER_SEQUENCE_USAGE]);
            addColumn(t, "TRIGGER_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "TRIGGER_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "TRIGGER_NAME", SQL_IDENTIFIER);     
            addColumn(t, "SEQUENCE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "SEQUENCE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "SEQUENCE_NAME", SQL_IDENTIFIER);    
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[TRIGGER_SEQUENCE_USAGE].name, false,
                SchemaObject.INDEX);
            t.createPrimaryKeyConstraint(name, new int[] {
                0, 1, 2, 3, 4, 5
            }, false);
            return t;
        }
        final int trigger_catalog  = 0;
        final int trigger_schema   = 1;
        final int trigger_name     = 2;
        final int sequence_catalog = 3;
        final int sequence_schema  = 4;
        final int sequence_name    = 5;
        Iterator it;
        Object[] row;
        it = database.schemaManager.databaseObjectIterator(
            SchemaObject.TRIGGER);
        while (it.hasNext()) {
            TriggerDef trigger = (TriggerDef) it.next();
            if (!session.getGrantee().isFullyAccessibleByRole(
                    trigger.getName())) {
                continue;
            }
            OrderedHashSet set = trigger.getReferences();
            for (int i = 0; i < set.size(); i++) {
                HsqlName refName = (HsqlName) set.get(i);
                if (refName.type != SchemaObject.SEQUENCE) {
                    continue;
                }
                row                   = t.getEmptyRowData();
                row[trigger_catalog]  = database.getCatalogName().name;
                row[trigger_schema]   = trigger.getSchemaName().name;
                row[trigger_name]     = trigger.getName().name;
                row[sequence_catalog] = database.getCatalogName().name;
                row[sequence_schema]  = refName.schema.name;
                row[sequence_name]    = refName.name;
                try {
                    t.insertSys(session, store, row);
                } catch (HsqlException e) {}
            }
        }
        return t;
    }
    Table TRIGGER_TABLE_USAGE(Session session, PersistentStore store) {
        Table t = sysTables[TRIGGER_TABLE_USAGE];
        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[TRIGGER_TABLE_USAGE]);
            addColumn(t, "TRIGGER_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "TRIGGER_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "TRIGGER_NAME", SQL_IDENTIFIER);    
            addColumn(t, "TABLE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "TABLE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "TABLE_NAME", SQL_IDENTIFIER);      
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[TRIGGER_TABLE_USAGE].name, false,
                SchemaObject.INDEX);
            t.createPrimaryKeyConstraint(name, new int[] {
                0, 1, 2, 3, 4, 5
            }, false);
            return t;
        }
        final int trigger_catalog = 0;
        final int trigger_schema  = 1;
        final int trigger_name    = 2;
        final int table_catalog   = 3;
        final int table_schema    = 4;
        final int table_name      = 5;
        Iterator it;
        Object[] row;
        it = database.schemaManager.databaseObjectIterator(
            SchemaObject.TRIGGER);
        while (it.hasNext()) {
            TriggerDef trigger = (TriggerDef) it.next();
            if (!session.getGrantee().isFullyAccessibleByRole(
                    trigger.getName())) {
                continue;
            }
            OrderedHashSet set = trigger.getReferences();
            for (int i = 0; i < set.size(); i++) {
                HsqlName refName = (HsqlName) set.get(i);
                if (refName.type != SchemaObject.TABLE
                        && refName.type != SchemaObject.VIEW) {
                    continue;
                }
                row                  = t.getEmptyRowData();
                row[trigger_catalog] = database.getCatalogName().name;
                row[trigger_schema]  = trigger.getSchemaName().name;
                row[trigger_name]    = trigger.getName().name;
                row[table_catalog]   = database.getCatalogName().name;
                row[table_schema]    = refName.schema.name;
                row[table_name]      = refName.name;
                try {
                    t.insertSys(session, store, row);
                } catch (HsqlException e) {}
            }
        }
        return t;
    }
    Table TRIGGERS(Session session, PersistentStore store) {
        Table t = sysTables[TRIGGERS];
        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[TRIGGERS]);
            addColumn(t, "TRIGGER_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "TRIGGER_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "TRIGGER_NAME", SQL_IDENTIFIER);
            addColumn(t, "EVENT_MANIPULATION", SQL_IDENTIFIER);
            addColumn(t, "EVENT_OBJECT_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "EVENT_OBJECT_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "EVENT_OBJECT_TABLE", SQL_IDENTIFIER);
            addColumn(t, "ACTION_ORDER", CARDINAL_NUMBER);
            addColumn(t, "ACTION_CONDITION", CHARACTER_DATA);
            addColumn(t, "ACTION_STATEMENT", CHARACTER_DATA);
            addColumn(t, "ACTION_ORIENTATION", CHARACTER_DATA);
            addColumn(t, "ACTION_TIMING", CHARACTER_DATA);
            addColumn(t, "ACTION_REFERENCE_OLD_TABLE", SQL_IDENTIFIER);
            addColumn(t, "ACTION_REFERENCE_NEW_TABLE", SQL_IDENTIFIER);
            addColumn(t, "ACTION_REFERENCE_OLD_ROW", SQL_IDENTIFIER);
            addColumn(t, "ACTION_REFERENCE_NEW_ROW", SQL_IDENTIFIER);
            addColumn(t, "CREATED", TIME_STAMP);
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[TRIGGERS].name, false, SchemaObject.INDEX);
            t.createPrimaryKeyConstraint(name, new int[] {
                0, 1, 2
            }, false);
            return t;
        }
        final int trigger_catalog            = 0;
        final int trigger_schema             = 1;
        final int trigger_name               = 2;
        final int event_manipulation         = 3;
        final int event_object_catalog       = 4;
        final int event_object_schema        = 5;
        final int event_object_table         = 6;
        final int action_order               = 7;
        final int action_condition           = 8;
        final int action_statement           = 9;
        final int action_orientation         = 10;
        final int action_timing              = 11;
        final int action_reference_old_table = 12;
        final int action_reference_new_table = 13;
        final int action_reference_old_row   = 14;
        final int action_reference_new_row   = 15;
        final int created                    = 16;
        Iterator it;
        Object[] row;
        it = database.schemaManager.databaseObjectIterator(
            SchemaObject.TRIGGER);
        while (it.hasNext()) {
            TriggerDef trigger = (TriggerDef) it.next();
            boolean isFullAccess =
                session.getGrantee().isFullyAccessibleByRole(
                    trigger.getName());
            if (!session.getGrantee().hasNonSelectTableRight(
                    trigger.getTable())) {
                continue;
            }
            row                       = t.getEmptyRowData();
            row[trigger_catalog]      = database.getCatalogName().name;
            row[trigger_schema]       = trigger.getSchemaName().name;
            row[trigger_name]         = trigger.getName().name;
            row[event_manipulation]   = trigger.getEventTypeString();
            row[event_object_catalog] = database.getCatalogName().name;
            row[event_object_schema] = trigger.getTable().getSchemaName().name;
            row[event_object_table]   = trigger.getTable().getName().name;
            int order =
                trigger.getTable().getTriggerIndex(trigger.getName().name);
            row[action_order]       = ValuePool.getLong(order);
            row[action_condition]   = isFullAccess ? trigger.getConditionSQL()
                                                   : null;
            row[action_statement]   = isFullAccess ? trigger.getProcedureSQL()
                                                   : null;
            row[action_orientation] = trigger.getActionOrientationString();
            row[action_timing]      = trigger.getActionTimingString();
            row[action_reference_old_table] =
                trigger.getOldTransitionTableName();
            row[action_reference_new_table] =
                trigger.getNewTransitionTableName();
            row[action_reference_old_row] = trigger.getOldTransitionRowName();
            row[action_reference_new_row] = trigger.getNewTransitionRowName();
            row[created]                  = null;
            t.insertSys(session, store, row);
        }
        return t;
    }
    Table TRIGGERED_UPDATE_COLUMNS(Session session, PersistentStore store) {
        Table t = sysTables[TRIGGERED_UPDATE_COLUMNS];
        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[TRIGGERED_UPDATE_COLUMNS]);
            addColumn(t, "TRIGGER_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "TRIGGER_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "TRIGGER_NAME", SQL_IDENTIFIER);            
            addColumn(t, "EVENT_OBJECT_CATALOG", SQL_IDENTIFIER);    
            addColumn(t, "EVENT_OBJECT_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "EVENT_OBJECT_TABLE", SQL_IDENTIFIER);
            addColumn(t, "EVENT_OBJECT_COLUMN", SQL_IDENTIFIER);     
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[TRIGGERED_UPDATE_COLUMNS].name, false,
                SchemaObject.INDEX);
            t.createPrimaryKeyConstraint(name, new int[] {
                0, 1, 2, 3, 4, 5, 6
            }, false);
            return t;
        }
        final int trigger_catalog      = 0;
        final int trigger_schema       = 1;
        final int trigger_name         = 2;
        final int event_object_catalog = 3;
        final int event_object_schema  = 4;
        final int event_object_table   = 5;
        final int event_object_column  = 6;
        Iterator it;
        Object[] row;
        it = database.schemaManager.databaseObjectIterator(
            SchemaObject.TRIGGER);
        while (it.hasNext()) {
            TriggerDef trigger = (TriggerDef) it.next();
            if (!session.getGrantee().isAccessible(trigger)) {
                continue;
            }
            int[] colIndexes = trigger.getUpdateColumnIndexes();
            if (colIndexes == null) {
                continue;
            }
            for (int i = 0; i < colIndexes.length; i++) {
                ColumnSchema column =
                    trigger.getTable().getColumn(colIndexes[i]);
                row                       = t.getEmptyRowData();
                row[trigger_catalog]      = database.getCatalogName().name;
                row[trigger_schema]       = trigger.getSchemaName().name;
                row[trigger_name]         = trigger.getName().name;
                row[event_object_catalog] = database.getCatalogName().name;
                row[event_object_schema] =
                    trigger.getTable().getSchemaName().name;
                row[event_object_table]  = trigger.getTable().getName().name;
                row[event_object_column] = column.getNameString();
                t.insertSys(session, store, row);
            }
        }
        return t;
    }
    Table UDT_PRIVILEGES(Session session, PersistentStore store) {
        Table t = sysTables[UDT_PRIVILEGES];
        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[UDT_PRIVILEGES]);
            addColumn(t, "GRANTOR", SQL_IDENTIFIER);
            addColumn(t, "GRANTEE", SQL_IDENTIFIER);
            addColumn(t, "UDT_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "UDT_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "UDT_NAME", SQL_IDENTIFIER);
            addColumn(t, "PRIVILEGE_TYPE", CHARACTER_DATA);
            addColumn(t, "IS_GRANTABLE", YES_OR_NO);
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[UDT_PRIVILEGES].name, false,
                SchemaObject.INDEX);
            t.createPrimaryKeyConstraint(name, new int[] {
                0, 1, 2, 3, 4
            }, false);
            return t;
        }
        final int grantor        = 0;
        final int grantee        = 1;
        final int udt_catalog    = 2;
        final int udt_schema     = 3;
        final int udt_name       = 4;
        final int privilege_type = 5;
        final int is_grantable   = 6;
        Iterator objects =
            database.schemaManager.databaseObjectIterator(SchemaObject.TYPE);
        OrderedHashSet grantees =
            session.getGrantee().getGranteeAndAllRolesWithPublic();
        while (objects.hasNext()) {
            SchemaObject object = (SchemaObject) objects.next();
            if (object.getType() != SchemaObject.TYPE) {
                continue;
            }
            for (int i = 0; i < grantees.size(); i++) {
                Grantee granteeObject = (Grantee) grantees.get(i);
                OrderedHashSet rights =
                    granteeObject.getAllDirectPrivileges(object);
                OrderedHashSet grants =
                    granteeObject.getAllGrantedPrivileges(object);
                if (!grants.isEmpty()) {
                    grants.addAll(rights);
                    rights = grants;
                }
                for (int j = 0; j < rights.size(); j++) {
                    Right    right          = (Right) rights.get(j);
                    Right    grantableRight = right.getGrantableRights();
                    Object[] row;
                    row                 = t.getEmptyRowData();
                    row[grantor]        = right.getGrantor().getName().name;
                    row[grantee]        = right.getGrantee().getName().name;
                    row[udt_catalog]    = database.getCatalogName().name;
                    row[udt_schema]     = object.getSchemaName().name;
                    row[udt_name]       = object.getName().name;
                    row[privilege_type] = Tokens.T_USAGE;
                    row[is_grantable] =
                        right.getGrantee() == object.getOwner()
                        || grantableRight.isFull() ? Tokens.T_YES
                                                   : Tokens.T_NO;;
                    try {
                        t.insertSys(session, store, row);
                    } catch (HsqlException e) {}
                }
            }
        }
        return t;
    }
    Table USAGE_PRIVILEGES(Session session, PersistentStore store) {
        Table t = sysTables[USAGE_PRIVILEGES];
        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[USAGE_PRIVILEGES]);
            addColumn(t, "GRANTOR", SQL_IDENTIFIER);        
            addColumn(t, "GRANTEE", SQL_IDENTIFIER);        
            addColumn(t, "OBJECT_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "OBJECT_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "OBJECT_NAME", SQL_IDENTIFIER);    
            addColumn(t, "OBJECT_TYPE", CHARACTER_DATA);    
            addColumn(t, "PRIVILEGE_TYPE", CHARACTER_DATA);
            addColumn(t, "IS_GRANTABLE", YES_OR_NO);        
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[USAGE_PRIVILEGES].name, false,
                SchemaObject.INDEX);
            t.createPrimaryKeyConstraint(name, new int[] {
                0, 1, 2, 3, 4, 5, 6, 7
            }, false);
            return t;
        }
        Object[] row;
        final int grantor        = 0;
        final int grantee        = 1;
        final int object_catalog = 2;
        final int object_schema  = 3;
        final int object_name    = 4;
        final int object_type    = 5;
        final int privilege_type = 6;
        final int is_grantable   = 7;
        Iterator objects =
            new WrapperIterator(database.schemaManager
                .databaseObjectIterator(SchemaObject.SEQUENCE), database
                .schemaManager.databaseObjectIterator(SchemaObject.COLLATION));
        objects = new WrapperIterator(
            objects,
            database.schemaManager.databaseObjectIterator(
                SchemaObject.CHARSET));
        objects = new WrapperIterator(
            objects,
            database.schemaManager.databaseObjectIterator(
                SchemaObject.DOMAIN));
        OrderedHashSet grantees =
            session.getGrantee().getGranteeAndAllRolesWithPublic();
        while (objects.hasNext()) {
            SchemaObject object = (SchemaObject) objects.next();
            for (int i = 0; i < grantees.size(); i++) {
                Grantee granteeObject = (Grantee) grantees.get(i);
                OrderedHashSet rights =
                    granteeObject.getAllDirectPrivileges(object);
                OrderedHashSet grants =
                    granteeObject.getAllGrantedPrivileges(object);
                if (!grants.isEmpty()) {
                    grants.addAll(rights);
                    rights = grants;
                }
                for (int j = 0; j < rights.size(); j++) {
                    Right right          = (Right) rights.get(j);
                    Right grantableRight = right.getGrantableRights();
                    row                 = t.getEmptyRowData();
                    row[grantor]        = right.getGrantor().getName().name;
                    row[grantee]        = right.getGrantee().getName().name;
                    row[object_catalog] = database.getCatalogName().name;
                    row[object_schema]  = object.getSchemaName().name;
                    row[object_name]    = object.getName().name;
                    row[object_type] =
                        SchemaObjectSet.getName(object.getName().type);
                    row[privilege_type] = Tokens.T_USAGE;
                    row[is_grantable] =
                        right.getGrantee() == object.getOwner()
                        || grantableRight.isFull() ? Tokens.T_YES
                                                   : Tokens.T_NO;;
                    try {
                        t.insertSys(session, store, row);
                    } catch (HsqlException e) {}
                }
            }
        }
        return t;
    }
    Table USER_DEFINED_TYPES(Session session, PersistentStore store) {
        Table t = sysTables[USER_DEFINED_TYPES];
        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[USER_DEFINED_TYPES]);
            addColumn(t, "USER_DEFINED_TYPE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "USER_DEFINED_TYPE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "USER_DEFINED_TYPE_NAME", SQL_IDENTIFIER);
            addColumn(t, "USER_DEFINED_TYPE_CATEGORY", SQL_IDENTIFIER);
            addColumn(t, "IS_INSTANTIABLE", YES_OR_NO);
            addColumn(t, "IS_FINAL", YES_OR_NO);
            addColumn(t, "ORDERING_FORM", SQL_IDENTIFIER);
            addColumn(t, "ORDERING_CATEGORY", SQL_IDENTIFIER);
            addColumn(t, "ORDERING_ROUTINE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "ORDERING_ROUTINE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "ORDERING_ROUTINE_NAME", SQL_IDENTIFIER);
            addColumn(t, "REFERENCE_TYPE", SQL_IDENTIFIER);
            addColumn(t, "DATA_TYPE", CHARACTER_DATA);
            addColumn(t, "CHARACTER_MAXIMUM_LENGTH", CARDINAL_NUMBER);
            addColumn(t, "CHARACTER_OCTET_LENGTH", CARDINAL_NUMBER);
            addColumn(t, "CHARACTER_SET_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "CHARACTER_SET_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "CHARACTER_SET_NAME", SQL_IDENTIFIER);
            addColumn(t, "COLLATION_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "COLLATION_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "COLLATION_NAME", SQL_IDENTIFIER);
            addColumn(t, "NUMERIC_PRECISION", CARDINAL_NUMBER);
            addColumn(t, "NUMERIC_PRECISION_RADIX", CARDINAL_NUMBER);
            addColumn(t, "NUMERIC_SCALE", CARDINAL_NUMBER);
            addColumn(t, "DATETIME_PRECISION", CARDINAL_NUMBER);
            addColumn(t, "INTERVAL_TYPE", CHARACTER_DATA);
            addColumn(t, "INTERVAL_PRECISION", CARDINAL_NUMBER);
            addColumn(t, "SOURCE_DTD_IDENTIFIER", CHARACTER_DATA);
            addColumn(t, "REF_DTD_IDENTIFIER", CHARACTER_DATA);
            addColumn(t, "DECLARED_DATA_TYPE", CHARACTER_DATA);
            addColumn(t, "DECLARED_NUMERIC_PRECISION", CARDINAL_NUMBER);
            addColumn(t, "DECLARED_NUMERIC_SCALE", CARDINAL_NUMBER);
            addColumn(t, "MAXIMUM_CARDINALITY", CARDINAL_NUMBER);
            addColumn(t, "EXTERNAL_NAME", CHARACTER_DATA);
            addColumn(t, "EXTERNAL_LANGUAGE", CHARACTER_DATA);
            addColumn(t, "JAVA_INTERFACE", CHARACTER_DATA);
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[USER_DEFINED_TYPES].name, false,
                SchemaObject.INDEX);
            t.createPrimaryKeyConstraint(name, new int[] {
                0, 1, 2, 4, 5, 6
            }, false);
            return t;
        }
        final int user_defined_type_catalog  = 0;
        final int user_defined_type_schema   = 1;
        final int user_defined_type_name     = 2;
        final int user_defined_type_category = 3;
        final int is_instantiable            = 4;
        final int is_final                   = 5;
        final int ordering_form              = 6;
        final int ordering_category          = 7;
        final int ordering_routine_catalog   = 8;
        final int ordering_routine_schema    = 9;
        final int ordering_routine_name      = 10;
        final int reference_type             = 11;
        final int data_type                  = 12;
        final int character_maximum_length   = 13;
        final int character_octet_length     = 14;
        final int character_set_catalog      = 15;
        final int character_set_schema       = 16;
        final int character_set_name         = 17;
        final int collation_catalog          = 18;
        final int collation_schema           = 19;
        final int collation_name             = 20;
        final int numeric_precision          = 21;
        final int numeric_precision_radix    = 22;
        final int numeric_scale              = 23;
        final int datetime_precision         = 24;
        final int interval_type              = 25;
        final int interval_precision         = 26;
        final int source_dtd_identifier      = 27;
        final int ref_dtd_identifier         = 28;
        final int declared_data_type         = 29;
        final int declared_numeric_precision = 30;
        final int declared_numeric_scale     = 31;
        final int maximum_cardinality        = 32;
        Iterator it =
            database.schemaManager.databaseObjectIterator(SchemaObject.TYPE);
        while (it.hasNext()) {
            Type type = (Type) it.next();
            if (!type.isDistinctType()) {
                continue;
            }
            Object[] row = t.getEmptyRowData();
            row[user_defined_type_catalog]  = database.getCatalogName().name;
            row[user_defined_type_schema]   = type.getSchemaName().name;
            row[user_defined_type_name]     = type.getName().name;
            row[data_type]                  = type.getFullNameString();
            row[user_defined_type_category] = "DISTINCT";
            row[is_instantiable]            = "YES";
            row[is_final]                   = "YES";
            row[ordering_form]              = "FULL";
            if (type.isCharacterType()) {
                row[character_maximum_length] =
                    ValuePool.getLong(type.precision);
                row[character_octet_length] = ValuePool.getLong(type.precision
                        * 2);
                row[character_set_catalog] = database.getCatalogName().name;
                row[character_set_schema] =
                    ((CharacterType) type).getCharacterSet().getSchemaName()
                        .name;
                row[character_set_name] =
                    ((CharacterType) type).getCharacterSet().getName().name;
                row[collation_catalog] = database.getCatalogName().name;
                row[collation_schema] =
                    ((CharacterType) type).getCollation().getSchemaName().name;
                row[collation_name] =
                    ((CharacterType) type).getCollation().getName().name;
            } else if (type.isNumberType()) {
                row[numeric_precision] = ValuePool.getLong(
                    ((NumberType) type).getNumericPrecisionInRadix());
                row[declared_numeric_precision] = ValuePool.getLong(
                    ((NumberType) type).getNumericPrecisionInRadix());
                if (type.isExactNumberType()) {
                    row[numeric_scale] = row[declared_numeric_scale] =
                        ValuePool.getLong(type.scale);
                }
                row[numeric_precision_radix] =
                    ValuePool.getLong(type.getPrecisionRadix());
            } else if (type.isBooleanType()) {}
            else if (type.isDateTimeType()) {
                row[datetime_precision] = ValuePool.getLong(type.scale);
            } else if (type.isIntervalType()) {
                row[data_type] = "INTERVAL";
                row[interval_type] =
                    ((IntervalType) type).getQualifier(type.typeCode);
                row[interval_precision] = ValuePool.getLong(type.precision);
                row[datetime_precision] = ValuePool.getLong(type.scale);
            } else if (type.isBinaryType()) {
                row[character_maximum_length] =
                    ValuePool.getLong(type.precision);
                row[character_octet_length] =
                    ValuePool.getLong(type.precision);
            } else if (type.isBitType()) {
                row[character_maximum_length] =
                    ValuePool.getLong(type.precision);
                row[character_octet_length] =
                    ValuePool.getLong(type.precision);
            } else if (type.isArrayType()) {
                row[maximum_cardinality] =
                    ValuePool.getLong(type.arrayLimitCardinality());
                row[data_type] = "ARRAY";
            }
            row[source_dtd_identifier] = type.getDefinition();
            row[declared_data_type]    = row[data_type];
            t.insertSys(session, store, row);
        }
        return t;
    }
    Table VIEW_COLUMN_USAGE(Session session, PersistentStore store) {
        Table t = sysTables[VIEW_COLUMN_USAGE];
        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[VIEW_COLUMN_USAGE]);
            addColumn(t, "VIEW_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "VIEW_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "VIEW_NAME", SQL_IDENTIFIER);
            addColumn(t, "TABLE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "TABLE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "TABLE_NAME", SQL_IDENTIFIER);
            addColumn(t, "COLUMN_NAME", SQL_IDENTIFIER);
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[VIEW_COLUMN_USAGE].name, false,
                SchemaObject.INDEX);
            t.createPrimaryKeyConstraint(name, new int[] {
                0, 1, 2, 3, 4, 5, 6
            }, false);
            return t;
        }
        String viewCatalog;
        String viewSchema;
        String viewName;
        Iterator tables;
        View     view;
        Table    table;
        Object[] row;
        Iterator iterator;
        final int view_catalog  = 0;
        final int view_schema   = 1;
        final int view_name     = 2;
        final int table_catalog = 3;
        final int table_schema  = 4;
        final int table_name    = 5;
        final int column_name   = 6;
        tables =
            database.schemaManager.databaseObjectIterator(SchemaObject.TABLE);
        while (tables.hasNext()) {
            table = (Table) tables.next();
            if (table.isView()
                    && session.getGrantee().isFullyAccessibleByRole(
                        table.getName())) {
            } else {
                continue;
            }
            viewCatalog = database.getCatalogName().name;
            viewSchema  = table.getSchemaName().name;
            viewName    = table.getName().name;
            view        = (View) table;
            OrderedHashSet references = view.getReferences();
            iterator = references.iterator();
            while (iterator.hasNext()) {
                HsqlName refName = (HsqlName) iterator.next();
                if (refName.type != SchemaObject.COLUMN) {
                    continue;
                }
                row                = t.getEmptyRowData();
                row[view_catalog]  = viewCatalog;
                row[view_schema]   = viewSchema;
                row[view_name]     = viewName;
                row[table_catalog] = viewCatalog;
                row[table_schema]  = refName.parent.schema.name;
                row[table_name]    = refName.parent.name;
                row[column_name]   = refName.name;
                try {
                    t.insertSys(session, store, row);
                } catch (HsqlException e) {}
            }
        }
        return t;
    }
    Table VIEW_ROUTINE_USAGE(Session session, PersistentStore store) {
        Table t = sysTables[VIEW_ROUTINE_USAGE];
        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[VIEW_ROUTINE_USAGE]);
            addColumn(t, "VIEW_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "VIEW_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "VIEW_NAME", SQL_IDENTIFIER);
            addColumn(t, "SPECIFIC_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "SPECIFIC_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "SPECIFIC_NAME", SQL_IDENTIFIER);
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[VIEW_ROUTINE_USAGE].name, false,
                SchemaObject.INDEX);
            t.createPrimaryKeyConstraint(name, new int[] {
                0, 1, 2, 3, 4, 5
            }, false);
            return t;
        }
        Iterator tables;
        Table    table;
        Object[] row;
        final int view_catalog     = 0;
        final int view_schema      = 1;
        final int view_name        = 2;
        final int specific_catalog = 3;
        final int specific_schema  = 4;
        final int specific_name    = 5;
        tables =
            database.schemaManager.databaseObjectIterator(SchemaObject.TABLE);
        while (tables.hasNext()) {
            table = (Table) tables.next();
            if (!table.isView()) {
                continue;
            }
            OrderedHashSet set = table.getReferences();
            for (int i = 0; i < set.size(); i++) {
                HsqlName refName = (HsqlName) set.get(i);
                if (refName.type != SchemaObject.SPECIFIC_ROUTINE) {
                    continue;
                }
                if (!session.getGrantee().isFullyAccessibleByRole(refName)) {
                    continue;
                }
                row                   = t.getEmptyRowData();
                row[view_catalog]     = database.getCatalogName().name;
                row[view_schema]      = table.getSchemaName().name;
                row[view_name]        = table.getName().name;
                row[specific_catalog] = database.getCatalogName().name;
                row[specific_schema]  = refName.schema.name;
                row[specific_name]    = refName.name;
                try {
                    t.insertSys(session, store, row);
                } catch (HsqlException e) {}
            }
        }
        return t;
    }
    Table VIEW_TABLE_USAGE(Session session, PersistentStore store) {
        Table t = sysTables[VIEW_TABLE_USAGE];
        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[VIEW_TABLE_USAGE]);
            addColumn(t, "VIEW_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "VIEW_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "VIEW_NAME", SQL_IDENTIFIER);     
            addColumn(t, "TABLE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "TABLE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "TABLE_NAME", SQL_IDENTIFIER);    
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[VIEW_TABLE_USAGE].name, false,
                SchemaObject.INDEX);
            t.createPrimaryKeyConstraint(name, new int[] {
                0, 1, 2, 3, 4, 5
            }, false);
            return t;
        }
        final int view_catalog  = 0;
        final int view_schema   = 1;
        final int view_name     = 2;
        final int table_catalog = 3;
        final int table_schema  = 4;
        final int table_name    = 5;
        Iterator tables;
        Table    table;
        Object[] row;
        tables =
            database.schemaManager.databaseObjectIterator(SchemaObject.TABLE);
        while (tables.hasNext()) {
            table = (Table) tables.next();
            if (!table.isView()) {
                continue;
            }
            OrderedHashSet references = table.getReferences();
            for (int i = 0; i < references.size(); i++) {
                HsqlName refName = (HsqlName) references.get(i);
                if (refName.type != SchemaObject.TABLE
                        && refName.type != SchemaObject.VIEW) {
                    continue;
                }
                if (!session.getGrantee().isFullyAccessibleByRole(refName)) {
                    continue;
                }
                row                = t.getEmptyRowData();
                row[view_catalog]  = database.getCatalogName().name;
                row[view_schema]   = table.getSchemaName().name;
                row[view_name]     = table.getName().name;
                row[table_catalog] = database.getCatalogName().name;
                row[table_schema]  = refName.schema.name;
                row[table_name]    = refName.name;
                try {
                    t.insertSys(session, store, row);
                } catch (HsqlException e) {}
            }
        }
        return t;
    }
    Table VIEWS(Session session, PersistentStore store) {
        Table t = sysTables[VIEWS];
        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[VIEWS]);
            addColumn(t, "TABLE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "TABLE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "TABLE_NAME", SQL_IDENTIFIER);               
            addColumn(t, "VIEW_DEFINITION", CHARACTER_DATA);          
            addColumn(t, "CHECK_OPTION", CHARACTER_DATA);             
            addColumn(t, "IS_UPDATABLE", YES_OR_NO);                  
            addColumn(t, "INSERTABLE_INTO", YES_OR_NO);               
            addColumn(t, "IS_TRIGGER_UPDATABLE", YES_OR_NO);          
            addColumn(t, "IS_TRIGGER_DELETABLE", YES_OR_NO);          
            addColumn(t, "IS_TRIGGER_INSERTABLE_INTO", YES_OR_NO);    
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[VIEWS].name, false, SchemaObject.INDEX);
            t.createPrimaryKeyConstraint(name, new int[] {
                1, 2, 0
            }, false);
            return t;
        }
        Iterator  tables;
        Table     table;
        Object[]  row;
        final int table_catalog              = 0;
        final int table_schema               = 1;
        final int table_name                 = 2;
        final int view_definition            = 3;
        final int check_option               = 4;
        final int is_updatable               = 5;
        final int insertable_into            = 6;
        final int is_trigger_updatable       = 7;
        final int is_trigger_deletable       = 8;
        final int is_trigger_insertable_into = 9;
        tables = allTables();
        while (tables.hasNext()) {
            table = (Table) tables.next();
            if (!table.isView()
                    && table.getSchemaName()
                       != SqlInvariants.INFORMATION_SCHEMA_HSQLNAME) {
                continue;
            }
            if (!isAccessibleTable(session, table)) {
                continue;
            }
            row                = t.getEmptyRowData();
            row[table_catalog] = database.getCatalogName().name;
            row[table_schema]  = table.getSchemaName().name;
            row[table_name]    = table.getName().name;
            String check = Tokens.T_NONE;
            if (table instanceof View) {
                if (session.getGrantee().isFullyAccessibleByRole(
                        table.getName())) {
                    row[view_definition] = ((View) table).getStatement();
                }
                switch (((View) table).getCheckOption()) {
                    case SchemaObject.ViewCheckModes.CHECK_NONE :
                        break;
                    case SchemaObject.ViewCheckModes.CHECK_LOCAL :
                        check = Tokens.T_LOCAL;
                        break;
                    case SchemaObject.ViewCheckModes.CHECK_CASCADE :
                        check = Tokens.T_CASCADED;
                        break;
                }
            }
            row[check_option]    = check;
            row[is_updatable]    = table.isUpdatable() ? Tokens.T_YES
                                                       : Tokens.T_NO;
            row[insertable_into] = table.isInsertable() ? Tokens.T_YES
                                                        : Tokens.T_NO;
            row[is_trigger_updatable] = table.isTriggerUpdatable()
                                        ? Tokens.T_YES
                                        : Tokens.T_NO;;
            row[is_trigger_deletable] = table.isTriggerDeletable()
                                        ? Tokens.T_YES
                                        : Tokens.T_NO;;
            row[is_trigger_insertable_into] = table.isTriggerInsertable()
                                              ? Tokens.T_YES
                                              : Tokens.T_NO;;
            t.insertSys(session, store, row);
        }
        return t;
    }
    Table ROLE_AUTHORIZATION_DESCRIPTORS(Session session,
                                         PersistentStore store) {
        Table t = sysTables[ROLE_AUTHORIZATION_DESCRIPTORS];
        if (t == null) {
            t = createBlankTable(
                sysTableHsqlNames[ROLE_AUTHORIZATION_DESCRIPTORS]);
            addColumn(t, "ROLE_NAME", SQL_IDENTIFIER);    
            addColumn(t, "GRANTEE", SQL_IDENTIFIER);      
            addColumn(t, "GRANTOR", SQL_IDENTIFIER);      
            addColumn(t, "IS_GRANTABLE", YES_OR_NO);      
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[ROLE_AUTHORIZATION_DESCRIPTORS].name, false,
                SchemaObject.INDEX);
            t.createPrimaryKeyConstraint(name, new int[] {
                0, 1
            }, true);
            return t;
        }
        String   grantorName = SqlInvariants.SYSTEM_AUTHORIZATION_NAME;
        Iterator grantees;
        Grantee  granteeObject;
        String   granteeName;
        Iterator roles;
        String   isGrantable;
        Object[] row;
        final int role_name    = 0;
        final int grantee      = 1;
        final int grantor      = 2;
        final int is_grantable = 3;
        grantees = session.getGrantee().visibleGrantees().iterator();
        while (grantees.hasNext()) {
            granteeObject = (Grantee) grantees.next();
            granteeName   = granteeObject.getName().getNameString();
            roles         = granteeObject.getDirectRoles().iterator();
            isGrantable   = granteeObject.isAdmin() ? Tokens.T_YES
                                                    : Tokens.T_NO;;
            while (roles.hasNext()) {
                Grantee role = (Grantee) roles.next();
                row               = t.getEmptyRowData();
                row[role_name]    = role.getName().getNameString();
                row[grantee]      = granteeName;
                row[grantor]      = grantorName;
                row[is_grantable] = isGrantable;
                t.insertSys(session, store, row);
            }
        }
        return t;
    }
}