


package org.hsqldb.dbinfo;

import org.hsqldb.ColumnSchema;
import org.hsqldb.Constraint;
import org.hsqldb.Database;
import org.hsqldb.HsqlException;
import org.hsqldb.HsqlNameManager;
import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.NumberSequence;
import org.hsqldb.Routine;
import org.hsqldb.RoutineSchema;
import org.hsqldb.SchemaObject;
import org.hsqldb.Session;
import org.hsqldb.SqlInvariants;
import org.hsqldb.Table;
import org.hsqldb.TableBase;
import org.hsqldb.Tokens;
import org.hsqldb.TypeInvariants;
import org.hsqldb.index.Index;
import org.hsqldb.lib.HashSet;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.Iterator;
import org.hsqldb.lib.OrderedHashSet;
import org.hsqldb.lib.WrapperIterator;
import org.hsqldb.persist.HsqlDatabaseProperties;
import org.hsqldb.persist.HsqlProperties;
import org.hsqldb.persist.PersistentStore;
import org.hsqldb.rights.GrantConstants;
import org.hsqldb.rights.Grantee;
import org.hsqldb.rights.GranteeManager;
import org.hsqldb.rights.Right;
import org.hsqldb.rights.User;
import org.hsqldb.store.ValuePool;
import org.hsqldb.types.DateTimeType;
import org.hsqldb.types.IntervalType;
import org.hsqldb.types.NumberType;
import org.hsqldb.types.Type;
import org.hsqldb.types.Types;















class DatabaseInformationMain extends DatabaseInformation {

    static Type CARDINAL_NUMBER = TypeInvariants.CARDINAL_NUMBER;
    static Type YES_OR_NO       = TypeInvariants.YES_OR_NO;
    static Type CHARACTER_DATA  = TypeInvariants.CHARACTER_DATA;
    static Type SQL_IDENTIFIER  = TypeInvariants.SQL_IDENTIFIER;
    static Type TIME_STAMP      = TypeInvariants.TIME_STAMP;

    
    protected static final HsqlName[] sysTableHsqlNames;

    
    protected static final boolean[] sysTableSessionDependent =
        new boolean[sysTableNames.length];

    
    protected static final HashSet nonCachedTablesSet;

    
    protected static final String[] tableTypes = new String[] {
        "GLOBAL TEMPORARY", "SYSTEM TABLE", "TABLE", "VIEW"
    };

    
    static {
        synchronized (DatabaseInformationMain.class) {
            nonCachedTablesSet = new HashSet();
            sysTableHsqlNames  = new HsqlName[sysTableNames.length];

            for (int i = 0; i < sysTableNames.length; i++) {
                sysTableHsqlNames[i] =
                    HsqlNameManager.newInfoSchemaTableName(sysTableNames[i]);
                sysTableHsqlNames[i].schema =
                    SqlInvariants.INFORMATION_SCHEMA_HSQLNAME;
                sysTableSessionDependent[i] = true;
            }

            
            nonCachedTablesSet.add("SYSTEM_CACHEINFO");
            nonCachedTablesSet.add("SYSTEM_SESSIONINFO");
            nonCachedTablesSet.add("SYSTEM_SESSIONS");
            nonCachedTablesSet.add("SYSTEM_PROPERTIES");
            nonCachedTablesSet.add("SYSTEM_SEQUENCES");
        }
    }

    
    protected final Table[] sysTables = new Table[sysTableNames.length];

    
    DatabaseInformationMain(Database db) {

        super(db);

        Session session = db.sessionManager.getSysSession();

        init(session);
    }

    protected final void addColumn(Table t, String name, Type type) {

        HsqlName     cn;
        ColumnSchema c;

        cn = database.nameManager.newInfoSchemaColumnName(name, t.getName());
        c  = new ColumnSchema(cn, type, true, false, null);

        t.addColumn(c);
    }

    
    protected final Iterator allTables() {

        return new WrapperIterator(
            database.schemaManager.databaseObjectIterator(SchemaObject.TABLE),
            new WrapperIterator(sysTables, true));
    }

    
    protected final void cacheClear(Session session) {

        int i = sysTables.length;

        while (i-- > 0) {
            Table t = sysTables[i];

            if (t != null) {
                t.clearAllData(session);
            }
        }
    }

    
    protected Table generateTable(Session session, PersistentStore store,
                                  int tableIndex) {


































        switch (tableIndex) {

            case SYSTEM_BESTROWIDENTIFIER :
                return SYSTEM_BESTROWIDENTIFIER(session, store);

            case SYSTEM_COLUMNS :
                return SYSTEM_COLUMNS(session, store);

            case SYSTEM_CONNECTION_PROPERTIES :
                return SYSTEM_CONNECTION_PROPERTIES(session, store);

            case SYSTEM_CROSSREFERENCE :
                return SYSTEM_CROSSREFERENCE(session, store);

            case SYSTEM_INDEXINFO :
                return SYSTEM_INDEXINFO(session, store);

            case SYSTEM_PRIMARYKEYS :
                return SYSTEM_PRIMARYKEYS(session, store);

            case SYSTEM_PROCEDURECOLUMNS :
                return SYSTEM_PROCEDURECOLUMNS(session, store);

            case SYSTEM_PROCEDURES :
                return SYSTEM_PROCEDURES(session, store);

            case SYSTEM_SCHEMAS :
                return SYSTEM_SCHEMAS(session, store);

            case SYSTEM_SEQUENCES :
                return SYSTEM_SEQUENCES(session, store);

            case SYSTEM_TABLES :
                return SYSTEM_TABLES(session, store);

            case SYSTEM_TABLETYPES :
                return SYSTEM_TABLETYPES(session, store);

            case SYSTEM_TYPEINFO :
                return SYSTEM_TYPEINFO(session, store);

            case SYSTEM_USERS :
                return SYSTEM_USERS(session, store);

            case SYSTEM_UDTS :
                return SYSTEM_UDTS(session, store);

            case SYSTEM_VERSIONCOLUMNS :
                return SYSTEM_VERSIONCOLUMNS(session, store);

            case COLUMN_PRIVILEGES :
                return COLUMN_PRIVILEGES(session, store);

            case SEQUENCES :
                return SEQUENCES(session, store);

            case TABLE_PRIVILEGES :
                return TABLE_PRIVILEGES(session, store);

            case INFORMATION_SCHEMA_CATALOG_NAME :
                return INFORMATION_SCHEMA_CATALOG_NAME(session, store);

            default :
                return null;
        }
    }

    
    protected final void init(Session session) {

        
        Table t;

        for (int i = 0; i < sysTables.length; i++) {
            t = sysTables[i] = generateTable(session, null, i);

            if (t != null) {
                t.setDataReadOnly(true);
            }
        }

        GranteeManager gm    = database.getGranteeManager();
        Right          right = new Right();

        right.set(GrantConstants.SELECT, null);

        for (int i = 0; i < sysTableHsqlNames.length; i++) {
            if (sysTables[i] != null) {
                gm.grantSystemToPublic(sysTables[i], right);
            }
        }

        right = Right.fullRights;

        gm.grantSystemToPublic(TypeInvariants.YES_OR_NO, right);
        gm.grantSystemToPublic(TypeInvariants.TIME_STAMP, right);
        gm.grantSystemToPublic(TypeInvariants.CARDINAL_NUMBER, right);
        gm.grantSystemToPublic(TypeInvariants.CHARACTER_DATA, right);
        gm.grantSystemToPublic(TypeInvariants.SQL_CHARACTER, right);
        gm.grantSystemToPublic(TypeInvariants.SQL_IDENTIFIER_CHARSET, right);
        gm.grantSystemToPublic(TypeInvariants.SQL_IDENTIFIER, right);
        gm.grantSystemToPublic(TypeInvariants.SQL_TEXT, right);
    }

    
    protected final boolean isAccessibleTable(Session session, Table table) {
        return session.getGrantee().isAccessible(table);
    }

    
    protected final Table createBlankTable(HsqlName name) {

        Table table = new Table(database, name, TableBase.INFO_SCHEMA_TABLE);

        return table;
    }

    
    public final Table getSystemTable(Session session, String name) {

        Table t;
        int   tableIndex;

        if (!isSystemTable(name)) {
            return null;
        }

        tableIndex = getSysTableID(name);
        t          = sysTables[tableIndex];

        
        if (t == null) {
            return t;
        }

        
        
        
        
        if (!withContent) {
            return t;
        }

        return t;
    }

    public boolean isNonCachedTable(String name) {
        return nonCachedTablesSet.contains(name);
    }

    public final void setStore(Session session, Table table,
                               PersistentStore store) {

        long dbscts = database.schemaManager.getSchemaChangeTimestamp();

        if (store.getTimestamp() == dbscts
                && !isNonCachedTable(table.getName().name)) {
            return;
        }

        
        store.removeAll();
        store.setTimestamp(dbscts);

        int tableIndex = getSysTableID(table.getName().name);

        generateTable(session, store, tableIndex);
    }

    
    final Table SYSTEM_BESTROWIDENTIFIER(Session session,
                                         PersistentStore store) {

        Table t = sysTables[SYSTEM_BESTROWIDENTIFIER];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SYSTEM_BESTROWIDENTIFIER]);

            addColumn(t, "SCOPE", Type.SQL_SMALLINT);            
            addColumn(t, "COLUMN_NAME", SQL_IDENTIFIER);         
            addColumn(t, "DATA_TYPE", Type.SQL_SMALLINT);        
            addColumn(t, "TYPE_NAME", SQL_IDENTIFIER);           
            addColumn(t, "COLUMN_SIZE", Type.SQL_INTEGER);
            addColumn(t, "BUFFER_LENGTH", Type.SQL_INTEGER);
            addColumn(t, "DECIMAL_DIGITS", Type.SQL_SMALLINT);
            addColumn(t, "PSEUDO_COLUMN", Type.SQL_SMALLINT);    
            addColumn(t, "TABLE_CAT", SQL_IDENTIFIER);
            addColumn(t, "TABLE_SCHEM", SQL_IDENTIFIER);
            addColumn(t, "TABLE_NAME", SQL_IDENTIFIER);          
            addColumn(t, "NULLABLE", Type.SQL_SMALLINT);         
            addColumn(t, "IN_KEY", Type.SQL_BOOLEAN);            

            
            
            
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[SYSTEM_BESTROWIDENTIFIER].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKeyConstraint(name, new int[] {
                0, 8, 9, 10, 1
            }, false);

            return t;
        }

        
        Integer scope;           
        Integer pseudo;

        
        
        
        
        
        
        
        String  tableCatalog;    
        String  tableSchema;     
        String  tableName;       
        Boolean inKey;           

        

        
        
        
        Iterator       tables;
        Table          table;
        DITableInfo    ti;
        int[]          cols;
        Object[]       row;
        HsqlProperties p;

        
        final int iscope          = 0;
        final int icolumn_name    = 1;
        final int idata_type      = 2;
        final int itype_name      = 3;
        final int icolumn_size    = 4;
        final int ibuffer_length  = 5;
        final int idecimal_digits = 6;
        final int ipseudo_column  = 7;
        final int itable_cat      = 8;
        final int itable_schem    = 9;
        final int itable_name     = 10;
        final int inullable       = 11;
        final int iinKey          = 12;

        
        ti = new DITableInfo();
        tables =
            database.schemaManager.databaseObjectIterator(SchemaObject.TABLE);

        boolean translateTTI = database.sqlTranslateTTI;

        
        while (tables.hasNext()) {
            table = (Table) tables.next();

            
            if (table.isView() || !isAccessibleTable(session, table)) {
                continue;
            }

            cols = table.getBestRowIdentifiers();

            if (cols == null) {
                continue;
            }

            ti.setTable(table);

            inKey = ValuePool.getBoolean(table.isBestRowIdentifiersStrict());
            tableCatalog = table.getCatalogName().name;
            tableSchema  = table.getSchemaName().name;
            tableName    = table.getName().name;

            Type[] types = table.getColumnTypes();

            scope  = ti.getBRIScope();
            pseudo = ti.getBRIPseudo();

            for (int i = 0; i < cols.length; i++) {
                ColumnSchema column = table.getColumn(i);
                Type         type   = types[i];

                if (translateTTI) {
                    if (type.isIntervalType()) {
                        type = ((IntervalType) type).getCharacterType();
                    } else if (type.isDateTimeTypeWithZone()) {
                        type = ((DateTimeType) type)
                            .getDateTimeTypeWithoutZone();
                    }
                }

                row                 = t.getEmptyRowData();
                row[iscope]         = scope;
                row[icolumn_name]   = column.getName().name;
                row[idata_type]     = ValuePool.getInt(type.getJDBCTypeCode());
                row[itype_name]     = type.getNameString();
                row[icolumn_size] = ValuePool.getInt(type.getJDBCPrecision());
                row[ibuffer_length] = null;
                row[idecimal_digits] = type.acceptsScale()
                                       ? ValuePool.getInt(type.getJDBCScale())
                                       : null;
                row[ipseudo_column] = pseudo;
                row[itable_cat]     = tableCatalog;
                row[itable_schem]   = tableSchema;
                row[itable_name]    = tableName;
                row[inullable] = ValuePool.getInt(column.getNullability());
                row[iinKey]         = inKey;

                t.insertSys(session, store, row);
            }
        }

        return t;
    }

    
    final Table SYSTEM_COLUMNS(Session session, PersistentStore store) {

        Table t = sysTables[SYSTEM_COLUMNS];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SYSTEM_COLUMNS]);

            addColumn(t, "TABLE_CAT", SQL_IDENTIFIER);              
            addColumn(t, "TABLE_SCHEM", SQL_IDENTIFIER);            
            addColumn(t, "TABLE_NAME", SQL_IDENTIFIER);             
            addColumn(t, "COLUMN_NAME", SQL_IDENTIFIER);            
            addColumn(t, "DATA_TYPE", Type.SQL_SMALLINT);           
            addColumn(t, "TYPE_NAME", SQL_IDENTIFIER);              
            addColumn(t, "COLUMN_SIZE", Type.SQL_INTEGER);          
            addColumn(t, "BUFFER_LENGTH", Type.SQL_INTEGER);        
            addColumn(t, "DECIMAL_DIGITS", Type.SQL_INTEGER);       
            addColumn(t, "NUM_PREC_RADIX", Type.SQL_INTEGER);       
            addColumn(t, "NULLABLE", Type.SQL_INTEGER);             
            addColumn(t, "REMARKS", CHARACTER_DATA);                
            addColumn(t, "COLUMN_DEF", CHARACTER_DATA);             
            addColumn(t, "SQL_DATA_TYPE", Type.SQL_INTEGER);        
            addColumn(t, "SQL_DATETIME_SUB", Type.SQL_INTEGER);     
            addColumn(t, "CHAR_OCTET_LENGTH", Type.SQL_INTEGER);    
            addColumn(t, "ORDINAL_POSITION", Type.SQL_INTEGER);     
            addColumn(t, "IS_NULLABLE", YES_OR_NO);                 
            addColumn(t, "SCOPE_CATALOG", SQL_IDENTIFIER);          
            addColumn(t, "SCOPE_SCHEMA", SQL_IDENTIFIER);           
            addColumn(t, "SCOPE_TABLE", SQL_IDENTIFIER);            
            addColumn(t, "SOURCE_DATA_TYPE", SQL_IDENTIFIER);       

            
            
            
            addColumn(t, "IS_AUTOINCREMENT", YES_OR_NO);            

            
            
            
            addColumn(t, "IS_GENERATEDCOLUMN", YES_OR_NO);          

            
            
            
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[SYSTEM_COLUMNS].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKeyConstraint(name, new int[] {
                0, 1, 2, 16
            }, false);

            return t;
        }

        
        String tableCatalog;
        String tableSchema;
        String tableName;

        
        int         columnCount;
        Iterator    tables;
        Table       table;
        Object[]    row;
        DITableInfo ti;

        
        final int itable_cat         = 0;
        final int itable_schem       = 1;
        final int itable_name        = 2;
        final int icolumn_name       = 3;
        final int idata_type         = 4;
        final int itype_name         = 5;
        final int icolumn_size       = 6;
        final int ibuffer_length     = 7;
        final int idecimal_digits    = 8;
        final int inum_prec_radix    = 9;
        final int inullable          = 10;
        final int iremark            = 11;
        final int icolumn_def        = 12;
        final int isql_data_type     = 13;
        final int isql_datetime_sub  = 14;
        final int ichar_octet_length = 15;
        final int iordinal_position  = 16;
        final int iis_nullable       = 17;
        final int iscope_cat         = 18;
        final int iscope_schem       = 19;
        final int iscope_table       = 20;
        final int isource_data_type  = 21;

        
        final int iis_autoinc = 22;

        
        final int iis_generated = 23;

        
        tables = allTables();
        ti     = new DITableInfo();

        boolean translateTTI = database.sqlTranslateTTI;

        
        while (tables.hasNext()) {
            table = (Table) tables.next();

            
            if (!isAccessibleTable(session, table)) {
                continue;
            }

            ti.setTable(table);

            tableCatalog = table.getCatalogName().name;
            tableSchema  = table.getSchemaName().name;
            tableName    = table.getName().name;
            columnCount  = table.getColumnCount();

            for (int i = 0; i < columnCount; i++) {
                ColumnSchema column = table.getColumn(i);
                Type         type   = column.getDataType();

                if (translateTTI) {
                    if (type.isIntervalType()) {
                        type = ((IntervalType) type).getCharacterType();
                    } else if (type.isDateTimeTypeWithZone()) {
                        type = ((DateTimeType) type)
                            .getDateTimeTypeWithoutZone();
                    }
                }

                row = t.getEmptyRowData();

                
                row[itable_cat]         = tableCatalog;
                row[itable_schem]       = tableSchema;
                row[itable_name]        = tableName;
                row[icolumn_name]       = column.getName().name;
                row[idata_type] = ValuePool.getInt(type.getJDBCTypeCode());
                row[itype_name]         = type.getNameString();
                row[icolumn_size]       = ValuePool.INTEGER_0;
                row[ichar_octet_length] = ValuePool.INTEGER_0;

                if (type.isArrayType()) {
                    row[itype_name] = type.getDefinition();
                }

                if (type.isCharacterType()) {
                    row[icolumn_size] =
                        ValuePool.getInt(type.getJDBCPrecision());

                    
                    row[ichar_octet_length] =
                        ValuePool.getInt(type.getJDBCPrecision());
                }

                if (type.isBinaryType()) {
                    row[icolumn_size] =
                        ValuePool.getInt(type.getJDBCPrecision());
                    row[ichar_octet_length] =
                        ValuePool.getInt(type.getJDBCPrecision());
                }

                if (type.isNumberType()) {
                    row[icolumn_size] = ValuePool.getInt(
                        ((NumberType) type).getNumericPrecisionInRadix());
                    row[inum_prec_radix] =
                        ValuePool.getInt(type.getPrecisionRadix());

                    if (type.isExactNumberType()) {
                        row[idecimal_digits] = ValuePool.getLong(type.scale);
                    }
                }

                if (type.isDateTimeType()) {
                    int size = (int) column.getDataType().displaySize();

                    row[icolumn_size] = ValuePool.getInt(size);
                    row[isql_datetime_sub] = ValuePool.getInt(
                        ((DateTimeType) type).getSqlDateTimeSub());
                }

                row[inullable] = ValuePool.getInt(column.getNullability());
                row[iremark]           = ti.getColRemarks(i);
                row[icolumn_def]       = column.getDefaultSQL();
                row[isql_data_type]    = ValuePool.getInt(type.typeCode);
                row[iordinal_position] = ValuePool.getInt(i + 1);
                row[iis_nullable]      = column.isNullable() ? "YES"
                                                             : "NO";

                if (type.isDistinctType()) {
                    row[isource_data_type] =
                        type.getName().getSchemaQualifiedStatementName();
                }

                
                row[iis_autoinc]   = column.isIdentity() ? "YES"
                                                         : "NO";
                row[iis_generated] = column.isGenerated() ? "YES"
                                                          : "NO";

                t.insertSys(session, store, row);
            }
        }

        return t;
    }

    
    final Table SYSTEM_CROSSREFERENCE(Session session, PersistentStore store) {

        Table t = sysTables[SYSTEM_CROSSREFERENCE];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SYSTEM_CROSSREFERENCE]);

            addColumn(t, "PKTABLE_CAT", SQL_IDENTIFIER);
            addColumn(t, "PKTABLE_SCHEM", SQL_IDENTIFIER);
            addColumn(t, "PKTABLE_NAME", SQL_IDENTIFIER);        
            addColumn(t, "PKCOLUMN_NAME", SQL_IDENTIFIER);       
            addColumn(t, "FKTABLE_CAT", SQL_IDENTIFIER);
            addColumn(t, "FKTABLE_SCHEM", SQL_IDENTIFIER);
            addColumn(t, "FKTABLE_NAME", SQL_IDENTIFIER);        
            addColumn(t, "FKCOLUMN_NAME", SQL_IDENTIFIER);       
            addColumn(t, "KEY_SEQ", Type.SQL_SMALLINT);          
            addColumn(t, "UPDATE_RULE", Type.SQL_SMALLINT);      
            addColumn(t, "DELETE_RULE", Type.SQL_SMALLINT);      
            addColumn(t, "FK_NAME", SQL_IDENTIFIER);
            addColumn(t, "PK_NAME", SQL_IDENTIFIER);
            addColumn(t, "DEFERRABILITY", Type.SQL_SMALLINT);    

            
            
            
            
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[SYSTEM_CROSSREFERENCE].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKeyConstraint(name, new int[] {
                4, 5, 6, 8, 11
            }, false);

            return t;
        }

        
        String  pkTableCatalog;
        String  pkTableSchema;
        String  pkTableName;
        String  pkColumnName;
        String  fkTableCatalog;
        String  fkTableSchema;
        String  fkTableName;
        String  fkColumnName;
        Integer keySequence;
        Integer updateRule;
        Integer deleteRule;
        String  fkName;
        String  pkName;
        Integer deferrability;

        
        Iterator      tables;
        Table         table;
        Table         fkTable;
        Table         pkTable;
        int           columnCount;
        int[]         mainCols;
        int[]         refCols;
        Constraint[]  constraints;
        Constraint    constraint;
        int           constraintCount;
        HsqlArrayList fkConstraintsList;
        Object[]      row;

        
        final int ipk_table_cat   = 0;
        final int ipk_table_schem = 1;
        final int ipk_table_name  = 2;
        final int ipk_column_name = 3;
        final int ifk_table_cat   = 4;
        final int ifk_table_schem = 5;
        final int ifk_table_name  = 6;
        final int ifk_column_name = 7;
        final int ikey_seq        = 8;
        final int iupdate_rule    = 9;
        final int idelete_rule    = 10;
        final int ifk_name        = 11;
        final int ipk_name        = 12;
        final int ideferrability  = 13;

        tables =
            database.schemaManager.databaseObjectIterator(SchemaObject.TABLE);

        
        
        
        
        
        
        
        
        fkConstraintsList = new HsqlArrayList();

        while (tables.hasNext()) {
            table = (Table) tables.next();

            if (table.isView() || !isAccessibleTable(session, table)) {
                continue;
            }

            constraints     = table.getConstraints();
            constraintCount = constraints.length;

            for (int i = 0; i < constraintCount; i++) {
                constraint = (Constraint) constraints[i];

                if (constraint.getConstraintType() == SchemaObject
                        .ConstraintTypes
                        .FOREIGN_KEY && isAccessibleTable(session, constraint
                            .getRef())) {
                    fkConstraintsList.add(constraint);
                }
            }
        }

        
        
        
        
        for (int i = 0; i < fkConstraintsList.size(); i++) {
            constraint     = (Constraint) fkConstraintsList.get(i);
            pkTable        = constraint.getMain();
            pkTableName    = pkTable.getName().name;
            fkTable        = constraint.getRef();
            fkTableName    = fkTable.getName().name;
            pkTableCatalog = pkTable.getCatalogName().name;
            pkTableSchema  = pkTable.getSchemaName().name;
            fkTableCatalog = fkTable.getCatalogName().name;
            fkTableSchema  = fkTable.getSchemaName().name;
            mainCols       = constraint.getMainColumns();
            refCols        = constraint.getRefColumns();
            columnCount    = refCols.length;
            fkName         = constraint.getRefName().name;
            pkName         = constraint.getUniqueName().name;
            deferrability  = ValuePool.getInt(constraint.getDeferability());

            
            deleteRule = ValuePool.getInt(constraint.getDeleteAction());
            updateRule = ValuePool.getInt(constraint.getUpdateAction());

            for (int j = 0; j < columnCount; j++) {
                keySequence          = ValuePool.getInt(j + 1);
                pkColumnName = pkTable.getColumn(mainCols[j]).getNameString();
                fkColumnName = fkTable.getColumn(refCols[j]).getNameString();
                row                  = t.getEmptyRowData();
                row[ipk_table_cat]   = pkTableCatalog;
                row[ipk_table_schem] = pkTableSchema;
                row[ipk_table_name]  = pkTableName;
                row[ipk_column_name] = pkColumnName;
                row[ifk_table_cat]   = fkTableCatalog;
                row[ifk_table_schem] = fkTableSchema;
                row[ifk_table_name]  = fkTableName;
                row[ifk_column_name] = fkColumnName;
                row[ikey_seq]        = keySequence;
                row[iupdate_rule]    = updateRule;
                row[idelete_rule]    = deleteRule;
                row[ifk_name]        = fkName;
                row[ipk_name]        = pkName;
                row[ideferrability]  = deferrability;

                t.insertSys(session, store, row);
            }
        }

        return t;
    }

    
    final Table SYSTEM_INDEXINFO(Session session, PersistentStore store) {

        Table t = sysTables[SYSTEM_INDEXINFO];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SYSTEM_INDEXINFO]);

            
            addColumn(t, "TABLE_CAT", SQL_IDENTIFIER);
            addColumn(t, "TABLE_SCHEM", SQL_IDENTIFIER);
            addColumn(t, "TABLE_NAME", SQL_IDENTIFIER);             
            addColumn(t, "NON_UNIQUE", Type.SQL_BOOLEAN);           
            addColumn(t, "INDEX_QUALIFIER", SQL_IDENTIFIER);
            addColumn(t, "INDEX_NAME", SQL_IDENTIFIER);
            addColumn(t, "TYPE", Type.SQL_SMALLINT);                
            addColumn(t, "ORDINAL_POSITION", Type.SQL_SMALLINT);    
            addColumn(t, "COLUMN_NAME", SQL_IDENTIFIER);
            addColumn(t, "ASC_OR_DESC", CHARACTER_DATA);
            addColumn(t, "CARDINALITY", Type.SQL_INTEGER);
            addColumn(t, "PAGES", Type.SQL_INTEGER);
            addColumn(t, "FILTER_CONDITION", CHARACTER_DATA);

            
            addColumn(t, "ROW_CARDINALITY", Type.SQL_INTEGER);

            
            
            
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[SYSTEM_INDEXINFO].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKeyConstraint(name, new int[] {
                0, 1, 2, 3, 4, 5, 7
            }, false);

            return t;
        }

        
        String  tableCatalog;
        String  tableSchema;
        String  tableName;
        Boolean nonUnique;
        String  indexQualifier;
        String  indexName;
        Integer indexType;

        
        
        
        Integer cardinality;
        Integer pages;
        String  filterCondition;
        Integer rowCardinality;

        
        Iterator tables;
        Table    table;
        int      indexCount;
        int[]    cols;
        int      col;
        int      colCount;
        Object[] row;

        
        final int itable_cat        = 0;
        final int itable_schem      = 1;
        final int itable_name       = 2;
        final int inon_unique       = 3;
        final int iindex_qualifier  = 4;
        final int iindex_name       = 5;
        final int itype             = 6;
        final int iordinal_position = 7;
        final int icolumn_name      = 8;
        final int iasc_or_desc      = 9;
        final int icardinality      = 10;
        final int ipages            = 11;
        final int ifilter_condition = 12;
        final int irow_cardinality  = 13;

        
        tables =
            database.schemaManager.databaseObjectIterator(SchemaObject.TABLE);

        
        while (tables.hasNext()) {
            table = (Table) tables.next();

            if (table.isView() || !isAccessibleTable(session, table)) {
                continue;
            }

            tableCatalog = table.getCatalogName().name;
            tableSchema  = table.getSchemaName().name;
            tableName    = table.getName().name;

            
            filterCondition = null;

            
            indexQualifier = tableCatalog;
            indexCount     = table.getIndexCount();

            
            for (int i = 0; i < indexCount; i++) {
                Index index = table.getIndex(i);

                colCount = table.getIndex(i).getColumnCount();

                if (colCount < 1) {
                    continue;
                }

                indexName      = index.getName().name;
                nonUnique      = index.isUnique() ? Boolean.FALSE
                                                  : Boolean.TRUE;
                cardinality    = null;
                pages          = ValuePool.INTEGER_0;
                rowCardinality = null;
                cols           = index.getColumns();
                indexType      = ValuePool.getInt(3);

                for (int k = 0; k < colCount; k++) {
                    col                    = cols[k];
                    row                    = t.getEmptyRowData();
                    row[itable_cat]        = tableCatalog;
                    row[itable_schem]      = tableSchema;
                    row[itable_name]       = tableName;
                    row[inon_unique]       = nonUnique;
                    row[iindex_qualifier]  = indexQualifier;
                    row[iindex_name]       = indexName;
                    row[itype]             = indexType;
                    row[iordinal_position] = ValuePool.getInt(k + 1);
                    row[icolumn_name] =
                        table.getColumn(cols[k]).getName().name;
                    row[iasc_or_desc]      = "A";
                    row[icardinality]      = cardinality;
                    row[ipages]            = pages;
                    row[irow_cardinality]  = rowCardinality;
                    row[ifilter_condition] = filterCondition;

                    t.insertSys(session, store, row);
                }
            }
        }

        return t;
    }

    
    final Table SYSTEM_PRIMARYKEYS(Session session, PersistentStore store) {

        Table t = sysTables[SYSTEM_PRIMARYKEYS];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SYSTEM_PRIMARYKEYS]);

            addColumn(t, "TABLE_CAT", SQL_IDENTIFIER);
            addColumn(t, "TABLE_SCHEM", SQL_IDENTIFIER);
            addColumn(t, "TABLE_NAME", SQL_IDENTIFIER);     
            addColumn(t, "COLUMN_NAME", SQL_IDENTIFIER);    
            addColumn(t, "KEY_SEQ", Type.SQL_SMALLINT);     
            addColumn(t, "PK_NAME", SQL_IDENTIFIER);

            
            
            
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[SYSTEM_PRIMARYKEYS].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKeyConstraint(name, new int[] {
                3, 2, 1, 0
            }, false);

            return t;
        }

        
        String tableCatalog;
        String tableSchema;
        String tableName;

        
        
        String primaryKeyName;

        
        Iterator       tables;
        Table          table;
        Object[]       row;
        Constraint     constraint;
        int[]          cols;
        int            colCount;
        HsqlProperties p;

        
        final int itable_cat   = 0;
        final int itable_schem = 1;
        final int itable_name  = 2;
        final int icolumn_name = 3;
        final int ikey_seq     = 4;
        final int ipk_name     = 5;

        
        p = database.getProperties();
        tables =
            database.schemaManager.databaseObjectIterator(SchemaObject.TABLE);

        while (tables.hasNext()) {
            table = (Table) tables.next();

            if (table.isView() || !isAccessibleTable(session, table)
                    || !table.hasPrimaryKey()) {
                continue;
            }

            constraint     = table.getPrimaryConstraint();
            tableCatalog   = table.getCatalogName().name;
            tableSchema    = table.getSchemaName().name;
            tableName      = table.getName().name;
            primaryKeyName = constraint.getName().name;
            cols           = constraint.getMainColumns();
            colCount       = cols.length;

            for (int j = 0; j < colCount; j++) {
                row               = t.getEmptyRowData();
                row[itable_cat]   = tableCatalog;
                row[itable_schem] = tableSchema;
                row[itable_name]  = tableName;
                row[icolumn_name] = table.getColumn(cols[j]).getName().name;
                row[ikey_seq]     = ValuePool.getInt(j + 1);
                row[ipk_name]     = primaryKeyName;

                t.insertSys(session, store, row);
            }
        }

        return t;
    }

    
    Table SYSTEM_PROCEDURECOLUMNS(Session session, PersistentStore store) {

        Table t = sysTables[SYSTEM_PROCEDURECOLUMNS];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SYSTEM_PROCEDURECOLUMNS]);

            
            
            
            addColumn(t, "PROCEDURE_CAT", SQL_IDENTIFIER);          
            addColumn(t, "PROCEDURE_SCHEM", SQL_IDENTIFIER);        
            addColumn(t, "PROCEDURE_NAME", SQL_IDENTIFIER);         
            addColumn(t, "COLUMN_NAME", SQL_IDENTIFIER);            
            addColumn(t, "COLUMN_TYPE", Type.SQL_SMALLINT);         
            addColumn(t, "DATA_TYPE", Type.SQL_SMALLINT);           
            addColumn(t, "TYPE_NAME", SQL_IDENTIFIER);              
            addColumn(t, "PRECISION", Type.SQL_INTEGER);            
            addColumn(t, "LENGTH", Type.SQL_INTEGER);               
            addColumn(t, "SCALE", Type.SQL_SMALLINT);               
            addColumn(t, "RADIX", Type.SQL_SMALLINT);               
            addColumn(t, "NULLABLE", Type.SQL_SMALLINT);            
            addColumn(t, "REMARKS", CHARACTER_DATA);                

            
            
            
            addColumn(t, "COLUMN_DEF", CHARACTER_DATA);             
            addColumn(t, "SQL_DATA_TYPE", Type.SQL_INTEGER);        
            addColumn(t, "SQL_DATETIME_SUB", Type.SQL_INTEGER);     
            addColumn(t, "CHAR_OCTET_LENGTH", Type.SQL_INTEGER);    
            addColumn(t, "ORDINAL_POSITION", Type.SQL_INTEGER);     
            addColumn(t, "IS_NULLABLE", CHARACTER_DATA);            
            addColumn(t, "SPECIFIC_NAME", SQL_IDENTIFIER);          

            
            
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[SYSTEM_PROCEDURECOLUMNS].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKeyConstraint(name, new int[] {
                0, 1, 2, 19, 17
            }, false);

            return t;
        }

        
        final int specific_cat            = 0;
        final int specific_schem          = 1;
        final int procedure_name          = 2;
        final int parameter_name          = 3;
        final int parameter_mode          = 4;
        final int data_type_sql_id        = 5;
        final int data_type               = 6;
        final int numeric_precision       = 7;
        final int byte_length             = 8;
        final int numeric_scale           = 9;
        final int numeric_precision_radix = 10;
        final int nullable                = 11;
        final int remark                  = 12;
        final int default_val             = 13;
        final int sql_data_type           = 14;
        final int sql_datetime_sub        = 15;
        final int character_octet_length  = 16;
        final int ordinal_position        = 17;
        final int is_nullable             = 18;
        final int specific_name           = 19;

        
        int           columnCount;
        Iterator      routines;
        RoutineSchema routineSchema;
        Routine       routine;
        Object[]      row;
        Type          type;

        
        boolean translateTTI = database.sqlTranslateTTI;

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

                    row  = t.getEmptyRowData();
                    type = column.getDataType();

                    if (translateTTI) {
                        if (type.isIntervalType()) {
                            type = ((IntervalType) type).getCharacterType();
                        } else if (type.isDateTimeTypeWithZone()) {
                            type = ((DateTimeType) type)
                                .getDateTimeTypeWithoutZone();
                        }
                    }

                    row[specific_cat]     = database.getCatalogName().name;
                    row[specific_schem]   = routine.getSchemaName().name;
                    row[specific_name]    = routine.getSpecificName().name;
                    row[procedure_name]   = routine.getName().name;
                    row[parameter_name]   = column.getName().name;
                    row[ordinal_position] = ValuePool.getInt(j + 1);
                    row[parameter_mode] =
                        ValuePool.getInt(column.getParameterMode());
                    row[data_type] = type.getFullNameString();
                    row[data_type_sql_id] =
                        ValuePool.getInt(type.getJDBCTypeCode());
                    row[numeric_precision]      = ValuePool.INTEGER_0;
                    row[character_octet_length] = ValuePool.INTEGER_0;

                    if (type.isCharacterType()) {
                        row[numeric_precision] =
                            ValuePool.getInt(type.getJDBCPrecision());

                        
                        row[character_octet_length] =
                            ValuePool.getInt(type.getJDBCPrecision());
                    }

                    if (type.isBinaryType()) {
                        row[numeric_precision] =
                            ValuePool.getInt(type.getJDBCPrecision());
                        row[character_octet_length] =
                            ValuePool.getInt(type.getJDBCPrecision());
                    }

                    if (type.isNumberType()) {
                        row[numeric_precision] = ValuePool.getInt(
                            ((NumberType) type).getNumericPrecisionInRadix());
                        row[numeric_precision_radix] =
                            ValuePool.getLong(type.getPrecisionRadix());

                        if (type.isExactNumberType()) {
                            row[numeric_scale] = ValuePool.getLong(type.scale);
                        }
                    }

                    if (type.isDateTimeType()) {
                        int size = (int) column.getDataType().displaySize();

                        row[numeric_precision] = ValuePool.getInt(size);
                    }

                    row[sql_data_type] =
                        ValuePool.getInt(column.getDataType().typeCode);
                    row[nullable] = ValuePool.getInt(column.getNullability());
                    row[is_nullable] = column.isNullable() ? "YES"
                                                           : "NO";

                    t.insertSys(session, store, row);
                }
            }
        }

        return t;
    }

    
    Table SYSTEM_PROCEDURES(Session session, PersistentStore store) {

        Table t = sysTables[SYSTEM_PROCEDURES];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SYSTEM_PROCEDURES]);

            
            
            
            addColumn(t, "PROCEDURE_CAT", SQL_IDENTIFIER);        
            addColumn(t, "PROCEDURE_SCHEM", SQL_IDENTIFIER);      
            addColumn(t, "PROCEDURE_NAME", SQL_IDENTIFIER);       
            addColumn(t, "COL_4", Type.SQL_INTEGER);              
            addColumn(t, "COL_5", Type.SQL_INTEGER);              
            addColumn(t, "COL_6", Type.SQL_INTEGER);              
            addColumn(t, "REMARKS", CHARACTER_DATA);              

            
            
            addColumn(t, "PROCEDURE_TYPE", Type.SQL_SMALLINT);    

            
            
            
            addColumn(t, "SPECIFIC_NAME", SQL_IDENTIFIER);        

            
            
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[SYSTEM_PROCEDURES].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKeyConstraint(name, new int[] {
                0, 1, 2, 8
            }, false);

            return t;
        }

        
        final int procedure_catalog = 0;
        final int procedure_schema  = 1;
        final int procedure_name    = 2;
        final int col_4             = 3;
        final int col_5             = 4;
        final int col_6             = 5;
        final int remarks           = 6;
        final int procedure_type    = 7;
        final int specific_name     = 8;

        
        Iterator it = database.schemaManager.databaseObjectIterator(
            SchemaObject.SPECIFIC_ROUTINE);

        while (it.hasNext()) {
            Routine  routine = (Routine) it.next();
            Object[] row     = t.getEmptyRowData();

            row[procedure_catalog] = row[procedure_catalog] =
                database.getCatalogName().name;
            row[procedure_schema] = routine.getSchemaName().name;
            row[procedure_name]   = routine.getName().name;
            row[remarks]          = routine.getName().comment;
            row[procedure_type] = routine.isProcedure() ? ValuePool.INTEGER_1
                                                        : ValuePool.INTEGER_2;
            row[specific_name]    = routine.getSpecificName().name;

            t.insertSys(session, store, row);
        }

        return t;
    }

    
    final Table SYSTEM_CONNECTION_PROPERTIES(Session session,
            PersistentStore store) {

        Table t = sysTables[SYSTEM_CONNECTION_PROPERTIES];

        if (t == null) {
            t = createBlankTable(
                sysTableHsqlNames[SYSTEM_CONNECTION_PROPERTIES]);

            addColumn(t, "NAME", SQL_IDENTIFIER);
            addColumn(t, "MAX_LEN", Type.SQL_INTEGER);
            addColumn(t, "DEFAULT_VALUE", SQL_IDENTIFIER);    
            addColumn(t, "DESCRIPTION", SQL_IDENTIFIER);      

            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[SYSTEM_PRIMARYKEYS].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKeyConstraint(name, new int[]{ 0 }, true);

            return t;
        }

        Object[] row;

        
        final int iname          = 0;
        final int imax_len       = 1;
        final int idefault_value = 2;
        final int idescription   = 3;
        Iterator  it = HsqlDatabaseProperties.getPropertiesMetaIterator();

        while (it.hasNext()) {
            Object[] meta = (Object[]) it.next();
            int propType =
                ((Integer) meta[HsqlProperties.indexType]).intValue();

            if (propType == HsqlDatabaseProperties.FILE_PROPERTY) {
                if (HsqlDatabaseProperties.hsqldb_readonly.equals(
                        meta[HsqlProperties.indexName]) || HsqlDatabaseProperties
                            .hsqldb_files_readonly.equals(
                                meta[HsqlProperties.indexName])) {}
                else {
                    continue;
                }
            } else if (propType != HsqlDatabaseProperties.SQL_PROPERTY) {
                continue;
            }

            row = t.getEmptyRowData();

            Object def = meta[HsqlProperties.indexDefaultValue];

            row[iname]          = meta[HsqlProperties.indexName];
            row[imax_len]       = ValuePool.getInt(8);
            row[idefault_value] = def == null ? null
                                              : def.toString();
            row[idescription]   = "see HyperSQL guide";

            t.insertSys(session, store, row);
        }

        return t;
    }

    
    protected void addProcRows(Session session, Table t, HsqlArrayList l,
                               String cat, String schem, String pName,
                               Integer ip, Integer op, Integer rs,
                               String remark, Integer pType,
                               String specificName, String origin) {

        PersistentStore store = t.getRowStore(session);

        
        final int icat          = 0;
        final int ischem        = 1;
        final int ipname        = 2;
        final int iinput_parms  = 3;
        final int ioutput_parms = 4;
        final int iresult_sets  = 5;
        final int iremark       = 6;
        final int iptype        = 7;
        final int isn           = 8;
        final int iporigin      = 9;
        Object[]  row           = t.getEmptyRowData();

        row[icat]          = cat;
        row[ischem]        = schem;
        row[ipname]        = pName;
        row[iinput_parms]  = ip;
        row[ioutput_parms] = op;
        row[iresult_sets]  = rs;
        row[iremark]       = remark;
        row[iptype]        = pType;
        row[iporigin]      = origin;
        row[isn]           = specificName;

        t.insertSys(session, store, row);

        if (l != null) {
            int size = l.size();

            for (int i = 0; i < size; i++) {
                row                = t.getEmptyRowData();
                pName              = (String) l.get(i);
                row[icat]          = cat;
                row[ischem]        = schem;
                row[ipname]        = pName;
                row[iinput_parms]  = ip;
                row[ioutput_parms] = op;
                row[iresult_sets]  = rs;
                row[iremark]       = remark;
                row[iptype]        = pType;
                row[iporigin]      = "ALIAS";
                row[isn]           = specificName;

                t.insertSys(session, store, row);
            }
        }
    }

    
    protected void addPColRows(Session session, Table t, HsqlArrayList l,
                               String cat, String schem, String pName,
                               String cName, Integer cType, Integer dType,
                               String tName, Integer prec, Integer len,
                               Integer scale, Integer radix,
                               Integer nullability, String remark,
                               String colDefault, Integer sqlDataType,
                               Integer sqlDateTimeSub,
                               Integer charOctetLength,
                               Integer ordinalPosition, String isNullable,
                               String specificName) {

        PersistentStore store = t.getRowStore(session);

        
        final int icat       = 0;
        final int ischem     = 1;
        final int iname      = 2;
        final int icol_name  = 3;
        final int icol_type  = 4;
        final int idata_type = 5;
        final int itype_name = 6;
        final int iprec      = 7;
        final int ilength    = 8;
        final int iscale     = 9;
        final int iradix     = 10;
        final int inullable  = 11;
        final int iremark    = 12;

        
        final int icol_default      = 13;
        final int isql_data_type    = 14;
        final int isql_datetime_sub = 15;
        final int ichar_octet_len   = 16;
        final int iordinal_position = 17;
        final int iis_nullable      = 18;
        final int ispecific_name    = 19;

        
        Object[] row = t.getEmptyRowData();

        
        row[icat]       = cat;
        row[ischem]     = schem;
        row[iname]      = pName;
        row[icol_name]  = cName;
        row[icol_type]  = cType;
        row[idata_type] = dType;
        row[itype_name] = tName;
        row[iprec]      = prec;
        row[ilength]    = len;
        row[iscale]     = scale;
        row[iradix]     = radix;
        row[inullable]  = nullability;
        row[iremark]    = remark;

        
        row[icol_default]      = colDefault;
        row[isql_data_type]    = sqlDataType;
        row[isql_datetime_sub] = sqlDateTimeSub;
        row[ichar_octet_len]   = charOctetLength;
        row[iordinal_position] = ordinalPosition;
        row[iis_nullable]      = isNullable;
        row[ispecific_name]    = specificName;

        t.insertSys(session, store, row);

        if (l != null) {
            int size = l.size();

            for (int i = 0; i < size; i++) {
                row             = t.getEmptyRowData();
                pName           = (String) l.get(i);
                row[icat]       = cat;
                row[ischem]     = schem;
                row[iname]      = pName;
                row[icol_name]  = cName;
                row[icol_type]  = cType;
                row[idata_type] = dType;
                row[itype_name] = tName;
                row[iprec]      = prec;
                row[ilength]    = len;
                row[iscale]     = scale;
                row[iradix]     = radix;
                row[inullable]  = nullability;
                row[iremark]    = remark;

                
                row[icol_default]      = colDefault;
                row[isql_data_type]    = sqlDataType;
                row[isql_datetime_sub] = sqlDateTimeSub;
                row[ichar_octet_len]   = charOctetLength;
                row[iordinal_position] = ordinalPosition;
                row[iis_nullable]      = isNullable;
                row[ispecific_name]    = specificName;

                t.insertSys(session, store, row);
            }
        }
    }

    
    final Table SYSTEM_SCHEMAS(Session session, PersistentStore store) {

        Table t = sysTables[SYSTEM_SCHEMAS];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SYSTEM_SCHEMAS]);

            addColumn(t, "TABLE_SCHEM", SQL_IDENTIFIER);    
            addColumn(t, "TABLE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "IS_DEFAULT", Type.SQL_BOOLEAN);

            
            
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[SYSTEM_SCHEMAS].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKeyConstraint(name, new int[]{ 0 }, true);

            return t;
        }

        Object[] row;

        
        String[] schemas = database.schemaManager.getSchemaNamesArray();
        String defschema =
            database.schemaManager.getDefaultSchemaHsqlName().name;

        
        for (int i = 0; i < schemas.length; i++) {
            row = t.getEmptyRowData();

            String schema = schemas[i];

            row[0] = schema;
            row[1] = database.getCatalogName().name;
            row[2] = schema.equals(defschema) ? Boolean.TRUE
                                              : Boolean.FALSE;

            t.insertSys(session, store, row);
        }

        return t;
    }

    
    final Table SYSTEM_TABLES(Session session, PersistentStore store) {

        Table t = sysTables[SYSTEM_TABLES];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SYSTEM_TABLES]);

            
            
            
            addColumn(t, "TABLE_CAT", SQL_IDENTIFIER);
            addColumn(t, "TABLE_SCHEM", SQL_IDENTIFIER);
            addColumn(t, "TABLE_NAME", SQL_IDENTIFIER);       
            addColumn(t, "TABLE_TYPE", CHARACTER_DATA);       
            addColumn(t, "REMARKS", CHARACTER_DATA);

            
            
            
            addColumn(t, "TYPE_CAT", SQL_IDENTIFIER);
            addColumn(t, "TYPE_SCHEM", SQL_IDENTIFIER);
            addColumn(t, "TYPE_NAME", SQL_IDENTIFIER);
            addColumn(t, "SELF_REFERENCING_COL_NAME", SQL_IDENTIFIER);
            addColumn(t, "REF_GENERATION", CHARACTER_DATA);

            
            
            
            addColumn(t, "HSQLDB_TYPE", SQL_IDENTIFIER);
            addColumn(t, "READ_ONLY", Type.SQL_BOOLEAN);      
            addColumn(t, "COMMIT_ACTION", CHARACTER_DATA);    

            
            
            
            
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[SYSTEM_TABLES].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKeyConstraint(name, new int[] {
                3, 1, 2, 0
            }, false);

            return t;
        }

        
        Iterator    tables;
        Table       table;
        Object[]    row;
        HsqlName    accessKey;
        DITableInfo ti;

        
        
        final int itable_cat   = 0;
        final int itable_schem = 1;
        final int itable_name  = 2;
        final int itable_type  = 3;
        final int iremark      = 4;

        
        final int itype_cat   = 5;
        final int itype_schem = 6;
        final int itype_name  = 7;
        final int isref_cname = 8;
        final int iref_gen    = 9;

        
        final int ihsqldb_type   = 10;
        final int iread_only     = 11;
        final int icommit_action = 12;

        
        tables = allTables();
        ti     = new DITableInfo();

        
        while (tables.hasNext()) {
            table = (Table) tables.next();

            if (!isAccessibleTable(session, table)) {
                continue;
            }

            ti.setTable(table);

            row               = t.getEmptyRowData();
            row[itable_cat]   = database.getCatalogName().name;
            row[itable_schem] = table.getSchemaName().name;
            row[itable_name]  = table.getName().name;
            row[itable_type]  = ti.getJDBCStandardType();
            row[iremark]      = ti.getRemark();
            row[ihsqldb_type] = ti.getHsqlType();
            row[iread_only]   = table.isDataReadOnly() ? Boolean.TRUE
                                                       : Boolean.FALSE;
            row[icommit_action] = table.isTemp()
                                  ? (table.onCommitPreserve() ? "PRESERVE"
                                                              : "DELETE")
                                  : null;

            t.insertSys(session, store, row);
        }

        return t;
    }

    
    Table SYSTEM_TABLETYPES(Session session, PersistentStore store) {

        Table t = sysTables[SYSTEM_TABLETYPES];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SYSTEM_TABLETYPES]);

            addColumn(t, "TABLE_TYPE", SQL_IDENTIFIER);    

            
            
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[SYSTEM_TABLETYPES].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKeyConstraint(name, new int[]{ 0 }, true);

            return t;
        }

        Object[] row;

        for (int i = 0; i < tableTypes.length; i++) {
            row    = t.getEmptyRowData();
            row[0] = tableTypes[i];

            t.insertSys(session, store, row);
        }

        return t;
    }

    
    final Table SYSTEM_TYPEINFO(Session session, PersistentStore store) {

        Table t = sysTables[SYSTEM_TYPEINFO];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SYSTEM_TYPEINFO]);

            
            
            
            addColumn(t, "TYPE_NAME", SQL_IDENTIFIER);
            addColumn(t, "DATA_TYPE", Type.SQL_SMALLINT);
            addColumn(t, "PRECISION", Type.SQL_INTEGER);
            addColumn(t, "LITERAL_PREFIX", CHARACTER_DATA);
            addColumn(t, "LITERAL_SUFFIX", CHARACTER_DATA);
            addColumn(t, "CREATE_PARAMS", CHARACTER_DATA);
            addColumn(t, "NULLABLE", Type.SQL_SMALLINT);
            addColumn(t, "CASE_SENSITIVE", Type.SQL_BOOLEAN);
            addColumn(t, "SEARCHABLE", Type.SQL_INTEGER);
            addColumn(t, "UNSIGNED_ATTRIBUTE", Type.SQL_BOOLEAN);
            addColumn(t, "FIXED_PREC_SCALE", Type.SQL_BOOLEAN);
            addColumn(t, "AUTO_INCREMENT", Type.SQL_BOOLEAN);
            addColumn(t, "LOCAL_TYPE_NAME", SQL_IDENTIFIER);
            addColumn(t, "MINIMUM_SCALE", Type.SQL_SMALLINT);
            addColumn(t, "MAXIMUM_SCALE", Type.SQL_SMALLINT);
            addColumn(t, "SQL_DATA_TYPE", Type.SQL_INTEGER);
            addColumn(t, "SQL_DATETIME_SUB", Type.SQL_INTEGER);
            addColumn(t, "NUM_PREC_RADIX", Type.SQL_INTEGER);

            
            
            
            addColumn(t, "INTERVAL_PRECISION", Type.SQL_INTEGER);

            
            
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[SYSTEM_TYPEINFO].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKeyConstraint(name, new int[] {
                1, 0
            }, true);

            return t;
        }

        
        
        
        final int itype_name          = 0;
        final int idata_type          = 1;
        final int iprecision          = 2;
        final int iliteral_prefix     = 3;
        final int iliteral_suffix     = 4;
        final int icreate_params      = 5;
        final int inullable           = 6;
        final int icase_sensitive     = 7;
        final int isearchable         = 8;
        final int iunsigned_attribute = 9;
        final int ifixed_prec_scale   = 10;
        final int iauto_increment     = 11;
        final int ilocal_type_name    = 12;
        final int iminimum_scale      = 13;
        final int imaximum_scale      = 14;
        final int isql_data_type      = 15;
        final int isql_datetime_sub   = 16;
        final int inum_prec_radix     = 17;

        
        
        
        
        
        final int iinterval_precision = 18;
        Object[]  row;
        Iterator  it           = Type.typeNames.keySet().iterator();
        boolean   translateTTI = database.sqlTranslateTTI;

        while (it.hasNext()) {
            String typeName = (String) it.next();
            int    typeCode = Type.typeNames.get(typeName);
            Type   type     = Type.getDefaultType(typeCode);

            if (type == null) {
                continue;
            }

            if (translateTTI) {
                if (type.isIntervalType()) {
                    type = ((IntervalType) type).getCharacterType();
                } else if (type.isDateTimeTypeWithZone()) {
                    type = ((DateTimeType) type).getDateTimeTypeWithoutZone();
                }
            }

            row             = t.getEmptyRowData();
            row[itype_name] = typeName;
            row[idata_type] = ValuePool.getInt(type.getJDBCTypeCode());

            long maxPrecision = type.getMaxPrecision();

            row[iprecision] = maxPrecision > Integer.MAX_VALUE
                              ? ValuePool.INTEGER_MAX
                              : ValuePool.getInt((int) maxPrecision);

            if (type.isBinaryType() || type.isCharacterType()
                    || type.isDateTimeType() || type.isIntervalType()) {
                row[iliteral_prefix] = "\'";
                row[iliteral_suffix] = "\'";
            }

            if (type.acceptsPrecision() && type.acceptsScale()) {
                row[icreate_params] = "PRECISION,SCALE";
            } else if (type.acceptsPrecision()) {
                row[icreate_params] = type.isNumberType() ? "PRECISION"
                                                          : "LENGTH";
            } else if (type.acceptsScale()) {
                row[icreate_params] = "SCALE";
            }

            row[inullable] = ValuePool.INTEGER_1;
            row[icase_sensitive] =
                type.isCharacterType()
                && type.typeCode != Types.VARCHAR_IGNORECASE ? Boolean.TRUE
                                                             : Boolean.FALSE;

            if (type.isLobType()) {
                row[isearchable] = ValuePool.INTEGER_0;
            } else if (type.isCharacterType()
                       || (type.isBinaryType() && !type.isBitType())) {
                row[isearchable] = ValuePool.getInt(3);
            } else {
                row[isearchable] = ValuePool.getInt(2);
            }

            row[iunsigned_attribute] = Boolean.FALSE;
            row[ifixed_prec_scale] =
                type.typeCode == Types.SQL_NUMERIC
                || type.typeCode == Types.SQL_DECIMAL ? Boolean.TRUE
                                                      : Boolean.FALSE;
            row[iauto_increment]   = type.isIntegralType() ? Boolean.TRUE
                                                           : Boolean.FALSE;
            row[ilocal_type_name]  = null;
            row[iminimum_scale]    = ValuePool.INTEGER_0;
            row[imaximum_scale]    = ValuePool.getInt(type.getMaxScale());
            row[isql_data_type]    = null;
            row[isql_datetime_sub] = null;
            row[inum_prec_radix] = ValuePool.getInt(type.getPrecisionRadix());

            
            if (type.isIntervalType()) {
                row[iinterval_precision] = null;
            }

            t.insertSys(session, store, row);
        }

        row             = t.getEmptyRowData();
        row[itype_name] = "DISTINCT";
        row[idata_type] = ValuePool.getInt(Types.DISTINCT);

        return t;
    }

    
    Table SYSTEM_UDTS(Session session, PersistentStore store) {

        Table t = sysTables[SYSTEM_UDTS];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SYSTEM_UDTS]);

            addColumn(t, "TYPE_CAT", SQL_IDENTIFIER);
            addColumn(t, "TYPE_SCHEM", SQL_IDENTIFIER);
            addColumn(t, "TYPE_NAME", SQL_IDENTIFIER);
            addColumn(t, "CLASS_NAME", CHARACTER_DATA);
            addColumn(t, "DATA_TYPE", Type.SQL_INTEGER);
            addColumn(t, "REMARKS", CHARACTER_DATA);
            addColumn(t, "BASE_TYPE", Type.SQL_SMALLINT);

            
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[SYSTEM_UDTS].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKeyConstraint(name, null, false);

            return t;
        }

        boolean translateTTI = database.sqlTranslateTTI;

        
        final int type_catalog = 0;
        final int type_schema  = 1;
        final int type_name    = 2;
        final int class_name   = 3;
        final int data_type    = 4;
        final int remarks      = 5;
        final int base_type    = 6;
        Iterator it =
            database.schemaManager.databaseObjectIterator(SchemaObject.TYPE);

        while (it.hasNext()) {
            Type distinct = (Type) it.next();

            if (!distinct.isDistinctType()) {
                continue;
            }

            Object[] data = t.getEmptyRowData();
            Type     type = distinct;

            if (translateTTI) {
                if (type.isIntervalType()) {
                    type = ((IntervalType) type).getCharacterType();
                } else if (type.isDateTimeTypeWithZone()) {
                    type = ((DateTimeType) type).getDateTimeTypeWithoutZone();
                }
            }

            data[type_catalog] = database.getCatalogName().name;
            data[type_schema]  = distinct.getSchemaName().name;
            data[type_name]    = distinct.getName().name;
            data[class_name]   = type.getJDBCClassName();
            data[data_type]    = ValuePool.getInt(Types.DISTINCT);
            data[remarks]      = null;
            data[base_type]    = ValuePool.getInt(type.getJDBCTypeCode());

            t.insertSys(session, store, data);
        }

        return t;
    }

    
    Table SYSTEM_VERSIONCOLUMNS(Session session, PersistentStore store) {

        Table t = sysTables[SYSTEM_VERSIONCOLUMNS];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SYSTEM_VERSIONCOLUMNS]);

            
            
            
            addColumn(t, "SCOPE", Type.SQL_INTEGER);
            addColumn(t, "COLUMN_NAME", SQL_IDENTIFIER);         
            addColumn(t, "DATA_TYPE", Type.SQL_SMALLINT);        
            addColumn(t, "TYPE_NAME", SQL_IDENTIFIER);           
            addColumn(t, "COLUMN_SIZE", Type.SQL_SMALLINT);
            addColumn(t, "BUFFER_LENGTH", Type.SQL_INTEGER);
            addColumn(t, "DECIMAL_DIGITS", Type.SQL_SMALLINT);
            addColumn(t, "PSEUDO_COLUMN", Type.SQL_SMALLINT);    

            
            
            
            addColumn(t, "TABLE_CAT", SQL_IDENTIFIER);
            addColumn(t, "TABLE_SCHEM", SQL_IDENTIFIER);
            addColumn(t, "TABLE_NAME", SQL_IDENTIFIER);          

            
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[SYSTEM_VERSIONCOLUMNS].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKeyConstraint(name, null, false);

            return t;
        }

        return t;
    }

    
    Table SYSTEM_USERS(Session session, PersistentStore store) {

        Table t = sysTables[SYSTEM_USERS];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SYSTEM_USERS]);

            addColumn(t, "USER_NAME", SQL_IDENTIFIER);
            addColumn(t, "ADMIN", Type.SQL_BOOLEAN);
            addColumn(t, "INITIAL_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "AUTHENTICATION", SQL_IDENTIFIER);
            addColumn(t, "PASSWORD_DIGEST", SQL_IDENTIFIER);

            
            
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[SYSTEM_USERS].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKeyConstraint(name, new int[]{ 0 }, true);

            return t;
        }

        
        HsqlArrayList users;
        User          user;
        Object[]      row;
        HsqlName      initialSchema;

        
        users = database.getUserManager().listVisibleUsers(session);

        
        for (int i = 0; i < users.size(); i++) {
            row           = t.getEmptyRowData();
            user          = (User) users.get(i);
            initialSchema = user.getInitialSchema();
            row[0]        = user.getName().getNameString();
            row[1]        = ValuePool.getBoolean(user.isAdmin());
            row[2]        = ((initialSchema == null) ? null
                                                     : initialSchema.name);
            row[3]        = user.isLocalOnly ? Tokens.T_LOCAL
                                             : user.isExternalOnly
                                               ? Tokens.T_EXTERNAL
                                               : Tokens.T_ANY;
            row[4] = user.getPasswordDigest();

            t.insertSys(session, store, row);
        }

        return t;
    }





    
    final Table COLUMN_PRIVILEGES(Session session, PersistentStore store) {

        Table t = sysTables[COLUMN_PRIVILEGES];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[COLUMN_PRIVILEGES]);

            addColumn(t, "GRANTOR", SQL_IDENTIFIER);           
            addColumn(t, "GRANTEE", SQL_IDENTIFIER);           
            addColumn(t, "TABLE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "TABLE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "TABLE_NAME", SQL_IDENTIFIER);        
            addColumn(t, "COLUMN_NAME", SQL_IDENTIFIER);       
            addColumn(t, "PRIVILEGE_TYPE", CHARACTER_DATA);    
            addColumn(t, "IS_GRANTABLE", YES_OR_NO);           

            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[COLUMN_PRIVILEGES].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKeyConstraint(name, new int[] {
                2, 3, 4, 5, 6, 1, 0
            }, false);

            return t;
        }


        String  tableCatalog;
        String  tableSchema;
        String  tableName;
        Grantee granteeObject;


        User     user;
        Iterator tables;
        Table    table;
        Object[] row;


        final int grantor        = 0;
        final int grantee        = 1;
        final int table_catalog  = 2;
        final int table_schema   = 3;
        final int table_name     = 4;
        final int column_name    = 5;
        final int privilege_type = 6;
        final int is_grantable   = 7;

        
        OrderedHashSet grantees =
            session.getGrantee().getGranteeAndAllRolesWithPublic();


        tables = allTables();

        while (tables.hasNext()) {
            table        = (Table) tables.next();
            tableName    = table.getName().name;
            tableCatalog = database.getCatalogName().name;
            tableSchema  = table.getSchemaName().name;

            for (int i = 0; i < grantees.size(); i++) {
                granteeObject = (Grantee) grantees.get(i);

                OrderedHashSet rights =
                    granteeObject.getAllDirectPrivileges(table);
                OrderedHashSet grants =
                    granteeObject.getAllGrantedPrivileges(table);

                if (!grants.isEmpty()) {
                    grants.addAll(rights);

                    rights = grants;
                }

                for (int j = 0; j < rights.size(); j++) {
                    Right right          = (Right) rights.get(j);
                    Right grantableRight = right.getGrantableRights();

                    for (int k = 0; k < Right.privilegeTypes.length; k++) {
                        OrderedHashSet columnList =
                            right.getColumnsForPrivilege(
                                table, Right.privilegeTypes[k]);
                        OrderedHashSet grantableList =
                            grantableRight.getColumnsForPrivilege(table,
                                Right.privilegeTypes[k]);

                        for (int l = 0; l < columnList.size(); l++) {
                            HsqlName fullName = ((HsqlName) columnList.get(l));

                            row                 = t.getEmptyRowData();
                            row[grantor] = right.getGrantor().getName().name;
                            row[grantee] = right.getGrantee().getName().name;
                            row[table_catalog]  = tableCatalog;
                            row[table_schema]   = tableSchema;
                            row[table_name]     = tableName;
                            row[column_name]    = fullName.name;
                            row[privilege_type] = Right.privilegeNames[k];
                            row[is_grantable] =
                                right.getGrantee() == table.getOwner()
                                || grantableList.contains(fullName) ? "YES"
                                                                    : "NO";

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

    
    final Table SEQUENCES(Session session, PersistentStore store) {

        Table t = sysTables[SEQUENCES];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SEQUENCES]);

            addColumn(t, "SEQUENCE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "SEQUENCE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "SEQUENCE_NAME", SQL_IDENTIFIER);
            addColumn(t, "DATA_TYPE", CHARACTER_DATA);
            addColumn(t, "NUMERIC_PRECISION", CARDINAL_NUMBER);
            addColumn(t, "NUMERIC_PRECISION_RADIX", CARDINAL_NUMBER);
            addColumn(t, "NUMERIC_SCALE", CARDINAL_NUMBER);
            addColumn(t, "MAXIMUM_VALUE", CHARACTER_DATA);
            addColumn(t, "MINIMUM_VALUE", CHARACTER_DATA);
            addColumn(t, "INCREMENT", CHARACTER_DATA);
            addColumn(t, "CYCLE_OPTION", YES_OR_NO);
            addColumn(t, "DECLARED_DATA_TYPE", CHARACTER_DATA);
            addColumn(t, "DECLARED_NUMERIC_PRECISION", CARDINAL_NUMBER);
            addColumn(t, "DECLARED_NUMERIC_SCALE", CARDINAL_NUMBER);

            
            addColumn(t, "START_WITH", CHARACTER_DATA);
            addColumn(t, "NEXT_VALUE", CHARACTER_DATA);

            
            
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[SEQUENCES].name, false, SchemaObject.INDEX);

            t.createPrimaryKeyConstraint(name, new int[] {
                0, 1, 2
            }, false);

            return t;
        }

        
        final int sequence_catalog           = 0;
        final int sequence_schema            = 1;
        final int sequence_name              = 2;
        final int data_type                  = 3;
        final int numeric_precision          = 4;
        final int numeric_precision_radix    = 5;
        final int numeric_scale              = 6;
        final int maximum_value              = 7;
        final int minimum_value              = 8;
        final int increment                  = 9;
        final int cycle_option               = 10;
        final int declared_data_type         = 11;
        final int declared_numeric_precision = 12;
        final int declared_numeric_scale     = 13;
        final int start_with                 = 14;
        final int next_value                 = 15;

        
        Iterator       it;
        Object[]       row;
        NumberSequence sequence;

        it = database.schemaManager.databaseObjectIterator(
            SchemaObject.SEQUENCE);

        while (it.hasNext()) {
            sequence = (NumberSequence) it.next();

            if (!session.getGrantee().isAccessible(sequence)) {
                continue;
            }

            row = t.getEmptyRowData();

            NumberType type = (NumberType) sequence.getDataType();
            int radix =
                (type.typeCode == Types.SQL_NUMERIC || type.typeCode == Types
                    .SQL_DECIMAL) ? 10
                                  : 2;

            row[sequence_catalog] = database.getCatalogName().name;
            row[sequence_schema]  = sequence.getSchemaName().name;
            row[sequence_name]    = sequence.getName().name;
            row[data_type]        = sequence.getDataType().getFullNameString();
            row[numeric_precision] =
                ValuePool.getInt((int) type.getPrecision());
            row[numeric_precision_radix]    = ValuePool.getInt(radix);
            row[numeric_scale]              = ValuePool.INTEGER_0;
            row[maximum_value] = String.valueOf(sequence.getMaxValue());
            row[minimum_value] = String.valueOf(sequence.getMinValue());
            row[increment] = String.valueOf(sequence.getIncrement());
            row[cycle_option]               = sequence.isCycle() ? "YES"
                                                                 : "NO";
            row[declared_data_type]         = row[data_type];
            row[declared_numeric_precision] = row[numeric_precision];
            row[declared_numeric_scale]     = row[declared_numeric_scale];
            row[start_with] = String.valueOf(sequence.getStartValue());
            row[next_value]                 = String.valueOf(sequence.peek());

            t.insertSys(session, store, row);
        }

        return t;
    }

    final Table SYSTEM_SEQUENCES(Session session, PersistentStore store) {

        Table t = sysTables[SYSTEM_SEQUENCES];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[SYSTEM_SEQUENCES]);

            addColumn(t, "SEQUENCE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "SEQUENCE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "SEQUENCE_NAME", SQL_IDENTIFIER);
            addColumn(t, "DATA_TYPE", CHARACTER_DATA);
            addColumn(t, "NUMERIC_PRECISION", CARDINAL_NUMBER);
            addColumn(t, "NUMERIC_PRECISION_RADIX", CARDINAL_NUMBER);
            addColumn(t, "NUMERIC_SCALE", CARDINAL_NUMBER);
            addColumn(t, "MAXIMUM_VALUE", CHARACTER_DATA);
            addColumn(t, "MINIMUM_VALUE", CHARACTER_DATA);
            addColumn(t, "INCREMENT", CHARACTER_DATA);
            addColumn(t, "CYCLE_OPTION", YES_OR_NO);
            addColumn(t, "DECLARED_DATA_TYPE", CHARACTER_DATA);
            addColumn(t, "DECLARED_NUMERIC_PRECISION", CARDINAL_NUMBER);
            addColumn(t, "DECLARED_NUMERIC_SCALE", CARDINAL_NUMBER);

            
            addColumn(t, "START_WITH", CHARACTER_DATA);
            addColumn(t, "NEXT_VALUE", CHARACTER_DATA);

            
            
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[SYSTEM_SEQUENCES].name, false,
                SchemaObject.INDEX);

            t.createPrimaryKeyConstraint(name, new int[] {
                0, 1, 2
            }, false);

            return t;
        }

        
        final int sequence_catalog           = 0;
        final int sequence_schema            = 1;
        final int sequence_name              = 2;
        final int data_type                  = 3;
        final int numeric_precision          = 4;
        final int numeric_precision_radix    = 5;
        final int numeric_scale              = 6;
        final int maximum_value              = 7;
        final int minimum_value              = 8;
        final int increment                  = 9;
        final int cycle_option               = 10;
        final int declared_data_type         = 11;
        final int declared_numeric_precision = 12;
        final int declared_numeric_scale     = 13;
        final int start_with                 = 14;
        final int next_value                 = 15;

        
        Iterator       it;
        Object[]       row;
        NumberSequence sequence;

        it = database.schemaManager.databaseObjectIterator(
            SchemaObject.SEQUENCE);

        while (it.hasNext()) {
            sequence = (NumberSequence) it.next();

            if (!session.getGrantee().isAccessible(sequence)) {
                continue;
            }

            row = t.getEmptyRowData();

            NumberType type = (NumberType) sequence.getDataType();
            int radix =
                (type.typeCode == Types.SQL_NUMERIC || type.typeCode == Types
                    .SQL_DECIMAL) ? 10
                                  : 2;

            row[sequence_catalog] = database.getCatalogName().name;
            row[sequence_schema]  = sequence.getSchemaName().name;
            row[sequence_name]    = sequence.getName().name;
            row[data_type]        = sequence.getDataType().getFullNameString();
            row[numeric_precision] =
                ValuePool.getInt((int) type.getPrecision());
            row[numeric_precision_radix]    = ValuePool.getInt(radix);
            row[numeric_scale]              = ValuePool.INTEGER_0;
            row[maximum_value] = String.valueOf(sequence.getMaxValue());
            row[minimum_value] = String.valueOf(sequence.getMinValue());
            row[increment] = String.valueOf(sequence.getIncrement());
            row[cycle_option]               = sequence.isCycle() ? "YES"
                                                                 : "NO";
            row[declared_data_type]         = row[data_type];
            row[declared_numeric_precision] = row[numeric_precision];
            row[declared_numeric_scale]     = row[declared_numeric_scale];
            row[start_with] = String.valueOf(sequence.getStartValue());
            row[next_value]                 = String.valueOf(sequence.peek());

            t.insertSys(session, store, row);
        }

        return t;
    }




    final Table TABLE_PRIVILEGES(Session session, PersistentStore store) {

        Table t = sysTables[TABLE_PRIVILEGES];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[TABLE_PRIVILEGES]);

            addColumn(t, "GRANTOR", SQL_IDENTIFIER);           
            addColumn(t, "GRANTEE", SQL_IDENTIFIER);           
            addColumn(t, "TABLE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "TABLE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "TABLE_NAME", SQL_IDENTIFIER);        
            addColumn(t, "PRIVILEGE_TYPE", CHARACTER_DATA);    
            addColumn(t, "IS_GRANTABLE", YES_OR_NO);           
            addColumn(t, "WITH_HIERARCHY", YES_OR_NO);

            
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[SEQUENCES].name, false, SchemaObject.INDEX);

            t.createPrimaryKeyConstraint(name, new int[] {
                0, 1, 2, 3, 4, 5, 6
            }, false);

            return t;
        }

        
        String  tableCatalog;
        String  tableSchema;
        String  tableName;
        Grantee granteeObject;
        String  privilege;

        
        Iterator tables;
        Table    table;
        Object[] row;

        
        final int grantor        = 0;
        final int grantee        = 1;
        final int table_catalog  = 2;
        final int table_schema   = 3;
        final int table_name     = 4;
        final int privilege_type = 5;
        final int is_grantable   = 6;
        final int with_hierarchy = 7;
        OrderedHashSet grantees =
            session.getGrantee().getGranteeAndAllRolesWithPublic();

        tables = allTables();

        while (tables.hasNext()) {
            table        = (Table) tables.next();
            tableName    = table.getName().name;
            tableCatalog = table.getCatalogName().name;
            tableSchema  = table.getSchemaName().name;

            for (int i = 0; i < grantees.size(); i++) {
                granteeObject = (Grantee) grantees.get(i);

                OrderedHashSet rights =
                    granteeObject.getAllDirectPrivileges(table);
                OrderedHashSet grants =
                    granteeObject.getAllGrantedPrivileges(table);

                if (!grants.isEmpty()) {
                    grants.addAll(rights);

                    rights = grants;
                }

                for (int j = 0; j < rights.size(); j++) {
                    Right right          = (Right) rights.get(j);
                    Right grantableRight = right.getGrantableRights();

                    for (int k = 0; k < Right.privilegeTypes.length; k++) {
                        if (!right.canAccessFully(Right.privilegeTypes[k])) {
                            continue;
                        }

                        privilege           = Right.privilegeNames[k];
                        row                 = t.getEmptyRowData();
                        row[grantor] = right.getGrantor().getName().name;
                        row[grantee] = right.getGrantee().getName().name;
                        row[table_catalog]  = tableCatalog;
                        row[table_schema]   = tableSchema;
                        row[table_name]     = tableName;
                        row[privilege_type] = privilege;
                        row[is_grantable] =
                            right.getGrantee() == table.getOwner()
                            || grantableRight.canAccessFully(
                                Right.privilegeTypes[k]) ? "YES"
                                                         : "NO";
                        row[with_hierarchy] = "NO";

                        try {
                            t.insertSys(session, store, row);
                        } catch (HsqlException e) {}
                    }
                }
            }
        }

        return t;
    }

    Table TABLES(Session session, PersistentStore store) {

        Table t = sysTables[TABLES];

        if (t == null) {
            t = createBlankTable(sysTableHsqlNames[TABLES]);

            addColumn(t, "TABLE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "TABLE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "TABLE_NAME", SQL_IDENTIFIER);
            addColumn(t, "TABLE_TYPE", CHARACTER_DATA);
            addColumn(t, "SELF_REFERENCING_COLUMN_NAME", SQL_IDENTIFIER);
            addColumn(t, "REFERENCE_GENERATION", CHARACTER_DATA);
            addColumn(t, "USER_DEFINED_TYPE_CATALOG", SQL_IDENTIFIER);
            addColumn(t, "USER_DEFINED_TYPE_SCHEMA", SQL_IDENTIFIER);
            addColumn(t, "USER_DEFINED_TYPE_NAME", SQL_IDENTIFIER);
            addColumn(t, "IS_INSERTABLE_INTO", YES_OR_NO);
            addColumn(t, "IS_TYPED", YES_OR_NO);
            addColumn(t, "COMMIT_ACTION", CHARACTER_DATA);

            
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[TABLES].name, false, SchemaObject.INDEX);

            t.createPrimaryKeyConstraint(name, new int[] {
                0, 1, 2,
            }, false);

            return t;
        }

        
        Iterator  tables;
        Table     table;
        Object[]  row;
        final int table_catalog                = 0;
        final int table_schema                 = 1;
        final int table_name                   = 2;
        final int table_type                   = 3;
        final int self_referencing_column_name = 4;
        final int reference_generation         = 5;
        final int user_defined_type_catalog    = 6;
        final int user_defined_type_schema     = 7;
        final int user_defined_type_name       = 8;
        final int is_insertable_into           = 9;
        final int is_typed                     = 10;
        final int commit_action                = 11;

        
        tables = allTables();

        
        while (tables.hasNext()) {
            table = (Table) tables.next();

            if (!isAccessibleTable(session, table)) {
                continue;
            }

            row                = t.getEmptyRowData();
            row[table_catalog] = database.getCatalogName().name;
            row[table_schema]  = table.getSchemaName().name;
            row[table_name]    = table.getName().name;

            switch (table.getTableType()) {

                case TableBase.INFO_SCHEMA_TABLE :
                case TableBase.VIEW_TABLE :
                    row[table_type] = "VIEW";
                    row[is_insertable_into] = table.isInsertable()
                                              ? Tokens.T_YES
                                              : Tokens.T_NO;
                    break;

                case TableBase.TEMP_TABLE :
                case TableBase.TEMP_TEXT_TABLE :
                    row[table_type]         = "GLOBAL TEMPORARY";
                    row[is_insertable_into] = "YES";
                    break;

                default :
                    row[table_type] = "BASE TABLE";
                    row[is_insertable_into] = table.isInsertable()
                                              ? Tokens.T_YES
                                              : Tokens.T_NO;
                    break;
            }

            row[self_referencing_column_name] = null;
            row[reference_generation]         = null;
            row[user_defined_type_catalog]    = null;
            row[user_defined_type_schema]     = null;
            row[user_defined_type_name]       = null;
            row[is_typed]                     = "NO";
            row[commit_action] = table.isTemp()
                                 ? (table.onCommitPreserve() ? "PRESERVE"
                                                             : "DELETE")
                                 : null;

            t.insertSys(session, store, row);
        }

        return t;
    }




    
    final Table INFORMATION_SCHEMA_CATALOG_NAME(Session session,
            PersistentStore store) {

        Table t = sysTables[INFORMATION_SCHEMA_CATALOG_NAME];

        if (t == null) {
            t = createBlankTable(
                sysTableHsqlNames[INFORMATION_SCHEMA_CATALOG_NAME]);

            addColumn(t, "CATALOG_NAME", SQL_IDENTIFIER);    

            
            
            HsqlName name = HsqlNameManager.newInfoSchemaObjectName(
                sysTableHsqlNames[INFORMATION_SCHEMA_CATALOG_NAME].name,
                false, SchemaObject.INDEX);

            t.createPrimaryKeyConstraint(name, new int[]{ 0 }, true);

            return t;
        }

        Object[] row = t.getEmptyRowData();

        row[0] = database.getCatalogName().name;

        t.insertSys(session, store, row);

        return t;
    }
}
