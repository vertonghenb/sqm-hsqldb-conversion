


package org.hsqldb;

import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.index.Index;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.navigator.RowIterator;
import org.hsqldb.persist.PersistentStore;
import org.hsqldb.types.Type;


public class TableBase {

    
    public static final int INFO_SCHEMA_TABLE = 1;
    public static final int SYSTEM_SUBQUERY   = 2;
    public static final int TEMP_TABLE        = 3;
    public static final int MEMORY_TABLE      = 4;
    public static final int CACHED_TABLE      = 5;
    public static final int TEMP_TEXT_TABLE   = 6;
    public static final int TEXT_TABLE        = 7;
    public static final int VIEW_TABLE        = 8;
    public static final int RESULT_TABLE      = 9;
    public static final int TRANSITION_TABLE  = 10;
    public static final int FUNCTION_TABLE    = 11;
    public static final int SYSTEM_TABLE      = 12;
    public static final int CHANGE_SET_TABLE  = 13;

    
    public static final int SCOPE_STATEMENT   = 21;
    public static final int SCOPE_TRANSACTION = 22;
    public static final int SCOPE_SESSION     = 23;
    public static final int SCOPE_FULL        = 24;

    
    public PersistentStore store;
    public int             persistenceScope;
    public long            persistenceId;

    
    int[]  primaryKeyCols;                      
    Type[] primaryKeyTypes;
    int[]  primaryKeyColsSequence;              

    
    
    Index[]         indexList;                  
    public Database database;
    int[]           bestRowIdentifierCols;      
    boolean         bestRowIdentifierStrict;    
    int[]           bestIndexForColumn;         
    Index           bestIndex;                  
    Index         fullIndex;                    
    boolean[]     colNotNull;                   
    Type[]        colTypes;                     
    protected int columnCount;

    
    int               tableType;
    protected boolean isReadOnly;
    protected boolean isTemp;
    protected boolean isCached;
    protected boolean isText;
    boolean           isView;
    protected boolean isWithDataSource;
    public boolean    isSessionBased;
    protected boolean isSchemaBased;
    protected boolean isLogged;
    private boolean   isTransactional = true;
    boolean           hasLobColumn;

    
    TableBase() {}

    
    public TableBase(Session session, Database database, int scope, int type,
                     Type[] colTypes) {

        tableType        = type;
        persistenceScope = scope;
        isSessionBased   = true;
        persistenceId    = database.persistentStoreCollection.getNextId();
        this.database    = database;
        this.colTypes    = colTypes;
        columnCount      = colTypes.length;
        primaryKeyCols   = new int[]{};
        primaryKeyTypes  = new Type[]{};
        indexList        = new Index[0];

        createPrimaryIndex(primaryKeyCols, primaryKeyTypes, null);
    }

    public TableBase duplicate() {

        TableBase copy = new TableBase();

        copy.tableType        = tableType;
        copy.persistenceScope = persistenceScope;
        copy.isSessionBased   = isSessionBased;
        copy.persistenceId    = database.persistentStoreCollection.getNextId();
        copy.database         = database;
        copy.colTypes         = colTypes;
        copy.columnCount      = colTypes.length;
        copy.primaryKeyCols   = primaryKeyCols;
        copy.primaryKeyTypes  = primaryKeyTypes;
        copy.indexList        = indexList;

        return copy;
    }

    public final int getTableType() {
        return tableType;
    }

    public long getPersistenceId() {
        return persistenceId;
    }

    int getId() {
        return 0;
    }

    public final boolean onCommitPreserve() {
        return persistenceScope == TableBase.SCOPE_SESSION;
    }

    public final RowIterator rowIterator(Session session) {

        PersistentStore store = getRowStore(session);

        return getPrimaryIndex().firstRow(session, store);
    }

    public final RowIterator rowIterator(PersistentStore store) {
        return getPrimaryIndex().firstRow(store);
    }

    public final int getIndexCount() {
        return indexList.length;
    }

    public final Index getPrimaryIndex() {
        return indexList.length > 0 ? indexList[0]
                                    : null;
    }

    public final Type[] getPrimaryKeyTypes() {
        return primaryKeyTypes;
    }

    public final boolean hasPrimaryKey() {
        return !(primaryKeyCols.length == 0);
    }

    public final int[] getPrimaryKey() {
        return primaryKeyCols;
    }

    
    public final Type[] getColumnTypes() {
        return colTypes;
    }

    
    public Index getFullIndex() {
        return fullIndex;
    }

    
    public final Index getIndex(int i) {
        return indexList[i];
    }

    
    public final Index[] getIndexList() {
        return indexList;
    }

    
    public final boolean[] getNewColumnCheckList() {
        return new boolean[getColumnCount()];
    }

    
    public int getColumnCount() {
        return columnCount;
    }

    
    public final int getDataColumnCount() {
        return colTypes.length;
    }

    public boolean isTransactional() {
        return isTransactional;
    }

    public void setTransactional(boolean value) {
        isTransactional = value;
    }

    
    public final void setBestRowIdentifiers() {

        int[]   briCols      = null;
        int     briColsCount = 0;
        boolean isStrict     = false;
        int     nNullCount   = 0;

        
        if (colNotNull == null) {
            return;
        }

        bestIndex          = null;
        bestIndexForColumn = new int[colTypes.length];

        ArrayUtil.fillArray(bestIndexForColumn, -1);

        for (int i = 0; i < indexList.length; i++) {
            Index index     = indexList[i];
            int[] cols      = index.getColumns();
            int   colsCount = index.getVisibleColumns();

            if (colsCount == 0) {
                continue;
            }

            if (i == 0) {
                isStrict = true;
            }

            if (bestIndexForColumn[cols[0]] == -1) {
                bestIndexForColumn[cols[0]] = i;
            } else {
                Index existing = indexList[bestIndexForColumn[cols[0]]];

                if (colsCount > existing.getColumns().length) {
                    bestIndexForColumn[cols[0]] = i;
                }
            }

            if (!index.isUnique()) {
                if (bestIndex == null) {
                    bestIndex = index;
                }

                continue;
            }

            int nnullc = 0;

            for (int j = 0; j < colsCount; j++) {
                if (colNotNull[cols[j]]) {
                    nnullc++;
                }
            }

            if (bestIndex != null) {
                bestIndex = index;
            }

            if (nnullc == colsCount) {
                if (briCols == null || briColsCount != nNullCount
                        || colsCount < briColsCount) {

                    
                    
                    
                    briCols      = cols;
                    briColsCount = colsCount;
                    nNullCount   = colsCount;
                    isStrict     = true;
                }

                continue;
            } else if (isStrict) {
                continue;
            } else if (briCols == null || colsCount < briColsCount
                       || nnullc > nNullCount) {

                
                
                
                briCols      = cols;
                briColsCount = colsCount;
                nNullCount   = nnullc;
            }
        }

        if (briCols == null || briColsCount == briCols.length) {
            bestRowIdentifierCols = briCols;
        } else {
            bestRowIdentifierCols = ArrayUtil.arraySlice(briCols, 0,
                    briColsCount);
        }

        bestRowIdentifierStrict = isStrict;

        if (indexList[0].getColumnCount() > 0) {
            bestIndex = indexList[0];
        }
    }

    public final void createPrimaryIndex(int[] pkcols, Type[] pktypes,
                                         HsqlName name) {

        long id = database.persistentStoreCollection.getNextId();
        Index newIndex = database.logger.newIndex(name, id, this, pkcols,
            null, null, pktypes, true, pkcols.length > 0, pkcols.length > 0,
            false);

        try {
            addIndex(newIndex);
        } catch (HsqlException e) {}
    }

    public final Index createAndAddIndexStructure(HsqlName name,
            int[] columns, boolean[] descending, boolean[] nullsLast,
            boolean unique, boolean constraint, boolean forward
            ) {

        Index newindex = createIndexStructure(name, columns, descending,
                                              nullsLast, unique, constraint,
                                              forward);

        addIndex(newindex);

        return newindex;
    }

    final Index createIndexStructure(HsqlName name, int[] columns,
                                     boolean[] descending,
                                     boolean[] nullsLast, boolean unique,
                                     boolean constraint, boolean forward) {

        if (primaryKeyCols == null) {
            throw Error.runtimeError(ErrorCode.U_S0500, "createIndex");
        }

        int    s     = columns.length;
        int[]  cols  = new int[s];
        Type[] types = new Type[s];

        for (int j = 0; j < s; j++) {
            cols[j]  = columns[j];
            types[j] = colTypes[cols[j]];
        }

        long id = database.persistentStoreCollection.getNextId();
        Index newIndex = database.logger.newIndex(name, id, this, cols,
            descending, nullsLast, types, false, unique, constraint, forward);

        return newIndex;
    }

    
    public void dropIndex(int todrop) {

        indexList = (Index[]) ArrayUtil.toAdjustedArray(indexList, null,
                todrop, -1);

        for (int i = 0; i < indexList.length; i++) {
            indexList[i].setPosition(i);
        }

        setBestRowIdentifiers();

        if (store != null) {
            store.resetAccessorKeys(indexList);
        }
    }

    final void addIndex(Index index) {

        int i = 0;

        for (; i < indexList.length; i++) {
            Index current = indexList[i];
            int order = index.getIndexOrderValue()
                        - current.getIndexOrderValue();

            if (order < 0) {
                break;
            }
        }

        indexList = (Index[]) ArrayUtil.toAdjustedArray(indexList, index, i,
                1);

        for (i = 0; i < indexList.length; i++) {
            indexList[i].setPosition(i);
        }

        if (store != null) {
            try {
                store.resetAccessorKeys(indexList);
            } catch (HsqlException e) {
                indexList = (Index[]) ArrayUtil.toAdjustedArray(indexList,
                        null, index.getPosition(), -1);

                for (i = 0; i < indexList.length; i++) {
                    indexList[i].setPosition(i);
                }

                throw e;
            }
        }

        setBestRowIdentifiers();
    }

    final void removeIndex(int position) {
        setBestRowIdentifiers();
    }

    public final void setIndexes(Index[] indexes) {
        this.indexList = indexes;
    }

    public final Object[] getEmptyRowData() {
        return new Object[getDataColumnCount()];
    }

    
    public final Index createIndex(Session session, HsqlName name,
                                   int[] columns, boolean[] descending,
                                   boolean[] nullsLast, boolean unique,
                                   boolean constraint, boolean forward) {

        Index newIndex = createAndAddIndexStructure(name, columns, descending,
            nullsLast, unique, constraint, forward);

        return newIndex;
    }

    public void clearAllData(Session session) {

        PersistentStore store = getRowStore(session);

        store.removeAll();
    }

    public void clearAllData(PersistentStore store) {
        store.removeAll();
    }

    
    public final boolean isEmpty(Session session) {

        if (getIndexCount() == 0) {
            return true;
        }

        PersistentStore store = getRowStore(session);

        return getIndex(0).isEmpty(store);
    }

    public PersistentStore getRowStore(Session session) {

        return store == null
               ? session.sessionData.persistentStoreCollection.getStore(this)
               : store;
    }
}
