


package org.hsqldb;

import java.util.Comparator;

import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.HsqlNameManager.SimpleName;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.navigator.RowIterator;
import org.hsqldb.navigator.RowSetNavigatorData;
import org.hsqldb.navigator.RowSetNavigatorDataTable;
import org.hsqldb.persist.PersistentStore;
import org.hsqldb.result.Result;


class SubQuery implements Comparator {

    int                  level;
    private boolean      isResolved;
    private boolean      isCorrelated;
    private boolean      isExistsPredicate;
    private boolean      isRecursive;
    private boolean      uniqueRows;
    private boolean      fullOrder;
    QueryExpression      queryExpression;
    Database             database;
    private TableDerived table;
    View                 view;
    View                 parentView;
    String               sql;

    
    Expression dataExpression;
    boolean    isDataExpression;

    
    SubQuery recursiveSubQuery;

    
    SimpleName[] columnNames;

    
    int parsePosition;

    
    public static final SubQuery[] emptySubqueryArray = new SubQuery[]{};

    SubQuery(Database database, int level, QueryExpression queryExpression,
             int mode) {

        this.level           = level;
        this.queryExpression = queryExpression;
        this.database        = database;

        switch (mode) {

            case OpTypes.EXISTS :
                isExistsPredicate = true;
                break;

            case OpTypes.IN :
                uniqueRows = true;

                if (queryExpression != null) {
                    queryExpression.setFullOrder();
                }
                break;

            case OpTypes.UNIQUE :
                fullOrder = true;

                if (queryExpression != null) {
                    queryExpression.setFullOrder();
                }
                break;
        }
    }

    SubQuery(Database database, int level, QueryExpression queryExpression,
             SubQuery sq) {

        this.level             = level;
        this.queryExpression   = queryExpression;
        this.database          = database;
        this.isRecursive       = true;
        this.recursiveSubQuery = sq;
    }

    SubQuery(Database database, int level, Expression dataExpression,
             int mode) {

        this.level              = level;
        this.database           = database;
        this.dataExpression     = dataExpression;
        dataExpression.subQuery = this;
        isDataExpression        = true;

        switch (mode) {

            case OpTypes.IN :
                uniqueRows = true;
                break;
        }
    }

    SubQuery(Database database, int level, QueryExpression queryExpression,
             View view) {

        this.level           = level;
        this.queryExpression = queryExpression;
        this.database        = database;
        this.view            = view;
    }

    public boolean isResolved() {
        return isResolved;
    }

    public boolean isCorrelated() {
        return isCorrelated;
    }

    public void setCorrelated() {
        isCorrelated = true;
    }

    public void setUniqueRows() {
        uniqueRows = true;
    }

    public TableDerived getTable() {
        return table;
    }

    public void createTable() {

        HsqlName name = database.nameManager.getSubqueryTableName();

        table = new TableDerived(database, name, TableBase.SYSTEM_SUBQUERY,
                                 queryExpression, this);
    }

    public void prepareTable(Session session, HsqlName name,
                             HsqlName[] columns) {

        if (isResolved) {
            return;
        }

        if (table == null) {
            table = new TableDerived(database, name,
                                     TableBase.SYSTEM_SUBQUERY,
                                     queryExpression, this);
        }

        table.columnCount = queryExpression.getColumnCount();
        table.columnList  = queryExpression.getColumns();

        if (columns != null) {
            if (columns.length != table.columnList.size()) {
                throw Error.error(ErrorCode.X_42593);
            }

            for (int i = 0; i < table.columnCount; i++) {
                table.columnList.setKey(i, columns[i].name);

                ColumnSchema col = (ColumnSchema) table.columnList.get(i);

                col.getName().rename(columns[i]);
            }
        }

        TableUtil.setTableIndexesForSubquery(table, uniqueRows || fullOrder,
                                             uniqueRows);

        isResolved = true;
    }

    public void prepareTable(Session session) {

        if (isResolved) {
            return;
        }

        if (view == null) {
            if (table == null) {
                HsqlName name = database.nameManager.getSubqueryTableName();

                table = new TableDerived(database, name,
                                         TableBase.SYSTEM_SUBQUERY,
                                         queryExpression, this);
            }

            if (isDataExpression) {
                TableUtil.addAutoColumns(table, dataExpression.nodeDataTypes);
                TableUtil.setTableIndexesForSubquery(table,
                                                     uniqueRows || fullOrder,
                                                     uniqueRows);
            } else {
                table.columnList  = queryExpression.getColumns();
                table.columnCount = queryExpression.getColumnCount();

                TableUtil.setTableIndexesForSubquery(table,
                                                     uniqueRows || fullOrder,
                                                     uniqueRows);
            }
        } else {
            table = new TableDerived(database, view.getName(),
                                     TableBase.VIEW_TABLE, queryExpression,
                                     this);
            table.columnList  = view.columnList;
            table.columnCount = table.columnList.size();

            table.createPrimaryKey();
        }

        isResolved = true;
    }

    public void setColumnNames(SimpleName[] names) {
        columnNames = names;
    }

    public SimpleName[] getColumnNames() {
        return columnNames;
    }

    public void materialiseCorrelated(Session session) {

        if (isCorrelated) {
            materialise(session);
        }
    }

    
    public void materialise(Session session) {

        PersistentStore store;

        
        if (isDataExpression) {
            store = session.sessionData.getSubqueryRowStore(table);

            dataExpression.insertValuesIntoSubqueryTable(session, store);

            return;
        }

        Result result;

        if (isRecursive) {
            result = queryExpression.getResultRecursive(session,
                    recursiveSubQuery.table);
        } else {
            result = queryExpression.getResult(session, isExistsPredicate ? 1
                                                                          : 0);
        }

        if (uniqueRows) {
            RowSetNavigatorData navigator =
                ((RowSetNavigatorData) result.getNavigator());

            navigator.removeDuplicates(session);
        }

        store = session.sessionData.getSubqueryRowStore(table);

        table.insertResult(session, store, result);
        result.getNavigator().release();
    }

    public boolean hasUniqueNotNullRows(Session session) {

        RowSetNavigatorData navigator = new RowSetNavigatorDataTable(session,
            table);
        boolean result = navigator.hasUniqueNotNullRows(session);

        return result;
    }

    public Object[] getValues(Session session) {

        RowIterator it = table.rowIterator(session);

        if (it.hasNext()) {
            Row row = it.getNextRow();

            if (it.hasNext()) {
                throw Error.error(ErrorCode.X_21000);
            }

            return row.getData();
        } else {
            return new Object[table.getColumnCount()];
        }
    }

    public Object getValue(Session session) {

        Object[] data = getValues(session);

        return data[0];
    }

    public RowSetNavigatorData getNavigator(Session session) {

        RowSetNavigatorData navigator = new RowSetNavigatorDataTable(session,
            table);

        return navigator;
    }

    
    public int compare(Object a, Object b) {

        SubQuery sqa = (SubQuery) a;
        SubQuery sqb = (SubQuery) b;

        if (sqa.parentView == null && sqb.parentView == null) {
            return sqb.level - sqa.level;
        } else if (sqa.parentView != null && sqb.parentView != null) {
            int ia = database.schemaManager.getTableIndex(sqa.parentView);
            int ib = database.schemaManager.getTableIndex(sqb.parentView);

            if (ia == -1) {
                ia = database.schemaManager.getTables(
                    sqa.parentView.getSchemaName().name).size();
            }

            if (ib == -1) {
                ib = database.schemaManager.getTables(
                    sqb.parentView.getSchemaName().name).size();
            }

            int diff = ia - ib;

            return diff == 0 ? sqb.level - sqa.level
                             : diff;
        } else {
            return sqa.parentView == null ? 1
                                          : -1;
        }
    }
}
