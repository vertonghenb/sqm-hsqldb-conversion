


package org.hsqldb;

import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.lib.HashMappedList;
import org.hsqldb.store.ValuePool;
import org.hsqldb.types.Type;


public class TableDerived extends Table {

    QueryExpression queryExpression;
    View            view;
    SubQuery        subQuery;

    public TableDerived(Database database, HsqlName name, int type) {

        super(database, name, type);

        switch (type) {

            
            case TableBase.CHANGE_SET_TABLE :
            case TableBase.SYSTEM_TABLE :
            case TableBase.FUNCTION_TABLE :
            case TableBase.VIEW_TABLE :
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "Table");
        }
    }

    public TableDerived(Database database, HsqlName name, int type,
                        QueryExpression queryExpression, SubQuery subQuery) {

        super(database, name, type);

        switch (type) {

            case TableBase.CHANGE_SET_TABLE :
            case TableBase.SYSTEM_SUBQUERY :
            case TableBase.VIEW_TABLE :
            case TableBase.RESULT_TABLE :
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "Table");
        }

        this.queryExpression = queryExpression;
        this.subQuery        = subQuery;
    }

    public TableDerived(Database database, HsqlName name, int type,
                        Type[] columnTypes, HashMappedList columnList) {
        this(database, name, type, columnTypes, columnList,
             ValuePool.emptyIntArray);
    }

    public TableDerived(Database database, HsqlName name, int type,
                        Type[] columnTypes, HashMappedList columnList,
                        int[] pkColumns) {

        this(database, name, type, (QueryExpression) null, (SubQuery) null);

        this.colTypes          = columnTypes;
        this.columnList        = columnList;
        columnCount            = columnList.size();

        createPrimaryKey(null, pkColumns, true);
    }

    public int getId() {
        return 0;
    }

    public boolean isWritable() {
        return true;
    }

    public boolean isInsertable() {
        return queryExpression == null ? false
                                       : queryExpression.isInsertable();
    }

    public boolean isUpdatable() {
        return queryExpression == null ? false
                                       : queryExpression.isUpdatable();
    }

    public int[] getUpdatableColumns() {
        return defaultColumnMap;
    }

    public Table getBaseTable() {
        return queryExpression == null ? this
                                       : queryExpression.getBaseTable();
    }

    public int[] getBaseTableColumnMap() {

        return queryExpression == null ? null
                                       : queryExpression
                                           .getBaseTableColumnMap();
    }

    public SubQuery getSubQuery() {
        return subQuery;
    }

    public QueryExpression getQueryExpression() {
        return queryExpression;
    }
}
