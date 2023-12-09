


package org.hsqldb;

import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.ArraySort;
import org.hsqldb.lib.OrderedHashSet;





public class View extends TableDerived {

    SubQuery       viewSubQuery;
    private String statement;

    
    private HsqlName[] columnNames;

    
    SubQuery[] viewSubqueries;

    
    private OrderedHashSet schemaObjectNames;

    
    private int check;

    
    private Table baseTable;

    
    Expression checkExpression;

    
    boolean isTriggerInsertable;
    boolean isTriggerUpdatable;
    boolean isTriggerDeletable;

    View(Database db, HsqlName name, HsqlName[] columnNames, int check) {

        super(db, name, TableBase.VIEW_TABLE);

        this.columnNames = columnNames;
        this.check       = check;
    }

    public int getType() {
        return SchemaObject.VIEW;
    }

    public OrderedHashSet getReferences() {
        return schemaObjectNames;
    }

    public OrderedHashSet getComponents() {
        return null;
    }

    
    public void compile(Session session, SchemaObject parentObject) {

        ParserDQL p = new ParserDQL(session, new Scanner(statement));

        p.read();

        viewSubQuery    = p.XreadViewSubquery(this);
        queryExpression = viewSubQuery.queryExpression;

        if (getColumnCount() == 0) {
            if (columnNames == null) {
                columnNames =
                    viewSubQuery.queryExpression.getResultColumnNames();
            }

            if (columnNames.length
                    != viewSubQuery.queryExpression.getColumnCount()) {
                throw Error.error(ErrorCode.X_42593, getName().statementName);
            }

            TableUtil.setColumnsInSchemaTable(
                this, columnNames, queryExpression.getColumnTypes());
        }

        
        OrderedHashSet set = queryExpression.getSubqueries();

        if (set == null) {
            viewSubqueries = new SubQuery[]{ viewSubQuery };
        } else {
            set.add(viewSubQuery);

            viewSubqueries = new SubQuery[set.size()];

            set.toArray(viewSubqueries);
            ArraySort.sort(viewSubqueries, 0, viewSubqueries.length,
                           viewSubqueries[0]);
        }

        for (int i = 0; i < viewSubqueries.length; i++) {
            if (viewSubqueries[i].parentView == null) {
                viewSubqueries[i].parentView = this;
            }

            viewSubqueries[i].prepareTable(session);
        }

        
        viewSubQuery.getTable().view       = this;
        viewSubQuery.getTable().columnList = columnList;
        schemaObjectNames = p.compileContext.getSchemaObjectNames();
        baseTable                          = queryExpression.getBaseTable();

        if (baseTable == null) {
            return;
        }

        switch (check) {

            case SchemaObject.ViewCheckModes.CHECK_NONE :
                break;

            case SchemaObject.ViewCheckModes.CHECK_LOCAL :
                checkExpression = queryExpression.getCheckCondition();
                break;

            case SchemaObject.ViewCheckModes.CHECK_CASCADE :
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "View");
        }
    }

    public String getSQL() {

        StringBuffer sb = new StringBuffer(128);

        sb.append(Tokens.T_CREATE).append(' ').append(Tokens.T_VIEW);
        sb.append(' ');
        sb.append(getName().getSchemaQualifiedStatementName()).append(' ');
        sb.append('(');

        int count = getColumnCount();

        for (int j = 0; j < count; j++) {
            sb.append(getColumn(j).getName().statementName);

            if (j < count - 1) {
                sb.append(',');
            }
        }

        sb.append(')').append(' ').append(Tokens.T_AS).append(' ');
        sb.append(getStatement());

        return sb.toString();
    }

    public int[] getUpdatableColumns() {
        return queryExpression.getBaseTableColumnMap();
    }

    public long getChangeTimestamp() {
        return changeTimestamp;
    }

    public boolean isTriggerInsertable() {
        return isTriggerInsertable;
    }

    public boolean isTriggerUpdatable() {
        return isTriggerUpdatable;
    }

    public boolean isTriggerDeletable() {
        return isTriggerDeletable;
    }

    public boolean isInsertable() {
        return isTriggerInsertable ? false : super.isInsertable();
    }

    public boolean isUpdatable() {
        return isTriggerUpdatable ? false : super.isUpdatable();
    }


    void addTrigger(TriggerDef td, HsqlName otherName) {

        switch (td.operationType) {

            case StatementTypes.INSERT :
                if (isTriggerInsertable) {
                    throw Error.error(ErrorCode.X_42538);
                }

                isTriggerInsertable = true;
                break;

            case StatementTypes.DELETE_WHERE :
                if (isTriggerDeletable) {
                    throw Error.error(ErrorCode.X_42538);
                }

                isTriggerDeletable = true;
                break;

            case StatementTypes.UPDATE_WHERE :
                if (isTriggerUpdatable) {
                    throw Error.error(ErrorCode.X_42538);
                }

                isTriggerUpdatable = true;
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "View");
        }

        super.addTrigger(td, otherName);
    }

    void removeTrigger(TriggerDef td) {

        switch (td.operationType) {

            case StatementTypes.INSERT :
                isTriggerInsertable = false;
                break;

            case StatementTypes.DELETE_WHERE :
                isTriggerDeletable = false;
                break;

            case StatementTypes.UPDATE_WHERE :
                isTriggerUpdatable = false;
                break;

            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "View");
        }

        super.removeTrigger(td);
    }

    public int getCheckOption() {
        return check;
    }

    
    public String getStatement() {
        return statement;
    }

    public void setStatement(String sql) {
        statement = sql;
    }

    
    public void setDataReadOnly(boolean value) {
        throw Error.error(ErrorCode.X_28000);
    }

    public void collectAllFunctionExpressions(OrderedHashSet collector) {

        
    }

    public Table getSubqueryTable() {
        return viewSubQuery.getTable();
    }

    public SubQuery[] getSubqueries() {
        return viewSubqueries;
    }
}
