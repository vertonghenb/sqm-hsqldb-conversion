


package org.hsqldb;

import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.navigator.RowSetNavigator;
import org.hsqldb.navigator.RowSetNavigatorData;
import org.hsqldb.persist.PersistentStore;
import org.hsqldb.result.Result;
import org.hsqldb.store.ValuePool;
import org.hsqldb.types.RowType;
import org.hsqldb.types.Type;


public class ExpressionTable extends Expression {

    boolean isTable;
    boolean ordinality = false;

    
    ExpressionTable(Expression[] e, SubQuery sq, boolean ordinality) {

        super(OpTypes.TABLE);

        nodes           = e;
        this.subQuery   = sq;
        this.ordinality = ordinality;
    }

    public String getSQL() {

        if (isTable) {
            return Tokens.T_TABLE;
        } else {
            return Tokens.T_UNNEST;
        }
    }

    protected String describe(Session session, int blanks) {

        StringBuffer sb = new StringBuffer(64);

        sb.append('\n');

        for (int i = 0; i < blanks; i++) {
            sb.append(' ');
        }

        if (isTable) {
            sb.append(Tokens.T_TABLE).append(' ');
        } else {
            sb.append(Tokens.T_UNNEST).append(' ');
        }

        sb.append(nodes[LEFT].describe(session, blanks));

        return sb.toString();
    }

    public void resolveTypes(Session session, Expression parent) {

        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] != null) {
                nodes[i].resolveTypes(session, this);
            }
        }

        if (nodes.length == 1) {
            if (nodes[LEFT].dataType.isRowType()) {
                if (ordinality) {
                    throw Error.error(ErrorCode.X_42581, Tokens.T_ORDINALITY);
                }

                nodeDataTypes =
                    ((RowType) nodes[LEFT].dataType).getTypesArray();

                subQuery.prepareTable(session);

                subQuery.getTable().columnList =
                    ((FunctionSQLInvoked) nodes[LEFT]).routine.getTable()
                        .columnList;
                isTable = true;

                return;
            }
        }

        for (int i = 0; i < nodes.length; i++) {
            if (!nodes[i].dataType.isArrayType()) {
                throw Error.error(ErrorCode.X_42563, Tokens.T_UNNEST);
            }
        }

        int columnCount = ordinality ? nodes.length + 1
                                     : nodes.length;

        nodeDataTypes = new Type[columnCount];

        for (int i = 0; i < nodes.length; i++) {
            nodeDataTypes[i] = nodes[i].dataType.collectionBaseType();

            if (nodeDataTypes[i] == null
                    || nodeDataTypes[i] == Type.SQL_ALL_TYPES) {
                throw Error.error(ErrorCode.X_42567, Tokens.T_UNNEST);
            }
        }

        if (ordinality) {
            nodeDataTypes[nodes.length] = Type.SQL_INTEGER;
        }

        subQuery.prepareTable(session);
    }

    public Result getResult(Session session) {

        switch (opType) {

            case OpTypes.TABLE : {
                RowSetNavigatorData navigator = subQuery.getNavigator(session);
                Result              result    = Result.newResult(navigator);

                result.metaData = subQuery.queryExpression.getMetaData();

                return result;
            }
            default : {
                throw Error.runtimeError(ErrorCode.U_S0500, "ExpressionTable");
            }
        }
    }

    public Object[] getRowValue(Session session) {

        switch (opType) {

            case OpTypes.TABLE : {
                return subQuery.queryExpression.getValues(session);
            }
            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "Expression");
        }
    }

    Object getValue(Session session, Type type) {

        switch (opType) {

            case OpTypes.TABLE : {
                materialise(session);

                Object[] value = subQuery.getValues(session);

                if (value.length == 1) {
                    return ((Object[]) value)[0];
                }

                return value;
            }
            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "Expression");
        }
    }

    public Object getValue(Session session) {
        return valueData;
    }

    void insertValuesIntoSubqueryTable(Session session,
                                       PersistentStore store) {

        if (isTable) {
            insertTableValues(session, store);
        } else {
            insertArrayValues(session, store);
        }
    }

    private void insertTableValues(Session session, PersistentStore store) {

        Result          result = nodes[LEFT].getResult(session);
        RowSetNavigator nav    = result.navigator;
        int             size   = nav.getSize();

        while (nav.hasNext()) {
            Object[] data = nav.getNext();
            Row row = (Row) store.getNewCachedObject(session, data, false);

            try {
                store.indexRow(session, row);
            } catch (HsqlException e) {}
        }
    }

    private void insertArrayValues(Session session, PersistentStore store) {

        Object[][] array = new Object[nodes.length][];

        for (int i = 0; i < array.length; i++) {
            Object[] data = (Object[]) nodes[i].getValue(session);

            if (data == null) {
                data = ValuePool.emptyObjectArray;
            }

            array[i] = data;
        }

        for (int i = 0; ; i++) {
            boolean  isRow = false;
            Object[] data  = new Object[nodeDataTypes.length];

            for (int arrayIndex = 0; arrayIndex < array.length; arrayIndex++) {
                if (i < array[arrayIndex].length) {
                    data[arrayIndex] = array[arrayIndex][i];
                    isRow            = true;
                }
            }

            if (!isRow) {
                break;
            }

            if (ordinality) {
                data[nodes.length] = ValuePool.getInt(i + 1);
            }

            Row row = (Row) store.getNewCachedObject(session, data, false);

            store.indexRow(session, row);
        }
    }
}
