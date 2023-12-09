


package org.hsqldb;

import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.HsqlList;
import org.hsqldb.types.Type;


public class ExpressionAccessor extends Expression {

    ExpressionAccessor(Expression left, Expression right) {

        super(OpTypes.ARRAY_ACCESS);

        nodes = new Expression[] {
            left, right
        };
    }

    public ColumnSchema getColumn() {
        return nodes[LEFT].getColumn();
    }

    public HsqlList resolveColumnReferences(Session session,
            RangeVariable[] rangeVarArray, int rangeCount,
            HsqlList unresolvedSet, boolean acceptsSequences) {

        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] == null) {
                continue;
            }

            unresolvedSet = nodes[i].resolveColumnReferences(session,
                    rangeVarArray, rangeCount, unresolvedSet,
                    acceptsSequences);
        }

        return unresolvedSet;
    }

    public void resolveTypes(Session session, Expression parent) {

        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] != null) {
                nodes[i].resolveTypes(session, this);
            }
        }

        if (nodes[LEFT].dataType == null) {
            throw Error.error(ErrorCode.X_42567);
        }

        if (!nodes[LEFT].dataType.isArrayType()) {
            throw Error.error(ErrorCode.X_42563);
        }

        dataType = nodes[LEFT].dataType.collectionBaseType();

        if (nodes[RIGHT].opType == OpTypes.DYNAMIC_PARAM) {
            nodes[RIGHT].dataType = Type.SQL_INTEGER;
        }
    }

    public Object getValue(Session session) {

        Object[] array = (Object[]) nodes[LEFT].getValue(session);

        if (array == null) {
            return null;
        }

        Number index = (Number) nodes[RIGHT].getValue(session);

        if (index == null) {
            return null;
        }

        if (index.intValue() < 1 || index.intValue() > array.length) {
            throw Error.error(ErrorCode.X_2202E);
        }

        return array[index.intValue() - 1];
    }

    
    public Object[] getUpdatedArray(Session session, Object[] array,
                                    Object value, boolean copy) {

        if (array == null) {
            throw Error.error(ErrorCode.X_2200E);
        }

        Number index = (Number) nodes[RIGHT].getValue(session);

        if (index == null) {
            throw Error.error(ErrorCode.X_2202E);
        }

        int i = index.intValue() - 1;

        if (i < 0) {
            throw Error.error(ErrorCode.X_2202E);
        }

        if (i >= nodes[LEFT].dataType.arrayLimitCardinality()) {
            throw Error.error(ErrorCode.X_2202E);
        }

        Object[] newArray = array;

        if (i >= array.length) {
            newArray = new Object[i + 1];

            System.arraycopy(array, 0, newArray, 0, array.length);
        } else if (copy) {
            newArray = new Object[array.length];

            System.arraycopy(array, 0, newArray, 0, array.length);
        }

        newArray[i] = value;

        return newArray;
    }

    public String getSQL() {

        StringBuffer sb   = new StringBuffer(64);
        String       left = getContextSQL(nodes[LEFT]);

        sb.append(left).append('[');
        sb.append(nodes[RIGHT].getSQL()).append(']');

        return sb.toString();
    }

    protected String describe(Session session, int blanks) {

        StringBuffer sb = new StringBuffer(64);

        sb.append('\n');

        for (int i = 0; i < blanks; i++) {
            sb.append(' ');
        }

        sb.append("ARRAY ACCESS");

        if (getLeftNode() != null) {
            sb.append(" array=[");
            sb.append(nodes[LEFT].describe(session, blanks + 1));
            sb.append(']');
        }

        if (getRightNode() != null) {
            sb.append(" array_index=[");
            sb.append(nodes[RIGHT].describe(session, blanks + 1));
            sb.append(']');
        }

        return sb.toString();
    }
}
