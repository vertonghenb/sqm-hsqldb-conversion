


package org.hsqldb;

import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;


public class ExpressionOrderBy extends Expression {

    private boolean isDescending;
    private boolean isNullsLast;

    ExpressionOrderBy(Expression e) {

        super(OpTypes.ORDER_BY);

        nodes       = new Expression[UNARY];
        nodes[LEFT] = e;
        collation   = e.collation;
        e.collation = null;
    }

    
    void setDescending() {
        isDescending = true;
    }

    
    boolean isDescending() {
        return isDescending;
    }

    
    void setNullsLast(boolean value) {
        isNullsLast = value;
    }

    
    boolean isNullsLast() {
        return isNullsLast;
    }

    public Object getValue(Session session) {
        return nodes[LEFT].getValue(session);
    }

    public void resolveTypes(Session session, Expression parent) {

        nodes[LEFT].resolveTypes(session, parent);

        if (nodes[LEFT].isUnresolvedParam()) {
            throw Error.error(ErrorCode.X_42567);
        }

        dataType = nodes[LEFT].dataType;

        if (collation != null && !dataType.isCharacterType()) {
            throw Error.error(ErrorCode.X_2H000,
                              collation.getName().statementName);
        }
    }

    public String getSQL() {

        StringBuffer sb = new StringBuffer();

        sb.append(Tokens.T_ORDER).append(' ').append(Tokens.T_BY).append(' ');

        if (nodes[LEFT].alias != null) {
            sb.append(nodes[LEFT].alias.name);
        } else {
            sb.append(nodes[LEFT].getSQL());
        }

        if (collation != null) {
            sb.append(' ').append(
                collation.getName().getSchemaQualifiedStatementName());
        }

        if (isDescending) {
            sb.append(' ').append(Tokens.T_DESC);
        }

        return sb.toString();
    }

    protected String describe(Session session, int blanks) {

        StringBuffer sb = new StringBuffer();

        for (int i = 0; i < blanks; i++) {
            sb.append(' ');
        }

        sb.append(getLeftNode().describe(session, blanks));

        if (isDescending) {
            sb.append(Tokens.T_DESC).append(' ');
        }

        return sb.toString();
    }
}
