package org.hsqldb;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.HsqlList;
import org.hsqldb.types.BinaryData;
import org.hsqldb.types.Type;
import org.hsqldb.types.Types;
public final class ExpressionLike extends ExpressionLogical {
    private static final int ESCAPE = 2;
    private Like             likeObject;
    ExpressionLike(Expression left, Expression right, Expression escape,
                   boolean noOptimisation) {
        super(OpTypes.LIKE);
        nodes               = new Expression[TERNARY];
        nodes[LEFT]         = left;
        nodes[RIGHT]        = right;
        nodes[ESCAPE]       = escape;
        likeObject          = new Like();
        this.noOptimisation = noOptimisation;
    }
    private ExpressionLike(ExpressionLike other) {
        super(OpTypes.LIKE);
        this.nodes      = other.nodes;
        this.likeObject = other.likeObject;
    }
    public HsqlList resolveColumnReferences(Session session,
            RangeVariable[] rangeVarArray, int rangeCount,
            HsqlList unresolvedSet, boolean acceptsSequences) {
        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] != null) {
                unresolvedSet = nodes[i].resolveColumnReferences(session,
                        rangeVarArray, rangeCount, unresolvedSet,
                        acceptsSequences);
            }
        }
        return unresolvedSet;
    }
    public Object getValue(Session session) {
        if (opType != OpTypes.LIKE) {
            return super.getValue(session);
        }
        Object leftValue   = nodes[LEFT].getValue(session);
        Object rightValue  = nodes[RIGHT].getValue(session);
        Object escapeValue = nodes[ESCAPE] == null ? null
                                                   : nodes[ESCAPE].getValue(
                                                       session);
        if (likeObject.isVariable) {
            synchronized (likeObject) {
                likeObject.setPattern(session, rightValue, escapeValue,
                                      nodes[ESCAPE] != null);
                return likeObject.compare(session, leftValue);
            }
        }
        return likeObject.compare(session, leftValue);
    }
    public void resolveTypes(Session session, Expression parent) {
        if (opType != OpTypes.LIKE) {
            return;
        }
        for (int i = 0; i < nodes.length; i++) {
            if (nodes[i] != null) {
                nodes[i].resolveTypes(session, this);
            }
        }
        boolean isEscapeFixedConstant = true;
        if (nodes[ESCAPE] != null) {
            if (nodes[ESCAPE].isUnresolvedParam()) {
                throw Error.error(ErrorCode.X_42567);
            }
            nodes[ESCAPE].resolveTypes(session, this);
            isEscapeFixedConstant = nodes[ESCAPE].opType == OpTypes.VALUE;
            if (isEscapeFixedConstant) {
                nodes[ESCAPE].setAsConstantValue(session);
                if (nodes[ESCAPE].dataType == null) {
                    throw Error.error(ErrorCode.X_42567);
                }
                if (nodes[ESCAPE].valueData != null) {
                    long length;
                    switch (nodes[ESCAPE].dataType.typeCode) {
                        case Types.SQL_CHAR :
                        case Types.SQL_VARCHAR :
                            length =
                                ((String) nodes[ESCAPE].valueData).length();
                            break;
                        case Types.SQL_BINARY :
                        case Types.SQL_VARBINARY :
                            length =
                                ((BinaryData) nodes[ESCAPE].valueData).length(
                                    session);
                            break;
                        default :
                            throw Error.error(ErrorCode.X_42563);
                    }
                    if (length != 1) {
                        throw Error.error(ErrorCode.X_22019);
                    }
                }
            }
        }
        if (nodes[LEFT].isUnresolvedParam()
                && nodes[RIGHT].isUnresolvedParam()) {
            nodes[LEFT].dataType = Type.SQL_VARCHAR_DEFAULT;
        }
        if (nodes[LEFT].dataType == null && nodes[RIGHT].dataType == null) {
            throw Error.error(ErrorCode.X_42567);
        }
        if (nodes[LEFT].isUnresolvedParam()) {
            nodes[LEFT].dataType = nodes[RIGHT].dataType.isBinaryType()
                                   ? Type.SQL_VARBINARY_DEFAULT
                                   : Type.SQL_VARCHAR_DEFAULT;
        } else if (nodes[RIGHT].isUnresolvedParam()) {
            nodes[RIGHT].dataType = nodes[LEFT].dataType.isBinaryType()
                                    ? Type.SQL_VARBINARY_DEFAULT
                                    : Type.SQL_VARCHAR_DEFAULT;
        }
        if (nodes[LEFT].dataType == null || nodes[RIGHT].dataType == null) {
            throw Error.error(ErrorCode.X_42567);
        }
        switch (nodes[LEFT].dataType.typeComparisonGroup) {
            case Types.SQL_VARCHAR : {
                if (nodes[RIGHT].dataType.isCharacterType()
                        && (nodes[ESCAPE] == null
                            || nodes[ESCAPE].dataType.isCharacterType())) {
                    boolean ignoreCase =
                        nodes[LEFT].dataType.typeCode == Types
                            .VARCHAR_IGNORECASE || nodes[RIGHT].dataType
                            .typeCode == Types.VARCHAR_IGNORECASE;
                    likeObject.setIgnoreCase(ignoreCase);
                } else {
                    throw Error.error(ErrorCode.X_42563);
                }
                break;
            }
            case Types.SQL_VARBINARY : {
                if (nodes[RIGHT].dataType.isBinaryType()
                        && (nodes[ESCAPE] == null
                            || nodes[ESCAPE].dataType.isBinaryType())) {
                    likeObject.isBinary = true;
                } else {
                    throw Error.error(ErrorCode.X_42563);
                }
                break;
            }
            case Types.OTHER : {
                throw Error.error(ErrorCode.X_42563);
            }
            default : {
                if (session.database.sqlEnforceTypes) {
                    throw Error.error(ErrorCode.X_42562);
                }
                nodes[LEFT] = ExpressionOp.getCastExpression(session,
                        nodes[LEFT], Type.SQL_VARCHAR_DEFAULT);
                if (nodes[RIGHT].dataType.isCharacterType()
                        && (nodes[ESCAPE] == null
                            || nodes[ESCAPE].dataType.isCharacterType())) {
                    boolean ignoreCase = nodes[RIGHT].dataType.typeCode
                                         == Types.VARCHAR_IGNORECASE;
                    likeObject.setIgnoreCase(ignoreCase);
                } else {
                    throw Error.error(ErrorCode.X_42563);
                }
                break;
            }
        }
        likeObject.dataType = nodes[LEFT].dataType;
        boolean isRightArgFixedConstant = nodes[RIGHT].opType == OpTypes.VALUE;
        if (isRightArgFixedConstant && isEscapeFixedConstant) {
            if (nodes[LEFT].opType == OpTypes.VALUE) {
                setAsConstantValue(session);
                likeObject = null;
                return;
            }
            likeObject.isVariable = false;
        }
        Object pattern = isRightArgFixedConstant
                         ? nodes[RIGHT].getValue(session)
                         : null;
        boolean constantEscape = isEscapeFixedConstant
                                 && nodes[ESCAPE] != null;
        Object escape = constantEscape ? nodes[ESCAPE].getValue(session)
                                       : null;
        likeObject.setPattern(session, pattern, escape, nodes[ESCAPE] != null);
        if (noOptimisation) {
            return;
        }
        if (likeObject.isEquivalentToUnknownPredicate()) {
            this.setAsConstantValue(session);
            likeObject = null;
            return;
        }
        if (likeObject.isEquivalentToEqualsPredicate()) {
            opType = OpTypes.EQUAL;
            nodes[RIGHT] = new ExpressionValue(likeObject.getRangeLow(),
                                               Type.SQL_VARCHAR);
            likeObject = null;
            return;
        }
        if (likeObject.isEquivalentToNotNullPredicate()) {
            Expression notNull = new ExpressionLogical(OpTypes.IS_NULL,
                nodes[LEFT]);
            opType      = OpTypes.NOT;
            nodes       = new Expression[UNARY];
            nodes[LEFT] = notNull;
            likeObject  = null;
            return;
        }
        if (nodes[LEFT].opType == OpTypes.COLUMN) {
            ExpressionLike newLike = new ExpressionLike(this);
            Expression prefix = new ExpressionOp(OpTypes.LIKE_ARG,
                                                 nodes[RIGHT], nodes[ESCAPE]);
            prefix.resolveTypes(session, null);
            Expression cast = new ExpressionOp(OpTypes.PREFIX, nodes[LEFT],
                                               prefix);
            Expression equ = new ExpressionLogical(OpTypes.EQUAL, cast,
                                                   prefix);
            equ = new ExpressionLogical(OpTypes.GREATER_EQUAL, nodes[LEFT],
                                        prefix, equ);
            equ.setSubType(OpTypes.LIKE);
            nodes        = new Expression[BINARY];
            likeObject   = null;
            nodes[LEFT]  = equ;
            nodes[RIGHT] = newLike;
            opType       = OpTypes.AND;
        }
    }
    public String getSQL() {
        if (likeObject == null) {
            return super.getSQL();
        }
        String       left  = getContextSQL(nodes[LEFT]);
        String       right = getContextSQL(nodes[RIGHT]);
        StringBuffer sb    = new StringBuffer();
        sb.append(left).append(' ').append(Tokens.T_LIKE).append(' ');
        sb.append(right);
        if (nodes[ESCAPE] != null) {
            sb.append(' ').append(Tokens.T_ESCAPE).append(' ');
            sb.append(nodes[ESCAPE].getSQL());
            sb.append(' ');
        }
        return sb.toString();
    }
    protected String describe(Session session, int blanks) {
        if (likeObject == null) {
            return super.describe(session, blanks);
        }
        StringBuffer sb = new StringBuffer();
        sb.append('\n');
        for (int i = 0; i < blanks; i++) {
            sb.append(' ');
        }
        sb.append("LIKE ");
        sb.append(likeObject.describe(session));
        return sb.toString();
    }
}