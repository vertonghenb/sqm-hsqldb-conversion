package org.hsqldb.types;
import org.hsqldb.Session;
import org.hsqldb.SessionInterface;
import org.hsqldb.Tokens;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
public final class NullType extends Type {
    static final NullType nullType = new NullType();
    private NullType() {
        super(Types.SQL_ALL_TYPES, Types.SQL_ALL_TYPES, 0, 0);
    }
    public int displaySize() {
        return 4;
    }
    public int getJDBCTypeCode() {
        return typeCode;
    }
    public Class getJDBCClass() {
        return java.lang.Void.class;
    }
    public String getJDBCClassName() {
        return "java.lang.Void";
    }
    public String getNameString() {
        return Tokens.T_NULL;
    }
    public String getDefinition() {
        return Tokens.T_NULL;
    }
    public Type getAggregateType(Type other) {
        return other;
    }
    public Type getCombinedType(Session session, Type other, int operation) {
        return other;
    }
    public int compare(Session session, Object a, Object b) {
        throw Error.runtimeError(ErrorCode.U_S0500, "NullType");
    }
    public Object convertToTypeLimits(SessionInterface session, Object a) {
        return null;
    }
    public Object convertToType(SessionInterface session, Object a,
                                Type otherType) {
        return null;
    }
    public Object convertToDefaultType(SessionInterface session, Object a) {
        return null;
    }
    public String convertToString(Object a) {
        throw Error.runtimeError(ErrorCode.U_S0500, "NullType");
    }
    public String convertToSQLString(Object a) {
        throw Error.runtimeError(ErrorCode.U_S0500, "NullType");
    }
    public boolean canConvertFrom(Type otherType) {
        return true;
    }
    public static Type getNullType() {
        return nullType;
    }
}