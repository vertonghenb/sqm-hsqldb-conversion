


package org.hsqldb.types;

import java.io.Serializable;

import org.hsqldb.Session;
import org.hsqldb.SessionInterface;
import org.hsqldb.Tokens;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.StringConverter;


public final class OtherType extends Type {

    static final OtherType otherType = new OtherType();

    private OtherType() {
        super(Types.OTHER, Types.OTHER, 0, 0);
    }

    public int displaySize() {
        return precision > Integer.MAX_VALUE ? Integer.MAX_VALUE
                                             : (int) precision;
    }

    public int getJDBCTypeCode() {
        return typeCode;
    }

    public Class getJDBCClass() {
        return java.lang.Object.class;
    }

    public String getJDBCClassName() {
        return "java.lang.Object";
    }

    public int getSQLGenericTypeCode() {

        
        return typeCode;
    }

    public int typeCode() {

        
        return typeCode;
    }

    public String getNameString() {
        return Tokens.T_OTHER;
    }

    public String getDefinition() {
        return Tokens.T_OTHER;
    }

    public Type getAggregateType(Type other) {

        if (other == null) {
            return this;
        }

        if (other == SQL_ALL_TYPES) {
            return this;
        }

        if (typeCode == other.typeCode) {
            return this;
        }

        throw Error.error(ErrorCode.X_42562);
    }

    public Type getCombinedType(Session session, Type other, int operation) {
        return this;
    }

    public int compare(Session session, Object a, Object b) {

        if (a == null) {
            return -1;
        }

        if (b == null) {
            return 1;
        }

        return 0;
    }

    public Object convertToTypeLimits(SessionInterface session, Object a) {
        return a;
    }

    
    public Object convertToType(SessionInterface session, Object a,
                                Type otherType) {
        return a;
    }

    public Object convertToDefaultType(SessionInterface session, Object a) {

        if (a instanceof Serializable) {
            return a;
        }

        throw Error.error(ErrorCode.X_42561);
    }

    public String convertToString(Object a) {

        if (a == null) {
            return null;
        }

        return StringConverter.byteArrayToHexString(
            ((JavaObjectData) a).getBytes());
    }

    public String convertToSQLString(Object a) {

        if (a == null) {
            return Tokens.T_NULL;
        }

        return StringConverter.byteArrayToSQLHexString(
            ((JavaObjectData) a).getBytes());
    }

    public Object convertSQLToJava(SessionInterface session, Object a) {

        if (a == null) {
            return null;
        }

        return ((JavaObjectData) a).getObject();
    }

    public boolean canConvertFrom(Type otherType) {

        if (otherType.typeCode == typeCode) {
            return true;
        }

        if (otherType.typeCode == Types.SQL_ALL_TYPES) {
            return true;
        }

        return false;
    }

    public boolean isObjectType() {
        return true;
    }

    public static OtherType getOtherType() {
        return otherType;
    }
}
