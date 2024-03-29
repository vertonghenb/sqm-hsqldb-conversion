package org.hsqldb.types;
import java.sql.Array;
import org.hsqldb.OpTypes;
import org.hsqldb.Session;
import org.hsqldb.SessionInterface;
import org.hsqldb.SortAndSlice;
import org.hsqldb.Tokens;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.jdbc.JDBCArray;
import org.hsqldb.jdbc.JDBCArrayBasic;
import org.hsqldb.lib.ArraySort;
public class ArrayType extends Type {
    public static final int defaultArrayCardinality      = 1024;
    public static final int defaultLargeArrayCardinality = Integer.MAX_VALUE;
    final Type              dataType;
    final int               maxCardinality;
    public ArrayType(Type dataType, int cardinality) {
        super(Types.SQL_ARRAY, Types.SQL_ARRAY, 0, 0);
        if (dataType == null) {
            dataType = Type.SQL_ALL_TYPES;
        }
        this.dataType       = dataType;
        this.maxCardinality = cardinality;
    }
    public int displaySize() {
        return 7 + (dataType.displaySize() + 1) * maxCardinality;
    }
    public int getJDBCTypeCode() {
        return Types.ARRAY;
    }
    public Class getJDBCClass() {
        return java.sql.Array.class;
    }
    public String getJDBCClassName() {
        return "java.sql.Array";
    }
    public int getJDBCScale() {
        return 0;
    }
    public int getJDBCPrecision() {
        return 0;
    }
    public int getSQLGenericTypeCode() {
        return 0;
    }
    public String getNameString() {
        StringBuffer sb = new StringBuffer();
        sb.append(dataType.getNameString()).append(' ');
        sb.append(Tokens.T_ARRAY);
        if (maxCardinality != defaultArrayCardinality) {
            sb.append('[').append(maxCardinality).append(']');
        }
        return sb.toString();
    }
    public String getFullNameString() {
        StringBuffer sb = new StringBuffer();
        sb.append(dataType.getFullNameString()).append(' ');
        sb.append(Tokens.T_ARRAY);
        if (maxCardinality != defaultArrayCardinality) {
            sb.append('[').append(maxCardinality).append(']');
        }
        return sb.toString();
    }
    public String getDefinition() {
        StringBuffer sb = new StringBuffer();
        sb.append(dataType.getDefinition()).append(' ');
        sb.append(Tokens.T_ARRAY);
        if (maxCardinality != defaultArrayCardinality) {
            sb.append('[').append(maxCardinality).append(']');
        }
        return sb.toString();
    }
    public int compare(Session session, Object a, Object b) {
        if (a == b) {
            return 0;
        }
        if (a == null) {
            return -1;
        }
        if (b == null) {
            return 1;
        }
        Object[] arra   = (Object[]) a;
        Object[] arrb   = (Object[]) b;
        int      length = arra.length;
        if (arrb.length < length) {
            length = arrb.length;
        }
        for (int i = 0; i < length; i++) {
            int result = dataType.compare(session, arra[i], arrb[i]);
            if (result != 0) {
                return result;
            }
        }
        if (arra.length > arrb.length) {
            return 1;
        } else if (arra.length < arrb.length) {
            return -1;
        }
        return 0;
    }
    public Object convertToTypeLimits(SessionInterface session, Object a) {
        if (a == null) {
            return null;
        }
        Object[] arra = (Object[]) a;
        if (arra.length > maxCardinality) {
            throw Error.error(ErrorCode.X_2202F);
        }
        Object[] arrb = new Object[arra.length];
        for (int i = 0; i < arra.length; i++) {
            arrb[i] = dataType.convertToTypeLimits(session, arra[i]);
        }
        return arrb;
    }
    public Object convertToType(SessionInterface session, Object a,
                                Type otherType) {
        if (a == null) {
            return null;
        }
        if (otherType == null) {
            return a;
        }
        if (!otherType.isArrayType()) {
            throw Error.error(ErrorCode.X_42562);
        }
        Object[] arra = (Object[]) a;
        if (arra.length > maxCardinality) {
            throw Error.error(ErrorCode.X_2202F);
        }
        Type otherComponent = otherType.collectionBaseType();
        if (dataType.equals(otherComponent)) {
            return a;
        }
        Object[] arrb = new Object[arra.length];
        for (int i = 0; i < arra.length; i++) {
            arrb[i] = dataType.convertToType(session, arra[i], otherComponent);
        }
        return arrb;
    }
    public Object convertJavaToSQL(SessionInterface session, Object a) {
        Object[] data;
        boolean  convert = false;
        if (a == null) {
            return null;
        }
        if (a instanceof Object[]) {
            data    = (Object[]) a;
            convert = true;
        } else if (a instanceof JDBCArray) {
            data = ((JDBCArray) a).getArrayInternal();
        } else if (a instanceof JDBCArrayBasic) {
            data    = (Object[]) ((JDBCArrayBasic) a).getArray();
            convert = true;
        } else if (a instanceof java.sql.Array) {
            try {
                data    = (Object[]) ((Array) a).getArray();
                convert = true;
            } catch (Exception e) {
                throw Error.error(ErrorCode.X_42561);
            }
        } else {
            throw Error.error(ErrorCode.X_42561);
        }
        if (convert) {
            Object[] array = new Object[data.length];
            for (int i = 0; i < data.length; i++) {
                array[i] = dataType.convertJavaToSQL(session, data[i]);
                array[i] = dataType.convertToTypeLimits(session, data[i]);
            }
            return array;
        }
        return data;
    }
    public Object convertSQLToJava(SessionInterface session, Object a) {
        if (a instanceof Object[]) {
            Object[] data = (Object[]) a;
            return new JDBCArray(data, this.collectionBaseType(), this,
                                 session);
        }
        throw Error.error(ErrorCode.X_42561);
    }
    public Object convertToDefaultType(SessionInterface sessionInterface,
                                       Object o) {
        return o;
    }
    public String convertToString(Object a) {
        if (a == null) {
            return null;
        }
        return convertToSQLString(a);
    }
    public String convertToSQLString(Object a) {
        if (a == null) {
            return Tokens.T_NULL;
        }
        Object[]     arra = (Object[]) a;
        StringBuffer sb   = new StringBuffer();
        sb.append(Tokens.T_ARRAY);
        sb.append('[');
        for (int i = 0; i < arra.length; i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(dataType.convertToSQLString(arra[i]));
        }
        sb.append(']');
        return sb.toString();
    }
    public boolean canConvertFrom(Type otherType) {
        if (otherType == null) {
            return true;
        }
        if (!otherType.isArrayType()) {
            return false;
        }
        Type otherComponent = otherType.collectionBaseType();
        return dataType.canConvertFrom(otherComponent);
    }
    public int canMoveFrom(Type otherType) {
        if (otherType == this) {
            return 0;
        }
        if (!otherType.isArrayType()) {
            return -1;
        }
        if (maxCardinality >= ((ArrayType) otherType).maxCardinality) {
            return dataType.canMoveFrom((ArrayType) otherType);
        } else {
            if (dataType.canMoveFrom((ArrayType) otherType) == -1) {
                return -1;
            }
            return 1;
        }
    }
    public boolean canBeAssignedFrom(Type otherType) {
        if (otherType == null) {
            return true;
        }
        Type otherComponent = otherType.collectionBaseType();
        return otherComponent != null
               && dataType.canBeAssignedFrom(otherComponent);
    }
    public Type collectionBaseType() {
        return dataType;
    }
    public int arrayLimitCardinality() {
        return maxCardinality;
    }
    public boolean isArrayType() {
        return true;
    }
    public Type getAggregateType(Type other) {
        if (other == null) {
            return this;
        }
        if (other == SQL_ALL_TYPES) {
            return this;
        }
        if (this == other) {
            return this;
        }
        if (!other.isArrayType()) {
            throw Error.error(ErrorCode.X_42562);
        }
        Type otherComponent = other.collectionBaseType();
        if (dataType.equals(otherComponent)) {
            return ((ArrayType) other).maxCardinality > maxCardinality ? other
                                                                       : this;
        }
        Type newComponent = dataType.getAggregateType(otherComponent);
        int cardinality = ((ArrayType) other).maxCardinality > maxCardinality
                          ? ((ArrayType) other).maxCardinality
                          : maxCardinality;
        return new ArrayType(newComponent, cardinality);
    }
    public Type getCombinedType(Session session, Type other, int operation) {
        ArrayType type = (ArrayType) getAggregateType(other);
        if (other == null) {
            return type;
        }
        if (operation != OpTypes.CONCAT) {
            return type;
        }
        if (type.maxCardinality == ArrayType.defaultLargeArrayCardinality) {
            return type;
        }
        long card = (long) ((ArrayType) other).maxCardinality + maxCardinality;
        if (card > ArrayType.defaultLargeArrayCardinality) {
            card = ArrayType.defaultLargeArrayCardinality;
        }
        return new ArrayType(dataType, (int) card);
    }
    public int cardinality(Session session, Object a) {
        if (a == null) {
            return 0;
        }
        return ((Object[]) a).length;
    }
    public Object concat(Session session, Object a, Object b) {
        if (a == null || b == null) {
            return null;
        }
        int      size  = ((Object[]) a).length + ((Object[]) b).length;
        Object[] array = new Object[size];
        System.arraycopy(a, 0, array, 0, ((Object[]) a).length);
        System.arraycopy(b, 0, array, ((Object[]) a).length,
                         ((Object[]) b).length);
        return array;
    }
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if (other instanceof Type) {
            if (((Type) other).typeCode != Types.SQL_ARRAY) {
                return false;
            }
            return maxCardinality == ((ArrayType) other).maxCardinality
                   && dataType.equals(((ArrayType) other).dataType);
        }
        return false;
    }
    public void sort(Session session, Object a, SortAndSlice sort) {
        Object[]        array      = (Object[]) a;
        TypedComparator comparator = session.getComparator();
        comparator.setType(dataType, sort);
        ArraySort.sort(array, 0, array.length, comparator);
    }
    public int deDuplicate(Session session, Object a, SortAndSlice sort) {
        Object[]        array      = (Object[]) a;
        TypedComparator comparator = session.getComparator();
        comparator.setType(dataType, sort);
        return ArraySort.deDuplicate(array, 0, array.length, comparator);
    }
}