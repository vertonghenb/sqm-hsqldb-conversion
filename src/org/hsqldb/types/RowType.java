package org.hsqldb.types;
import org.hsqldb.OpTypes;
import org.hsqldb.Session;
import org.hsqldb.SessionInterface;
import org.hsqldb.SortAndSlice;
import org.hsqldb.Tokens;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
public class RowType extends Type {
    final Type[] dataTypes;
    public RowType(Type[] dataTypes) {
        super(Types.SQL_ROW, Types.SQL_ROW, 0, 0);
        this.dataTypes = dataTypes;
    }
    public int displaySize() {
        return 0;
    }
    public int getJDBCTypeCode() {
        return Types.NULL;
    }
    public Class getJDBCClass() {
        return java.sql.ResultSet.class;
    }
    public String getJDBCClassName() {
        return "java.sql.ResultSet";
    }
    public int getJDBCScale() {
        return 0;
    }
    public int getJDBCPrecision() {
        return 0;
    }
    public int getSQLGenericTypeCode() {
        return Types.SQL_ROW;
    }
    public boolean isRowType() {
        return true;
    }
    public String getNameString() {
        StringBuffer sb = new StringBuffer();
        sb.append(Tokens.T_ROW);
        sb.append('(');
        for (int i = 0; i < dataTypes.length; i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(dataTypes[i].getDefinition());
        }
        sb.append(')');
        return sb.toString();
    }
    public String getDefinition() {
        return getNameString();
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
            int result = dataTypes[i].compare(session, arra[i], arrb[i]);
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
        Object[] arrb = new Object[arra.length];
        for (int i = 0; i < arra.length; i++) {
            arrb[i] = dataTypes[i].convertToTypeLimits(session, arra[i]);
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
        if (!otherType.isRowType()) {
            throw Error.error(ErrorCode.X_42562);
        }
        Type[] otherTypes = ((RowType) otherType).getTypesArray();
        if (dataTypes.length != otherTypes.length) {
            throw Error.error(ErrorCode.X_42564);
        }
        Object[] arra = (Object[]) a;
        Object[] arrb = new Object[arra.length];
        for (int i = 0; i < arra.length; i++) {
            arrb[i] = dataTypes[i].convertToType(session, arra[i],
                                                 otherTypes[i]);
        }
        return arrb;
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
        sb.append(Tokens.T_ROW);
        sb.append('(');
        for (int i = 0; i < arra.length; i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(dataTypes[i].convertToSQLString(arra[i]));
        }
        sb.append(')');
        return sb.toString();
    }
    public boolean canConvertFrom(Type otherType) {
        if (otherType == null) {
            return true;
        }
        if (!otherType.isRowType()) {
            return false;
        }
        Type[] otherTypes = ((RowType) otherType).getTypesArray();
        if (dataTypes.length != otherTypes.length) {
            return false;
        }
        for (int i = 0; i < dataTypes.length; i++) {
            if (!dataTypes[i].canConvertFrom(otherTypes[i])) {
                return false;
            }
        }
        return true;
    }
    public boolean canBeAssignedFrom(Type otherType) {
        if (otherType == null) {
            return true;
        }
        if (!otherType.isRowType()) {
            return false;
        }
        Type[] otherTypes = ((RowType) otherType).getTypesArray();
        if (dataTypes.length != otherTypes.length) {
            return false;
        }
        for (int i = 0; i < dataTypes.length; i++) {
            if (!dataTypes[i].canBeAssignedFrom(otherTypes[i])) {
                return false;
            }
        }
        return true;
    }
    public Type getAggregateType(Type other) {
        if (other == null) {
            return this;
        }
        if (other == SQL_ALL_TYPES) {
            return this;
        }
        if (other == this) {
            return this;
        }
        if (!other.isRowType()) {
            throw Error.error(ErrorCode.X_42562);
        }
        Type[] newTypes   = new Type[dataTypes.length];
        Type[] otherTypes = ((RowType) other).getTypesArray();
        if (dataTypes.length != otherTypes.length) {
            throw Error.error(ErrorCode.X_42564);
        }
        for (int i = 0; i < dataTypes.length; i++) {
            newTypes[i] = dataTypes[i].getAggregateType(otherTypes[i]);
        }
        return new RowType(newTypes);
    }
    public Type getCombinedType(Session session, Type other, int operation) {
        if (operation != OpTypes.CONCAT) {
            return getAggregateType(other);
        }
        if (other == null) {
            return this;
        }
        if (!other.isRowType()) {
            throw Error.error(ErrorCode.X_42562);
        }
        Type[] newTypes   = new Type[dataTypes.length];
        Type[] otherTypes = ((RowType) other).getTypesArray();
        if (dataTypes.length != otherTypes.length) {
            throw Error.error(ErrorCode.X_42564);
        }
        for (int i = 0; i < dataTypes.length; i++) {
            newTypes[i] = dataTypes[i].getAggregateType(otherTypes[i]);
        }
        return new RowType(newTypes);
    }
    public Type[] getTypesArray() {
        return dataTypes;
    }
    public int compare(Session session, Object a, Object b,
                       SortAndSlice sort) {
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
        int      length = sort.sortOrder.length;
        for (int i = 0; i < length; i++) {
            a = arra[sort.sortOrder[i]];
            b = arrb[sort.sortOrder[i]];
            if (a == b) {
                continue;
            }
            if (sort.sortNullsLast[i]) {
                if (a == null) {
                    return 1;
                }
                if (b == null) {
                    return -1;
                }
            }
            int result = dataTypes[i].compare(session, a, b);
            if (result != 0) {
                if (sort.sortDescending[i]) {
                    return -result;
                }
                return result;
            }
        }
        return 0;
    }
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if (other instanceof Type) {
            if (((Type) other).typeCode != Types.SQL_ROW) {
                return false;
            }
            Type[] otherTypes = ((RowType) other).dataTypes;
            if (otherTypes.length != dataTypes.length) {
                return false;
            }
            for (int i = 0; i < dataTypes.length; i++) {
                if (!dataTypes[i].equals(otherTypes[i])) {
                    return false;
                }
                return true;
            }
        }
        return false;
    }
    public static String convertToSQLString(Object[] array, Type[] types,
            int maxUnitLength) {
        if (array == null) {
            return Tokens.T_NULL;
        }
        StringBuffer sb = new StringBuffer();
        sb.append('(');
        for (int i = 0; i < array.length; i++) {
            String value;
            if (i > 0) {
                sb.append(',');
            }
            value = types[i].convertToSQLString(array[i]);
            if (maxUnitLength > 7 && value.length() > maxUnitLength) {
                sb.append(value.substring(0, maxUnitLength - 4));
                sb.append(" ...");
            } else {
                sb.append(value);
            }
        }
        sb.append(')');
        return sb.toString();
    }
}