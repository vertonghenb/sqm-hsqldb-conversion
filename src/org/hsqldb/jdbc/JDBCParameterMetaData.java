


package org.hsqldb.jdbc;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.ParameterMetaData;
import java.sql.SQLException;

import org.hsqldb.persist.HsqlDatabaseProperties;
import org.hsqldb.result.ResultMetaData;
import org.hsqldb.types.DateTimeType;
import org.hsqldb.types.IntervalType;
import org.hsqldb.types.Type;















public class JDBCParameterMetaData implements ParameterMetaData,
        java.sql.Wrapper {






    
    public int getParameterCount() throws SQLException {
        return parameterCount;
    }

    
    public int isNullable(int param) throws SQLException {

        checkRange(param);

        return ParameterMetaData.parameterNullableUnknown;
    }

    
    public boolean isSigned(int param) throws SQLException {

        checkRange(param);

        Type type = translateType(rmd.columnTypes[--param]);

        return type.isNumberType();
    }

    
    public int getPrecision(int param) throws SQLException {

        checkRange(param);

        Type type = translateType(rmd.columnTypes[--param]);

        if (type.isDateTimeType()) {
            return type.displaySize();
        } else {
            long size = type.precision;

            if (size > Integer.MAX_VALUE) {
                size = 0;
            }

            return (int) size;
        }
    }

    
    public int getScale(int param) throws SQLException {

        checkRange(param);

        Type type = translateType(rmd.columnTypes[--param]);

        return type.scale;
    }

    
    public int getParameterType(int param) throws SQLException {

        checkRange(param);

        Type type = translateType(rmd.columnTypes[--param]);

        return type.getJDBCTypeCode();
    }

    
    public String getParameterTypeName(int param) throws SQLException {

        checkRange(param);

        Type type = translateType(rmd.columnTypes[--param]);

        return type.getNameString();
    }

    
    public String getParameterClassName(int param) throws SQLException {

        checkRange(param);

        Type type = translateType(rmd.columnTypes[--param]);

        return type.getJDBCClassName();
    }

    
    public int getParameterMode(int param) throws SQLException {

        checkRange(param);

        return rmd.paramModes[--param];
    }

    
    

    

    @SuppressWarnings("unchecked")
    public <T>T unwrap(java.lang.Class<T> iface) throws java.sql.SQLException {

        if (isWrapperFor(iface)) {
            return (T) this;
        }

        throw Util.invalidArgument("iface: " + iface);
    }



    

    public boolean isWrapperFor(
            java.lang.Class<?> iface) throws java.sql.SQLException {
        return (iface != null && iface.isAssignableFrom(this.getClass()));
    }


    

    
    ResultMetaData rmd;

    
    String[] classNames;

    
    int             parameterCount;
    private boolean translateTTIType;

    
    JDBCParameterMetaData(JDBCConnection conn,
                          ResultMetaData metaData) throws SQLException {

        rmd            = metaData;
        parameterCount = rmd.getColumnCount();

        if (conn.clientProperties != null) {
            translateTTIType = conn.clientProperties.isPropertyTrue(
                HsqlDatabaseProperties.jdbc_translate_tti_types);
        }
    }

    
    private Type translateType(Type type) {

        if (this.translateTTIType) {
            if (type.isIntervalType()) {
                type = ((IntervalType) type).getCharacterType();
            } else if (type.isDateTimeTypeWithZone()) {
                type = ((DateTimeType) type).getDateTimeTypeWithoutZone();
            }
        }

        return type;
    }

    
    void checkRange(int param) throws SQLException {

        if (param < 1 || param > parameterCount) {
            String msg = param + " is out of range";

            throw Util.outOfRangeArgument(msg);
        }
    }

    
    public String toString() {

        try {
            return toStringImpl();
        } catch (Throwable t) {
            return super.toString() + "[toStringImpl_exception=" + t + "]";
        }
    }

    
    private String toStringImpl() throws Exception {

        StringBuffer sb;
        Method[]     methods;
        Method       method;
        int          count;

        sb = new StringBuffer();

        sb.append(super.toString());

        count = getParameterCount();

        if (count == 0) {
            sb.append("[parameterCount=0]");

            return sb.toString();
        }
        methods = getClass().getDeclaredMethods();

        sb.append('[');

        int len = methods.length;

        for (int i = 0; i < count; i++) {
            sb.append('\n');
            sb.append("    parameter_");
            sb.append(i + 1);
            sb.append('=');
            sb.append('[');

            for (int j = 0; j < len; j++) {
                method = methods[j];

                if (!Modifier.isPublic(method.getModifiers())) {
                    continue;
                }

                if (method.getParameterTypes().length != 1) {
                    continue;
                }
                sb.append(method.getName());
                sb.append('=');
                sb.append(method.invoke(this,
                                        new Object[] { new Integer(i + 1) }));

                if (j + 1 < len) {
                    sb.append(',');
                    sb.append(' ');
                }
            }
            sb.append(']');

            if (i + 1 < count) {
                sb.append(',');
                sb.append(' ');
            }
        }
        sb.append('\n');
        sb.append(']');

        return sb.toString();
    }
}
