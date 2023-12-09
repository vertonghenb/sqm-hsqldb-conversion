


package org.hsqldb.jdbc;

import java.lang.reflect.Field;




public final class JDBCColumnMetaData {

    
    public String catalogName;

    
    public String columnClassName;

    
    public int columnDisplaySize;

    
    public String columnLabel;

    
    public String columnName;

    
    public int columnType;

    
    public int precision;

    
    public int scale;

    
    public String schemaName;

    
    public String tableName;

    
    public boolean isAutoIncrement;

    
    public boolean isCaseSensitive;

    
    public boolean isCurrency;

    
    public boolean isDefinitelyWritable;

    
    public int isNullable;

    
    public boolean isReadOnly;

    
    public boolean isSearchable;

    
    public boolean isSigned;

    
    public boolean isWritable;

    
    public String toString() {

        try {
            return toStringImpl();
        } catch (Exception e) {
            return super.toString() + "[" + e + "]";
        }
    }

    
    private String toStringImpl() throws Exception {

        StringBuffer sb;
        Field[]      fields;
        Field        field;

        sb = new StringBuffer();

        sb.append('[');

        fields = getClass().getFields();

        int len = fields.length;

        for (int i = 0; i < len; i++) {
            field = fields[i];

            sb.append(field.getName());
            sb.append('=');
            sb.append(field.get(this));

            if (i + 1 < len) {
                sb.append(',');
                sb.append(' ');
            }
        }
        sb.append(']');

        return sb.toString();
    }
}
