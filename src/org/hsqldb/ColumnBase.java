package org.hsqldb;
import org.hsqldb.types.Type;
public class ColumnBase {
    private String    name;
    private String    table;
    private String    schema;
    private String    catalog;
    private boolean   isWriteable;
    private boolean   isSearchable;
    protected byte    parameterMode;
    protected boolean isIdentity;
    protected byte    nullability = SchemaObject.Nullability.NULLABLE;
    protected Type    dataType;
    ColumnBase() {}
    public ColumnBase(String catalog, String schema, String table,
                      String name) {
        this.catalog = catalog;
        this.schema  = schema;
        this.table   = table;
        this.name    = name;
    }
    public String getNameString() {
        return name;
    }
    public String getTableNameString() {
        return table;
    }
    public String getSchemaNameString() {
        return schema;
    }
    public String getCatalogNameString() {
        return catalog;
    }
    public void setIdentity(boolean value) {
        isIdentity = value;
    }
    public boolean isIdentity() {
        return isIdentity;
    }
    protected void setType(ColumnBase other) {
        nullability = other.nullability;
        dataType    = other.dataType;
    }
    public void setType(Type type) {
        this.dataType = type;
    }
    public boolean isNullable() {
        return !isIdentity && nullability == SchemaObject.Nullability.NULLABLE;
    }
    protected void setNullable(boolean value) {
        nullability = value ? SchemaObject.Nullability.NULLABLE
                            : SchemaObject.Nullability.NO_NULLS;
    }
    public byte getNullability() {
        return isIdentity ? SchemaObject.Nullability.NO_NULLS
                          : nullability;
    }
    public void setNullability(byte value) {
        nullability = value;
    }
    public boolean isWriteable() {
        return isWriteable;
    }
    public boolean isSearchable() {
        return isSearchable;
    }
    public void setWriteable(boolean value) {
        isWriteable = value;
    }
    public Type getDataType() {
        return dataType;
    }
    public byte getParameterMode() {
        return parameterMode;
    }
    public void setParameterMode(byte mode) {
        this.parameterMode = mode;
    }
}