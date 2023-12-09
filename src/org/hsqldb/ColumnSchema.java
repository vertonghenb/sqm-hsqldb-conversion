


package org.hsqldb;

import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.Iterator;
import org.hsqldb.lib.OrderedHashSet;
import org.hsqldb.rights.Grantee;
import org.hsqldb.types.Type;
import org.hsqldb.types.Types;


public final class ColumnSchema extends ColumnBase implements SchemaObject {

    public static final ColumnSchema[] emptyArray = new ColumnSchema[]{};

    
    private HsqlName       columnName;
    private boolean        isPrimaryKey;
    private Expression     defaultExpression;
    private Expression     generatingExpression;
    private NumberSequence sequence;
    private OrderedHashSet references = new OrderedHashSet();
    private OrderedHashSet generatedColumnReferences;
    private Expression     accessor;

    
    public ColumnSchema(HsqlName name, Type type, boolean isNullable,
                        boolean isPrimaryKey, Expression defaultExpression) {

        columnName             = name;
        nullability = isNullable ? SchemaObject.Nullability.NULLABLE
                                 : SchemaObject.Nullability.NO_NULLS;
        this.dataType          = type;
        this.isPrimaryKey      = isPrimaryKey;
        this.defaultExpression = defaultExpression;

        setReferences();
    }

    public int getType() {
        return columnName.type;
    }

    public HsqlName getName() {
        return columnName;
    }

    public String getNameString() {
        return columnName.name;
    }

    public String getTableNameString() {
        return columnName.parent == null ? null
                                         : columnName.parent.name;
    }

    public HsqlName getSchemaName() {
        return columnName.schema;
    }

    public String getSchemaNameString() {
        return columnName.schema == null ? null
                                         : columnName.schema.name;
    }

    public HsqlName getCatalogName() {
        return columnName.schema == null ? null
                                         : columnName.schema.schema;
    }

    public String getCatalogNameString() {

        return columnName.schema == null ? null
                                         : columnName.schema.schema == null
                                           ? null
                                           : columnName.schema.schema.name;
    }

    public Grantee getOwner() {
        return columnName.schema == null ? null
                                         : columnName.schema.owner;
    }

    public OrderedHashSet getReferences() {
        return references;
    }

    public OrderedHashSet getComponents() {
        return null;
    }

    public void compile(Session session, SchemaObject table) {

        if (generatingExpression == null) {
            return;
        }

        generatingExpression.resetColumnReferences();
        generatingExpression.resolveCheckOrGenExpression(session,
                ((Table) table).defaultRanges, false);

        if (dataType.typeComparisonGroup
                != generatingExpression.getDataType().typeComparisonGroup) {
            throw Error.error(ErrorCode.X_42561);
        }

        setReferences();
    }

    public String getSQL() {

        StringBuffer sb = new StringBuffer();

        switch (parameterMode) {

            case SchemaObject.ParameterModes.PARAM_IN :
                sb.append(Tokens.T_IN).append(' ');
                break;

            case SchemaObject.ParameterModes.PARAM_OUT :
                sb.append(Tokens.T_OUT).append(' ');
                break;

            case SchemaObject.ParameterModes.PARAM_INOUT :
                sb.append(Tokens.T_INOUT).append(' ');
                break;
        }

        if (columnName != null) {
            sb.append(columnName.statementName);
            sb.append(' ');
        }

        sb.append(dataType.getTypeDefinition());

        if (dataType.hasCollation()) {
            sb.append(' ').append(Tokens.T_COLLATE).append(' ');
            sb.append(dataType.getCollationDefinition());
        }

        return sb.toString();
    }

    public long getChangeTimestamp() {
        return 0;
    }

    public void setType(Type type) {

        this.dataType = type;

        setReferences();
    }

    public void setName(HsqlName name) {
        this.columnName = name;
    }

    void setIdentity(NumberSequence sequence) {
        this.sequence = sequence;
        isIdentity    = sequence != null;
    }

    void setType(ColumnSchema other) {
        nullability = other.nullability;
        dataType    = other.dataType;
    }

    public NumberSequence getIdentitySequence() {
        return sequence;
    }

    
    public boolean isNullable() {

        boolean isNullable = super.isNullable();

        if (isNullable) {
            if (dataType.isDomainType()) {
                return dataType.userTypeModifier.isNullable();
            }
        }

        return isNullable;
    }

    public byte getNullability() {
        return isPrimaryKey ? SchemaObject.Nullability.NO_NULLS
                            : super.getNullability();
    }

    public boolean isGenerated() {
        return generatingExpression != null;
    }

    public boolean hasDefault() {
        return getDefaultExpression() != null;
    }

    
    public boolean isWriteable() {
        return !isGenerated();
    }

    public void setWriteable(boolean value) {
        throw Error.runtimeError(ErrorCode.U_S0500, "ColumnSchema");
    }

    public boolean isSearchable() {
        return Types.isSearchable(dataType.typeCode);
    }

    
    public boolean isPrimaryKey() {
        return isPrimaryKey;
    }

    
    void setPrimaryKey(boolean value) {
        isPrimaryKey = value;
    }

    
    public Object getDefaultValue(Session session) {

        return defaultExpression == null ? null
                                         : defaultExpression.getValue(session,
                                         dataType);
    }

    
    public Object getGeneratedValue(Session session) {

        return generatingExpression == null ? null
                                            : generatingExpression.getValue(
                                            session, dataType);
    }

    
    public String getDefaultSQL() {

        String ddl = null;

        ddl = defaultExpression == null ? null
                                        : defaultExpression.getSQL();

        return ddl;
    }

    
    Expression getDefaultExpression() {

        if (defaultExpression == null) {
            if (dataType.isDomainType()) {
                return dataType.userTypeModifier.getDefaultClause();
            }

            return null;
        } else {
            return defaultExpression;
        }
    }

    void setDefaultExpression(Expression expr) {
        defaultExpression = expr;
    }

    
    public Expression getGeneratingExpression() {
        return generatingExpression;
    }

    void setGeneratingExpression(Expression expr) {
        generatingExpression = expr;
    }

    public ColumnSchema duplicate() {

        ColumnSchema copy = new ColumnSchema(columnName, dataType, true,
                                             isPrimaryKey, defaultExpression);

        copy.setNullability(this.nullability);
        copy.setGeneratingExpression(generatingExpression);
        copy.setIdentity(sequence);

        return copy;
    }

    public Expression getAccessor() {

        if (accessor == null) {
            accessor = new ExpressionColumnAccessor(this);
        }

        return accessor;
    }

    public OrderedHashSet getGeneratedColumnReferences() {
        return generatedColumnReferences;
    }

    private void setReferences() {

        references.clear();

        if (generatedColumnReferences != null) {
            generatedColumnReferences.clear();
        }

        if (dataType.isDomainType() || dataType.isDistinctType()) {
            HsqlName name = ((SchemaObject) dataType).getName();

            references.add(name);
        }

        if (generatingExpression != null) {
            generatingExpression.collectObjectNames(references);

            Iterator it = references.iterator();

            while (it.hasNext()) {
                HsqlName name = (HsqlName) it.next();

                if (name.type == SchemaObject.COLUMN
                        || name.type == SchemaObject.TABLE) {
                    it.remove();

                    if (generatedColumnReferences == null) {
                        generatedColumnReferences = new OrderedHashSet();
                    }

                    if (name.type == SchemaObject.COLUMN) {
                        generatedColumnReferences.add(name);
                    }
                }
            }
        }
    }
}
