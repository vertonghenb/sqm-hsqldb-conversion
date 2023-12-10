package org.hsqldb.types;
import org.hsqldb.Constraint;
import org.hsqldb.Expression;
import org.hsqldb.HsqlNameManager;
import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.SchemaObject;
import org.hsqldb.Session;
import org.hsqldb.Tokens;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.lib.OrderedHashSet;
import org.hsqldb.rights.Grantee;
public class UserTypeModifier {
    final HsqlName name;
    final int      schemaObjectType;
    final Type     dataType;
    Constraint[]   constraints = Constraint.emptyArray;
    Expression     defaultExpression;
    boolean        isNullable = true;
    public UserTypeModifier(HsqlName name, int type, Type dataType) {
        this.name             = name;
        this.schemaObjectType = type;
        this.dataType         = dataType;
    }
    public int schemaObjectType() {
        return schemaObjectType;
    }
    public void addConstraint(Constraint c) {
        int position = constraints.length;
        constraints = (Constraint[]) ArrayUtil.resizeArray(constraints,
                position + 1);
        constraints[position] = c;
        setNotNull();
    }
    public void removeConstraint(String name) {
        for (int i = 0; i < constraints.length; i++) {
            if (constraints[i].getName().name.equals(name)) {
                constraints =
                    (Constraint[]) ArrayUtil.toAdjustedArray(constraints,
                        null, i, -1);
                break;
            }
        }
        setNotNull();
    }
    public Constraint getConstraint(String name) {
        for (int i = 0; i < constraints.length; i++) {
            if (constraints[i].getName().name.equals(name)) {
                return constraints[i];
            }
        }
        return null;
    }
    public Constraint[] getConstraints() {
        return constraints;
    }
    public boolean isNullable() {
        return isNullable;
    }
    public Expression getDefaultClause() {
        return defaultExpression;
    }
    public void setDefaultClause(Expression defaultExpression) {
        this.defaultExpression = defaultExpression;
    }
    public void removeDefaultClause() {
        defaultExpression = null;
    }
    private void setNotNull() {
        isNullable = true;
        for (int i = 0; i < constraints.length; i++) {
            if (constraints[i].isNotNull()) {
                isNullable = false;
            }
        }
    }
    public int getType() {
        return schemaObjectType;
    }
    public HsqlName getName() {
        return name;
    }
    public HsqlName getSchemaName() {
        return name.schema;
    }
    public Grantee getOwner() {
        return name.schema.owner;
    }
    public OrderedHashSet getReferences() {
        OrderedHashSet set = new OrderedHashSet();
        for (int i = 0; i < constraints.length; i++) {
            OrderedHashSet subSet = constraints[i].getReferences();
            if (subSet != null) {
                set.addAll(subSet);
            }
        }
        return set;
    }
    public final OrderedHashSet getComponents() {
        if (constraints == null) {
            return null;
        }
        OrderedHashSet set = new OrderedHashSet();
        set.addAll(constraints);
        return set;
    }
    public void compile(Session session) {
        for (int i = 0; i < constraints.length; i++) {
            constraints[i].compile(session, null);
        }
    }
    public String getSQL() {
        StringBuffer sb = new StringBuffer();
        if (schemaObjectType == SchemaObject.TYPE) {
            sb.append(Tokens.T_CREATE).append(' ').append(
                Tokens.T_TYPE).append(' ');
            sb.append(name.getSchemaQualifiedStatementName());
            sb.append(' ').append(Tokens.T_AS).append(' ');
            sb.append(dataType.getDefinition());
            if (dataType.hasCollation()) {
                sb.append(' ').append(Tokens.T_COLLATE).append(' ');
                sb.append(dataType.getCollationDefinition());
            }
        } else {
            sb.append(Tokens.T_CREATE).append(' ').append(
                Tokens.T_DOMAIN).append(' ');
            sb.append(name.getSchemaQualifiedStatementName());
            sb.append(' ').append(Tokens.T_AS).append(' ');
            sb.append(dataType.getDefinition());
            if (defaultExpression != null) {
                sb.append(' ').append(Tokens.T_DEFAULT).append(' ');
                sb.append(defaultExpression.getSQL());
            }
            for (int i = 0; i < constraints.length; i++) {
                sb.append(' ').append(Tokens.T_CONSTRAINT).append(' ');
                sb.append(constraints[i].getName().statementName).append(' ');
                sb.append(Tokens.T_CHECK).append('(').append(
                    constraints[i].getCheckSQL()).append(')');
            }
        }
        return sb.toString();
    }
}