package org.hsqldb;
import org.hsqldb.types.Type;
public class Token {
    String  tokenString = "";
    int     tokenType   = Tokens.X_UNKNOWN_TOKEN;
    Type    dataType;
    Object  tokenValue;
    String  namePrefix;
    String  namePrePrefix;
    String  namePrePrePrefix;
    String  charsetSchema;
    String  charsetName;
    String  fullString;
    int     lobMultiplierType = Tokens.X_UNKNOWN_TOKEN;
    boolean isDelimiter;
    boolean isDelimitedIdentifier;
    boolean isDelimitedPrefix;
    boolean isDelimitedPrePrefix;
    boolean isDelimitedPrePrePrefix;
    boolean isUndelimitedIdentifier;
    boolean isReservedIdentifier;
    boolean isCoreReservedIdentifier;
    boolean isHostParameter;
    boolean isMalformed;
    int    position;
    Object expression;
    void reset() {
        tokenString              = "";
        tokenType                = Tokens.X_UNKNOWN_TOKEN;
        dataType                 = null;
        tokenValue               = null;
        namePrefix               = null;
        namePrePrefix            = null;
        namePrePrePrefix         = null;
        charsetSchema            = null;
        charsetName              = null;
        fullString               = null;
        expression               = null;
        lobMultiplierType        = Tokens.X_UNKNOWN_TOKEN;
        isDelimiter              = false;
        isDelimitedIdentifier    = false;
        isDelimitedPrefix        = false;
        isDelimitedPrePrefix     = false;
        isDelimitedPrePrePrefix  = false;
        isUndelimitedIdentifier  = false;
        isReservedIdentifier     = false;
        isCoreReservedIdentifier = false;
        isHostParameter          = false;
        isMalformed              = false;
    }
    Token duplicate() {
        Token token = new Token();
        token.tokenString              = tokenString;
        token.tokenType                = tokenType;
        token.dataType                 = dataType;
        token.tokenValue               = tokenValue;
        token.namePrefix               = namePrefix;
        token.namePrePrefix            = namePrePrefix;
        token.namePrePrePrefix         = namePrePrePrefix;
        token.charsetSchema            = charsetSchema;
        token.charsetName              = charsetName;
        token.fullString               = fullString;
        token.lobMultiplierType        = lobMultiplierType;
        token.isDelimiter              = isDelimiter;
        token.isDelimitedIdentifier    = isDelimitedIdentifier;
        token.isDelimitedPrefix        = isDelimitedPrefix;
        token.isDelimitedPrePrefix     = isDelimitedPrePrefix;
        token.isDelimitedPrePrePrefix  = isDelimitedPrePrePrefix;
        token.isUndelimitedIdentifier  = isUndelimitedIdentifier;
        token.isReservedIdentifier     = isReservedIdentifier;
        token.isCoreReservedIdentifier = isCoreReservedIdentifier;
        token.isHostParameter          = isHostParameter;
        token.isMalformed              = isMalformed;
        return token;
    }
    public String getFullString() {
        return fullString;
    }
    public void setExpression(Object expression) {
        this.expression = expression;
    }
    String getSQL() {
        if (expression instanceof ExpressionColumn) {
            if (tokenType == Tokens.ASTERISK) {
                StringBuffer sb         = new StringBuffer();
                Expression   expression = (Expression) this.expression;
                if (expression != null
                        && expression.opType == OpTypes.MULTICOLUMN
                        && expression.nodes.length > 0) {
                    sb.append(' ');
                    for (int i = 0; i < expression.nodes.length; i++) {
                        Expression   e = expression.nodes[i];
                        ColumnSchema c = e.getColumn();
                        String       name;
                        if (e.opType == OpTypes.COALESCE) {
                            if (i > 0) {
                                sb.append(',');
                            }
                            sb.append(e.getColumnName());
                            continue;
                        }
                        if (e.getRangeVariable().tableAlias == null) {
                            name = c.getName()
                                .getSchemaQualifiedStatementName();
                        } else {
                            RangeVariable range = e.getRangeVariable();
                            name = range.tableAlias.getStatementName() + '.'
                                   + c.getName().statementName;
                        }
                        if (i > 0) {
                            sb.append(',');
                        }
                        sb.append(name);
                    }
                    sb.append(' ');
                } else {
                    return tokenString;
                }
                return sb.toString();
            }
        } else if (expression instanceof Type) {
            isDelimiter = false;
            Type type = (Type) expression;
            if (type.isDistinctType() || type.isDomainType()) {
                return type.getName().getSchemaQualifiedStatementName();
            }
            return type.getNameString();
        } else if (expression instanceof SchemaObject) {
            isDelimiter = false;
            return ((SchemaObject) expression).getName()
                .getSchemaQualifiedStatementName();
        }
        if (namePrefix == null && isUndelimitedIdentifier) {
            return tokenString;
        }
        if (tokenType == Tokens.X_VALUE) {
            return dataType.convertToSQLString(tokenValue);
        }
        StringBuffer sb = new StringBuffer();
        if (namePrePrefix != null) {
            if (isDelimitedPrePrefix) {
                sb.append('"');
                sb.append(namePrePrefix);
                sb.append('"');
            } else {
                sb.append(namePrePrefix);
            }
            sb.append('.');
        }
        if (namePrefix != null) {
            if (isDelimitedPrefix) {
                sb.append('"');
                sb.append(namePrefix);
                sb.append('"');
            } else {
                sb.append(namePrefix);
            }
            sb.append('.');
        }
        if (isDelimitedIdentifier) {
            sb.append('"');
            sb.append(tokenString);
            sb.append('"');
            isDelimiter = false;
        } else {
            sb.append(tokenString);
        }
        return sb.toString();
    }
    static String getSQL(Token[] statement) {
        boolean      wasDelimiter = true;
        StringBuffer sb           = new StringBuffer();
        for (int i = 0; i < statement.length; i++) {
            String sql = statement[i].getSQL();
            if (!statement[i].isDelimiter && !wasDelimiter) {
                sb.append(' ');
            }
            sb.append(sql);
            wasDelimiter = statement[i].isDelimiter;
        }
        return sb.toString();
    }
    static Object[] getSimplifiedTokens(Token[] tokens) {
        Object[] array = new Object[tokens.length];
        for (int i = 0; i < tokens.length; i++) {
            if (tokens[i].expression == null) {
                array[i] = tokens[i].getSQL();
            } else {
                array[i] = tokens[i].expression;
            }
        }
        return array;
    }
}