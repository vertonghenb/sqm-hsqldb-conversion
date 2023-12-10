package org.hsqldb;
import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.lib.OrderedHashSet;
import org.hsqldb.result.Result;
import org.hsqldb.result.ResultMetaData;
import org.hsqldb.result.ResultProperties;
public abstract class Statement {
    static final int META_RESET_VIEWS      = 1;
    static final int META_RESET_STATEMENTS = 2;
    static final Statement[] emptyArray = new Statement[]{};
    final int type;
    int       group;
    boolean   isLogged            = true;
    boolean   isValid             = true;
    int       statementReturnType = StatementTypes.RETURN_COUNT;
    HsqlName schemaName;
    Routine root;
    StatementCompound parent;
    boolean           isError;
    boolean           isTransactionStatement;
    boolean           isExplain;
    String sql;
    long id;
    long compileTimestamp;
    HsqlName[] readTableNames = HsqlName.emptyArray;
    HsqlName[] writeTableNames = HsqlName.emptyArray;;
    OrderedHashSet references;
    int cursorPropertiesRequest;
    public abstract Result execute(Session session);
    public void setParameters(ExpressionColumn[] params) {}
    Statement(int type) {
        this.type = type;
    }
    Statement(int type, int group) {
        this.type  = type;
        this.group = group;
    }
    public final boolean isError() {
        return isError;
    }
    public boolean isTransactionStatement() {
        return isTransactionStatement;
    }
    public boolean isAutoCommitStatement() {
        return false;
    }
    public void setCompileTimestamp(long ts) {
        compileTimestamp = ts;
    }
    public long getCompileTimestamp() {
        return compileTimestamp;
    }
    public final void setSQL(String sql) {
        this.sql = sql;
    }
    public String getSQL() {
        return sql;
    }
    public OrderedHashSet getReferences() {
        return references;
    }
    public final void setDescribe() {
        isExplain = true;
    }
    public abstract String describe(Session session);
    public HsqlName getSchemaName() {
        return schemaName;
    }
    public final void setSchemaHsqlName(HsqlName name) {
        schemaName = name;
    }
    public final void setID(long csid) {
        id = csid;
    }
    public final long getID() {
        return id;
    }
    public final int getType() {
        return type;
    }
    public final int getGroup() {
        return group;
    }
    public final boolean isValid() {
        return isValid;
    }
    public final boolean isLogged() {
        return isLogged;
    }
    public void clearVariables() {}
    public void resolve(Session session) {}
    public RangeVariable[] getRangeVariables() {
        return RangeVariable.emptyArray;
    }
    public final HsqlName[] getTableNamesForRead() {
        return readTableNames;
    }
    public final HsqlName[] getTableNamesForWrite() {
        return writeTableNames;
    }
    public boolean isCatalogLock() {
        switch (group) {
            case StatementTypes.X_SQL_SCHEMA_MANIPULATION :
                if (type == StatementTypes.ALTER_SEQUENCE) {
                    return false;
                }
            case StatementTypes.X_SQL_SCHEMA_DEFINITION :
            case StatementTypes.X_HSQLDB_SCHEMA_MANIPULATION :
                return true;
            case StatementTypes.X_HSQLDB_DATABASE_OPERATION :
                return true;
            default :
                return false;
        }
    }
    public boolean isCatalogChange() {
        switch (group) {
            case StatementTypes.X_SQL_SCHEMA_DEFINITION :
            case StatementTypes.X_SQL_SCHEMA_MANIPULATION :
            case StatementTypes.X_HSQLDB_SCHEMA_MANIPULATION :
                return true;
            default :
                return false;
        }
    }
    public void setParent(StatementCompound statement) {
        this.parent = statement;
    }
    public void setRoot(Routine root) {
        this.root = root;
    }
    public boolean hasGeneratedColumns() {
        return false;
    }
    public ResultMetaData generatedResultMetaData() {
        return null;
    }
    public void setGeneratedColumnInfo(int mode, ResultMetaData meta) {}
    public ResultMetaData getResultMetaData() {
        return ResultMetaData.emptyResultMetaData;
    }
    public ResultMetaData getParametersMetaData() {
        return ResultMetaData.emptyParamMetaData;
    }
    public int getResultProperties() {
        return ResultProperties.defaultPropsValue;
    }
    public int getStatementReturnType() {
        return statementReturnType;
    }
    public int getCursorPropertiesRequest() {
        return cursorPropertiesRequest;
    }
    public void setCursorPropertiesRequest(int props) {
        cursorPropertiesRequest = props;
    }
    public void clearStructures(Session session) {}
}