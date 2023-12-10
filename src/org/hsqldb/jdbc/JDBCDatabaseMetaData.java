package org.hsqldb.jdbc;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import org.hsqldb.FunctionCustom;
import org.hsqldb.lib.StringUtil;
import org.hsqldb.persist.HsqlDatabaseProperties;
import org.hsqldb.types.Type;
public class JDBCDatabaseMetaData implements DatabaseMetaData,
        java.sql.Wrapper {
    public boolean allProceduresAreCallable() throws SQLException {
        return true;
    }
    public boolean allTablesAreSelectable() throws SQLException {
        return true;
    }
    public String getURL() throws SQLException {
        return connection.getURL();
    }
    public String getUserName() throws SQLException {
        ResultSet rs = execute("CALL USER()");
        rs.next();
        String result = rs.getString(1);
        rs.close();
        return result;
    }
    public boolean isReadOnly() throws SQLException {
        ResultSet rs = execute("CALL IS_READONLY_DATABASE()");
        rs.next();
        boolean result = rs.getBoolean(1);
        rs.close();
        return result;
    }
    public boolean nullsAreSortedHigh() throws SQLException {
        return false;
    }
    public boolean nullsAreSortedLow() throws SQLException {
        return false;
    }
    public boolean nullsAreSortedAtStart() throws SQLException {
        return true;
    }
    public boolean nullsAreSortedAtEnd() throws SQLException {
        return false;
    }
    public String getDatabaseProductName() throws SQLException {
        return HsqlDatabaseProperties.PRODUCT_NAME;
    }
    public String getDatabaseProductVersion() throws SQLException {
        ResultSet rs = execute("call database_version()");
        rs.next();
        return rs.getString(1);
    }
    public String getDriverName() throws SQLException {
        return HsqlDatabaseProperties.PRODUCT_NAME + " Driver";
    }
    public String getDriverVersion() throws SQLException {
        return HsqlDatabaseProperties.THIS_VERSION;
    }
    public int getDriverMajorVersion() {
        return HsqlDatabaseProperties.MAJOR;
    }
    public int getDriverMinorVersion() {
        return HsqlDatabaseProperties.MINOR;
    }
    public boolean usesLocalFiles() throws SQLException {
        return false;
    }
    public boolean usesLocalFilePerTable() throws SQLException {
        return false;
    }
    public boolean supportsMixedCaseIdentifiers() throws SQLException {
        return false;
    }
    public boolean storesUpperCaseIdentifiers() throws SQLException {
        return true;
    }
    public boolean storesLowerCaseIdentifiers() throws SQLException {
        return false;
    }
    public boolean storesMixedCaseIdentifiers() throws SQLException {
        return false;
    }
    public boolean supportsMixedCaseQuotedIdentifiers() throws SQLException {
        return true;
    }
    public boolean storesUpperCaseQuotedIdentifiers() throws SQLException {
        return false;
    }
    public boolean storesLowerCaseQuotedIdentifiers() throws SQLException {
        return false;
    }
    public boolean storesMixedCaseQuotedIdentifiers() throws SQLException {
        return false;
    }
    public String getIdentifierQuoteString() throws SQLException {
        return "\"";
    }
    public String getSQLKeywords() throws SQLException {
        return "";
    }
    public String getNumericFunctions() throws SQLException {
        return StringUtil.getList(FunctionCustom.openGroupNumericFunctions,
                                  ",", "");
    }
    public String getStringFunctions() throws SQLException {
        return StringUtil.getList(FunctionCustom.openGroupStringFunctions,
                                  ",", "");
    }
    public String getSystemFunctions() throws SQLException {
        return StringUtil.getList(FunctionCustom.openGroupSystemFunctions,
                                  ",", "");
    }
    public String getTimeDateFunctions() throws SQLException {
        return StringUtil.getList(FunctionCustom.openGroupDateTimeFunctions,
                                  ",", "");
    }
    public String getSearchStringEscape() throws SQLException {
        return "\\";
    }
    public String getExtraNameCharacters() throws SQLException {
        return "";
    }
    public boolean supportsAlterTableWithAddColumn() throws SQLException {
        return true;
    }
    public boolean supportsAlterTableWithDropColumn() throws SQLException {
        return true;
    }
    public boolean supportsColumnAliasing() throws SQLException {
        return true;
    }
    public boolean nullPlusNonNullIsNull() throws SQLException {
        return true;
    }
    public boolean supportsConvert() throws SQLException {
        return true;
    }
    public boolean supportsConvert(int fromType,
                                   int toType) throws SQLException {
        Type from =
            Type.getDefaultTypeWithSize(Type.getHSQLDBTypeCode(fromType));
        Type to = Type.getDefaultTypeWithSize(Type.getHSQLDBTypeCode(toType));
        if (from == null || to == null) {
            return false;
        }
        if (fromType == java.sql.Types.NULL
                && toType == java.sql.Types.ARRAY) {
            return true;
        }
        return to.canConvertFrom(from);
    }
    public boolean supportsTableCorrelationNames() throws SQLException {
        return true;
    }
    public boolean supportsDifferentTableCorrelationNames() throws SQLException {
        return true;
    }
    public boolean supportsExpressionsInOrderBy() throws SQLException {
        return true;
    }
    public boolean supportsOrderByUnrelated() throws SQLException {
        return true;
    }
    public boolean supportsGroupBy() throws SQLException {
        return true;
    }
    public boolean supportsGroupByUnrelated() throws SQLException {
        return true;
    }
    public boolean supportsGroupByBeyondSelect() throws SQLException {
        return true;
    }
    public boolean supportsLikeEscapeClause() throws SQLException {
        return true;
    }
    public boolean supportsMultipleResultSets() throws SQLException {
        return true;
    }
    public boolean supportsMultipleTransactions() throws SQLException {
        return true;
    }
    public boolean supportsNonNullableColumns() throws SQLException {
        return true;
    }
    public boolean supportsMinimumSQLGrammar() throws SQLException {
        return true;
    }
    public boolean supportsCoreSQLGrammar() throws SQLException {
        return true;
    }
    public boolean supportsExtendedSQLGrammar() throws SQLException {
        return true;
    }
    public boolean supportsANSI92EntryLevelSQL() throws SQLException {
        return true;
    }
    public boolean supportsANSI92IntermediateSQL() throws SQLException {
        return true;
    }
    public boolean supportsANSI92FullSQL() throws SQLException {
        return true;
    }
    public boolean supportsIntegrityEnhancementFacility() throws SQLException {
        return true;
    }
    public boolean supportsOuterJoins() throws SQLException {
        return true;
    }
    public boolean supportsFullOuterJoins() throws SQLException {
        return true;
    }
    public boolean supportsLimitedOuterJoins() throws SQLException {
        return true;
    }
    public String getSchemaTerm() throws SQLException {
        return "SCHEMA";
    }
    public String getProcedureTerm() throws SQLException {
        return "PROCEDURE";
    }
    public String getCatalogTerm() throws SQLException {
        return "CATALOG";
    }
    public boolean isCatalogAtStart() throws SQLException {
        return true;
    }
    public String getCatalogSeparator() throws SQLException {
        return ".";
    }
    public boolean supportsSchemasInDataManipulation() throws SQLException {
        return !useSchemaDefault;
    }
    public boolean supportsSchemasInProcedureCalls() throws SQLException {
        return !useSchemaDefault;
    }
    public boolean supportsSchemasInTableDefinitions() throws SQLException {
        return !useSchemaDefault;
    }
    public boolean supportsSchemasInIndexDefinitions() throws SQLException {
        return !useSchemaDefault;
    }
    public boolean supportsSchemasInPrivilegeDefinitions() throws SQLException {
        return !useSchemaDefault;
    }
    public boolean supportsCatalogsInDataManipulation() throws SQLException {
        return !useSchemaDefault;
    }
    public boolean supportsCatalogsInProcedureCalls() throws SQLException {
        return !useSchemaDefault;
    }
    public boolean supportsCatalogsInTableDefinitions() throws SQLException {
        return !useSchemaDefault;
    }
    public boolean supportsCatalogsInIndexDefinitions() throws SQLException {
        return !useSchemaDefault;
    }
    public boolean supportsCatalogsInPrivilegeDefinitions() throws SQLException {
        return !useSchemaDefault;
    }
    public boolean supportsPositionedDelete() throws SQLException {
        return true;
    }
    public boolean supportsPositionedUpdate() throws SQLException {
        return true;
    }
    public boolean supportsSelectForUpdate() throws SQLException {
        return true;
    }
    public boolean supportsStoredProcedures() throws SQLException {
        return true;
    }
    public boolean supportsSubqueriesInComparisons() throws SQLException {
        return true;
    }
    public boolean supportsSubqueriesInExists() throws SQLException {
        return true;
    }
    public boolean supportsSubqueriesInIns() throws SQLException {
        return true;
    }
    public boolean supportsSubqueriesInQuantifieds() throws SQLException {
        return true;
    }
    public boolean supportsCorrelatedSubqueries() throws SQLException {
        return true;
    }
    public boolean supportsUnion() throws SQLException {
        return true;
    }
    public boolean supportsUnionAll() throws SQLException {
        return true;
    }
    public boolean supportsOpenCursorsAcrossCommit() throws SQLException {
        return true;
    }
    public boolean supportsOpenCursorsAcrossRollback() throws SQLException {
        return false;
    }
    public boolean supportsOpenStatementsAcrossCommit() throws SQLException {
        return true;
    }
    public boolean supportsOpenStatementsAcrossRollback() throws SQLException {
        return true;
    }
    public int getMaxBinaryLiteralLength() throws SQLException {
        return 0;
    }
    public int getMaxCharLiteralLength() throws SQLException {
        return 0;
    }
    public int getMaxColumnNameLength() throws SQLException {
        return 128;
    }
    public int getMaxColumnsInGroupBy() throws SQLException {
        return 0;
    }
    public int getMaxColumnsInIndex() throws SQLException {
        return 0;
    }
    public int getMaxColumnsInOrderBy() throws SQLException {
        return 0;
    }
    public int getMaxColumnsInSelect() throws SQLException {
        return 0;
    }
    public int getMaxColumnsInTable() throws SQLException {
        return 0;
    }
    public int getMaxConnections() throws SQLException {
        return 0;
    }
    public int getMaxCursorNameLength() throws SQLException {
        return 128;
    }
    public int getMaxIndexLength() throws SQLException {
        return 0;
    }
    public int getMaxSchemaNameLength() throws SQLException {
        return 128;
    }
    public int getMaxProcedureNameLength() throws SQLException {
        return 128;
    }
    public int getMaxCatalogNameLength() throws SQLException {
        return 128;
    }
    public int getMaxRowSize() throws SQLException {
        return 0;
    }
    public boolean doesMaxRowSizeIncludeBlobs() throws SQLException {
        return true;
    }
    public int getMaxStatementLength() throws SQLException {
        return 0;
    }
    public int getMaxStatements() throws SQLException {
        return 0;
    }
    public int getMaxTableNameLength() throws SQLException {
        return 128;
    }
    public int getMaxTablesInSelect() throws SQLException {
        return 0;
    }
    public int getMaxUserNameLength() throws SQLException {
        return 128;
    }
    public int getDefaultTransactionIsolation() throws SQLException {
        ResultSet rs = execute("CALL DATABASE_ISOLATION_LEVEL()");
        rs.next();
        String result = rs.getString(1);
        rs.close();
        if (result.startsWith("READ COMMITTED")) {
            return Connection.TRANSACTION_READ_COMMITTED;
        }
        if (result.startsWith("READ UNCOMMITTED")) {
            return Connection.TRANSACTION_READ_UNCOMMITTED;
        }
        if (result.startsWith("SERIALIZABLE")) {
            return Connection.TRANSACTION_SERIALIZABLE;
        }
        return Connection.TRANSACTION_READ_COMMITTED;
    }
    public boolean supportsTransactions() throws SQLException {
        return true;
    }
    public boolean supportsTransactionIsolationLevel(
            int level) throws SQLException {
        return level == Connection.TRANSACTION_READ_UNCOMMITTED
               || level == Connection.TRANSACTION_READ_COMMITTED
               || level == Connection.TRANSACTION_REPEATABLE_READ
               || level == Connection.TRANSACTION_SERIALIZABLE;
    }
    public boolean supportsDataDefinitionAndDataManipulationTransactions() throws SQLException {
        return false;
    }
    public boolean supportsDataManipulationTransactionsOnly() throws SQLException {
        return true;
    }
    public boolean dataDefinitionCausesTransactionCommit() throws SQLException {
        return true;
    }
    public boolean dataDefinitionIgnoredInTransactions() throws SQLException {
        return false;
    }
    public ResultSet getProcedures(
            String catalog, String schemaPattern,
            String procedureNamePattern) throws SQLException {
        if (wantsIsNull(procedureNamePattern)) {
            return executeSelect("SYSTEM_PROCEDURES", "0=1");
        }
        catalog       = translateCatalog(catalog);
        schemaPattern = translateSchema(schemaPattern);
        StringBuffer select =
            toQueryPrefix("SYSTEM_PROCEDURES").append(and("PROCEDURE_CAT",
                "=", catalog)).append(and("PROCEDURE_SCHEM", "LIKE",
                    schemaPattern)).append(and("PROCEDURE_NAME", "LIKE",
                        procedureNamePattern));
        return execute(select.toString());
    }
    public ResultSet getProcedureColumns(String catalog, String schemaPattern,
            String procedureNamePattern,
            String columnNamePattern) throws SQLException {
        if (wantsIsNull(procedureNamePattern)
                || wantsIsNull(columnNamePattern)) {
            return executeSelect("SYSTEM_PROCEDURECOLUMNS", "0=1");
        }
        catalog       = translateCatalog(catalog);
        schemaPattern = translateSchema(schemaPattern);
        StringBuffer select = toQueryPrefix("SYSTEM_PROCEDURECOLUMNS").append(
            and("PROCEDURE_CAT", "=", catalog)).append(
            and("PROCEDURE_SCHEM", "LIKE", schemaPattern)).append(
            and("PROCEDURE_NAME", "LIKE", procedureNamePattern)).append(
            and("COLUMN_NAME", "LIKE", columnNamePattern));
        return execute(select.toString());
    }
    public ResultSet getTables(String catalog, String schemaPattern,
                               String tableNamePattern,
                               String[] types) throws SQLException {
        if (wantsIsNull(tableNamePattern)
                || (types != null && types.length == 0)) {
            return executeSelect("SYSTEM_TABLES", "0=1");
        }
        catalog       = translateCatalog(catalog);
        schemaPattern = translateSchema(schemaPattern);
        StringBuffer select =
            toQueryPrefix("SYSTEM_TABLES").append(and("TABLE_CAT", "=",
                catalog)).append(and("TABLE_SCHEM", "LIKE",
                                     schemaPattern)).append(and("TABLE_NAME",
                                         "LIKE", tableNamePattern));
        if (types == null) {
        } else {
            select.append(" AND TABLE_TYPE IN (").append(
                StringUtil.getList(types, ",", "'")).append(')');
        }
        return execute(select.toString());
    }
    public ResultSet getSchemas() throws SQLException {
        return executeSelect("SYSTEM_SCHEMAS", null);
    }
    public ResultSet getCatalogs() throws SQLException {
        String select =
            "SELECT CATALOG_NAME AS TABLE_CAT FROM INFORMATION_SCHEMA.INFORMATION_SCHEMA_CATALOG_NAME";
        return execute(select);
    }
    public ResultSet getTableTypes() throws SQLException {
        return executeSelect("SYSTEM_TABLETYPES", null);
    }
    public ResultSet getColumns(String catalog, String schemaPattern,
                                String tableNamePattern,
                                String columnNamePattern) throws SQLException {
        if (wantsIsNull(tableNamePattern) || wantsIsNull(columnNamePattern)) {
            return executeSelect("SYSTEM_COLUMNS", "0=1");
        }
        catalog       = translateCatalog(catalog);
        schemaPattern = translateSchema(schemaPattern);
        StringBuffer select = toQueryPrefix("SYSTEM_COLUMNS").append(
            and("TABLE_CAT", "=", catalog)).append(
            and("TABLE_SCHEM", "LIKE", schemaPattern)).append(
            and("TABLE_NAME", "LIKE", tableNamePattern)).append(
            and("COLUMN_NAME", "LIKE", columnNamePattern));
        return execute(select.toString());
    }
    public ResultSet getColumnPrivileges(String catalog, String schema,
            String table, String columnNamePattern) throws SQLException {
        if (table == null) {
            throw Util.nullArgument("table");
        }
        catalog = translateCatalog(catalog);
        schema  = translateSchema(schema);
        String sql =
            "SELECT TABLE_CATALOG TABLE_CAT, TABLE_SCHEMA TABLE_SCHEM,"
            + "TABLE_NAME, COLUMN_NAME, GRANTOR, GRANTEE, PRIVILEGE_TYPE PRIVILEGE, IS_GRANTABLE "
            + "FROM INFORMATION_SCHEMA.COLUMN_PRIVILEGES WHERE TRUE "
            + and("TABLE_CATALOG", "=", catalog)
            + and("TABLE_SCHEMA", "=", schema) + and("TABLE_NAME", "=", table)
            + and("COLUMN_NAME", "LIKE", columnNamePattern)
        ;
        return execute(sql);
    }
    public ResultSet getTablePrivileges(
            String catalog, String schemaPattern,
            String tableNamePattern) throws SQLException {
        catalog       = translateCatalog(catalog);
        schemaPattern = translateSchema(schemaPattern);
        String sql =
            "SELECT TABLE_CATALOG TABLE_CAT, TABLE_SCHEMA TABLE_SCHEM,"
            + "TABLE_NAME, GRANTOR, GRANTEE, PRIVILEGE_TYPE PRIVILEGE, IS_GRANTABLE "
            + "FROM INFORMATION_SCHEMA.TABLE_PRIVILEGES WHERE TRUE "
            + and("TABLE_CATALOG", "=", catalog)
            + and("TABLE_SCHEMA", "LIKE", schemaPattern)
            + and("TABLE_NAME", "LIKE", tableNamePattern);
        return execute(sql);
    }
    public ResultSet getBestRowIdentifier(String catalog, String schema,
            String table, int scope, boolean nullable) throws SQLException {
        if (table == null) {
            throw Util.nullArgument("table");
        }
        String scopeIn;
        switch (scope) {
            case bestRowTemporary :
                scopeIn = BRI_TEMPORARY_SCOPE_IN_LIST;
                break;
            case bestRowTransaction :
                scopeIn = BRI_TRANSACTION_SCOPE_IN_LIST;
                break;
            case bestRowSession :
                scopeIn = BRI_SESSION_SCOPE_IN_LIST;
                break;
            default :
                throw Util.invalidArgument("scope");
        }
        catalog = translateCatalog(catalog);
        schema  = translateSchema(schema);
        Integer Nullable = (nullable) ? null
                                      : INT_COLUMNS_NO_NULLS;
        StringBuffer select =
            toQueryPrefix("SYSTEM_BESTROWIDENTIFIER").append(and("TABLE_CAT",
                "=", catalog)).append(and("TABLE_SCHEM", "=",
                    schema)).append(and("TABLE_NAME", "=",
                                        table)).append(and("NULLABLE", "=",
                                            Nullable)).append(" AND SCOPE IN "
                                                + scopeIn);
        return execute(select.toString());
    }
    public ResultSet getVersionColumns(String catalog, String schema,
                                       String table) throws SQLException {
        if (table == null) {
            throw Util.nullArgument("table");
        }
        catalog = translateCatalog(catalog);
        schema  = translateSchema(schema);
        StringBuffer select =
            toQueryPrefix("SYSTEM_VERSIONCOLUMNS").append(and("TABLE_CAT",
                "=", catalog)).append(and("TABLE_SCHEM", "=",
                    schema)).append(and("TABLE_NAME", "=", table));
        return execute(select.toString());
    }
    public ResultSet getPrimaryKeys(String catalog, String schema,
                                    String table) throws SQLException {
        if (table == null) {
            throw Util.nullArgument("table");
        }
        catalog = translateCatalog(catalog);
        schema  = translateSchema(schema);
        StringBuffer select =
            toQueryPrefix("SYSTEM_PRIMARYKEYS").append(and("TABLE_CAT", "=",
                catalog)).append(and("TABLE_SCHEM", "=",
                                     schema)).append(and("TABLE_NAME", "=",
                                         table));
        return execute(select.toString());
    }
    public ResultSet getImportedKeys(String catalog, String schema,
                                     String table) throws SQLException {
        if (table == null) {
            throw Util.nullArgument("table");
        }
        catalog = translateCatalog(catalog);
        schema  = translateSchema(schema);
        StringBuffer select = toQueryPrefix("SYSTEM_CROSSREFERENCE").append(
            and("FKTABLE_CAT", "=", catalog)).append(
            and("FKTABLE_SCHEM", "=", schema)).append(
            and("FKTABLE_NAME", "=", table)).append(
            " ORDER BY PKTABLE_CAT, PKTABLE_SCHEM, PKTABLE_NAME, KEY_SEQ");
        return execute(select.toString());
    }
    public ResultSet getExportedKeys(String catalog, String schema,
                                     String table) throws SQLException {
        if (table == null) {
            throw Util.nullArgument("table");
        }
        catalog = translateCatalog(catalog);
        schema  = translateSchema(schema);
        StringBuffer select =
            toQueryPrefix("SYSTEM_CROSSREFERENCE").append(and("PKTABLE_CAT",
                "=", catalog)).append(and("PKTABLE_SCHEM", "=",
                    schema)).append(and("PKTABLE_NAME", "=", table));
        return execute(select.toString());
    }
    public ResultSet getCrossReference(
            String parentCatalog, String parentSchema, String parentTable,
            String foreignCatalog, String foreignSchema,
            String foreignTable) throws SQLException {
        if (parentTable == null) {
            throw Util.nullArgument("parentTable");
        }
        if (foreignTable == null) {
            throw Util.nullArgument("foreignTable");
        }
        parentCatalog  = translateCatalog(parentCatalog);
        foreignCatalog = translateCatalog(foreignCatalog);
        parentSchema   = translateSchema(parentSchema);
        foreignSchema  = translateSchema(foreignSchema);
        StringBuffer select =
            toQueryPrefix("SYSTEM_CROSSREFERENCE").append(and("PKTABLE_CAT",
                "=", parentCatalog)).append(and("PKTABLE_SCHEM", "=",
                    parentSchema)).append(and("PKTABLE_NAME", "=",
                        parentTable)).append(and("FKTABLE_CAT", "=",
                            foreignCatalog)).append(and("FKTABLE_SCHEM", "=",
                                foreignSchema)).append(and("FKTABLE_NAME",
                                    "=", foreignTable));
        return execute(select.toString());
    }
    public ResultSet getTypeInfo() throws SQLException {
        return executeSelect("SYSTEM_TYPEINFO", null);
    }
    public ResultSet getIndexInfo(String catalog, String schema, String table,
                                  boolean unique,
                                  boolean approximate) throws SQLException {
        if (table == null) {
            throw Util.nullArgument("table");
        }
        catalog = translateCatalog(catalog);
        schema  = translateSchema(schema);
        Boolean nu = (unique) ? Boolean.FALSE
                              : null;
        StringBuffer select =
            toQueryPrefix("SYSTEM_INDEXINFO").append(and("TABLE_CAT", "=",
                catalog)).append(and("TABLE_SCHEM", "=",
                                     schema)).append(and("TABLE_NAME", "=",
                                         table)).append(and("NON_UNIQUE", "=",
                                             nu));
        return execute(select.toString());
    }
    public boolean supportsResultSetType(int type) throws SQLException {
        return (type == JDBCResultSet.TYPE_FORWARD_ONLY
                || type == JDBCResultSet.TYPE_SCROLL_INSENSITIVE);
    }
    public boolean supportsResultSetConcurrency(int type,
            int concurrency) throws SQLException {
        return supportsResultSetType(type)
               && (concurrency == JDBCResultSet.CONCUR_READ_ONLY
                   || concurrency == JDBCResultSet.CONCUR_UPDATABLE);
    }
    public boolean ownUpdatesAreVisible(int type) throws SQLException {
        return false;
    }
    public boolean ownDeletesAreVisible(int type) throws SQLException {
        return false;
    }
    public boolean ownInsertsAreVisible(int type) throws SQLException {
        return false;
    }
    public boolean othersUpdatesAreVisible(int type) throws SQLException {
        return false;
    }
    public boolean othersDeletesAreVisible(int type) throws SQLException {
        return false;
    }
    public boolean othersInsertsAreVisible(int type) throws SQLException {
        return false;
    }
    public boolean updatesAreDetected(int type) throws SQLException {
        return false;
    }
    public boolean deletesAreDetected(int type) throws SQLException {
        return false;
    }
    public boolean insertsAreDetected(int type) throws SQLException {
        return false;
    }
    public boolean supportsBatchUpdates() throws SQLException {
        return true;
    }
    public ResultSet getUDTs(String catalog, String schemaPattern,
                             String typeNamePattern,
                             int[] types) throws SQLException {
        if (wantsIsNull(typeNamePattern)
                || (types != null && types.length == 0)) {
            executeSelect("SYSTEM_UDTS", "0=1");
        }
        catalog       = translateCatalog(catalog);
        schemaPattern = translateSchema(schemaPattern);
        StringBuffer select =
            toQueryPrefix("SYSTEM_UDTS").append(and("TYPE_CAT", "=",
                catalog)).append(and("TYPE_SCHEM", "LIKE",
                                     schemaPattern)).append(and("TYPE_NAME",
                                         "LIKE", typeNamePattern));
        if (types == null) {
        } else {
            select.append(" AND DATA_TYPE IN (").append(
                StringUtil.getList(types, ",", "")).append(')');
        }
        return execute(select.toString());
    }
    public Connection getConnection() throws SQLException {
        return connection;
    }
    public boolean supportsSavepoints() throws SQLException {
        return true;
    }
    public boolean supportsNamedParameters() throws SQLException {
        return true;
    }
    public boolean supportsMultipleOpenResults() throws SQLException {
        return true;
    }
    public boolean supportsGetGeneratedKeys() throws SQLException {
        return true;
    }
    public ResultSet getSuperTypes(
            String catalog, String schemaPattern,
            String typeNamePattern) throws SQLException {
        if (wantsIsNull(typeNamePattern)) {
            return executeSelect("SYSTEM_SUPERTYPES", "0=1");
        }
        catalog       = translateCatalog(catalog);
        schemaPattern = translateSchema(schemaPattern);
        StringBuffer select = toQueryPrefixNoSelect(
            "SELECT * FROM (SELECT USER_DEFINED_TYPE_CATALOG, USER_DEFINED_TYPE_SCHEMA, USER_DEFINED_TYPE_NAME,"
            + "CAST (NULL AS INFORMATION_SCHEMA.SQL_IDENTIFIER), CAST (NULL AS INFORMATION_SCHEMA.SQL_IDENTIFIER), DATA_TYPE "
            + "FROM INFORMATION_SCHEMA.USER_DEFINED_TYPES "
            + "UNION SELECT DOMAIN_CATALOG, DOMAIN_SCHEMA, DOMAIN_NAME,NULL,NULL, DATA_TYPE "
            + "FROM INFORMATION_SCHEMA.DOMAINS) "
            + "AS SUPERTYPES(TYPE_CAT, TYPE_SCHEM, TYPE_NAME, SUPERTYPE_CAT, SUPERTYPE_SCHEM, SUPERTYPE_NAME) ").append(
                and("TYPE_CAT", "=", catalog)).append(
                and("TYPE_SCHEM", "LIKE", schemaPattern)).append(
                and("TYPE_NAME", "LIKE", typeNamePattern));
        return execute(select.toString());
    }
    public ResultSet getSuperTables(
            String catalog, String schemaPattern,
            String tableNamePattern) throws SQLException {
        StringBuffer select = toQueryPrefixNoSelect(
            "SELECT TABLE_NAME AS TABLE_CAT, TABLE_NAME AS TABLE_SCHEM, TABLE_NAME, TABLE_NAME AS SUPERTABLE_NAME "
            + "FROM INFORMATION_SCHEMA.TABLES ").append(
                and("TABLE_NAME", "=", ""));
        return execute(select.toString());
    }
    public ResultSet getAttributes(
            String catalog, String schemaPattern, String typeNamePattern,
            String attributeNamePattern) throws SQLException {
        StringBuffer select = toQueryPrefixNoSelect(
            "SELECT TABLE_NAME AS TYPE_CAT, TABLE_NAME AS TYPE_SCHME, TABLE_NAME AS TYPE_NAME, "
            + "TABLE_NAME AS ATTR_NAME, CAST(0 AS INTEGER) AS DATA_TYPE, TABLE_NAME AS ATTR_TYPE_NAME, "
            + "CAST(0 AS INTEGER) AS ATTR_SIZE, CAST(0 AS INTEGER) AS DECIMAL_DIGITS, "
            + "CAST(0 AS INTEGER) AS NUM_PREC_RADIX, CAST(0 AS INTEGER) AS NULLABLE, "
            + "'' AS REMARK, '' AS ATTR_DEF, CAST(0 AS INTEGER) AS SQL_DATA_TYPE, "
            + "CAST(0 AS INTEGER) AS SQL_DATETIME_SUB, CAST(0 AS INTEGER) AS CHAR_OCTECT_LENGTH, "
            + "CAST(0 AS INTEGER) AS ORDINAL_POSITION, '' AS NULLABLE, "
            + "'' AS SCOPE_CATALOG, '' AS SCOPE_SCHEMA, '' AS SCOPE_TABLE, "
            + "CAST(0 AS SMALLINT) AS SCOPE_DATA_TYPE "
            + "FROM INFORMATION_SCHEMA.TABLES ").append(
                and("TABLE_NAME", "=", ""));
        return execute(select.toString());
    }
    public boolean supportsResultSetHoldability(
            int holdability) throws SQLException {
        return holdability == JDBCResultSet.HOLD_CURSORS_OVER_COMMIT
               || holdability == JDBCResultSet.CLOSE_CURSORS_AT_COMMIT;
    }
    public int getResultSetHoldability() throws SQLException {
        return JDBCResultSet.HOLD_CURSORS_OVER_COMMIT;
    }
    public int getDatabaseMajorVersion() throws SQLException {
        ResultSet rs = execute("call database_version()");
        rs.next();
        String v = rs.getString(1);
        rs.close();
        return Integer.parseInt(v.substring(0, v.indexOf(".")));
    }
    public int getDatabaseMinorVersion() throws SQLException {
        ResultSet rs = execute("call database_version()");
        rs.next();
        String v = rs.getString(1);
        rs.close();
        int start = v.indexOf(".") + 1;
        return Integer.parseInt(v.substring(start, v.indexOf(".", start)));
    }
    public int getJDBCMajorVersion() throws SQLException {
        return JDBC_MAJOR;
    }
    public int getJDBCMinorVersion() throws SQLException {
        return 0;
    }
    public int getSQLStateType() throws SQLException {
        return sqlStateSQL99;
    }
    public boolean locatorsUpdateCopy() throws SQLException {
        return false;
    }
    public boolean supportsStatementPooling() throws SQLException {
        return (JDBC_MAJOR >= 4);
    }
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        return RowIdLifetime.ROWID_UNSUPPORTED;
    }
    public ResultSet getSchemas(String catalog,
                                String schemaPattern) throws SQLException {
        StringBuffer select =
            toQueryPrefix("SYSTEM_SCHEMAS").append(and("TABLE_CATALOG", "=",
                catalog)).append(and("TABLE_SCHEM", "LIKE", schemaPattern));
        return execute(select.toString());
    }
    public boolean supportsStoredFunctionsUsingCallSyntax() throws SQLException {
        return true;
    }
    public boolean autoCommitFailureClosesAllResultSets() throws SQLException {
        return false;
    }
    public ResultSet getClientInfoProperties() throws SQLException {
        String s =
            "SELECT * FROM INFORMATION_SCHEMA.SYSTEM_CONNECTION_PROPERTIES";
        return execute(s);
    }
    public ResultSet getFunctions(
            String catalog, String schemaPattern,
            String functionNamePattern) throws SQLException {
        StringBuffer sb = new StringBuffer(256);
        sb.append("select ").append(
            "sp.procedure_cat as FUNCTION_CAT,").append(
            "sp.procedure_schem as FUNCTION_SCHEM,").append(
            "sp.procedure_name as FUNCTION_NAME,").append(
            "sp.remarks as REMARKS,").append("1 as FUNCTION_TYPE,").append(
            "sp.specific_name as SPECIFIC_NAME ").append(
            "from information_schema.system_procedures sp ").append(
            "where sp.procedure_type = 2 ");
        if (wantsIsNull(functionNamePattern)) {
            return execute(sb.append("and 1=0").toString());
        }
        schemaPattern = translateSchema(schemaPattern);
        sb.append(and("sp.procedure_cat", "=",
                      catalog)).append(and("sp.procedure_schem", "LIKE",
                          schemaPattern)).append(and("sp.procedure_name",
                              "LIKE", functionNamePattern));
        return execute(sb.toString());
    }
    public ResultSet getFunctionColumns(
            String catalog, String schemaPattern, String functionNamePattern,
            String columnNamePattern) throws SQLException {
        StringBuffer sb = new StringBuffer(256);
        sb.append("select pc.procedure_cat as FUNCTION_CAT,").append(
            "pc.procedure_schem as FUNCTION_SCHEM,").append(
            "pc.procedure_name as FUNCTION_NAME,").append(
            "pc.column_name as COLUMN_NAME,").append(
            "case pc.column_type").append(" when 3 then 5").append(
            " when 4 then 3").append(" when 5 then 4").append(
            " else pc.column_type").append(" end as COLUMN_TYPE,").append(
            "pc.DATA_TYPE,").append("pc.TYPE_NAME,").append(
            "pc.PRECISION,").append("pc.LENGTH,").append("pc.SCALE,").append(
            "pc.RADIX,").append("pc.NULLABLE,").append("pc.REMARKS,").append(
            "pc.CHAR_OCTET_LENGTH,").append("pc.ORDINAL_POSITION,").append(
            "pc.IS_NULLABLE,").append("pc.SPECIFIC_NAME,").append(
            "case pc.column_type").append(" when 3 then 1").append(
            " else 0").append(" end AS COLUMN_GROUP ").append(
            "from information_schema.system_procedurecolumns pc ").append(
            "join (select procedure_schem,").append("procedure_name,").append(
            "specific_name ").append(
            "from information_schema.system_procedures ").append(
            "where procedure_type = 2) p ").append(
            "on pc.procedure_schem = p.procedure_schem ").append(
            "and pc.procedure_name = p.procedure_name ").append(
            "and pc.specific_name = p.specific_name ").append(
            "and ((pc.column_type = 3 and pc.column_name = '@p0') ").append(
            "or ").append("(pc.column_type <> 3)) ");
        if (wantsIsNull(functionNamePattern)
                || wantsIsNull(columnNamePattern)) {
            return execute(sb.append("where 1=0").toString());
        }
        schemaPattern = translateSchema(schemaPattern);
        sb.append("where 1=1 ").append(
            and("pc.procedure_cat", "=", catalog)).append(
            and("pc.procedure_schem", "LIKE", schemaPattern)).append(
            and("pc.procedure_name", "LIKE", functionNamePattern)).append(
            and("pc.column_name", "LIKE", columnNamePattern)).append(
            " order by 1, 2, 3, 17, 18 , 15");
        return execute(sb.toString());
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
    public ResultSet getPseudoColumns(
            String catalog, String schemaPattern, String tableNamePattern,
            String columnNamePattern) throws SQLException {
        throw Util.notSupported();
    }
    public boolean generatedKeyAlwaysReturned() throws SQLException {
        return true;
    }
    static final Integer INT_COLUMNS_NO_NULLS = new Integer(columnNoNulls);
    private JDBCConnection connection;
    final private boolean useSchemaDefault;
    private static final String BRI_SESSION_SCOPE_IN_LIST = "("
        + bestRowSession + ")";
    private static final String BRI_TEMPORARY_SCOPE_IN_LIST = "("
        + bestRowTemporary + "," + bestRowTransaction + "," + bestRowSession
        + ")";
    private static final String BRI_TRANSACTION_SCOPE_IN_LIST = "("
        + bestRowTransaction + "," + bestRowSession + ")";
    private static final String selstar = "SELECT * FROM INFORMATION_SCHEMA.";
    private static final String whereTrue = " WHERE TRUE";
    public static final int JDBC_MAJOR = 4;
    JDBCDatabaseMetaData(JDBCConnection c) throws SQLException {
        connection       = c;
        useSchemaDefault = c.isInternal ? false
                                        : c.connProperties
                                        .isPropertyTrue(HsqlDatabaseProperties
                                            .url_default_schema);
    }
    private static String and(String id, String op, Object val) {
        if (val == null) {
            return "";
        }
        StringBuffer sb    = new StringBuffer();
        boolean      isStr = (val instanceof String);
        if (isStr && ((String) val).length() == 0) {
            return sb.append(" AND ").append(id).append(" IS NULL").toString();
        }
        String v = isStr ? Type.SQL_VARCHAR.convertToSQLString(val)
                         : String.valueOf(val);
        sb.append(" AND ").append(id).append(' ');
        if (isStr && "LIKE".equalsIgnoreCase(op)) {
            if (v.indexOf('_') < 0 && v.indexOf('%') < 0) {
                sb.append("=").append(' ').append(v);
            } else {
                sb.append("LIKE").append(' ').append(v);
                if ((v.indexOf("\\_") >= 0) || (v.indexOf("\\%") >= 0)) {
                    sb.append(" ESCAPE '\\'");
                }
            }
        } else {
            sb.append(op).append(' ').append(v);
        }
        return sb.toString();
    }
    private ResultSet execute(String sql) throws SQLException {
        final int scroll = JDBCResultSet.TYPE_SCROLL_INSENSITIVE;
        final int concur = JDBCResultSet.CONCUR_READ_ONLY;
        JDBCStatement st = (JDBCStatement) connection.createStatement(scroll,
            concur);
        st.maxRows = -1;
        ResultSet r = st.executeQuery(sql);
        ((JDBCResultSet) r).autoClose = true;
        return r;
    }
    private ResultSet executeSelect(String table,
                                    String where) throws SQLException {
        String select = selstar + table;
        if (where != null) {
            select += " WHERE " + where;
        }
        return execute(select);
    }
    private StringBuffer toQueryPrefix(String t) {
        StringBuffer sb = new StringBuffer(255);
        return sb.append(selstar).append(t).append(whereTrue);
    }
    private StringBuffer toQueryPrefixNoSelect(String t) {
        StringBuffer sb = new StringBuffer(255);
        return sb.append(t).append(whereTrue);
    }
    private static boolean wantsIsNull(String s) {
        return (s != null && s.length() == 0);
    }
    String getDatabaseDefaultSchema() throws SQLException {
        final ResultSet rs = executeSelect("SYSTEM_SCHEMAS",
            "IS_DEFAULT=TRUE");
        return rs.next() ? rs.getString(1)
                         : null;
    }
    String getConnectionDefaultSchema() throws SQLException {
        ResultSet rs = execute("CALL CURRENT_SCHEMA");
        rs.next();
        String result = rs.getString(1);
        rs.close();
        return result;
    }
    void setConnectionDefaultSchema(String schemaName) throws SQLException {
        execute("SET SCHEMA "
                + org.hsqldb.lib.StringConverter.toQuotedString(schemaName,
                    '"', true));
    }
    private String translateSchema(String schemaName) throws SQLException {
        if (useSchemaDefault && schemaName != null
                && schemaName.length() == 0) {
            final String result = getDatabaseDefaultSchema();
            if (result != null) {
                schemaName = result;
            }
        }
        return schemaName;
    }
    String getDatabaseDefaultCatalog() throws SQLException {
        final ResultSet rs = executeSelect("SYSTEM_SCHEMAS",
            "IS_DEFAULT=TRUE");
        return rs.next() ? rs.getString(2)
                         : null;
    }
    private String translateCatalog(String catalogName) throws SQLException {
        if (useSchemaDefault && catalogName != null
                && catalogName.length() == 0) {
            String result = getDatabaseDefaultCatalog();
            if (result != null) {
                catalogName = result;
            }
        }
        return catalogName;
    }
}