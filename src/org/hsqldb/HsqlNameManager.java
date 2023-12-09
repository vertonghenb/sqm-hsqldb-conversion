


package org.hsqldb;

import org.hsqldb.lib.StringConverter;
import org.hsqldb.rights.Grantee;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;


public final class HsqlNameManager {

    public static final String DEFAULT_CATALOG_NAME = "PUBLIC";
    private static final HsqlNameManager staticManager =
        new HsqlNameManager(null);

    static {
        staticManager.serialNumber = Integer.MIN_VALUE;
    }

    private static final HsqlName[] autoColumnNames       = new HsqlName[32];
    private static final String[]   autoNoNameColumnNames = new String[32];

    static {
        for (int i = 0; i < autoColumnNames.length; i++) {
            autoColumnNames[i] = new HsqlName(staticManager, "C" + (i + 1), 0,
                                              false);
            autoNoNameColumnNames[i] = String.valueOf(i);
        }
    }

    private int      serialNumber = 1;        
    private int      sysNumber    = 10000;    
    private HsqlName catalogName;

    public HsqlNameManager(Database database) {
        catalogName = new HsqlName(this, DEFAULT_CATALOG_NAME,
                                   SchemaObject.CATALOG, false);
    }

    public HsqlName getCatalogName() {
        return catalogName;
    }

    public static HsqlName newSystemObjectName(String name, int type) {
        return new HsqlName(staticManager, name, type, false);
    }

    public static HsqlName newInfoSchemaColumnName(String name,
            HsqlName table) {

        HsqlName hsqlName = new HsqlName(staticManager, name, false,
                                         SchemaObject.COLUMN);

        hsqlName.schema = SqlInvariants.INFORMATION_SCHEMA_HSQLNAME;
        hsqlName.parent = table;

        return hsqlName;
    }

    public static HsqlName newInfoSchemaTableName(String name) {

        HsqlName hsqlName = new HsqlName(staticManager, name,
                                         SchemaObject.TABLE, false);

        hsqlName.schema = SqlInvariants.INFORMATION_SCHEMA_HSQLNAME;

        return hsqlName;
    }

    public static HsqlName newInfoSchemaObjectName(String name,
            boolean isQuoted, int type) {

        HsqlName hsqlName = new HsqlName(staticManager, name, type, isQuoted);

        hsqlName.schema = SqlInvariants.INFORMATION_SCHEMA_HSQLNAME;

        return hsqlName;
    }

    public HsqlName newHsqlName(HsqlName schema, String name, int type) {

        HsqlName hsqlName = new HsqlName(this, name, type, false);

        hsqlName.schema = schema;

        return hsqlName;
    }

    
    public HsqlName newHsqlName(String name, boolean isquoted, int type) {
        return new HsqlName(this, name, isquoted, type);
    }

    public HsqlName newHsqlName(HsqlName schema, String name,
                                boolean isquoted, int type) {

        HsqlName hsqlName = new HsqlName(this, name, isquoted, type);

        hsqlName.schema = schema;

        return hsqlName;
    }

    public HsqlName newHsqlName(HsqlName schema, String name,
                                boolean isquoted, int type, HsqlName parent) {

        HsqlName hsqlName = new HsqlName(this, name, isquoted, type);

        hsqlName.schema = schema;
        hsqlName.parent = parent;

        return hsqlName;
    }

    public HsqlName newColumnSchemaHsqlName(HsqlName table, SimpleName name) {
        return newColumnHsqlName(table, name.name, name.isNameQuoted);
    }

    public HsqlName newColumnHsqlName(HsqlName table, String name,
                                      boolean isquoted) {

        HsqlName hsqlName = new HsqlName(this, name, isquoted,
                                         SchemaObject.COLUMN);

        hsqlName.schema = table.schema;
        hsqlName.parent = table;

        return hsqlName;
    }

    
    public HsqlName getSubqueryTableName() {

        HsqlName hsqlName = new HsqlName(this, SqlInvariants.SYSTEM_SUBQUERY,
                                         false, SchemaObject.TABLE);

        hsqlName.schema = SqlInvariants.SYSTEM_SCHEMA_HSQLNAME;

        return hsqlName;
    }

    
    public HsqlName newAutoName(String prefix, HsqlName schema,
                                HsqlName parent, int type) {

        HsqlName name = newAutoName(prefix, (String) null, schema, parent,
                                    type);

        return name;
    }

    public HsqlName newSpecificRoutineName(HsqlName name) {

        StringBuffer sb = new StringBuffer();

        sb.append(name.name).append('_').append(++sysNumber);

        HsqlName hsqlName = new HsqlName(this, sb.toString(),
                                         SchemaObject.SPECIFIC_ROUTINE,
                                         name.isNameQuoted);

        hsqlName.parent = name;
        hsqlName.schema = name.schema;

        return hsqlName;
    }

    
    public static HsqlName getAutoColumnName(int i) {

        if (i < autoColumnNames.length) {
            return autoColumnNames[i];
        }

        return new HsqlName(staticManager, "C_" + (i + 1), 0, false);
    }

    
    public static String getAutoColumnNameString(int i) {

        if (i < autoColumnNames.length) {
            return autoColumnNames[i].name;
        }

        return "C" + (i + 1);
    }

    public static String getAutoNoNameColumnString(int i) {

        if (i < autoColumnNames.length) {
            return autoNoNameColumnNames[i];
        }

        return String.valueOf(i);
    }

    public static String getAutoSavepointNameString(long i, int j) {

        StringBuffer sb = new StringBuffer("S");

        sb.append(i).append('_').append(j);

        return sb.toString();
    }

    
    public HsqlName newAutoName(String prefix, String namepart,
                                HsqlName schema, HsqlName parent, int type) {

        StringBuffer sb = new StringBuffer();

        if (prefix != null) {
            if (prefix.length() != 0) {
                sb.append("SYS_");
                sb.append(prefix);
                sb.append('_');

                if (namepart != null) {
                    sb.append(namepart);
                    sb.append('_');
                }

                sb.append(++sysNumber);
            }
        } else {
            sb.append(namepart);
        }

        HsqlName name = new HsqlName(this, sb.toString(), type, false);

        name.schema = schema;
        name.parent = parent;

        return name;
    }

    void resetNumbering() {
        sysNumber    = 0;
        serialNumber = 0;
    }

    public static SimpleName getSimpleName(String name, boolean isNameQuoted) {
        return new SimpleName(name, isNameQuoted);
    }

    public static class SimpleName {

        public String  name;
        public boolean isNameQuoted;

        private SimpleName() {}

        private SimpleName(String name, boolean isNameQuoted) {
            this.name         = name;
            this.isNameQuoted = isNameQuoted;
        }

        public int hashCode() {
            return name.hashCode();
        }

        public boolean equals(Object other) {

            if (other instanceof SimpleName) {
                return ((SimpleName) other).isNameQuoted == isNameQuoted
                       && ((SimpleName) other).name.equals(name);
            }

            return false;
        }

        public String getStatementName() {

            return isNameQuoted
                   ? StringConverter.toQuotedString(name, '"', true)
                   : name;
        }

        public String getNameString() {
            return name;
        }
    }

    public static final class HsqlName extends SimpleName {

        static HsqlName[] emptyArray = new HsqlName[]{};

        
        HsqlNameManager   manager;
        public String     statementName;
        public String     comment;
        public HsqlName   schema;
        public HsqlName   parent;
        public Grantee    owner;
        public final int  type;
        private final int hashCode;

        private HsqlName(HsqlNameManager man, int type) {

            manager   = man;
            this.type = type;
            hashCode  = manager.serialNumber++;
        }

        private HsqlName(HsqlNameManager man, String name, boolean isquoted,
                         int type) {

            this(man, type);

            rename(name, isquoted);
        }

        
        private HsqlName(HsqlNameManager man, String name, int type,
                         boolean isQuoted) {

            this(man, type);

            this.name          = name;
            this.statementName = name;
            this.isNameQuoted  = isQuoted;

            if (isNameQuoted) {
                statementName = StringConverter.toQuotedString(name, '"',
                        true);
            }
        }

        public String getStatementName() {
            return statementName;
        }

        public String getSchemaQualifiedStatementName() {

            switch (type) {

                case SchemaObject.PARAMETER :
                case SchemaObject.VARIABLE : {
                    return statementName;
                }
                case SchemaObject.COLUMN : {
                    if (parent == null
                            || SqlInvariants.SYSTEM_SUBQUERY.equals(
                                parent.name)) {
                        return statementName;
                    }

                    StringBuffer sb = new StringBuffer();

                    if (schema != null) {
                        sb.append(schema.getStatementName());
                        sb.append('.');
                    }

                    sb.append(parent.getStatementName());
                    sb.append('.');
                    sb.append(statementName);

                    return sb.toString();
                }
            }

            if (schema == null) {
                return statementName;
            }

            StringBuffer sb = new StringBuffer();

            if (schema != null) {
                sb.append(schema.getStatementName());
                sb.append('.');
            }

            sb.append(statementName);

            return sb.toString();
        }

        public void rename(HsqlName name) {
            rename(name.name, name.isNameQuoted);
        }

        public void rename(String name, boolean isquoted) {

            if (name.length() > 128) {
                throw Error.error(ErrorCode.X_42501, name);
            }

            
            this.name          = new String(name);
            this.statementName = name;
            this.isNameQuoted  = isquoted;

            if (isNameQuoted) {
                statementName = StringConverter.toQuotedString(name, '"',
                        true);
            }

            if (name.startsWith("SYS_")) {
                int length = name.lastIndexOf('_') + 1;

                try {
                    int temp = Integer.parseInt(name.substring(length));

                    if (temp > manager.sysNumber) {
                        manager.sysNumber = temp;
                    }
                } catch (NumberFormatException e) {}
            }
        }

        void rename(String prefix, String name, boolean isquoted) {

            StringBuffer sbname = new StringBuffer(prefix);

            sbname.append('_');
            sbname.append(name);
            rename(sbname.toString(), isquoted);
        }

        public void setSchemaIfNull(HsqlName schema) {

            if (this.schema == null) {
                this.schema = schema;
            }
        }

        public boolean equals(Object other) {

            if (other instanceof HsqlName) {
                return hashCode == ((HsqlName) other).hashCode;
            }

            return false;
        }

        
        public int hashCode() {
            return hashCode;
        }

        
        static final String[] sysPrefixes = new String[] {
            "SYS_IDX_", "SYS_PK_", "SYS_REF_", "SYS_CT_", "SYS_FK_",
        };

        static int sysPrefixLength(String name) {

            for (int i = 0; i < sysPrefixes.length; i++) {
                if (name.startsWith(sysPrefixes[i])) {
                    return sysPrefixes[i].length();
                }
            }

            return 0;
        }

        static boolean isReservedName(String name) {
            return sysPrefixLength(name) > 0;
        }

        boolean isReservedName() {
            return isReservedName(name);
        }

        public String toString() {

            return getClass().getName() + super.hashCode()
                   + "[this.hashCode()=" + this.hashCode + ", name=" + name
                   + ", name.hashCode()=" + name.hashCode()
                   + ", isNameQuoted=" + isNameQuoted + "]";
        }

        public int compareTo(Object o) {
            return hashCode - o.hashCode();
        }

        
        static boolean isRegularIdentifier(String name) {

            for (int i = 0, length = name.length(); i < length; i++) {
                int c = name.charAt(i);

                if (c >= 'A' && c <= 'Z') {
                    continue;
                } else if (c == '_' && i > 0) {
                    continue;
                } else if (c >= '0' && c <= '9') {
                    continue;
                }

                return false;
            }

            return !Tokens.isKeyword(name);
        }
    }
}
