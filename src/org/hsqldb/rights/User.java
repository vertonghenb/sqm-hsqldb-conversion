


package org.hsqldb.rights;

import org.hsqldb.HsqlNameManager;
import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.Tokens;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.MD5;
import org.hsqldb.lib.StringConverter;


public class User extends Grantee {

    
    private String password;
    public boolean isLocalOnly;
    public boolean isExternalOnly;

    
    private HsqlName initialSchema = null;

    
    User(HsqlName name, GranteeManager manager) {

        super(name, manager);

        if (manager != null) {
            updateAllRights();
        }
    }

    public String getSQL() {

        StringBuffer sb = new StringBuffer();

        sb.append(Tokens.T_CREATE).append(' ').append(Tokens.T_USER);
        sb.append(' ').append(granteeName.statementName).append(' ');
        sb.append(Tokens.T_PASSWORD).append(' ').append(Tokens.T_DIGEST);
        sb.append(' ').append('\'').append(password).append('\'');

        return sb.toString();
    }

    public String getPasswordDigest() {
        return password;
    }

    public void setPassword(String password, boolean isDigest) {

        if (!isDigest) {
            password = MD5.encode(password, null);
        }

        this.password = password;
    }

    
    public void checkPassword(String value) {

        String digest = MD5.encode(value, null);

        if (!digest.equals(password)) {
            throw Error.error(ErrorCode.X_28000);
        }
    }

    
    public HsqlName getInitialSchema() {
        return initialSchema;
    }

    public HsqlName getInitialOrDefaultSchema() {

        if (initialSchema != null) {
            return initialSchema;
        }

        HsqlName schema =
            granteeManager.database.schemaManager.findSchemaHsqlName(
                getName().getNameString());

        if (schema == null) {
            return granteeManager.database.schemaManager
                .getDefaultSchemaHsqlName();
        } else {
            return schema;
        }
    }

    
    public void setInitialSchema(HsqlName schema) {
        initialSchema = schema;
    }

    public String getInitialSchemaSQL() {

        StringBuffer sb = new StringBuffer();

        sb.append(Tokens.T_ALTER).append(' ');
        sb.append(Tokens.T_USER).append(' ');
        sb.append(getName().getStatementName()).append(' ');
        sb.append(Tokens.T_SET).append(' ');
        sb.append(Tokens.T_INITIAL).append(' ');
        sb.append(Tokens.T_SCHEMA).append(' ');
        sb.append(initialSchema.getStatementName());

        return sb.toString();
    }

    
    public String getLocalUserSQL() {

        StringBuffer sb = new StringBuffer(64);

        sb.append(Tokens.T_ALTER).append(' ');
        sb.append(Tokens.T_USER).append(' ');
        sb.append(getName().getStatementName()).append(' ');
        sb.append(Tokens.T_SET).append(' ').append(Tokens.T_LOCAL);
        sb.append(' ').append(Tokens.T_TRUE);

        return sb.toString();
    }

    
    public String getSetPasswordDigestSQL() {

        StringBuffer sb = new StringBuffer(64);

        sb.append(Tokens.T_ALTER).append(' ');
        sb.append(Tokens.T_USER).append(' ');
        sb.append(getName().getStatementName()).append(' ');
        sb.append(Tokens.T_SET).append(' ');
        sb.append(Tokens.T_PASSWORD).append(' ').append(Tokens.T_DIGEST);
        sb.append(' ').append('\'').append(password).append('\'');

        return sb.toString();
    }

    
    public static String getSetCurrentPasswordDigestSQL(String password,
            boolean isDigest) {

        if (!isDigest) {
            password = MD5.encode(password, null);
        }

        StringBuffer sb = new StringBuffer(64);

        sb.append(Tokens.T_SET).append(' ');
        sb.append(Tokens.T_PASSWORD).append(' ').append(Tokens.T_DIGEST);
        sb.append(' ').append('\'').append(password).append('\'');

        return sb.toString();
    }

    
    public String getConnectUserSQL() {

        StringBuffer sb = new StringBuffer();

        sb.append(Tokens.T_SET).append(' ');
        sb.append(Tokens.T_SESSION).append(' ');
        sb.append(Tokens.T_AUTHORIZATION).append(' ');
        sb.append(StringConverter.toQuotedString(getName().getNameString(),
                '\'', true));

        return sb.toString();
    }
}
