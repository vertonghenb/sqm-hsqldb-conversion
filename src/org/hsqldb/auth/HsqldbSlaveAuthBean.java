package org.hsqldb.auth;
import java.util.Set;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.DriverManager;
import org.hsqldb.lib.FrameworkLogger;
public class HsqldbSlaveAuthBean implements AuthFunctionBean {
    private static FrameworkLogger logger =
            FrameworkLogger.getLog(HsqldbSlaveAuthBean.class);
    private String masterJdbcUrl, validationUser, validationPassword;
    private boolean delegateRolesSchema = true;
    protected boolean initialized;
    public void setValidationUser(String validationUser) {
        this.validationUser = validationUser;
    }
    public void setValidationPassword(String validationPassword) {
        this.validationPassword = validationPassword;
    }
    public void setMasterJdbcUrl(String masterJdbcUrl) {
        this.masterJdbcUrl = masterJdbcUrl;
    }
    public void setDelegateRolesSchema(boolean doDelegateRolesSchema) {
        delegateRolesSchema = doDelegateRolesSchema;
    }
    public HsqldbSlaveAuthBean() {
    }
    public void init() throws SQLException {
        if (masterJdbcUrl == null) {
            throw new IllegalStateException(
                    "Required property 'masterJdbcUrl' not set");
        }
        if (validationUser != null || validationPassword != null) {
            if (validationUser == null || validationPassword == null) {
                throw new IllegalStateException(
                        "If you set one property of 'validationUser' or "
                        + "'validationPassword', then you must set both.");
            }
            Connection c = null;
            SQLException problem = null;
            try {
                c = DriverManager.getConnection(
                        masterJdbcUrl, validationUser, validationPassword);
            } catch (SQLException se) {
                logger.error("Master/slave Connection validation failure", se);
                problem = se;  
            } finally {
                if (c != null) try {
                    c.close();
                    c = null;  
                } catch (SQLException nestedSe) {
                    logger.error(
                            "Failed to close test master/slave Connection",
                            nestedSe);
                    if (problem == null) {
                        throw nestedSe;
                    }
                }
            }
        }
        initialized = true;
    }
    public String[] authenticate(String userName, String password)
            throws DenyException {
        if (!initialized) {
            throw new IllegalStateException(
                "You must invoke the 'init' method to initialize the "
                + HsqldbSlaveAuthBean.class.getName() + " instance.");
        }
        Connection c = null;
        try {
            c = DriverManager.getConnection(masterJdbcUrl, userName, password);
            if (delegateRolesSchema) {
                Set<String> schemaAndRoles = AuthUtils.getEnabledRoles(c);
                String schemaOnMaster = AuthUtils.getInitialSchema(c);
                if (schemaOnMaster != null) {
                    schemaAndRoles.add(schemaOnMaster);
                }
                logger.finer("Slave delegating schema+roles: "
                        + schemaAndRoles);
                return schemaAndRoles.toArray(new String[0]);
            }
            return null;
        } catch (SQLException se) {
            throw new DenyException();
        } finally {
            if (c != null) try {
                c.close();
                c = null;  
            } catch (SQLException nestedSe) {
                logger.severe(
                        "Failed to close master/slave Connection", nestedSe);
            }
        }
    }
}