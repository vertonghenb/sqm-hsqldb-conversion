


package org.hsqldb.jdbc;

import java.util.Hashtable;
import java.util.Properties;

import javax.naming.Context;
import javax.naming.Name;
import javax.naming.Reference;
import javax.naming.RefAddr;
import javax.naming.spi.ObjectFactory;
import javax.sql.DataSource;


public class JDBCDataSourceFactory implements ObjectFactory {

    
    public static DataSource createDataSource(
            Properties props) throws Exception {

        JDBCDataSource ds =
            (JDBCDataSource) Class.forName(bdsClassName).newInstance();
        String value = props.getProperty(databaseName);

        if (value == null) {
            value = props.getProperty(urlName);
        }
        ds.setDatabase(value);

        value = props.getProperty(userName);

        if (value == null) {
            value = props.getProperty(userNameName);
        }
        ds.setUser(value);

        value = props.getProperty(passwordName);

        ds.setPassword(value);

        value = props.getProperty(loginTimeoutName);

        if (value != null) {
            value = value.trim();

            if (value.length() > 0) {
                try {
                    ds.setLoginTimeout(Integer.parseInt(value));
                } catch (NumberFormatException nfe) {
                }
            }
        }

        return ds;
    }

    
    public Object getObjectInstance(Object obj, Name name, Context nameCtx,
                                    Hashtable environment) throws Exception {

        if (!(obj instanceof Reference)) {
            return null;
        }

        Reference ref       = (Reference) obj;
        String    className = ref.getClassName();

        if (className.equals(bdsClassName) || className.equals(pdsClassName)
                || className.equals(xdsClassName)) {
            RefAddr refAddr;
            Object  value;
            JDBCCommonDataSource ds =
                (JDBCCommonDataSource) Class.forName(className).newInstance();

            refAddr = ref.get("database");

            if (refAddr == null) {
                throw new Exception(className + ": RefAddr not set: database");
            }
            value = refAddr.getContent();

            if (!(value instanceof String)) {
                throw new Exception(className + ": invalid RefAddr: database");
            }
            ds.setDatabase((String) value);

            refAddr = ref.get("user");

            if (refAddr == null) {
                throw new Exception(className + ": RefAddr not set: user");
            }
            value = ref.get("user").getContent();

            if (!(value instanceof String)) {
                throw new Exception(className + ": invalid RefAddr: user");
            }
            ds.setUser((String) value);

            refAddr = ref.get("password");

            if (refAddr == null) {
                value = "";
            } else {
                value = ref.get("password").getContent();

                if (!(value instanceof String)) {
                    throw new Exception(className
                                        + ": invalid RefAddr: password");
                }
            }
            ds.setPassword((String) value);

            refAddr = ref.get("loginTimeout");

            if (refAddr != null) {
                value = refAddr.getContent();

                if (value instanceof String) {
                    String loginTimeoutContent = ((String) value).trim();

                    if (loginTimeoutContent.length() > 0) {
                        try {
                            ds.setLoginTimeout(
                                Integer.parseInt(loginTimeoutContent));
                        } catch (NumberFormatException nfe) {
                        }
                    }
                }
            }

            return ds;
        } else {
            return null;
        }
    }

    
    private static final String urlName          = "url";
    private static final String databaseName     = "database";
    private static final String userName         = "user";
    private static final String userNameName     = "username";
    private static final String passwordName     = "password";
    private static final String loginTimeoutName = "loginTimeout";

    
    private static final String bdsClassName =
        "org.hsqldb.jdbc.JDBCDataSource";
    private static final String pdsClassName =
        "org.hsqldb.jdbc.pool.JDBCPooledDataSource";
    private static final String xdsClassName =
        "org.hsqldb.jdbc.pool.JDBCXADataSource";

    public JDBCDataSourceFactory() {
    }
}
