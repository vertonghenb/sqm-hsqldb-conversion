


package org.hsqldb.auth;

import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.Collections;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.ResultSet;
import org.hsqldb.jdbc.JDBCArrayBasic;
import org.hsqldb.types.Type;
import org.hsqldb.lib.FrameworkLogger;


public class AuthBeanMultiplexer {
    private static FrameworkLogger logger =
            FrameworkLogger.getLog(AuthBeanMultiplexer.class);

    
    private AuthBeanMultiplexer() {
        
    }

    private static AuthBeanMultiplexer singleton = new AuthBeanMultiplexer();

    
    private static Map<String, List<AuthFunctionBean>> beans =
            new HashMap<String, List<AuthFunctionBean>>();

    public static AuthBeanMultiplexer getSingleton() {
        return singleton;
    }

    
    public void clear() {
        AuthBeanMultiplexer.beans.clear();
    }

    
    public void setAuthFunctionBeans(
            Map<String, List<AuthFunctionBean>> authFunctionBeanMap) {
        if (AuthBeanMultiplexer.beans.size() > 0)
            throw new IllegalStateException(
                    "Use setAuthFunctionBeans(Map) only when the set is empty");
        AuthBeanMultiplexer.beans.putAll(authFunctionBeanMap);
    }

    protected static String getUniqueNameFor(Connection c) throws SQLException {
        ResultSet rs = c.createStatement().executeQuery("CALL database_name()");
        try {
            if (!rs.next()) {
                throw new SQLException(
                        "Engine did not reveal unique database name");
            }
            return rs.getString(1);
        } finally {
            if (rs != null) try {
                rs.close();
            } catch (SQLException se) {
                logger.error(
                        "Failed to close ResultSet for retrieving db name");
            }
            rs = null;  
        }
    }

    
    public void setAuthFunctionBeans(Connection c,
            List<AuthFunctionBean> authFunctionBeans) throws SQLException {
        setAuthFunctionBeans(getUniqueNameFor(c), authFunctionBeans);
    }

    
    public void setAuthFunctionBeans(String dbName,
            List<AuthFunctionBean> authFunctionBeans) {
        if (dbName == null || dbName.length() != 16) {
            throw new IllegalArgumentException(
                    "Database name not exactly 16 characters long: "
                    + dbName);
        }
        List<AuthFunctionBean> dbsBeans = AuthBeanMultiplexer.beans.get(dbName);
        if (dbsBeans == null) {
            dbsBeans = new ArrayList<AuthFunctionBean>();
            AuthBeanMultiplexer.beans.put(dbName, dbsBeans);
        } else {
            if (dbsBeans.size() > 0)
                throw new IllegalStateException(
                        "Use setAuthFunctionBeans(String, List) only when the "
                        + "db's AuthFunctionBean list is empty");
        }
        dbsBeans.addAll(authFunctionBeans);
    }

    
    public void setAuthFunctionBean(Connection c,
            AuthFunctionBean authFunctionBean) throws SQLException {
        setAuthFunctionBeans(getUniqueNameFor(c),
                Collections.singletonList(authFunctionBean));
    }

    
    public void setAuthFunctionBean(String dbName,
            AuthFunctionBean authFunctionBean) {
        setAuthFunctionBeans(
                dbName, Collections.singletonList(authFunctionBean));
    }

    
    public static java.sql.Array authenticate(
            String database, String user, String password)
            throws Exception {
        
        if (database == null || database.length() != 16) {
            throw new IllegalStateException("Internal problem.  "
                    + "Database name not exactly 16 characters long: "
                    + database);
        }
        List<AuthFunctionBean> beanList =
                AuthBeanMultiplexer.beans.get(database);
        if (beanList == null) {
            logger.error("Database '" + database
                    + "' has not been set up with "
                    + AuthBeanMultiplexer.class.getName());
            throw new IllegalArgumentException("Database '" + database
                    + "' has not been set up with "
                    + AuthBeanMultiplexer.class.getName());
        }
        Exception firstRTE = null;
        String[] beanRet;
        for (AuthFunctionBean nextBean : beanList) try {
            beanRet = nextBean.authenticate(user, password);
            return (beanRet == null)
                    ? null : new JDBCArrayBasic(beanRet, Type.SQL_VARCHAR);
        } catch (RuntimeException re) {
            if (firstRTE == null) {
                firstRTE = re;
            }
            logger.error("System failure of an AuthFunctionBean: "
                    + ((re.getMessage() == null)
                      ? re.toString() : re.getMessage()));
        } catch (Exception e) {
            throw e;
        }
        throw firstRTE;
    }
}
