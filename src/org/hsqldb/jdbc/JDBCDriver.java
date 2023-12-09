


package org.hsqldb.jdbc;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;

import org.hsqldb.DatabaseURL;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.persist.HsqlDatabaseProperties;
import org.hsqldb.persist.HsqlProperties;









public class JDBCDriver implements Driver {

    
    public JDBCDriver() {
    }

    
    public Connection connect(String url,
                              Properties info) throws SQLException {

        if (url.regionMatches(true, 0, DatabaseURL.S_URL_INTERNAL, 0,
                              DatabaseURL.S_URL_INTERNAL.length())) {
            JDBCConnection conn = (JDBCConnection) threadConnection.get();

            if (conn == null) {
                return null;
            }

            return conn;
        }

        return getConnection(url, info);
    }

    


    @SuppressWarnings("deprecation")


    public static Connection getConnection(String url,
            Properties info) throws SQLException {

        final HsqlProperties props = DatabaseURL.parseURL(url, true, false);

        if (props == null) {

            
            throw Util.invalidArgument();
        } else if (props.isEmpty()) {

            
            return null;
        }

        long timeout = 0;

        if (info != null && info.containsKey("loginTimeout")) {
            String loginTimeoutProperty = info.getProperty("loginTimeout");

            if (loginTimeoutProperty != null) {
                loginTimeoutProperty = loginTimeoutProperty.trim();

                if (loginTimeoutProperty.length() > 0) {
                    try {
                        timeout = Integer.parseInt(loginTimeoutProperty);
                    } catch (NumberFormatException nfe) {
                    }
                }
            }
        }
        props.addProperties(info);

        if (timeout == 0) {
            timeout = DriverManager.getLoginTimeout();
        }

        
        
        
        if (timeout == 0) {

            
            return new JDBCConnection(props);
        }

        String connType = props.getProperty("connection_type");

        if (DatabaseURL.isInProcessDatabaseType(connType)) {
            return new JDBCConnection(props);
        }

        
        final JDBCConnection[] conn = new JDBCConnection[1];
        final SQLException[]   ex   = new SQLException[1];
        Thread                 t    = new Thread() {

            public void run() {

                try {
                    conn[0] = new JDBCConnection(props);
                } catch (SQLException se) {
                    ex[0] = se;
                }
            }
        };

        t.start();

        final long start = System.currentTimeMillis();

        try {
            t.join(1000 * timeout);
        } catch (InterruptedException ie) {
        }

        try {

            
            
            
            
            
            
            
            t.stop();
        } catch (Exception e) {
        } finally {
            try {
                t.setContextClassLoader(null);
            } catch (Throwable th) {
            }
        }

        if (ex[0] != null) {
            throw ex[0];
        }

        if (conn[0] != null) {
            return conn[0];
        }

        throw Util.sqlException(ErrorCode.X_08501);
    }

    


    public boolean acceptsURL(String url) {

        if (url == null) {
            return false;
        }

        if (url.regionMatches(true, 0, DatabaseURL.S_URL_PREFIX, 0,
                              DatabaseURL.S_URL_PREFIX.length())) {
            return true;
        }

        if (url.regionMatches(true, 0, DatabaseURL.S_URL_INTERNAL, 0,
                              DatabaseURL.S_URL_INTERNAL.length())) {
            return true;
        }

        return false;
    }

    
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {

        if (!acceptsURL(url)) {
            return new DriverPropertyInfo[0];
        }

        String[]             choices = new String[] {
            "true", "false"
        };
        DriverPropertyInfo[] pinfo   = new DriverPropertyInfo[6];
        DriverPropertyInfo   p;

        if (info == null) {
            info = new Properties();
        }
        p          = new DriverPropertyInfo("user", null);
        p.value    = info.getProperty("user");
        p.required = true;
        pinfo[0]   = p;
        p          = new DriverPropertyInfo("password", null);
        p.value    = info.getProperty("password");
        p.required = true;
        pinfo[1]   = p;
        p          = new DriverPropertyInfo("get_column_name", null);
        p.value    = info.getProperty("get_column_name", "true");
        p.required = false;
        p.choices  = choices;
        pinfo[2]   = p;
        p          = new DriverPropertyInfo("ifexists", null);
        p.value    = info.getProperty("ifexists", "false");
        p.required = false;
        p.choices  = choices;
        pinfo[3]   = p;
        p          = new DriverPropertyInfo("default_schema", null);
        p.value    = info.getProperty("default_schema", "false");
        p.required = false;
        p.choices  = choices;
        pinfo[4]   = p;
        p          = new DriverPropertyInfo("shutdown", null);
        p.value    = info.getProperty("shutdown", "false");
        p.required = false;
        p.choices  = choices;
        pinfo[5]   = p;

        return pinfo;
    }

    
    public int getMajorVersion() {
        return HsqlDatabaseProperties.MAJOR;
    }

    
    public int getMinorVersion() {
        return HsqlDatabaseProperties.MINOR;
    }

    
    public boolean jdbcCompliant() {
        return true;
    }

    

    

    public java.util.logging
            .Logger getParentLogger() throws java.sql
                .SQLFeatureNotSupportedException {
        throw (java.sql.SQLFeatureNotSupportedException) Util.notSupported();
    }


    public static JDBCDriver driverInstance;

    static {
        try {
            driverInstance = new JDBCDriver();

            DriverManager.registerDriver(driverInstance);
        } catch (Exception e) {
        }
    }

    


    public final ThreadLocal<JDBCConnection> threadConnection =
        new ThreadLocal<JDBCConnection>();





}
