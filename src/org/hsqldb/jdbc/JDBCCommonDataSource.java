


package org.hsqldb.jdbc;

import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.Properties;


import javax.sql.CommonDataSource;





public abstract class JDBCCommonDataSource implements CommonDataSource {






    
    public PrintWriter getLogWriter() throws SQLException {
        return logWriter;
    }

    
    public void setLogWriter(java.io.PrintWriter out) throws SQLException {
        logWriter = out;
    }

    
    public void setLoginTimeout(int seconds) throws SQLException {

        loginTimeout = seconds;

        connectionProps.setProperty("loginTimeout",
                                    Integer.toString(loginTimeout));
    }

    
    public int getLoginTimeout() throws SQLException {
        return loginTimeout;
    }

    

    
    public String getDescription() {
        return description;
    }

    
    public String getDataSourceName() {
        return dataSourceName;
    }

    
    public String getNetworkProtocol() {
        return networkProtocol;
    }

    
    public String getServerName() {
        return serverName;
    }

    
    public String getDatabaseName() {
        return url;
    }

    
    public String getDatabase() {
        return url;
    }

    
    public String getUrl() {
        return url;
    }

    
    public String getUser() {
        return user;
    }

    
    public void setDescription(String description) {
        this.description = description;
    }

    
    public void setDataSourceName(String dataSourceName) {
        this.dataSourceName = dataSourceName;
    }

    
    public void setNetworkProtocol(String networkProtocol) {
        this.networkProtocol = networkProtocol;
    }

    
    public void setServerName(String serverName) {
        this.serverName = serverName;
    }

    
    public void setDatabaseName(String databaseName) {
        this.url = databaseName;
    }

    
    public void setDatabase(String database) {
        this.url = database;
    }

    
    public void setUrl(String url) {
        this.url = url;
    }

    
    public void setPassword(String password) {

        this.password = password;

        connectionProps.setProperty("password", password);
    }

    
    public void setUser(String user) {

        this.user = user;

        connectionProps.setProperty("user", user);
    }

    
    public void setProperties(Properties props) {

        connectionProps = (props == null) ? new Properties()
                : (Properties) props.clone();

        if (user != null) {
            props.setProperty("user", user);
        }

        if (password != null) {
            props.setProperty("password", password);
        }

        if (loginTimeout != 0) {
            props.setProperty("loginTimeout", Integer.toString(loginTimeout));
        }
    }

    

    

    public java.util.logging
            .Logger getParentLogger() throws java.sql
                .SQLFeatureNotSupportedException {
        throw (java.sql.SQLFeatureNotSupportedException) Util.notSupported();
    }


    
    protected Properties connectionProps = new Properties();

    
    protected String description = null;

    
    protected String dataSourceName = null;

    
    protected String serverName = null;

    
    protected String networkProtocol = null;

    
    protected int loginTimeout = 0;

    
    protected transient PrintWriter logWriter;

    
    protected String user = null;

    
    protected String password = null;

    
    protected String url = null;
}
