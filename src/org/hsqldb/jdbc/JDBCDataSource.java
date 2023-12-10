package org.hsqldb.jdbc;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Wrapper;
import java.util.Properties;
import javax.naming.NamingException;
import javax.naming.Reference;
import javax.naming.Referenceable;
import javax.naming.StringRefAddr;
import javax.sql.DataSource;
@SuppressWarnings("serial")
public class JDBCDataSource extends JDBCCommonDataSource implements DataSource,
        Serializable, Referenceable
, Wrapper
{
    public Connection getConnection() throws SQLException {
        if (url == null) {
            throw Util.nullArgument("url");
        }
        if (connectionProps == null) {
            if (user == null) {
                throw Util.invalidArgument("user");
            }
            if (password == null) {
                throw Util.invalidArgument("password");
            }
            return getConnection(user, password);
        }
        if (connectionProps == null) {
            return getConnection(url, new Properties());
        }
        return getConnection(url, connectionProps);
    }
    public Connection getConnection(String username,
                                    String password) throws SQLException {
        if (username == null) {
            throw Util.invalidArgument("user");
        }
        if (password == null) {
            throw Util.invalidArgument("password");
        }
        Properties props = new Properties();
        props.setProperty("user", username);
        props.setProperty("password", password);
        props.setProperty("loginTimeout", Integer.toString(loginTimeout));
        return getConnection(url, props);
    }
    private Connection getConnection(String url,
                                     Properties props) throws SQLException {
        if (!url.startsWith("jdbc:hsqldb:")) {
            url = "jdbc:hsqldb:" + url;
        }
        return JDBCDriver.getConnection(url, props);
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
    public Reference getReference() throws NamingException {
        String    cname = "org.hsqldb.jdbc.JDBCDataSourceFactory";
        Reference ref   = new Reference(getClass().getName(), cname, null);
        ref.add(new StringRefAddr("database", getDatabase()));
        ref.add(new StringRefAddr("user", getUser()));
        ref.add(new StringRefAddr("password", password));
        ref.add(new StringRefAddr("loginTimeout",
                                  Integer.toString(loginTimeout)));
        return ref;
    }
    public JDBCDataSource() {
    }
}