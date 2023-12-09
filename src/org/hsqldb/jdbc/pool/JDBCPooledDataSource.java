


package org.hsqldb.jdbc.pool;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.Properties;

import javax.naming.NamingException;
import javax.naming.Reference;
import javax.naming.Referenceable;
import javax.naming.StringRefAddr;
import javax.sql.ConnectionPoolDataSource;
import javax.sql.PooledConnection;


import javax.sql.CommonDataSource;


import org.hsqldb.jdbc.JDBCCommonDataSource;
import org.hsqldb.jdbc.JDBCConnection;
import org.hsqldb.jdbc.JDBCDriver;


public class JDBCPooledDataSource extends JDBCCommonDataSource
implements ConnectionPoolDataSource, Serializable, Referenceable


, CommonDataSource


{

    public PooledConnection getPooledConnection() throws SQLException {

        JDBCConnection connection =
            (JDBCConnection) JDBCDriver.getConnection(url, connectionProps);

        return new JDBCPooledConnection(connection);
    }

    public PooledConnection getPooledConnection(String user,
            String password) throws SQLException {

        Properties props = new Properties();

        props.setProperty("user", user);
        props.setProperty("password", password);

        JDBCConnection connection =
            (JDBCConnection) JDBCDriver.getConnection(url, props);

        return new JDBCPooledConnection(connection);
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
}
