package org.hsqldb.jdbc.pool;
import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.XAConnection;
import javax.transaction.xa.XAResource;
import org.hsqldb.jdbc.JDBCConnection;
public class JDBCXAConnection extends JDBCPooledConnection implements XAConnection {
    JDBCXAResource xaResource;
    public JDBCXAConnection(JDBCXADataSource dataSource, JDBCConnection connection) {
        super(connection);
        xaResource = new JDBCXAResource(dataSource, connection);
    }
    public XAResource getXAResource() throws SQLException {
        return xaResource;
    }
    public Connection getConnection() throws SQLException {
        if (isInUse) {
            throw new SQLException("Connection in use");
        }
        isInUse = true;
        return new JDBCXAConnectionWrapper(xaResource, connection);
    }
    public void close() throws SQLException {
        super.close();
    }
}