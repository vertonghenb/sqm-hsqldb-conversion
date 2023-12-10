package org.hsqldb.jdbc.pool;
import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.sql.PooledConnection;
import javax.sql.StatementEventListener;
import org.hsqldb.jdbc.JDBCConnection;
import org.hsqldb.jdbc.JDBCConnectionEventListener;
import org.hsqldb.lib.OrderedHashSet;
public class JDBCPooledConnection
implements PooledConnection, JDBCConnectionEventListener {
    synchronized public Connection getConnection() throws SQLException {
        if (isInUse) {
            throw new SQLException("Connection in use");
        }
        isInUse = true;
        userConnection = new JDBCConnection(connection, this);
        return userConnection;
    }
    public void close() throws SQLException {
        if (connection != null) {
            connection.closeFully();
            this.connection = null;
        }
    }
    public void addConnectionEventListener(ConnectionEventListener listener) {
        listeners.add(listener);
    }
    public void removeConnectionEventListener(
            ConnectionEventListener listener) {
        listeners.remove(listener);
    }
    public void addStatementEventListener(StatementEventListener listener) {}
    public void removeStatementEventListener(
            StatementEventListener listener) {}
    synchronized public void connectionClosed() {
        ConnectionEvent event = new ConnectionEvent(this);
        userConnection = null;
        release();
        for (int i = 0; i < listeners.size(); i++) {
            ConnectionEventListener connectionEventListener =
                (ConnectionEventListener) listeners.get(i);
            connectionEventListener.connectionClosed(event);
        }
    }
    synchronized public void connectionErrorOccured(SQLException e) {
        ConnectionEvent event = new ConnectionEvent(this, e);
        release();
        for (int i = 0; i < listeners.size(); i++) {
            ConnectionEventListener connectionEventListener =
                (ConnectionEventListener) listeners.get(i);
            connectionEventListener.connectionErrorOccurred(event);
        }
    }
    synchronized public boolean isInUse() {
        return isInUse;
    }
    synchronized public void release() {
        if (userConnection != null) {
            try {
                userConnection.close();
            } catch (SQLException e) {
            }
        }
        try {
            connection.reset();
        } catch (SQLException e) {
        }
        isInUse = false;
    }
    protected OrderedHashSet listeners = new OrderedHashSet();
    protected JDBCConnection connection;
    protected JDBCConnection userConnection;
    protected boolean        isInUse;
    public JDBCPooledConnection(JDBCConnection connection) {
        this.connection = connection;
    }
}