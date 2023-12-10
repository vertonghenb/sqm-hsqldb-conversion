package org.hsqldb.jdbc;
import java.sql.SQLException;
public interface JDBCConnectionEventListener {
    void connectionClosed();
    void connectionErrorOccured(SQLException e);
}