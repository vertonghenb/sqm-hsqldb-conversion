


package org.hsqldb.jdbc;

import java.sql.SQLException;
import java.sql.Savepoint;







public class JDBCSavepoint implements Savepoint {

    int            id;
    String         name;
    JDBCConnection connection;

    JDBCSavepoint(String name, JDBCConnection conn) throws SQLException {

        if (name == null) {
            throw Util.nullArgument("name");
        }

        if (conn == null) {
            throw Util.nullArgument("conn");
        }

        this.name       = name;
        this.id         = -1;
        this.connection = conn;
    }

    JDBCSavepoint(JDBCConnection conn) throws SQLException {

        if (conn == null) {
            throw Util.nullArgument("conn");
        }

        this.id         = conn.getSavepointID();
        this.name       = "SYSTEM_SAVEPOINT_" + id;
        this.connection = conn;
    }

    
    public int getSavepointId() throws SQLException {

        if (id != -1) {
            return id;
        }

        throw Util.notSupported();
    }

    
    public String getSavepointName() throws SQLException {

        if (id == -1) {
            return name;
        }

        throw Util.notSupported();
    }

    public String toString() {
        return super.toString() + "[name=" + name + "]";
    }
}
