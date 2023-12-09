


package org.hsqldb.jdbc.pool;

import org.hsqldb.jdbc.JDBCConnection;

import java.sql.SQLException;
import java.sql.Savepoint;




public class JDBCXAConnectionWrapper extends JDBCConnection {

    

    
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        validateNotWithinTransaction();
        super.setAutoCommit(autoCommit);
    }

    
    public void commit() throws SQLException {
        validateNotWithinTransaction();
        super.commit();
    }

    
    public void rollback() throws SQLException {
        validateNotWithinTransaction();
        super.rollback();
    }

    
    public void rollback(Savepoint savepoint) throws SQLException {
        validateNotWithinTransaction();
        super.rollback(savepoint);
    }

    
    public Savepoint setSavepoint() throws SQLException {

        validateNotWithinTransaction();

        return super.setSavepoint();
    }

    
    public Savepoint setSavepoint(String name) throws SQLException {

        validateNotWithinTransaction();

        return super.setSavepoint(name);
    }

    
    public void setTransactionIsolation(int level) throws SQLException {
        validateNotWithinTransaction();
        super.setTransactionIsolation(level);
    }

    
    private JDBCXAResource xaResource;

    public JDBCXAConnectionWrapper(JDBCXAResource xaResource,
                                   JDBCConnection databaseConnection)
                                   throws SQLException {
        
        
        
        
        
        
        
        
        
        
        
        
        super(databaseConnection, null);

        xaResource.setConnection(this);

        this.xaResource = xaResource;
    }

    
    private void validateNotWithinTransaction() throws SQLException {

        if (xaResource.withinGlobalTransaction()) {
            throw new SQLException(
                "Method prohibited within a global transaction");
        }
    }
}
