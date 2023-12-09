


package org.hsqldb.jdbc.pool;

import javax.transaction.xa.XAResource;
import javax.transaction.xa.XAException;
import javax.transaction.xa.Xid;





import org.hsqldb.jdbc.JDBCConnection;

import java.sql.SQLException;

import org.hsqldb.SessionInterface;
import org.hsqldb.HsqlException;




public class JDBCXAResource implements XAResource {

    
    private JDBCConnection   connection;
    private boolean          originalAutoCommitMode;
    static int               XA_STATE_INITIAL  = 0;
    static int               XA_STATE_STARTED  = 1;
    static int               XA_STATE_ENDED    = 2;
    static int               XA_STATE_PREPARED = 3;
    static int               XA_STATE_DISPOSED = 4;
    int                      state             = XA_STATE_INITIAL;
    private JDBCXADataSource xaDataSource;
    Xid                      xid = null;

    public boolean withinGlobalTransaction() {
        return state == XA_STATE_STARTED;
    }

    
    private void validateXid(Xid xid) throws XAException {

        if (xid == null) {
            throw new XAException("Null Xid");
        }

        if (this.xid == null) {
            throw new XAException(
                "There is no live transaction for this XAResource");
        }

        if (!xid.equals(this.xid)) {
            throw new XAException(
                "Given Xid is not that associated with this XAResource object");
        }
    }

    
    public JDBCXAResource(JDBCXADataSource xaDataSource,
                          JDBCConnection connection) {
        this.connection   = connection;
        this.xaDataSource = xaDataSource;
    }

    JDBCXADataSource getXADataSource() {
        return xaDataSource;
    }

    
    public void commit(Xid xid, boolean onePhase) throws XAException {

        

        JDBCXAResource resource = xaDataSource.getResource(xid);

        if (resource == null) {
            throw new XAException("The XADataSource has no such Xid:  " + xid);
        }

        resource.commitThis(onePhase);
    }

    
    
    public void commitThis(boolean onePhase) throws XAException {

        if (onePhase && state == XA_STATE_PREPARED) {
            throw new XAException(
                "Transaction is in a 2-phase state when 1-phase is requested");
        }

        if ((!onePhase) && state != XA_STATE_PREPARED) {
            throw new XAException("Attempt to do a 2-phase commit when "
                                  + "transaction is not prepared");
        }

        
        
        
        
        try {

            
            connection.commit();
        } catch (SQLException se) {
            throw new XAException(se.toString());
        }

        dispose();
    }

    private void dispose() {

        state = XA_STATE_DISPOSED;

        xaDataSource.removeResource(xid);

        xid = null;
    }

    public void end(Xid xid, int flags) throws XAException {

        validateXid(xid);

        if (state != XA_STATE_STARTED) {
            throw new XAException("Invalid XAResource state");
        }

        
        if (flags == XAResource.TMSUCCESS) {}

        state = XA_STATE_ENDED;

        try {
            connection.setAutoCommit(originalAutoCommitMode);    
        } catch (SQLException se) {
            throw new XAException(se.toString());
        }
    }

    
    public void forget(Xid xid) throws XAException {

        
        validateXid(xid);

        if (state != XA_STATE_PREPARED) {
            throw new XAException(
                "Attempted to forget a XAResource that "
                + "is not in a heuristically completed state");
        }

        dispose();

        state = XA_STATE_INITIAL;
    }

    
    public int getTransactionTimeout() throws XAException {
        throw new XAException("Transaction timeouts not implemented yet");
    }

    
    public boolean isSameRM(XAResource xares) throws XAException {

        if (!(xares instanceof JDBCXAResource)) {
            return false;
        }

        return xaDataSource == ((JDBCXAResource) xares).getXADataSource();
    }

    
    public int prepare(Xid xid) throws XAException {

        JDBCXAResource resource = xaDataSource.getResource(xid);

        if (resource == null) {
            throw new XAException("The XADataSource has no such Xid:  " + xid);
        }

        return resource.prepareThis();
    }

    public int prepareThis() throws XAException {

        

        
        if (state != XA_STATE_ENDED) {
            throw new XAException("Invalid XAResource state");
        }

        try {
            ((SessionInterface) connection).prepareCommit();
        } catch (HsqlException e) {
            state = XA_STATE_PREPARED;  

            throw new XAException(e.getMessage());
        }

        state = XA_STATE_PREPARED;

        return XA_OK;    
    }

    
    public Xid[] recover(int flag) throws XAException {
        return xaDataSource.getPreparedXids();
    }

    
    public void rollback(Xid xid) throws XAException {

        JDBCXAResource resource = xaDataSource.getResource(xid);

        if (resource == null) {
            throw new XAException(
                "The XADataSource has no such Xid in prepared state:  " + xid);
        }

        resource.rollbackThis();
    }

    
    
    public void rollbackThis() throws XAException {

        if (state != XA_STATE_PREPARED) {
            throw new XAException("Invalid XAResource state");
        }

        try {

            
            connection.rollback();    
        } catch (SQLException se) {
            throw new XAException(se.toString());
        }

        dispose();
    }

    
    public boolean setTransactionTimeout(int seconds) throws XAException {
        throw new XAException("Transaction timeouts not implemented yet");
    }

    public void start(Xid xid, int flags) throws XAException {

        

        if (state != XA_STATE_INITIAL && state != XA_STATE_DISPOSED) {
            throw new XAException("Invalid XAResource state");
        }

        if (xaDataSource == null) {
            throw new XAException(
                "JDBCXAResource has not been associated with a XADataSource");
        }

        if (xid == null) {

            
            
            throw new XAException("Null Xid");
        }

        try {
            originalAutoCommitMode = connection.getAutoCommit();    

            connection.setAutoCommit(false);                        
        } catch (SQLException se) {
            throw new XAException(se.toString());
        }

        this.xid = xid;
        state    = XA_STATE_STARTED;

        xaDataSource.addResource(this.xid, this);

        
        
        
    }

    JDBCConnection getConnection() {
        return this.connection;
    }

    void setConnection(JDBCConnection userConnection) {
        connection = userConnection;
    }
}
