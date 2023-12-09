


package org.hsqldb.jdbc.pool;

import java.io.Serializable;
import java.sql.SQLException;

import javax.naming.NamingException;
import javax.naming.Reference;
import javax.naming.Referenceable;
import javax.naming.StringRefAddr;


import javax.sql.CommonDataSource;


import javax.sql.XAConnection;
import javax.sql.XADataSource;
import javax.transaction.xa.Xid;

import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.jdbc.JDBCCommonDataSource;
import org.hsqldb.jdbc.JDBCConnection;
import org.hsqldb.jdbc.JDBCDriver;
import org.hsqldb.jdbc.Util;
import org.hsqldb.lib.HashMap;
import org.hsqldb.lib.HashSet;
import org.hsqldb.lib.Iterator;




public class JDBCXADataSource extends JDBCCommonDataSource
implements XADataSource, Serializable, Referenceable


, CommonDataSource


{

    
    public XAConnection getXAConnection() throws SQLException {

        

        
        JDBCConnection connection = (JDBCConnection) JDBCDriver.getConnection(url, connectionProps);
        JDBCXAConnection xaConnection = new JDBCXAConnection(this, connection);

        return xaConnection;
    }

    
    public XAConnection getXAConnection(String user,
                                        String password) throws SQLException {

        if (user == null || password == null) {
            throw Util.nullArgument();
        }

        if (user.equals(this.user) && password.equals(this.password)) {
            return getXAConnection();
        }

        throw Util.sqlException(Error.error(ErrorCode.X_28000));
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

    
    private HashMap resources = new HashMap();

    public void addResource(Xid xid, JDBCXAResource xaResource) {
        resources.put(xid, xaResource);
    }

    public JDBCXADataSource() throws SQLException {

        
    }

    public JDBCXAResource removeResource(Xid xid) {
        return (JDBCXAResource) resources.remove(xid);
    }

    
    Xid[] getPreparedXids() {

        Iterator it = resources.keySet().iterator();
        Xid      curXid;
        HashSet  preparedSet = new HashSet();

        while (it.hasNext()) {
            curXid = (Xid) it.next();

            if (((JDBCXAResource) resources.get(curXid)).state
                    == JDBCXAResource.XA_STATE_PREPARED) {
                preparedSet.add(curXid);
            }
        }

        return (Xid[]) preparedSet.toArray(new Xid[0]);
    }

    
    JDBCXAResource getResource(Xid xid) {
        return (JDBCXAResource) resources.get(xid);
    }
}
