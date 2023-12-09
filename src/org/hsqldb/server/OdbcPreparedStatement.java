


package org.hsqldb.server;

import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.hsqldb.Session;
import org.hsqldb.result.Result;
import org.hsqldb.result.ResultConstants;
import org.hsqldb.result.ResultProperties;

class OdbcPreparedStatement {

    public String  handle, query;
    public Result  ackResult;
    public Session session;
    private Map    containingMap;
    private List   portals = new ArrayList();

    protected OdbcPreparedStatement(OdbcPreparedStatement other) {
        this.handle    = other.handle;
        this.ackResult = other.ackResult;
    }

    
    public OdbcPreparedStatement(String handle, String query,
                                 Map containingMap,
                                 Session session)
                                 throws RecoverableOdbcFailure {

        this.handle        = handle;
        this.query         = query;
        this.containingMap = containingMap;
        this.session       = session;

        Result psResult = Result.newPrepareStatementRequest();

        psResult.setPrepareOrExecuteProperties(
            query, 0, 0, 0, 0,ResultProperties.defaultPropsValue,
            Statement.NO_GENERATED_KEYS, null, null);

        ackResult = session.execute(psResult);

        switch (ackResult.getType()) {

            case ResultConstants.PREPARE_ACK :
                break;

            case ResultConstants.ERROR :
                throw new RecoverableOdbcFailure(ackResult);
            default :
                throw new RecoverableOdbcFailure(
                    "Output Result from Statement prep is of "
                    + "unexpected type: " + ackResult.getType());
        }

        containingMap.put(handle, this);
    }

    
    public void close() {

        
        containingMap.remove(handle);

        while (portals.size() > 0) {
            ((StatementPortal) portals.remove(1)).close();
        }
    }

    
    public void addPortal(StatementPortal portal) {
        portals.add(portal);
    }
}
