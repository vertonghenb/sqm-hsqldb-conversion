package org.hsqldb.server;
import java.util.Map;
import org.hsqldb.Session;
import org.hsqldb.result.Result;
import org.hsqldb.result.ResultConstants;
class StatementPortal {
    public Object[] parameters;
    public Result bindResult, ackResult;
    public String lcQuery;
    public String handle;
    private Map containingMap;
    private Session session;
    public StatementPortal(String handle,
    OdbcPreparedStatement odbcPs, Map containingMap)
    throws RecoverableOdbcFailure {
        this(handle, odbcPs, new Object[0], containingMap);
    }
    public StatementPortal(String handle, OdbcPreparedStatement odbcPs,
    Object[] paramObjs, Map containingMap) throws RecoverableOdbcFailure {
        this.handle = handle;
        lcQuery = odbcPs.query.toLowerCase();
        ackResult = odbcPs.ackResult;
        session = odbcPs.session;
        this.containingMap = containingMap;
        bindResult = Result.newPreparedExecuteRequest(
            odbcPs.ackResult.parameterMetaData.getParameterTypes(),
            odbcPs.ackResult.getStatementID());
        switch (bindResult.getType()) {
            case ResultConstants.EXECUTE:
                break;
            case ResultConstants.ERROR:
                throw new RecoverableOdbcFailure(bindResult);
            default:
                throw new RecoverableOdbcFailure(
                    "Output Result from seconary Statement prep is of "
                    + "unexpected type: " + bindResult.getType());
        }
        if (paramObjs.length < 1) {
            parameters = new Object[0];
        } else {
            org.hsqldb.result.ResultMetaData pmd =
                odbcPs.ackResult.parameterMetaData;
            if (pmd == null) {
                throw new RecoverableOdbcFailure("No metadata for Result ack");
            }
            org.hsqldb.types.Type[] paramTypes = pmd.getParameterTypes();
            if (paramTypes.length != paramObjs.length) {
                throw new RecoverableOdbcFailure(null,
                    "Client didn't specify all " + paramTypes.length
                    + " parameters (" + paramObjs.length + ')', "08P01");
            }
            parameters = new Object[paramObjs.length];
            try {
                for (int i = 0; i < parameters.length; i++) {
                    parameters[i] = (paramObjs[i] instanceof String)
                        ? PgType.getPgType(paramTypes[i], true)
                            .getParameter((String) paramObjs[i], session)
                        : paramObjs[i];
                }
            } catch (java.sql.SQLException se) {
                throw new RecoverableOdbcFailure("Typing failure: " + se);
            }
        }
        containingMap.put(handle, this);
    }
    public void close() {
        containingMap.remove(handle);
    }
}