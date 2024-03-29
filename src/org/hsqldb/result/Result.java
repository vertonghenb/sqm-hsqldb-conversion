package org.hsqldb.result;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.StringReader;
import org.hsqldb.ColumnBase;
import org.hsqldb.Database;
import org.hsqldb.HsqlException;
import org.hsqldb.Session;
import org.hsqldb.SessionInterface;
import org.hsqldb.SqlInvariants;
import org.hsqldb.Statement;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.lib.DataOutputStream;
import org.hsqldb.navigator.RowSetNavigator;
import org.hsqldb.navigator.RowSetNavigatorClient;
import org.hsqldb.rowio.RowInputBinary;
import org.hsqldb.rowio.RowOutputInterface;
import org.hsqldb.store.ValuePool;
import org.hsqldb.types.Type;
public class Result {
    public static final ResultMetaData sessionAttributesMetaData =
        ResultMetaData.newResultMetaData(SessionInterface.INFO_LIMIT);
    static {
        SqlInvariants.isSystemSchemaName("SYSTEM");
        for (int i = 0; i < Session.INFO_LIMIT; i++) {
            sessionAttributesMetaData.columns[i] = new ColumnBase(null, null,
                    null, null);
        }
        sessionAttributesMetaData.columns[Session.INFO_ID].setType(
            Type.SQL_INTEGER);
        sessionAttributesMetaData.columns[Session.INFO_INTEGER].setType(
            Type.SQL_INTEGER);
        sessionAttributesMetaData.columns[Session.INFO_BOOLEAN].setType(
            Type.SQL_BOOLEAN);
        sessionAttributesMetaData.columns[Session.INFO_VARCHAR].setType(
            Type.SQL_VARCHAR);
        sessionAttributesMetaData.prepareData();
    }
    private static final ResultMetaData emptyMeta =
        ResultMetaData.newResultMetaData(0);
    public static final Result emptyGeneratedResult =
        Result.newDataResult(emptyMeta);
    public static final Result updateZeroResult = newUpdateCountResult(0);
    public static final Result updateOneResult  = newUpdateCountResult(1);
    public byte mode;
    int databaseID;
    long sessionID;
    private long id;
    private String databaseName;
    private String mainString;
    private String subString;
    private String zoneString;
    int errorCode;
    private HsqlException exception;
    long statementID;
    int statementReturnType;
    public int updateCount;
    private int fetchSize;
    private Result chainedResult;
    private int lobCount;
    ResultLob   lobResults;
    public ResultMetaData metaData;
    public ResultMetaData parameterMetaData;
    public ResultMetaData generatedMetaData;
    public int rsProperties;
    public int queryTimeout;
    int generateKeys;
    public Object valueData;
    public Statement statement;
    Result(int mode) {
        this.mode = (byte) mode;
    }
    public Result(int mode, int count) {
        this.mode   = (byte) mode;
        updateCount = count;
    }
    public static Result newResult(RowSetNavigator nav) {
        Result result = new Result(ResultConstants.DATA);
        result.navigator = nav;
        return result;
    }
    public static Result newResult(int type) {
        RowSetNavigator navigator = null;
        Result          result    = null;
        switch (type) {
            case ResultConstants.CALL_RESPONSE :
            case ResultConstants.EXECUTE :
            case ResultConstants.UPDATE_RESULT :
                break;
            case ResultConstants.BATCHEXECUTE :
            case ResultConstants.BATCHEXECDIRECT :
                navigator = new RowSetNavigatorClient(4);
                break;
            case ResultConstants.SETSESSIONATTR :
            case ResultConstants.PARAM_METADATA :
                navigator = new RowSetNavigatorClient(1);
                break;
            case ResultConstants.BATCHEXECRESPONSE :
                navigator = new RowSetNavigatorClient(4);
                break;
            case ResultConstants.DATA :
            case ResultConstants.DATAHEAD :
            case ResultConstants.DATAROWS :
            case ResultConstants.GENERATED :
                break;
            case ResultConstants.LARGE_OBJECT_OP :
                throw Error.runtimeError(ErrorCode.U_S0500, "Result");
            default :
        }
        result           = new Result(type);
        result.navigator = navigator;
        return result;
    }
    public static Result newResult(DataInput dataInput,
                                   RowInputBinary in)
                                   throws IOException, HsqlException {
        return newResult(null, dataInput.readByte(), dataInput, in);
    }
    public static Result newResult(Session session, int mode,
                                   DataInput dataInput,
                                   RowInputBinary in)
                                   throws IOException, HsqlException {
        try {
            if (mode == ResultConstants.LARGE_OBJECT_OP) {
                return ResultLob.newLob(dataInput, false);
            }
            Result result = newResult(session, dataInput, in, mode);
            return result;
        } catch (IOException e) {
            throw Error.error(ErrorCode.X_08000);
        }
    }
    public void readAdditionalResults(SessionInterface session,
                                      DataInputStream inputStream,
                                      RowInputBinary in)
                                      throws IOException, HsqlException {
        Result currentResult = this;
        setSession(session);
        while (true) {
            int addedResultMode = inputStream.readByte();
            if (addedResultMode == ResultConstants.NONE) {
                return;
            }
            currentResult = newResult(null, inputStream, in, addedResultMode);
            addChainedResult(currentResult);
        }
    }
    public void readLobResults(SessionInterface session,
                               DataInputStream inputStream,
                               RowInputBinary in)
                               throws IOException, HsqlException {
        Result  currentResult = this;
        boolean hasLob        = false;
        setSession(session);
        while (true) {
            int addedResultMode = inputStream.readByte();
            if (addedResultMode == ResultConstants.LARGE_OBJECT_OP) {
                ResultLob resultLob = ResultLob.newLob(inputStream, false);
                if (session instanceof Session) {
                    ((Session) session).allocateResultLob(resultLob,
                                                          inputStream);
                } else {
                    currentResult.addLobResult(resultLob);
                }
                hasLob = true;
                continue;
            } else if (addedResultMode == ResultConstants.NONE) {
                break;
            } else {
                throw Error.runtimeError(ErrorCode.U_S0500, "Result");
            }
        }
        if (hasLob) {
            ((Session) session).registerResultLobs(currentResult);
        }
    }
    private static Result newResult(Session session, DataInput dataInput,
                                    RowInputBinary in,
                                    int mode)
                                    throws IOException, HsqlException {
        Result result = newResult(mode);
        int    length = dataInput.readInt();
        in.resetRow(0, length);
        byte[]    byteArray = in.getBuffer();
        final int offset    = 4;
        dataInput.readFully(byteArray, offset, length - offset);
        switch (mode) {
            case ResultConstants.GETSESSIONATTR :
                result.statementReturnType = in.readByte();
                break;
            case ResultConstants.DISCONNECT :
            case ResultConstants.RESETSESSION :
            case ResultConstants.STARTTRAN :
                break;
            case ResultConstants.PREPARE :
                result.setStatementType(in.readByte());
                result.mainString   = in.readString();
                result.rsProperties = in.readByte();
                result.generateKeys = in.readByte();
                if (result.generateKeys == ResultConstants
                        .RETURN_GENERATED_KEYS_COL_NAMES || result
                        .generateKeys == ResultConstants
                        .RETURN_GENERATED_KEYS_COL_INDEXES) {
                    result.generatedMetaData = new ResultMetaData(in);
                }
                break;
            case ResultConstants.CLOSE_RESULT :
                result.id = in.readLong();
                break;
            case ResultConstants.FREESTMT :
                result.statementID = in.readLong();
                break;
            case ResultConstants.EXECDIRECT :
                result.updateCount         = in.readInt();
                result.fetchSize           = in.readInt();
                result.statementReturnType = in.readByte();
                result.mainString          = in.readString();
                result.rsProperties        = in.readByte();
                result.queryTimeout        = in.readShort();
                result.generateKeys        = in.readByte();
                if (result.generateKeys == ResultConstants
                        .RETURN_GENERATED_KEYS_COL_NAMES || result
                        .generateKeys == ResultConstants
                        .RETURN_GENERATED_KEYS_COL_INDEXES) {
                    result.generatedMetaData = new ResultMetaData(in);
                }
                break;
            case ResultConstants.CONNECT :
                result.databaseName = in.readString();
                result.mainString   = in.readString();
                result.subString    = in.readString();
                result.zoneString   = in.readString();
                result.updateCount  = in.readInt();
                break;
            case ResultConstants.ERROR :
            case ResultConstants.WARNING :
                result.mainString = in.readString();
                result.subString  = in.readString();
                result.errorCode  = in.readInt();
                break;
            case ResultConstants.CONNECTACKNOWLEDGE :
                result.databaseID   = in.readInt();
                result.sessionID    = in.readLong();
                result.databaseName = in.readString();
                result.mainString   = in.readString();
                break;
            case ResultConstants.UPDATECOUNT :
                result.updateCount = in.readInt();
                break;
            case ResultConstants.ENDTRAN : {
                int type = in.readInt();
                result.setActionType(type);                     
                switch (type) {
                    case ResultConstants.TX_SAVEPOINT_NAME_RELEASE :
                    case ResultConstants.TX_SAVEPOINT_NAME_ROLLBACK :
                        result.mainString = in.readString();    
                        break;
                    case ResultConstants.TX_COMMIT :
                    case ResultConstants.TX_ROLLBACK :
                    case ResultConstants.TX_COMMIT_AND_CHAIN :
                    case ResultConstants.TX_ROLLBACK_AND_CHAIN :
                        break;
                    default :
                        throw Error.runtimeError(ErrorCode.U_S0500, "Result");
                }
                break;
            }
            case ResultConstants.SETCONNECTATTR : {
                int type = in.readInt();                        
                result.setConnectionAttrType(type);
                switch (type) {
                    case ResultConstants.SQL_ATTR_SAVEPOINT_NAME :
                        result.mainString = in.readString();    
                        break;
                    default :
                        throw Error.runtimeError(ErrorCode.U_S0500, "Result");
                }
                break;
            }
            case ResultConstants.PREPARE_ACK :
                result.statementReturnType = in.readByte();
                result.statementID         = in.readLong();
                result.rsProperties        = in.readByte();
                result.metaData            = new ResultMetaData(in);
                result.parameterMetaData   = new ResultMetaData(in);
                break;
            case ResultConstants.CALL_RESPONSE :
                result.updateCount         = in.readInt();
                result.fetchSize           = in.readInt();
                result.statementID         = in.readLong();
                result.statementReturnType = in.readByte();
                result.rsProperties        = in.readByte();
                result.metaData            = new ResultMetaData(in);
                result.valueData           = readSimple(in, result.metaData);
                break;
            case ResultConstants.EXECUTE :
                result.updateCount  = in.readInt();
                result.fetchSize    = in.readInt();
                result.statementID  = in.readLong();
                result.rsProperties = in.readByte();
                result.queryTimeout = in.readShort();
                Statement statement =
                    session.statementManager.getStatement(session,
                        result.statementID);
                if (statement == null) {
                    result.mode      = ResultConstants.EXECUTE_INVALID;
                    result.valueData = ValuePool.emptyObjectArray;
                    break;
                }
                result.statement = statement;
                result.metaData  = result.statement.getParametersMetaData();
                result.valueData = readSimple(in, result.metaData);
                break;
            case ResultConstants.UPDATE_RESULT : {
                result.id = in.readLong();
                int type = in.readInt();
                result.setActionType(type);
                result.metaData  = new ResultMetaData(in);
                result.valueData = readSimple(in, result.metaData);
                break;
            }
            case ResultConstants.BATCHEXECRESPONSE :
            case ResultConstants.BATCHEXECUTE :
            case ResultConstants.BATCHEXECDIRECT :
            case ResultConstants.SETSESSIONATTR : {
                result.updateCount  = in.readInt();
                result.fetchSize    = in.readInt();
                result.statementID  = in.readLong();
                result.queryTimeout = in.readShort();
                result.metaData     = new ResultMetaData(in);
                result.navigator.readSimple(in, result.metaData);
                break;
            }
            case ResultConstants.PARAM_METADATA : {
                result.metaData = new ResultMetaData(in);
                result.navigator.read(in, result.metaData);
                break;
            }
            case ResultConstants.REQUESTDATA : {
                result.id          = in.readLong();
                result.updateCount = in.readInt();
                result.fetchSize   = in.readInt();
                break;
            }
            case ResultConstants.DATAHEAD :
            case ResultConstants.DATA :
            case ResultConstants.GENERATED : {
                result.id           = in.readLong();
                result.updateCount  = in.readInt();
                result.fetchSize    = in.readInt();
                result.rsProperties = in.readByte();
                result.metaData     = new ResultMetaData(in);
                result.navigator    = new RowSetNavigatorClient();
                result.navigator.read(in, result.metaData);
                break;
            }
            case ResultConstants.DATAROWS : {
                result.metaData  = new ResultMetaData(in);
                result.navigator = new RowSetNavigatorClient();
                result.navigator.read(in, result.metaData);
                break;
            }
            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "Result");
        }
        return result;
    }
    public static Result newPSMResult(int type, String label, Object value) {
        Result result = newResult(ResultConstants.VALUE);
        result.errorCode  = type;
        result.mainString = label;
        result.valueData  = value;
        return result;
    }
    public static Result newPSMResult(Object value) {
        Result result = newResult(ResultConstants.VALUE);
        result.valueData = value;
        return result;
    }
    public static Result newPrepareStatementRequest() {
        return newResult(ResultConstants.PREPARE);
    }
    public static Result newPreparedExecuteRequest(Type[] types,
            long statementId) {
        Result result = newResult(ResultConstants.EXECUTE);
        result.metaData    = ResultMetaData.newSimpleResultMetaData(types);
        result.statementID = statementId;
        result.valueData   = ValuePool.emptyObjectArray;
        return result;
    }
    public static Result newCallResponse(Type[] types, long statementId,
                                         Object[] values) {
        Result result = newResult(ResultConstants.CALL_RESPONSE);
        result.metaData    = ResultMetaData.newSimpleResultMetaData(types);
        result.statementID = statementId;
        result.valueData   = values;
        return result;
    }
    public static Result newUpdateResultRequest(Type[] types, long id) {
        Result result = newResult(ResultConstants.UPDATE_RESULT);
        result.metaData  = ResultMetaData.newUpdateResultMetaData(types);
        result.id        = id;
        result.valueData = new Object[]{};
        return result;
    }
    public void setPreparedResultUpdateProperties(Object[] parameterValues) {
        valueData = parameterValues;
    }
    public void setPreparedExecuteProperties(Object[] parameterValues,
            int maxRows, int fetchSize, int resultProps) {
        mode              = ResultConstants.EXECUTE;
        valueData         = parameterValues;
        updateCount       = maxRows;
        this.fetchSize    = fetchSize;
        this.rsProperties = resultProps;
    }
    public void setBatchedPreparedExecuteRequest() {
        mode = ResultConstants.BATCHEXECUTE;
        if (navigator == null) {
            navigator = new RowSetNavigatorClient(4);
        } else {
            navigator.clear();
        }
        updateCount    = 0;
        this.fetchSize = 0;
    }
    public void addBatchedPreparedExecuteRequest(Object[] parameterValues) {
        ((RowSetNavigatorClient) navigator).add(parameterValues);
    }
    public static Result newBatchedExecuteRequest() {
        Type[] types  = new Type[]{ Type.SQL_VARCHAR };
        Result result = newResult(ResultConstants.BATCHEXECDIRECT);
        result.metaData = ResultMetaData.newSimpleResultMetaData(types);
        return result;
    }
    public static Result newBatchedExecuteResponse(int[] updateCounts,
            Result generatedResult, Result e) {
        Result result = newResult(ResultConstants.BATCHEXECRESPONSE);
        result.addChainedResult(generatedResult);
        result.addChainedResult(e);
        Type[] types = new Type[]{ Type.SQL_INTEGER };
        result.metaData = ResultMetaData.newSimpleResultMetaData(types);
        Object[][] table = new Object[updateCounts.length][];
        for (int i = 0; i < updateCounts.length; i++) {
            table[i] = new Object[]{ ValuePool.getInt(updateCounts[i]) };
        }
        ((RowSetNavigatorClient) result.navigator).setData(table);
        return result;
    }
    public static Result newResetSessionRequest() {
        Result result = newResult(ResultConstants.RESETSESSION);
        return result;
    }
    public static Result newConnectionAttemptRequest(String user,
            String password, String database, String zoneString,
            int timeZoneSeconds) {
        Result result = newResult(ResultConstants.CONNECT);
        result.mainString   = user;
        result.subString    = password;
        result.zoneString   = zoneString;
        result.databaseName = database;
        result.updateCount  = timeZoneSeconds;
        return result;
    }
    public static Result newConnectionAcknowledgeResponse(Database database,
            long sessionID, int databaseID) {
        Result result = newResult(ResultConstants.CONNECTACKNOWLEDGE);
        result.sessionID    = sessionID;
        result.databaseID   = databaseID;
        result.databaseName = database.getUniqueName();
        result.mainString =
            database.getProperties().getClientPropertiesAsString();
        return result;
    }
    public static Result newUpdateZeroResult() {
        return new Result(ResultConstants.UPDATECOUNT, 0);
    }
    public static Result newUpdateCountResult(int count) {
        return new Result(ResultConstants.UPDATECOUNT, count);
    }
    public static Result newUpdateCountResult(ResultMetaData meta, int count) {
        Result result     = newResult(ResultConstants.UPDATECOUNT);
        Result dataResult = newGeneratedDataResult(meta);
        result.updateCount = count;
        result.addChainedResult(dataResult);
        return result;
    }
    public static Result newSingleColumnResult(ResultMetaData meta) {
        Result result = newResult(ResultConstants.DATA);
        result.metaData  = meta;
        result.navigator = new RowSetNavigatorClient();
        return result;
    }
    public static Result newSingleColumnResult(String colName) {
        Result result = newResult(ResultConstants.DATA);
        result.metaData  = ResultMetaData.newSingleColumnMetaData(colName);
        result.navigator = new RowSetNavigatorClient(8);
        return result;
    }
    public static Result newSingleColumnStringResult(String colName,
            String contents) {
        Result result = Result.newSingleColumnResult(colName);
        LineNumberReader lnr =
            new LineNumberReader(new StringReader(contents));
        while (true) {
            String line = null;
            try {
                line = lnr.readLine();
            } catch (Exception e) {}
            if (line == null) {
                break;
            }
            result.getNavigator().add(new Object[]{ line });
        }
        return result;
    }
    public static Result newPrepareResponse(Statement statement) {
        Result r = newResult(ResultConstants.PREPARE_ACK);
        r.statement   = statement;
        r.statementID = statement.getID();
        int csType = statement.getType();
        r.statementReturnType = statement.getStatementReturnType();
        r.metaData            = statement.getResultMetaData();
        r.parameterMetaData   = statement.getParametersMetaData();
        return r;
    }
    public static Result newFreeStmtRequest(long statementID) {
        Result r = newResult(ResultConstants.FREESTMT);
        r.statementID = statementID;
        return r;
    }
    public static Result newExecuteDirectRequest() {
        return newResult(ResultConstants.EXECDIRECT);
    }
    public void setPrepareOrExecuteProperties(String sql, int maxRows,
            int fetchSize, int statementReturnType, int timeout,
            int resultSetProperties, int keyMode, int[] generatedIndexes,
            String[] generatedNames) {
        mainString               = sql;
        updateCount              = maxRows;
        this.fetchSize           = fetchSize;
        this.statementReturnType = statementReturnType;
        this.queryTimeout        = timeout;
        rsProperties             = resultSetProperties;
        generateKeys             = keyMode;
        generatedMetaData =
            ResultMetaData.newGeneratedColumnsMetaData(generatedIndexes,
                generatedNames);
    }
    public static Result newSetSavepointRequest(String name) {
        Result result;
        result = newResult(ResultConstants.SETCONNECTATTR);
        result.setConnectionAttrType(ResultConstants.SQL_ATTR_SAVEPOINT_NAME);
        result.setMainString(name);
        return result;
    }
    public static Result newRequestDataResult(long id, int offset, int count) {
        Result result = newResult(ResultConstants.REQUESTDATA);
        result.id          = id;
        result.updateCount = offset;
        result.fetchSize   = count;
        return result;
    }
    public static Result newDataResult(ResultMetaData md) {
        Result result = newResult(ResultConstants.DATA);
        result.navigator = new RowSetNavigatorClient();
        result.metaData  = md;
        return result;
    }
    public static Result newGeneratedDataResult(ResultMetaData md) {
        Result result = newResult(ResultConstants.GENERATED);
        result.navigator = new RowSetNavigatorClient();
        result.metaData  = md;
        return result;
    }
    public int getExecuteProperties() {
        return rsProperties;
    }
    public void setDataResultProperties(int maxRows, int fetchSize,
                                        int resultSetScrollability,
                                        int resultSetConcurrency,
                                        int resultSetHoldability) {
        updateCount    = maxRows;
        this.fetchSize = fetchSize;
        rsProperties = ResultProperties.getValueForJDBC(resultSetScrollability,
                resultSetConcurrency, resultSetHoldability);
    }
    public static Result newDataHeadResult(SessionInterface session,
                                           Result source, int offset,
                                           int count) {
        if (offset + count > source.navigator.getSize()) {
            count = source.navigator.getSize() - offset;
        }
        Result result = newResult(ResultConstants.DATAHEAD);
        result.metaData = source.metaData;
        result.navigator = new RowSetNavigatorClient(source.navigator, offset,
                count);
        result.navigator.setId(source.navigator.getId());
        result.setSession(session);
        result.rsProperties = source.rsProperties;
        result.fetchSize    = source.fetchSize;
        return result;
    }
    public static Result newDataRowsResult(Result source, int offset,
                                           int count) {
        if (offset + count > source.navigator.getSize()) {
            count = source.navigator.getSize() - offset;
        }
        Result result = newResult(ResultConstants.DATAROWS);
        result.id       = source.id;
        result.metaData = source.metaData;
        result.navigator = new RowSetNavigatorClient(source.navigator, offset,
                count);
        return result;
    }
    public static Result newDataRowsResult(RowSetNavigator navigator) {
        Result result = newResult(ResultConstants.DATAROWS);
        result.navigator = navigator;
        return result;
    }
    public static Result newSessionAttributesResult() {
        Result result = newResult(ResultConstants.DATA);
        result.navigator = new RowSetNavigatorClient(1);
        result.metaData  = sessionAttributesMetaData;
        result.navigator.add(new Object[SessionInterface.INFO_LIMIT]);
        return result;
    }
    public static Result newWarningResult(HsqlException w) {
        Result result = newResult(ResultConstants.WARNING);
        result.mainString = w.getMessage();
        result.subString  = w.getSQLState();
        result.errorCode  = w.getErrorCode();
        return result;
    }
    public static Result newErrorResult(Throwable t) {
        return newErrorResult(t, null);
    }
    public static Result newErrorResult(Throwable t, String statement) {
        Result result = newResult(ResultConstants.ERROR);
        if (t instanceof HsqlException) {
            result.exception  = (HsqlException) t;
            result.mainString = result.exception.getMessage();
            result.subString  = result.exception.getSQLState();
            if (statement != null) {
                result.mainString += " in statement [" + statement + "]";
            }
            result.errorCode = result.exception.getErrorCode();
        } else if (t instanceof OutOfMemoryError) {
            System.gc();
            result.exception  = Error.error(ErrorCode.OUT_OF_MEMORY, t);
            result.mainString = result.exception.getMessage();
            result.subString  = result.exception.getSQLState();
            result.errorCode  = result.exception.getErrorCode();
        } else {
            result.exception = Error.error(ErrorCode.GENERAL_ERROR, t);
            result.mainString = result.exception.getMessage() + " "
                                + t.toString();
            result.subString = result.exception.getSQLState();
            result.errorCode = result.exception.getErrorCode();
            if (statement != null) {
                result.mainString += " in statement [" + statement + "]";
            }
        }
        return result;
    }
    public void write(SessionInterface session, DataOutputStream dataOut,
                      RowOutputInterface rowOut)
                      throws IOException, HsqlException {
        rowOut.reset();
        rowOut.writeByte(mode);
        int startPos = rowOut.size();
        rowOut.writeSize(0);
        switch (mode) {
            case ResultConstants.GETSESSIONATTR :
                rowOut.writeByte(statementReturnType);
                break;
            case ResultConstants.DISCONNECT :
            case ResultConstants.RESETSESSION :
            case ResultConstants.STARTTRAN :
                break;
            case ResultConstants.PREPARE :
                rowOut.writeByte(statementReturnType);
                rowOut.writeString(mainString);
                rowOut.writeByte(rsProperties);
                rowOut.writeByte(generateKeys);
                if (generateKeys == ResultConstants
                        .RETURN_GENERATED_KEYS_COL_NAMES || generateKeys == ResultConstants
                        .RETURN_GENERATED_KEYS_COL_INDEXES) {
                    generatedMetaData.write(rowOut);
                }
                break;
            case ResultConstants.FREESTMT :
                rowOut.writeLong(statementID);
                break;
            case ResultConstants.CLOSE_RESULT :
                rowOut.writeLong(id);
                break;
            case ResultConstants.EXECDIRECT :
                rowOut.writeInt(updateCount);
                rowOut.writeInt(fetchSize);
                rowOut.writeByte(statementReturnType);
                rowOut.writeString(mainString);
                rowOut.writeByte(rsProperties);
                rowOut.writeShort(queryTimeout);
                rowOut.writeByte(generateKeys);
                if (generateKeys == ResultConstants
                        .RETURN_GENERATED_KEYS_COL_NAMES || generateKeys == ResultConstants
                        .RETURN_GENERATED_KEYS_COL_INDEXES) {
                    generatedMetaData.write(rowOut);
                }
                break;
            case ResultConstants.CONNECT :
                rowOut.writeString(databaseName);
                rowOut.writeString(mainString);
                rowOut.writeString(subString);
                rowOut.writeString(zoneString);
                rowOut.writeInt(updateCount);
                break;
            case ResultConstants.ERROR :
            case ResultConstants.WARNING :
                rowOut.writeString(mainString);
                rowOut.writeString(subString);
                rowOut.writeInt(errorCode);
                break;
            case ResultConstants.CONNECTACKNOWLEDGE :
                rowOut.writeInt(databaseID);
                rowOut.writeLong(sessionID);
                rowOut.writeString(databaseName);
                rowOut.writeString(mainString);
                break;
            case ResultConstants.UPDATECOUNT :
                rowOut.writeInt(updateCount);
                break;
            case ResultConstants.ENDTRAN : {
                int type = getActionType();
                rowOut.writeInt(type);                     
                switch (type) {
                    case ResultConstants.TX_SAVEPOINT_NAME_RELEASE :
                    case ResultConstants.TX_SAVEPOINT_NAME_ROLLBACK :
                        rowOut.writeString(mainString);    
                        break;
                    case ResultConstants.TX_COMMIT :
                    case ResultConstants.TX_ROLLBACK :
                    case ResultConstants.TX_COMMIT_AND_CHAIN :
                    case ResultConstants.TX_ROLLBACK_AND_CHAIN :
                        break;
                    default :
                        throw Error.runtimeError(ErrorCode.U_S0500, "Result");
                }
                break;
            }
            case ResultConstants.PREPARE_ACK :
                rowOut.writeByte(statementReturnType);
                rowOut.writeLong(statementID);
                rowOut.writeByte(rsProperties);
                metaData.write(rowOut);
                parameterMetaData.write(rowOut);
                break;
            case ResultConstants.CALL_RESPONSE :
                rowOut.writeInt(updateCount);
                rowOut.writeInt(fetchSize);
                rowOut.writeLong(statementID);
                rowOut.writeByte(statementReturnType);
                rowOut.writeByte(rsProperties);
                metaData.write(rowOut);
                writeSimple(rowOut, metaData, (Object[]) valueData);
                break;
            case ResultConstants.EXECUTE :
                rowOut.writeInt(updateCount);
                rowOut.writeInt(fetchSize);
                rowOut.writeLong(statementID);
                rowOut.writeByte(rsProperties);
                rowOut.writeShort(queryTimeout);
                writeSimple(rowOut, metaData, (Object[]) valueData);
                break;
            case ResultConstants.UPDATE_RESULT :
                rowOut.writeLong(id);
                rowOut.writeInt(getActionType());
                metaData.write(rowOut);
                writeSimple(rowOut, metaData, (Object[]) valueData);
                break;
            case ResultConstants.BATCHEXECRESPONSE :
            case ResultConstants.BATCHEXECUTE :
            case ResultConstants.BATCHEXECDIRECT :
            case ResultConstants.SETSESSIONATTR : {
                rowOut.writeInt(updateCount);
                rowOut.writeInt(fetchSize);
                rowOut.writeLong(statementID);
                rowOut.writeShort(queryTimeout);
                metaData.write(rowOut);
                navigator.writeSimple(rowOut, metaData);
                break;
            }
            case ResultConstants.PARAM_METADATA : {
                metaData.write(rowOut);
                navigator.write(rowOut, metaData);
                break;
            }
            case ResultConstants.SETCONNECTATTR : {
                int type = getConnectionAttrType();
                rowOut.writeInt(type);                     
                switch (type) {
                    case ResultConstants.SQL_ATTR_SAVEPOINT_NAME :
                        rowOut.writeString(mainString);    
                        break;
                    default :
                        throw Error.runtimeError(ErrorCode.U_S0500, "Result");
                }
                break;
            }
            case ResultConstants.REQUESTDATA : {
                rowOut.writeLong(id);
                rowOut.writeInt(updateCount);
                rowOut.writeInt(fetchSize);
                break;
            }
            case ResultConstants.DATAROWS :
                metaData.write(rowOut);
                navigator.write(rowOut, metaData);
                break;
            case ResultConstants.DATAHEAD :
            case ResultConstants.DATA :
            case ResultConstants.GENERATED :
                rowOut.writeLong(id);
                rowOut.writeInt(updateCount);
                rowOut.writeInt(fetchSize);
                rowOut.writeByte(rsProperties);
                metaData.write(rowOut);
                navigator.write(rowOut, metaData);
                break;
            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "Result");
        }
        rowOut.writeIntData(rowOut.size() - startPos, startPos);
        dataOut.write(rowOut.getOutputStream().getBuffer(), 0, rowOut.size());
        int    count   = getLobCount();
        Result current = this;
        for (int i = 0; i < count; i++) {
            ResultLob lob = current.lobResults;
            lob.writeBody(session, dataOut);
            current = current.lobResults;
        }
        if (chainedResult == null) {
            dataOut.writeByte(ResultConstants.NONE);
        } else {
            chainedResult.write(session, dataOut, rowOut);
        }
        dataOut.flush();
    }
    public int getType() {
        return mode;
    }
    public boolean isData() {
        return mode == ResultConstants.DATA
               || mode == ResultConstants.DATAHEAD;
    }
    public boolean isError() {
        return mode == ResultConstants.ERROR;
    }
    public boolean isWarning() {
        return mode == ResultConstants.WARNING;
    }
    public boolean isUpdateCount() {
        return mode == ResultConstants.UPDATECOUNT;
    }
    public boolean isSimpleValue() {
        return mode == ResultConstants.VALUE;
    }
    public boolean hasGeneratedKeys() {
        return mode == ResultConstants.UPDATECOUNT && chainedResult != null;
    }
    public HsqlException getException() {
        return exception;
    }
    public long getStatementID() {
        return statementID;
    }
    public void setStatementID(long statementId) {
        this.statementID = statementId;
    }
    public String getMainString() {
        return mainString;
    }
    public void setMainString(String sql) {
        this.mainString = sql;
    }
    public String getSubString() {
        return subString;
    }
    public String getZoneString() {
        return zoneString;
    }
    public int getErrorCode() {
        return errorCode;
    }
    public Object getValueObject() {
        return valueData;
    }
    public void setValueObject(Object value) {
        valueData = value;
    }
    public Statement getStatement() {
        return statement;
    }
    public void setStatement(Statement statement) {
        this.statement = statement;
    }
    public String getDatabaseName() {
        return databaseName;
    }
    public void setMaxRows(int count) {
        updateCount = count;
    }
    public int getFetchSize() {
        return this.fetchSize;
    }
    public void setFetchSize(int count) {
        fetchSize = count;
    }
    public int getUpdateCount() {
        return updateCount;
    }
    public int getConnectionAttrType() {
        return updateCount;
    }
    public void setConnectionAttrType(int type) {
        updateCount = type;
    }
    public int getActionType() {
        return updateCount;
    }
    public void setActionType(int type) {
        updateCount = type;
    }
    public long getSessionId() {
        return sessionID;
    }
    public void setSessionId(long id) {
        sessionID = id;
    }
    public void setSession(SessionInterface session) {
        if (navigator != null) {
            navigator.setSession(session);
        }
    }
    public int getDatabaseId() {
        return databaseID;
    }
    public void setDatabaseId(int id) {
        databaseID = id;
    }
    public long getResultId() {
        return id;
    }
    public void setResultId(long id) {
        this.id = id;
        if (navigator != null) {
            navigator.setId(id);
        }
    }
    public void setUpdateCount(int count) {
        updateCount = count;
    }
    public void setAsTransactionEndRequest(int subType, String savepoint) {
        mode        = ResultConstants.ENDTRAN;
        updateCount = subType;
        mainString  = savepoint == null ? ""
                                        : savepoint;
    }
    public Object[] getSingleRowData() {
        Object[] data = (Object[]) initialiseNavigator().getNext();
        data = (Object[]) ArrayUtil.resizeArrayIfDifferent(data,
                metaData.getColumnCount());
        return data;
    }
    public Object[] getParameterData() {
        return (Object[]) valueData;
    }
    public Object[] getSessionAttributes() {
        return (Object[]) initialiseNavigator().getNext();
    }
    public void setResultType(int type) {
        mode = (byte) type;
    }
    public void setStatementType(int type) {
        statementReturnType = type;
    }
    public int getStatementType() {
        return statementReturnType;
    }
    public int getGeneratedResultType() {
        return generateKeys;
    }
    public ResultMetaData getGeneratedResultMetaData() {
        return generatedMetaData;
    }
    public Result getChainedResult() {
        return chainedResult;
    }
    public Result getUnlinkChainedResult() {
        Result result = chainedResult;
        chainedResult = null;
        return result;
    }
    public void addChainedResult(Result result) {
        Result current = this;
        while (current.chainedResult != null) {
            current = current.chainedResult;
        }
        current.chainedResult = result;
    }
    public void addWarnings(HsqlException[] warnings) {
        for (int i = 0; i < warnings.length; i++) {
            Result warning = newWarningResult(warnings[i]);
            addChainedResult(warning);
        }
    }
    public int getLobCount() {
        return lobCount;
    }
    public ResultLob getLOBResult() {
        return lobResults;
    }
    public void addLobResult(ResultLob result) {
        Result current = this;
        while (current.lobResults != null) {
            current = current.lobResults;
        }
        current.lobResults = result;
        lobCount++;
    }
    public void clearLobResults() {
        lobResults = null;
        lobCount   = 0;
    }
    private static Object[] readSimple(RowInputBinary in,
                                       ResultMetaData meta)
                                       throws IOException {
        int size = in.readInt();
        return in.readData(meta.columnTypes);
    }
    private static void writeSimple(RowOutputInterface out,
                                    ResultMetaData meta,
                                    Object[] data) throws IOException {
        out.writeInt(1);
        out.writeData(meta.getColumnCount(), meta.columnTypes, data, null,
                      null);
    }
    public RowSetNavigator navigator;
    public RowSetNavigator getNavigator() {
        return navigator;
    }
    public void setNavigator(RowSetNavigator navigator) {
        this.navigator = navigator;
    }
    public RowSetNavigator initialiseNavigator() {
        switch (mode) {
            case ResultConstants.BATCHEXECUTE :
            case ResultConstants.BATCHEXECDIRECT :
            case ResultConstants.BATCHEXECRESPONSE :
            case ResultConstants.SETSESSIONATTR :
            case ResultConstants.PARAM_METADATA :
                navigator.beforeFirst();
                return navigator;
            case ResultConstants.DATA :
            case ResultConstants.DATAHEAD :
            case ResultConstants.GENERATED :
                navigator.reset();
                return navigator;
            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "Result");
        }
    }
}