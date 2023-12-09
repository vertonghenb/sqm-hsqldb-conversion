


package org.hsqldb.server;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;

import org.hsqldb.ClientConnection;
import org.hsqldb.ColumnBase;
import org.hsqldb.DatabaseManager;
import org.hsqldb.HsqlException;
import org.hsqldb.Session;
import org.hsqldb.StatementTypes;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.DataOutputStream;
import org.hsqldb.navigator.RowSetNavigator;
import org.hsqldb.persist.HsqlDatabaseProperties;
import org.hsqldb.resources.BundleHandler;
import org.hsqldb.result.Result;
import org.hsqldb.result.ResultConstants;
import org.hsqldb.result.ResultMetaData;
import org.hsqldb.result.ResultProperties;
import org.hsqldb.rowio.RowInputBinary;
import org.hsqldb.rowio.RowOutputBinary;
import org.hsqldb.rowio.RowOutputInterface;
import org.hsqldb.types.Type;








class ServerConnection implements Runnable {

    boolean                  keepAlive;
    private String           user;
    int                      dbID;
    int                      dbIndex;
    private volatile Session session;
    private Socket           socket;
    private Server           server;
    private DataInputStream  dataInput;
    private DataOutputStream dataOutput;
    private static int       mCurrentThread = 0;
    private int              mThread;
    static final int         BUFFER_SIZE = 0x1000;
    final byte[]             mainBuffer  = new byte[BUFFER_SIZE];
    RowOutputInterface       rowOut;
    RowInputBinary           rowIn;
    Thread                   runnerThread;
    protected static String  TEXTBANNER_PART1 = null;
    protected static String  TEXTBANNER_PART2 = null;

    static {
        int serverBundleHandle =
            BundleHandler.getBundleHandle("org_hsqldb_Server_messages", null);

        if (serverBundleHandle < 0) {
            throw new RuntimeException(
                "MISSING Resource Bundle.  See source code");

            
            
        }

        TEXTBANNER_PART1 = BundleHandler.getString(serverBundleHandle,
                "textbanner.part1");
        TEXTBANNER_PART2 = BundleHandler.getString(serverBundleHandle,
                "textbanner.part2");

        if (TEXTBANNER_PART1 == null || TEXTBANNER_PART2 == null) {
            throw new RuntimeException(
                "MISSING Resource Bundle msg definition.  See source code");

            
            
        }
    }

    
    ServerConnection(Socket socket, Server server) {

        RowOutputBinary rowOutTemp = new RowOutputBinary(mainBuffer);

        rowIn  = new RowInputBinary(rowOutTemp);
        rowOut = rowOutTemp;

        
        Thread runnerThread;

        this.socket = socket;
        this.server = server;

        synchronized (ServerConnection.class) {
            mThread = mCurrentThread++;
        }

        synchronized (server.serverConnSet) {
            server.serverConnSet.add(this);
        }
    }

    
    void signalClose() {

        keepAlive = false;

        if (!Thread.currentThread().equals(runnerThread)) {
            close();
        }
    }

    
    private void close() {

        if (session != null) {
            session.close();

            session = null;
        }

        
        try {
            synchronized (this) {
                if (socket != null) {
                    socket.close();
                }
            }
        } catch (IOException e) {}

        socket = null;

        synchronized (server.serverConnSet) {
            server.serverConnSet.remove(this);
        }

        try {
            runnerThread.setContextClassLoader(null);
        } catch (Throwable t) {}
    }

    
    private void init() {

        runnerThread = Thread.currentThread();
        keepAlive    = true;

        try {
            socket.setTcpNoDelay(true);

            dataInput = new DataInputStream(
                new BufferedInputStream(socket.getInputStream()));
            dataOutput = new DataOutputStream(socket.getOutputStream());

            int firstInt = handshake();

            switch (streamProtocol) {

                case HSQL_STREAM_PROTOCOL :
                    if (firstInt
                            != ClientConnection
                                .NETWORK_COMPATIBILITY_VERSION_INT) {
                        if (firstInt == -1900000) {
                            firstInt = -2000000;
                        }

                        String verString =
                            ClientConnection.toNetCompVersionString(firstInt);

                        throw Error.error(
                            null, ErrorCode.SERVER_VERSIONS_INCOMPATIBLE, 0,
                            new String[] {
                            verString, HsqlDatabaseProperties.hsqldb_version
                        });
                    }

                    Result resultIn = Result.newResult(dataInput, rowIn);

                    resultIn.readAdditionalResults(session, dataInput, rowIn);

                    Result resultOut;

                    resultOut = setDatabase(resultIn);

                    resultOut.write(session, dataOutput, rowOut);
                    break;

                case ODBC_STREAM_PROTOCOL :
                    odbcConnect(firstInt);
                    break;

                default :

                    
                    
                    keepAlive = false;
            }
        } catch (Exception e) {

            
            
            
            
            StringBuffer sb = new StringBuffer(mThread
                                               + ":Failed to connect client.");

            if (user != null) {
                sb.append("  User '" + user + "'.");
            }

            server.printWithThread(sb.toString() + "  Stack trace follows.");
            server.printStackTrace(e);
        }
    }

    private static class CleanExit extends Exception {}

    private static class ClientFailure extends Exception {

        private String clientMessage = null;

        public ClientFailure(String ourMessage, String clientMessage) {

            super(ourMessage);

            this.clientMessage = clientMessage;
        }

        public String getClientMessage() {
            return clientMessage;
        }
    }

    private CleanExit cleanExit = new CleanExit();

    private void receiveResult(int resultMode) throws CleanExit, IOException {

        boolean terminate = false;
        Result resultIn = Result.newResult(session, resultMode, dataInput,
                                           rowIn);

        resultIn.readLobResults(session, dataInput, rowIn);
        server.printRequest(mThread, resultIn);

        Result resultOut = null;

        switch (resultIn.getType()) {

            case ResultConstants.CONNECT : {
                resultOut = setDatabase(resultIn);

                break;
            }
            case ResultConstants.DISCONNECT : {
                resultOut = Result.updateZeroResult;
                terminate = true;

                break;
            }
            case ResultConstants.RESETSESSION : {
                session.resetSession();

                resultOut = Result.updateZeroResult;

                break;
            }
            case ResultConstants.EXECUTE_INVALID : {
                resultOut =
                    Result.newErrorResult(Error.error(ErrorCode.X_07502));

                break;
            }
            default : {
                resultOut = session.execute(resultIn);

                break;
            }
        }

        resultOut.write(session, dataOutput, rowOut);
        rowOut.setBuffer(mainBuffer);
        rowIn.resetRow(mainBuffer.length);

        if (terminate) {
            throw cleanExit;
        }
    }

    private OdbcPacketOutputStream outPacket = null;

    private void receiveOdbcPacket(char inC) throws IOException, CleanExit {

        
        char    c;
        boolean sendReadyForQuery = false;
        String  psHandle, portalHandle, handle, dataString, tmpStr;

        
        
        String                interposedStatement = null;
        Result                r, rOut;
        int                   paramCount, lastSemi;
        OdbcPreparedStatement odbcPs;
        StatementPortal       portal;
        ResultMetaData        pmd;
        OdbcPacketInputStream inPacket = null;
        Type[]                colTypes;
        PgType[]              pgTypes;

        try {
            inPacket = OdbcPacketInputStream.newOdbcPacketInputStream(inC,
                    dataInput);

            server.printWithThread("Got op (" + inPacket.packetType + ')');
            server.printWithThread("Got packet length of "
                                   + inPacket.available()
                                   + " + type byte + 4 size header");

            if (inPacket.available() >= 1000000000) {
                throw new IOException("Insane packet length: "
                                      + inPacket.available()
                                      + " + type byte + 4 size header");
            }
        } catch (SocketException se) {
            server.printWithThread("Ungraceful client exit: " + se);

            throw cleanExit;    
        } catch (IOException ioe) {
            server.printWithThread("Fatal ODBC protocol failure: " + ioe);

            try {
                OdbcUtil.alertClient(OdbcUtil.ODBC_SEVERITY_FATAL,
                                     ioe.toString(), "08P01", dataOutput);

                
            } catch (Exception e) {

                
            }

            throw cleanExit;    
        }

        
        switch (odbcCommMode) {

            case OdbcUtil.ODBC_EXT_RECOVER_MODE :
                if (inPacket.packetType != 'S') {
                    if (server.isTrace()) {
                        server.printWithThread("Ignoring a '"
                                               + inPacket.packetType + "'");
                    }

                    return;
                }

                odbcCommMode = OdbcUtil.ODBC_EXTENDED_MODE;

                server.printWithThread(
                    "EXTENDED comm session being recovered");

                
                
                break;

            case OdbcUtil.ODBC_SIMPLE_MODE :
                switch (inPacket.packetType) {

                    case 'P' :

                    
                    
                    
                    
                    
                    case 'H' :
                    case 'S' :
                    case 'D' :
                    case 'B' :
                    case 'E' :
                    case 'C' :
                        odbcCommMode = OdbcUtil.ODBC_EXTENDED_MODE;

                        server.printWithThread(
                            "Switching mode from SIMPLE to EXTENDED");

                    
                    
                    
                }
                break;

            case OdbcUtil.ODBC_EXTENDED_MODE :
                switch (inPacket.packetType) {

                    case 'Q' :
                        odbcCommMode = OdbcUtil.ODBC_SIMPLE_MODE;

                        server.printWithThread(
                            "Switching mode from EXTENDED to SIMPLE");

                    
                    
                    
                }
                break;

            default :
                throw new RuntimeException("Unexpected ODBC comm mode value: "
                                           + odbcCommMode);
        }

        outPacket.reset();

        try {

            
            
            
            
            
            
            
            
            MAIN_ODBC_COMM_SWITCH:
            switch (inPacket.packetType) {

                case 'Q' :                                    
                    String sql = inPacket.readString();

                    
                    
                    if (sql.startsWith("BEGIN;") || sql.equals("BEGIN")) {
                        
                        sql = sql.equals("BEGIN") ? null
                                                  : sql.substring(
                                                      "BEGIN;".length());

                        server.printWithThread(
                            "ODBC Trans started.  Session AutoCommit -> F");

                        try {
                            session.setAutoCommit(false);
                        } catch (HsqlException he) {
                            throw new RecoverableOdbcFailure(
                                "Failed to change transaction state: "
                                + he.getMessage(), he.getSQLState());
                        }

                        
                        outPacket.write("BEGIN");
                        outPacket.xmit('C', dataOutput);

                        if (sql == null) {
                            sendReadyForQuery = true;

                            break;
                        }
                    }

                    if (sql.startsWith("SAVEPOINT ") && sql.indexOf(';') > 0) {
                        int firstSemi = sql.indexOf(';');

                        server.printWithThread(
                            "Interposing BEFORE primary statement: "
                            + sql.substring(0, firstSemi));
                        odbcExecDirect(sql.substring(0, firstSemi));

                        sql = sql.substring(firstSemi + 1);
                    }

                    lastSemi = sql.lastIndexOf(';');

                    if (lastSemi > 0) {
                        String suffix = sql.substring(lastSemi + 1);

                        if (suffix.startsWith("RELEASE ")) {
                            interposedStatement = suffix;
                            sql                 = sql.substring(0, lastSemi);
                        }
                    }

                    
                    String normalized = sql.trim().toLowerCase();

                    if (server.isTrace()) {
                        server.printWithThread("Received query (" + sql + ')');
                    }

                    
                    if (normalized.startsWith("select current_schema()")) {
                        server.printWithThread(
                            "Implement 'select current_schema() emulation!");

                        throw new RecoverableOdbcFailure(
                            "current_schema() not supported yet", "0A000");
                    }

                    if (normalized.startsWith("select n.nspname,")) {

                        
                        server.printWithThread(
                            "Swallowing 'select n.nspname,...'");
                        outPacket.writeShort(1);              
                        outPacket.write("oid");
                        outPacket.writeInt(201);
                        outPacket.writeShort(1);
                        outPacket.writeInt(23);
                        outPacket.writeShort(4);
                        outPacket.writeInt(-1);
                        outPacket.writeShort(0);
                        outPacket.xmit('T', dataOutput);      

                        
                        outPacket.write("SELECT");
                        outPacket.xmit('C', dataOutput);

                        sendReadyForQuery = true;

                        break;
                    }

                    if (normalized.startsWith(
                            "select oid, typbasetype from")) {

                        
                        server.printWithThread(
                            "Simulating 'select oid, typbasetype...'");
                        
                        outPacket.writeShort(2);              
                        outPacket.write("oid");               
                        outPacket.writeInt(101);              
                        outPacket.writeShort(102);            
                        outPacket.writeInt(26);               
                        outPacket.writeShort(4);              
                        outPacket.writeInt(-1);               
                        outPacket.writeShort(0);              
                        outPacket.write("typbasetype");       
                        outPacket.writeInt(101);              
                        outPacket.writeShort(103);            
                        outPacket.writeInt(26);               
                        outPacket.writeShort(4);              
                        outPacket.writeInt(-1);               
                        outPacket.writeShort(0);              
                        outPacket.xmit('T', dataOutput);      

                        
                        outPacket.write("SELECT");
                        outPacket.xmit('C', dataOutput);

                        sendReadyForQuery = true;

                        break;
                    }

                    if (normalized.startsWith("select ")) {
                        server.printWithThread(
                            "Performing a real non-prepared SELECT...");

                        r = Result.newExecuteDirectRequest();

                        r.setPrepareOrExecuteProperties(
                            sql, 0, 0, StatementTypes.RETURN_RESULT, 0,
                            ResultProperties.defaultPropsValue,
                            java.sql.Statement.NO_GENERATED_KEYS, null, null);

                        rOut = session.execute(r);

                        switch (rOut.getType()) {

                            case ResultConstants.DATA :
                                break;

                            case ResultConstants.ERROR :
                                throw new RecoverableOdbcFailure(rOut);
                            default :
                                throw new RecoverableOdbcFailure(
                                    "Output Result from Query execution is of "
                                    + "unexpected type: " + rOut.getType());
                        }

                        
                        
                        RowSetNavigator navigator = rOut.getNavigator();
                        ResultMetaData  md        = rOut.metaData;

                        if (md == null) {
                            throw new RecoverableOdbcFailure(
                                "Failed to get metadata for query results");
                        }

                        int      columnCount = md.getColumnCount();
                        String[] colLabels   = md.getGeneratedColumnNames();

                        colTypes = md.columnTypes;
                        pgTypes  = new PgType[columnCount];

                        for (int i = 0; i < pgTypes.length; i++) {
                            pgTypes[i] = PgType.getPgType(colTypes[i],
                                                          md.isTableColumn(i));
                        }

                        
                        
                        
                        
                        
                        ColumnBase[] colDefs = md.columns;

                        
                        outPacket.writeShort(columnCount);

                        for (int i = 0; i < columnCount; i++) {

                            
                            if (colLabels[i] != null) {
                                outPacket.write(colLabels[i]);
                            } else {
                                outPacket.write(colDefs[i].getNameString());
                            }

                            
                            outPacket.writeInt(OdbcUtil.getTableOidForColumn(i,
                                    md));

                            
                            outPacket.writeShort(OdbcUtil.getIdForColumn(i,
                                    md));
                            outPacket.writeInt(pgTypes[i].getOid());

                            
                            outPacket.writeShort(pgTypes[i].getTypeWidth());
                            outPacket.writeInt(pgTypes[i].getLPConstraint());

                            
                            
                            
                            
                            outPacket.writeShort(0);

                            
                            
                            
                        }

                        outPacket.xmit('T', dataOutput);      

                        int rowNum = 0;

                        while (navigator.next()) {
                            rowNum++;

                            Object[] rowData = navigator.getCurrent();

                            
                            
                            
                            if (rowData == null) {
                                throw new RecoverableOdbcFailure("Null row?");
                            }

                            if (rowData.length < columnCount) {
                                throw new RecoverableOdbcFailure(
                                    "Data element mismatch. " + columnCount
                                    + " metadata cols, yet " + rowData.length
                                    + " data elements for row " + rowNum);
                            }

                            
                            
                            outPacket.writeShort(columnCount);

                            
                            
                            for (int i = 0; i < columnCount; i++) {
                                if (rowData[i] == null) {
                                    
                                    outPacket.writeInt(-1);
                                } else {
                                    dataString =
                                        pgTypes[i].valueString(rowData[i]);

                                    outPacket.writeSized(dataString);

                                    if (server.isTrace()) {
                                        server.printWithThread(
                                            "R" + rowNum + "C" + (i + 1)
                                            + " => ("
                                            + rowData[i].getClass().getName()
                                            + ") [" + dataString + ']');
                                    }
                                }
                            }

                            outPacket.xmit('D', dataOutput);
                        }

                        outPacket.write("SELECT");
                        outPacket.xmit('C', dataOutput);

                        sendReadyForQuery = true;

                        break;
                    }

                    if (normalized.startsWith("deallocate \"")
                            && normalized.charAt(normalized.length() - 1)
                               == '"') {
                        tmpStr = sql.trim().substring(
                            "deallocate \"".length()).trim();

                        
                        handle = tmpStr.substring(0, tmpStr.length() - 1);
                        odbcPs = (OdbcPreparedStatement) sessionOdbcPsMap.get(
                            handle);

                        if (odbcPs != null) {
                            odbcPs.close();
                        }

                        portal =
                            (StatementPortal) sessionOdbcPortalMap.get(handle);

                        if (portal != null) {
                            portal.close();
                        }

                        if (odbcPs == null && portal == null) {
                            
                            server.printWithThread(
                                "Ignoring bad 'DEALLOCATE' cmd");
                        }

                        if (server.isTrace()) {
                            server.printWithThread("Deallocated PS/Portal '"
                                                   + handle + "'");
                        }

                        outPacket.write("DEALLOCATE");
                        outPacket.xmit('C', dataOutput);

                        sendReadyForQuery = true;

                        break;
                    }

                    if (normalized.startsWith("set client_encoding to ")) {
                        server.printWithThread("Stubbing EXECDIR for: " + sql);
                        outPacket.write("SET");
                        outPacket.xmit('C', dataOutput);

                        sendReadyForQuery = true;

                        break;
                    }

                    
                    server.printWithThread("Performing a real EXECDIRECT...");
                    odbcExecDirect(sql);

                    sendReadyForQuery = true;
                    break;

                case 'X' :                                    
                    if (sessionOdbcPsMap.size()
                            > (sessionOdbcPsMap.containsKey("") ? 1
                                                                : 0)) {
                        server.printWithThread("Client left "
                                               + sessionOdbcPsMap.size()
                                               + " PS objects open");
                    }

                    if (sessionOdbcPortalMap.size()
                            > (sessionOdbcPortalMap.containsKey("") ? 1
                                                                    : 0)) {
                        server.printWithThread("Client left "
                                               + sessionOdbcPortalMap.size()
                                               + " Portal objects open");
                    }

                    OdbcUtil.validateInputPacketSize(inPacket);

                    throw cleanExit;
                case 'H' :                                    

                    
                    
                    
                    break;

                case 'S' :                                    

                    
                    
                    if (session.isAutoCommit()) {
                        try {

                            
                            
                            
                            
                            server.printWithThread(
                                "Silly implicit commit by Sync");
                            session.commit(true);

                            
                        } catch (HsqlException he) {
                            server.printWithThread("Implicit commit failed: "
                                                   + he);
                            OdbcUtil.alertClient(OdbcUtil.ODBC_SEVERITY_ERROR,
                                                 "Implicit commit failed",
                                                 he.getSQLState(), dataOutput);
                        }
                    }

                    sendReadyForQuery = true;
                    break;

                case 'P' :                                    
                    psHandle = inPacket.readString();

                    String query = OdbcUtil.revertMungledPreparedQuery(
                        inPacket.readString());

                    paramCount = inPacket.readUnsignedShort();

                    for (int i = 0; i < paramCount; i++) {
                        if (inPacket.readInt() != 0) {
                            throw new RecoverableOdbcFailure(
                                null,
                                "Parameter-type OID specifiers not supported yet",
                                "0A000");
                        }
                    }

                    if (server.isTrace()) {
                        server.printWithThread(
                            "Received Prepare request for query (" + query
                            + ") with handle '" + psHandle + "'");
                    }

                    if (psHandle.length() > 0
                            && sessionOdbcPsMap.containsKey(psHandle)) {
                        throw new RecoverableOdbcFailure(
                            null,
                            "PS handle '" + psHandle + "' already in use.  "
                            + "You must close it before recreating", "08P01");
                    }

                    new OdbcPreparedStatement(psHandle, query,
                                              sessionOdbcPsMap, session);
                    outPacket.xmit('1', dataOutput);
                    break;

                case 'D' :                                    
                    c      = inPacket.readByteChar();
                    handle = inPacket.readString();
                    odbcPs = null;
                    portal = null;

                    if (c == 'S') {
                        odbcPs = (OdbcPreparedStatement) sessionOdbcPsMap.get(
                            handle);
                    } else if (c == 'P') {
                        portal =
                            (StatementPortal) sessionOdbcPortalMap.get(handle);
                    } else {
                        throw new RecoverableOdbcFailure(
                            null,
                            "Description packet request type invalid: " + c,
                            "08P01");
                    }

                    if (server.isTrace()) {
                        server.printWithThread("Received Describe request for "
                                               + c + " of  handle '" + handle
                                               + "'");
                    }

                    if (odbcPs == null && portal == null) {
                        throw new RecoverableOdbcFailure(
                            null,
                            "No object present for " + c + " handle: "
                            + handle, "08P01");
                    }

                    Result ackResult = (odbcPs == null) ? portal.ackResult
                                                        : odbcPs.ackResult;

                    pmd        = ackResult.parameterMetaData;
                    paramCount = pmd.getColumnCount();

                    Type[] paramTypes = pmd.getParameterTypes();

                    if (paramCount != paramTypes.length) {
                        throw new RecoverableOdbcFailure(
                            "Parameter count mismatch.  Count of "
                            + paramCount + " reported, but there are "
                            + paramTypes.length + " param md objects");
                    }

                    if (c == 'S') {
                        outPacket.writeShort(paramCount);

                        for (int i = 0; i < paramTypes.length; i++) {
                            outPacket.writeInt(
                                PgType.getPgType(
                                    paramTypes[i], true).getOid());

                            
                            
                            
                        }

                        outPacket.xmit('t', dataOutput);

                        
                    }

                    ResultMetaData md = ackResult.metaData;

                    if (md.getColumnCount() < 1) {
                        if (server.isTrace()) {
                            server.printWithThread(
                                "Non-rowset query so returning NoData packet");
                        }

                        
                        
                        outPacket.xmit('n', dataOutput);

                        break;
                    }

                    
                    
                    
                    
                    String[] colNames = md.getGeneratedColumnNames();

                    if (md.getColumnCount() != colNames.length) {
                        throw new RecoverableOdbcFailure(
                            "Couldn't get all column names: "
                            + md.getColumnCount() + " cols. but only got "
                            + colNames.length + " col. names");
                    }

                    colTypes = md.columnTypes;
                    pgTypes  = new PgType[colNames.length];

                    ColumnBase[] colDefs = md.columns;

                    for (int i = 0; i < pgTypes.length; i++) {
                        pgTypes[i] = PgType.getPgType(colTypes[i],
                                                      md.isTableColumn(i));
                    }

                    if (colNames.length != colDefs.length) {
                        throw new RecoverableOdbcFailure(
                            "Col data mismatch.  " + colDefs.length
                            + " col instances but " + colNames.length
                            + " col names");
                    }

                    outPacket.writeShort(colNames.length);    

                    for (int i = 0; i < colNames.length; i++) {
                        outPacket.write(colNames[i]);         

                        
                        outPacket.writeInt(OdbcUtil.getTableOidForColumn(i,
                                md));

                        
                        outPacket.writeShort(OdbcUtil.getIdForColumn(i, md));
                        outPacket.writeInt(pgTypes[i].getOid());

                        
                        outPacket.writeShort(pgTypes[i].getTypeWidth());
                        outPacket.writeInt(pgTypes[i].getLPConstraint());

                        
                        
                        
                        
                        outPacket.writeShort(0);

                        
                        
                        
                    }

                    outPacket.xmit('T', dataOutput);          
                    break;

                case 'B' :                                    
                    portalHandle = inPacket.readString();
                    psHandle     = inPacket.readString();

                    int       paramFormatCount = inPacket.readUnsignedShort();
                    boolean[] paramBinary      = new boolean[paramFormatCount];

                    for (int i = 0; i < paramFormatCount; i++) {
                        paramBinary[i] = inPacket.readUnsignedShort() != 0;

                        if (server.isTrace() && paramBinary[i]) {
                            server.printWithThread("Binary param #" + i);
                        }
                    }

                    paramCount = inPacket.readUnsignedShort();

                    Object[] paramVals = new Object[paramCount];

                    for (int i = 0; i < paramVals.length; i++) {
                        if (i < paramBinary.length && paramBinary[i]) {
                            paramVals[i] = inPacket.readSizedBinaryData();
                        } else {
                            paramVals[i] = inPacket.readSizedString();
                        }
                    }

                    int outFormatCount = inPacket.readUnsignedShort();

                    for (int i = 0; i < outFormatCount; i++) {
                        if (inPacket.readUnsignedShort() != 0) {
                            throw new RecoverableOdbcFailure(
                                null, "Binary output values not supported",
                                "0A000");
                        }
                    }

                    if (server.isTrace()) {
                        server.printWithThread(
                            "Received Bind request to make Portal from ("
                            + psHandle + ")' with handle '" + portalHandle
                            + "'");
                    }

                    odbcPs =
                        (OdbcPreparedStatement) sessionOdbcPsMap.get(psHandle);

                    if (odbcPs == null) {
                        throw new RecoverableOdbcFailure(
                            null,
                            "No object present for PS handle: " + psHandle,
                            "08P01");
                    }

                    if (portalHandle.length() > 0
                            && sessionOdbcPortalMap.containsKey(
                                portalHandle)) {
                        throw new RecoverableOdbcFailure(
                            null,
                            "Portal handle '" + portalHandle
                            + "' already in use.  "
                            + "You must close it before recreating", "08P01");
                    }

                    pmd = odbcPs.ackResult.parameterMetaData;

                    if (paramCount != pmd.getColumnCount()) {
                        throw new RecoverableOdbcFailure(
                            null,
                            "Client didn't specify all "
                            + pmd.getColumnCount() + " parameters ("
                            + paramCount + ')', "08P01");
                    }

                    new StatementPortal(portalHandle, odbcPs, paramVals,
                                        sessionOdbcPortalMap);
                    outPacket.xmit('2', dataOutput);
                    break;

                case 'E' :                                    
                    portalHandle = inPacket.readString();

                    int fetchRows = inPacket.readInt();

                    if (server.isTrace()) {
                        server.printWithThread("Received Exec request for "
                                               + fetchRows
                                               + " rows from portal handle '"
                                               + portalHandle + "'");
                    }

                    portal = (StatementPortal) sessionOdbcPortalMap.get(
                        portalHandle);

                    if (portal == null) {
                        throw new RecoverableOdbcFailure(
                            null,
                            "No object present for Portal handle: "
                            + portalHandle, "08P01");
                    }

                    
                    portal.bindResult.setPreparedExecuteProperties(
                        portal.parameters, fetchRows, 0, 0);

                    
                    rOut = session.execute(portal.bindResult);

                    switch (rOut.getType()) {

                        case ResultConstants.UPDATECOUNT :
                            outPacket.write(
                                OdbcUtil.echoBackReplyString(
                                    portal.lcQuery, rOut.getUpdateCount()));
                            outPacket.xmit('C', dataOutput);

                            
                            
                            
                            if (portal.lcQuery.equals("commit")
                                    || portal.lcQuery.startsWith("commit ")
                                    || portal.lcQuery.equals("rollback")
                                    || portal.lcQuery.startsWith(
                                        "rollback ")) {
                                try {
                                    session.setAutoCommit(true);
                                } catch (HsqlException he) {
                                    throw new RecoverableOdbcFailure(
                                        "Failed to change transaction state: "
                                        + he.getMessage(), he.getSQLState());
                                }
                            }
                            break MAIN_ODBC_COMM_SWITCH;

                        case ResultConstants.DATA :
                            break;

                        case ResultConstants.ERROR :
                            throw new RecoverableOdbcFailure(rOut);
                        default :
                            throw new RecoverableOdbcFailure(
                                "Output Result from Portal execution is of "
                                + "unexpected type: " + rOut.getType());
                    }

                    
                    
                    RowSetNavigator navigator = rOut.getNavigator();
                    int             rowNum    = 0;
                    int colCount = portal.ackResult.metaData.getColumnCount();

                    while (navigator.next()) {
                        rowNum++;

                        Object[] rowData = navigator.getCurrent();

                        if (rowData == null) {
                            throw new RecoverableOdbcFailure("Null row?");
                        }

                        if (rowData.length < colCount) {
                            throw new RecoverableOdbcFailure(
                                "Data element mismatch. " + colCount
                                + " metadata cols, yet " + rowData.length
                                + " data elements for row " + rowNum);
                        }

                        
                        
                        outPacket.writeShort(colCount);

                        
                        
                        colTypes = portal.ackResult.metaData.columnTypes;
                        pgTypes  = new PgType[colCount];

                        for (int i = 0; i < pgTypes.length; i++) {
                            pgTypes[i] = PgType.getPgType(
                                colTypes[i],
                                portal.ackResult.metaData.isTableColumn(i));
                        }

                        for (int i = 0; i < colCount; i++) {
                            if (rowData[i] == null) {
                                
                                outPacket.writeInt(-1);
                            } else {
                                dataString =
                                    pgTypes[i].valueString(rowData[i]);

                                outPacket.writeSized(dataString);

                                if (server.isTrace()) {
                                    server.printWithThread(
                                        "R" + rowNum + "C" + (i + 1) + " => ("
                                        + rowData[i].getClass().getName()
                                        + ") [" + dataString + ']');
                                }
                            }
                        }

                        outPacket.xmit('D', dataOutput);
                    }

                    if (navigator.afterLast()) {
                        outPacket.write("SELECT");
                        outPacket.xmit('C', dataOutput);

                        
                    } else {
                        outPacket.xmit('s', dataOutput);
                    }

                    
                    
                    
                    break;

                case 'C' :                                    
                    c      = inPacket.readByteChar();
                    handle = inPacket.readString();
                    odbcPs = null;
                    portal = null;

                    if (c == 'S') {
                        odbcPs = (OdbcPreparedStatement) sessionOdbcPsMap.get(
                            handle);

                        if (odbcPs != null) {
                            odbcPs.close();
                        }
                    } else if (c == 'P') {
                        portal =
                            (StatementPortal) sessionOdbcPortalMap.get(handle);

                        if (portal != null) {
                            portal.close();
                        }
                    } else {
                        throw new RecoverableOdbcFailure(
                            null,
                            "Description packet request type invalid: " + c,
                            "08P01");
                    }

                    
                    
                    
                    if (server.isTrace()) {
                        server.printWithThread("Closed " + c + " '" + handle
                                               + "'? "
                                               + (odbcPs != null
                                                  || portal != null));
                    }

                    outPacket.xmit('3', dataOutput);
                    break;

                default :
                    throw new RecoverableOdbcFailure(
                        null,
                        "Unsupported operation type (" + inPacket.packetType
                        + ')', "0A000");
            }

            OdbcUtil.validateInputPacketSize(inPacket);

            if (interposedStatement != null) {
                server.printWithThread("Interposing AFTER primary statement: "
                                       + interposedStatement);
                odbcExecDirect(interposedStatement);
            }

            if (sendReadyForQuery) {
                outPacket.reset();

                
                
                
                outPacket.writeByte(session.isAutoCommit() ? 'I'
                                                           : 'T');
                outPacket.xmit('Z', dataOutput);
            }
        } catch (RecoverableOdbcFailure rf) {
            Result errorResult = rf.getErrorResult();

            if (errorResult == null) {
                String stateCode = rf.getSqlStateCode();
                String svrMsg    = rf.toString();
                String cliMsg    = rf.getClientMessage();

                if (svrMsg != null) {
                    server.printWithThread(svrMsg);
                } else if (server.isTrace()) {
                    server.printWithThread("Client error: " + cliMsg);
                }

                if (cliMsg != null) {
                    OdbcUtil.alertClient(OdbcUtil.ODBC_SEVERITY_ERROR, cliMsg,
                                         stateCode, dataOutput);
                }
            } else {
                if (server.isTrace()) {
                    server.printWithThread("Result object error: "
                                           + errorResult.getMainString());
                }

                
                
                OdbcUtil.alertClient(OdbcUtil.ODBC_SEVERITY_ERROR,
                                     errorResult.getMainString(),
                                     errorResult.getSubString(), dataOutput);
            }

            switch (odbcCommMode) {

                case OdbcUtil.ODBC_SIMPLE_MODE :
                    outPacket.reset();                        
                    outPacket.writeByte('E');                 

                    
                    
                    
                    
                    outPacket.xmit('Z', dataOutput);
                    break;

                case OdbcUtil.ODBC_EXTENDED_MODE :
                    odbcCommMode = OdbcUtil.ODBC_EXT_RECOVER_MODE;

                    server.printWithThread("Reverting to EXT_RECOVER mode");
                    break;
            }
        }
    }

    
    public void run() {

        int msgType;

        init();

        if (session != null) {
            try {
                while (keepAlive) {
                    msgType = dataInput.readByte();

                    if (msgType < ResultConstants.MODE_UPPER_LIMIT) {
                        receiveResult(msgType);
                    } else {
                        receiveOdbcPacket((char) msgType);
                    }
                }
            } catch (CleanExit ce) {
                keepAlive = false;
            } catch (IOException e) {

                
                server.printWithThread(mThread + ":disconnected " + user);
            } catch (HsqlException e) {

                
                if (keepAlive) {
                    server.printStackTrace(e);
                }
            } catch (Throwable e) {

                
                if (keepAlive) {
                    server.printStackTrace(e);
                }
            }
        }

        close();
    }

    private Result setDatabase(Result resultIn) {

        try {
            String databaseName = resultIn.getDatabaseName();

            dbIndex = server.getDBIndex(databaseName);
            dbID    = server.dbID[dbIndex];
            user    = resultIn.getMainString();

            if (!server.isSilent()) {
                server.printWithThread(mThread + ":Trying to connect user '"
                                       + user + "' to DB (" + databaseName
                                       + ')');
            }

            session = DatabaseManager.newSession(dbID, user,
                                                 resultIn.getSubString(),
                                                 resultIn.getZoneString(),
                                                 resultIn.getUpdateCount());

            if (!server.isSilent()) {
                server.printWithThread(mThread + ":Connected user '" + user
                                       + "'");
            }

            return Result.newConnectionAcknowledgeResponse(
                session.getDatabase(), session.getId(),
                session.getDatabase().getDatabaseID());
        } catch (HsqlException e) {
            session = null;

            return Result.newErrorResult(e);
        } catch (RuntimeException e) {
            session = null;

            return Result.newErrorResult(e);
        }
    }

    
    String getConnectionThreadName() {
        return "HSQLDB Connection @" + Integer.toString(hashCode(), 16);
    }

    
    public static long MAX_WAIT_FOR_CLIENT_DATA   = 1000;    
    public static long CLIENT_DATA_POLLING_PERIOD = 100;     

    
    public int handshake() throws IOException {

        long clientDataDeadline = new java.util.Date().getTime()
                                  + MAX_WAIT_FOR_CLIENT_DATA;

        if (!(socket instanceof javax.net.ssl.SSLSocket)) {

            
            do {
                try {
                    Thread.sleep(CLIENT_DATA_POLLING_PERIOD);
                } catch (InterruptedException ie) {}
            } while (dataInput.available() < 5
                     && new java.util.Date().getTime() < clientDataDeadline);

            
            
            
            if (dataInput.available() < 1) {
                dataOutput.write(
                    (TEXTBANNER_PART1
                     + ClientConnection.NETWORK_COMPATIBILITY_VERSION
                     + TEXTBANNER_PART2 + '\n').getBytes());
                dataOutput.flush();

                throw Error.error(ErrorCode.SERVER_UNKNOWN_CLIENT);
            }
        }

        int firstInt = dataInput.readInt();

        switch (firstInt >> 24) {

            case 80 :    
                server.print(
                    "Rejected attempt from client using hsql HTTP protocol");

                return 0;

            case 0 :

                
                
                
                streamProtocol = ODBC_STREAM_PROTOCOL;
                break;

            default :
                streamProtocol = HSQL_STREAM_PROTOCOL;

            
        }

        return firstInt;
    }

    private void odbcConnect(int firstInt) throws IOException {

        
        int major = dataInput.readUnsignedShort();
        int minor = dataInput.readUnsignedShort();

        
        if (major == 1 && minor == 7) {

            
            
            server.print("A pre-9.0 client attempted to connect.  "
                         + "We rejected them.");

            return;
        }

        if (major == 1234 && minor == 5679) {

            
            dataOutput.writeByte('N');    

            
            odbcConnect(dataInput.readInt());

            return;
        }

        if (major == 1234 && minor == 5678) {

            
            if (firstInt != 16) {
                server.print(
                    "ODBC cancellation request sent wrong packet length: "
                    + firstInt);
            }

            server.print(
                "Got an ODBC cancelation request for thread ID "
                + dataInput.readInt() + ", but we don't support "
                + "OOB cancellation yet.  "
                + "Ignoring this request and closing the connection.");

            
            return;
        }

        server.printWithThread("ODBC client connected.  "
                               + "ODBC Protocol Compatibility Version "
                               + major + '.' + minor);

        OdbcPacketInputStream inPacket =
            OdbcPacketInputStream.newOdbcPacketInputStream('\0', dataInput,
                firstInt - 8);

        
        java.util.Map stringPairs = inPacket.readStringPairs();

        if (server.isTrace()) {
            server.print("String Pairs from ODBC client: " + stringPairs);
        }

        try {
            try {
                OdbcUtil.validateInputPacketSize(inPacket);
            } catch (RecoverableOdbcFailure rf) {

                
                throw new ClientFailure(rf.toString(), rf.getClientMessage());
            }

            inPacket.close();

            if (!stringPairs.containsKey("database")) {
                throw new ClientFailure("Client did not identify database",
                                        "Target database not identified");
            }

            if (!stringPairs.containsKey("user")) {
                throw new ClientFailure("Client did not identify user",
                                        "Target account not identified");
            }

            String databaseName = (String) stringPairs.get("database");

            user = (String) stringPairs.get("user");

            if (databaseName.equals("/")) {

                
                databaseName = "";
            }

            
            dataOutput.writeByte('R');
            dataOutput.writeInt(8);    
            dataOutput.writeInt(OdbcUtil.ODBC_AUTH_REQ_PASSWORD);
            dataOutput.flush();

            
            char c = '\0';

            try {
                c = (char) dataInput.readByte();
            } catch (EOFException eofe) {
                server.printWithThread(
                    "Looks like we got a goofy psql no-auth attempt.  "
                    + "Will probably retry properly very shortly");

                return;
            }

            if (c != 'p') {
                throw new ClientFailure(
                    "Expected password prefix 'p', " + "but got '" + c + "'",
                    "Password value not prefixed with 'p'");
            }

            int len = dataInput.readInt() - 5;

            
            if (len < 0) {
                throw new ClientFailure(
                    "Client submitted invalid password length " + len,
                    "Invalid password length " + len);
            }

            String password = ServerConnection.readNullTermdUTF(len,
                dataInput);

            dbIndex = server.getDBIndex(databaseName);
            dbID    = server.dbID[dbIndex];

            if (!server.isSilent()) {
                server.printWithThread(mThread + ":Trying to connect user '"
                                       + user + "' to DB (" + databaseName
                                       + ')');
            }

            try {
                session = DatabaseManager.newSession(dbID, user, password,
                                                     null, 0);

                
                
            } catch (Exception e) {
                throw new ClientFailure("User name or password denied: " + e,
                                        "Login attempt rejected");
            }
        } catch (ClientFailure cf) {
            server.print(cf.toString());

            
            OdbcUtil.alertClient(OdbcUtil.ODBC_SEVERITY_FATAL,
                                 cf.getClientMessage(), "08006", dataOutput);

            return;
        }

        outPacket = OdbcPacketOutputStream.newOdbcPacketOutputStream();

        outPacket.writeInt(OdbcUtil.ODBC_AUTH_REQ_OK);    
        outPacket.xmit('R', dataOutput);                  

        for (int i = 0; i < OdbcUtil.hardcodedParams.length; i++) {
            OdbcUtil.writeParam(OdbcUtil.hardcodedParams[i][0],
                                OdbcUtil.hardcodedParams[i][1], dataOutput);
        }

        
        
        outPacket.writeByte('I');           
        outPacket.xmit('Z', dataOutput);    

        
        
        OdbcUtil.alertClient(
            OdbcUtil.ODBC_SEVERITY_INFO,
            "MHello\nYou have connected to HyperSQL ODBC Server", dataOutput);
        dataOutput.flush();
    }

    private java.util.Map sessionOdbcPsMap     = new java.util.HashMap();
    private java.util.Map sessionOdbcPortalMap = new java.util.HashMap();

    
    private static String readNullTermdUTF(int reqLength,
                                           java.io.InputStream istream)
                                           throws IOException {

        
        int    bytesRead = 0;
        byte[] ba        = new byte[reqLength + 3];

        ba[0] = (byte) (reqLength >>> 8);
        ba[1] = (byte) reqLength;

        while (bytesRead < reqLength + 1) {
            bytesRead += istream.read(ba, 2 + bytesRead,
                                      reqLength + 1 - bytesRead);
        }

        if (ba[ba.length - 1] != 0) {
            throw new IOException("String not null-terminated");
        }

        for (int i = 2; i < ba.length - 1; i++) {
            if (ba[i] == 0) {
                throw new RuntimeException("Null internal to String at offset "
                                           + (i - 2));
            }
        }

        java.io.DataInputStream dis =
            new java.io.DataInputStream(new ByteArrayInputStream(ba));
        String s = dis.readUTF();

        
        
        
        dis.close();

        return s;
    }

    
    private int      streamProtocol            = UNDEFINED_STREAM_PROTOCOL;
    static final int UNDEFINED_STREAM_PROTOCOL = 0;
    static final int HSQL_STREAM_PROTOCOL      = 1;
    static final int ODBC_STREAM_PROTOCOL      = 2;
    int              odbcCommMode              = OdbcUtil.ODBC_SIMPLE_MODE;

    private void odbcExecDirect(String inStatement)
    throws RecoverableOdbcFailure, IOException {

        String statement = inStatement;
        String norm      = statement.trim().toLowerCase();

        if (norm.startsWith("release ")
                && !norm.startsWith("release savepoint")) {
            server.printWithThread(
                "Transmogrifying 'RELEASE ...' to 'RELEASE SAVEPOINT...");

            statement = statement.trim().substring(0, "release ".length())
                        + "SAVEPOINT "
                        + statement.trim().substring("release ".length());
        }

        Result r = Result.newExecuteDirectRequest();

        r.setPrepareOrExecuteProperties(
            statement, 0, 0, StatementTypes.RETURN_COUNT, 0,
            ResultProperties.defaultPropsValue,
            ResultConstants.RETURN_NO_GENERATED_KEYS, null, null);

        Result rOut = session.execute(r);

        switch (rOut.getType()) {

            case ResultConstants.UPDATECOUNT :
                break;

            case ResultConstants.ERROR :
                throw new RecoverableOdbcFailure(rOut);
            default :
                throw new RecoverableOdbcFailure(
                    "Output Result from execution is of "
                    + "unexpected type: " + rOut.getType());
        }

        outPacket.reset();
        outPacket.write(OdbcUtil.echoBackReplyString(norm,
                rOut.getUpdateCount()));

        
        
        outPacket.xmit('C', dataOutput);

        if (norm.equals("commit") || norm.startsWith("commit ")
                || norm.equals("rollback") || norm.startsWith("rollback ")) {
            try {
                session.setAutoCommit(true);
            } catch (HsqlException he) {
                throw new RecoverableOdbcFailure(
                    "Failed to change transaction state: " + he.getMessage(),
                    he.getSQLState());
            }
        }
    }
}
