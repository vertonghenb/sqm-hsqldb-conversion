package org.hsqldb;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.jdbc.JDBCConnection;
import org.hsqldb.lib.DataOutputStream;
import org.hsqldb.navigator.RowSetNavigatorClient;
import org.hsqldb.persist.HsqlProperties;
import org.hsqldb.result.Result;
import org.hsqldb.result.ResultConstants;
import org.hsqldb.result.ResultLob;
import org.hsqldb.rowio.RowInputBinary;
import org.hsqldb.rowio.RowOutputBinary;
import org.hsqldb.rowio.RowOutputInterface;
import org.hsqldb.server.HsqlSocketFactory;
import org.hsqldb.store.ValuePool;
import org.hsqldb.types.BlobDataID;
import org.hsqldb.types.ClobDataID;
import org.hsqldb.types.TimestampData;
public class ClientConnection implements SessionInterface {
    public static final String NETWORK_COMPATIBILITY_VERSION     = "2.1.0.0";
    public static final int    NETWORK_COMPATIBILITY_VERSION_INT = -2010000;
    static final int             BUFFER_SIZE = 0x1000;
    final byte[]                 mainBuffer  = new byte[BUFFER_SIZE];
    private boolean              isClosed;
    private Socket               socket;
    protected DataOutputStream   dataOutput;
    protected DataInputStream    dataInput;
    protected RowOutputInterface rowOut;
    protected RowInputBinary     rowIn;
    private Result               resultOut;
    private long                 sessionID;
    private long                 lobIDSequence = -1;
    private boolean  isReadOnlyDefault = false;
    private boolean  isAutoCommit      = true;
    private int      zoneSeconds;
    private Scanner  scanner;
    private String   zoneString;
    private Calendar calendar;
    JDBCConnection connection;
    String         host;
    int            port;
    String         path;
    String         database;
    boolean        isTLS;
    int            databaseID;
    String         clientPropertiesString;
    HsqlProperties clientProperties;
    String         databaseUniqueName;
    public ClientConnection(String host, int port, String path,
                            String database, boolean isTLS, String user,
                            String password, int timeZoneSeconds) {
        this.host        = host;
        this.port        = port;
        this.path        = path;
        this.database    = database;
        this.isTLS       = isTLS;
        this.zoneSeconds = timeZoneSeconds;
        this.zoneString  = TimeZone.getDefault().getID();
        initStructures();
        Result login = Result.newConnectionAttemptRequest(user, password,
            database, zoneString, timeZoneSeconds);
        initConnection(host, port, isTLS);
        Result resultIn = execute(login);
        if (resultIn.isError()) {
            throw Error.error(resultIn);
        }
        sessionID              = resultIn.getSessionId();
        databaseID             = resultIn.getDatabaseId();
        databaseUniqueName     = resultIn.getDatabaseName();
        clientPropertiesString = resultIn.getMainString();
    }
    private void initStructures() {
        RowOutputBinary rowOutTemp = new RowOutputBinary(mainBuffer);
        rowOut    = rowOutTemp;
        rowIn     = new RowInputBinary(rowOutTemp);
        resultOut = Result.newSessionAttributesResult();
    }
    protected void initConnection(String host, int port, boolean isTLS) {
        openConnection(host, port, isTLS);
    }
    protected void openConnection(String host, int port, boolean isTLS) {
        try {
            socket = HsqlSocketFactory.getInstance(isTLS).createSocket(host,
                                                   port);
            socket.setTcpNoDelay(true);
            dataOutput = new DataOutputStream(socket.getOutputStream());
            dataInput = new DataInputStream(
                new BufferedInputStream(socket.getInputStream()));
            handshake();
        } catch (Exception e) {
            throw new HsqlException(e, Error.getStateString(ErrorCode.X_08001),
                                    -ErrorCode.X_08001);
        }
    }
    protected void closeConnection() {
        try {
            if (socket != null) {
                socket.close();
            }
        } catch (Exception e) {}
        socket = null;
    }
    public synchronized Result execute(Result r) {
        try {
            r.setSessionId(sessionID);
            r.setDatabaseId(databaseID);
            write(r);
            return read();
        } catch (Throwable e) {
            throw Error.error(ErrorCode.X_08006, e.toString());
        }
    }
    public synchronized RowSetNavigatorClient getRows(long navigatorId,
            int offset, int size) {
        try {
            resultOut.setResultType(ResultConstants.REQUESTDATA);
            resultOut.setResultId(navigatorId);
            resultOut.setUpdateCount(offset);
            resultOut.setFetchSize(size);
            Result result = execute(resultOut);
            return (RowSetNavigatorClient) result.getNavigator();
        } catch (Throwable e) {
            throw Error.error(ErrorCode.X_08006, e.toString());
        }
    }
    public synchronized void closeNavigator(long navigatorId) {
        try {
            resultOut.setResultType(ResultConstants.CLOSE_RESULT);
            resultOut.setResultId(navigatorId);
            execute(resultOut);
        } catch (Throwable e) {}
    }
    public synchronized void close() {
        if (isClosed) {
            return;
        }
        isClosed = true;
        try {
            resultOut.setResultType(ResultConstants.DISCONNECT);
            execute(resultOut);
        } catch (Exception e) {}
        try {
            closeConnection();
        } catch (Exception e) {}
    }
    public synchronized Object getAttribute(int id) {
        resultOut.setResultType(ResultConstants.GETSESSIONATTR);
        resultOut.setStatementType(id);
        Result in = execute(resultOut);
        if (in.isError()) {
            throw Error.error(in);
        }
        Object[] data = in.getSingleRowData();
        switch (id) {
            case SessionInterface.INFO_AUTOCOMMIT :
                return data[SessionInterface.INFO_BOOLEAN];
            case SessionInterface.INFO_CONNECTION_READONLY :
                return data[SessionInterface.INFO_BOOLEAN];
            case SessionInterface.INFO_ISOLATION :
                return data[SessionInterface.INFO_INTEGER];
            case SessionInterface.INFO_CATALOG :
                return data[SessionInterface.INFO_VARCHAR];
        }
        return null;
    }
    public synchronized void setAttribute(int id, Object value) {
        resultOut.setResultType(ResultConstants.SETSESSIONATTR);
        Object[] data = resultOut.getSingleRowData();
        data[SessionInterface.INFO_ID] = ValuePool.getInt(id);
        switch (id) {
            case SessionInterface.INFO_AUTOCOMMIT :
            case SessionInterface.INFO_CONNECTION_READONLY :
                data[SessionInterface.INFO_BOOLEAN] = value;
                break;
            case SessionInterface.INFO_ISOLATION :
                data[SessionInterface.INFO_INTEGER] = value;
                break;
            case SessionInterface.INFO_CATALOG :
                data[SessionInterface.INFO_VARCHAR] = value;
                break;
        }
        Result resultIn = execute(resultOut);
        if (resultIn.isError()) {
            throw Error.error(resultIn);
        }
    }
    public synchronized boolean isReadOnlyDefault() {
        Object info = getAttribute(SessionInterface.INFO_CONNECTION_READONLY);
        isReadOnlyDefault = ((Boolean) info).booleanValue();
        return isReadOnlyDefault;
    }
    public synchronized void setReadOnlyDefault(boolean mode) {
        if (mode != isReadOnlyDefault) {
            setAttribute(SessionInterface.INFO_CONNECTION_READONLY,
                         mode ? Boolean.TRUE
                              : Boolean.FALSE);
            isReadOnlyDefault = mode;
        }
    }
    public synchronized boolean isAutoCommit() {
        Object info = getAttribute(SessionInterface.INFO_AUTOCOMMIT);
        isAutoCommit = ((Boolean) info).booleanValue();
        return isAutoCommit;
    }
    public synchronized void setAutoCommit(boolean mode) {
        if (mode != isAutoCommit) {
            setAttribute(SessionInterface.INFO_AUTOCOMMIT, mode ? Boolean.TRUE
                                                                : Boolean
                                                                .FALSE);
            isAutoCommit = mode;
        }
    }
    public synchronized void setIsolationDefault(int level) {
        setAttribute(SessionInterface.INFO_ISOLATION, ValuePool.getInt(level));
    }
    public synchronized int getIsolation() {
        Object info = getAttribute(SessionInterface.INFO_ISOLATION);
        return ((Integer) info).intValue();
    }
    public synchronized boolean isClosed() {
        return isClosed;
    }
    public Session getSession() {
        return null;
    }
    public synchronized void startPhasedTransaction() {}
    public synchronized void prepareCommit() {
        resultOut.setAsTransactionEndRequest(ResultConstants.PREPARECOMMIT,
                                             null);
        Result in = execute(resultOut);
        if (in.isError()) {
            throw Error.error(in);
        }
    }
    public synchronized void commit(boolean chain) {
        resultOut.setAsTransactionEndRequest(ResultConstants.TX_COMMIT, null);
        Result in = execute(resultOut);
        if (in.isError()) {
            throw Error.error(in);
        }
    }
    public synchronized void rollback(boolean chain) {
        resultOut.setAsTransactionEndRequest(ResultConstants.TX_ROLLBACK,
                                             null);
        Result in = execute(resultOut);
        if (in.isError()) {
            throw Error.error(in);
        }
    }
    public synchronized void rollbackToSavepoint(String name) {
        resultOut.setAsTransactionEndRequest(
            ResultConstants.TX_SAVEPOINT_NAME_ROLLBACK, name);
        Result in = execute(resultOut);
        if (in.isError()) {
            throw Error.error(in);
        }
    }
    public synchronized void savepoint(String name) {
        Result result = Result.newSetSavepointRequest(name);
        Result in     = execute(result);
        if (in.isError()) {
            throw Error.error(in);
        }
    }
    public synchronized void releaseSavepoint(String name) {
        resultOut.setAsTransactionEndRequest(
            ResultConstants.TX_SAVEPOINT_NAME_RELEASE, name);
        Result in = execute(resultOut);
        if (in.isError()) {
            throw Error.error(in);
        }
    }
    public void addWarning(HsqlException warning) {}
    public synchronized long getId() {
        return sessionID;
    }
    public synchronized void resetSession() {
        Result login    = Result.newResetSessionRequest();
        Result resultIn = execute(login);
        if (resultIn.isError()) {
            isClosed = true;
            closeConnection();
            throw Error.error(resultIn);
        }
        sessionID  = resultIn.getSessionId();
        databaseID = resultIn.getDatabaseId();
    }
    protected void write(Result r) throws IOException, HsqlException {
        r.write(this, dataOutput, rowOut);
    }
    protected Result read() throws IOException, HsqlException {
        Result result = Result.newResult(dataInput, rowIn);
        result.readAdditionalResults(this, dataInput, rowIn);
        rowOut.setBuffer(mainBuffer);
        rowIn.resetRow(mainBuffer.length);
        return result;
    }
    public synchronized String getInternalConnectionURL() {
        return null;
    }
    public synchronized long getLobId() {
        return lobIDSequence--;
    }
    public BlobDataID createBlob(long length) {
        BlobDataID blob = new BlobDataID(getLobId());
        return blob;
    }
    public ClobDataID createClob(long length) {
        ClobDataID clob = new ClobDataID(getLobId());
        return clob;
    }
    public void allocateResultLob(ResultLob resultLob,
                                  InputStream dataInput) {}
    public Scanner getScanner() {
        if (scanner == null) {
            scanner = new Scanner();
        }
        return scanner;
    }
    public Calendar getCalendar() {
        if (calendar == null) {
            TimeZone zone = TimeZone.getTimeZone(zoneString);
            calendar = new GregorianCalendar(zone);
        }
        return calendar;
    }
    public TimestampData getCurrentDate() {
        long currentMillis = System.currentTimeMillis();
        long seconds = HsqlDateTime.getCurrentDateMillis(currentMillis) / 1000;
        return new TimestampData(seconds);
    }
    public int getZoneSeconds() {
        return zoneSeconds;
    }
    public int getStreamBlockSize() {
        return lobStreamBlockSize;
    }
    public HsqlProperties getClientProperties() {
        if (clientProperties == null) {
            if (clientPropertiesString.length() > 0) {
                clientProperties = HsqlProperties.delimitedArgPairsToProps(
                    clientPropertiesString, "=", ";", null);
            } else {
                clientProperties = new HsqlProperties();
            }
        }
        return clientProperties;
    }
    public JDBCConnection getJDBCConnection() {
        return connection;
    }
    public void setJDBCConnection(JDBCConnection connection) {
        this.connection = connection;
    }
    public String getDatabaseUniqueName() {
        return databaseUniqueName;
    }
    public static String toNetCompVersionString(int i) {
        StringBuffer sb = new StringBuffer();
        i *= -1;
        sb.append(i / 1000000);
        i %= 1000000;
        sb.append('.');
        sb.append(i / 10000);
        i %= 10000;
        sb.append('.');
        sb.append(i / 100);
        i %= 100;
        sb.append('.');
        sb.append(i);
        return sb.toString();
    }
    protected void handshake() throws IOException {
        dataOutput.writeInt(NETWORK_COMPATIBILITY_VERSION_INT);
        dataOutput.flush();
    }
}