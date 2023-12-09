


package org.hsqldb;

import java.io.InputStream;
import java.util.Calendar;

import org.hsqldb.jdbc.JDBCConnection;
import org.hsqldb.navigator.RowSetNavigatorClient;
import org.hsqldb.persist.HsqlProperties;
import org.hsqldb.result.Result;
import org.hsqldb.result.ResultLob;
import org.hsqldb.types.BlobDataID;
import org.hsqldb.types.ClobDataID;
import org.hsqldb.types.TimestampData;


public interface SessionInterface {

    int INFO_ID      = 0;                
    int INFO_INTEGER = 1;                
    int INFO_BOOLEAN = 2;                
    int INFO_VARCHAR = 3;                
    int INFO_LIMIT   = 4;

    
    int INFO_ISOLATION           = 0;    
    int INFO_AUTOCOMMIT          = 1;    
    int INFO_CONNECTION_READONLY = 2;    
    int INFO_CATALOG             = 3;    

    
    int TX_READ_UNCOMMITTED = 1;
    int TX_READ_COMMITTED   = 2;
    int TX_REPEATABLE_READ  = 4;
    int TX_SERIALIZABLE     = 8;

    
    int lobStreamBlockSize = 512 * 1024;

    Result execute(Result r);

    RowSetNavigatorClient getRows(long navigatorId, int offset, int size);

    void closeNavigator(long id);

    void close();

    boolean isClosed();

    boolean isReadOnlyDefault();

    void setReadOnlyDefault(boolean readonly);

    boolean isAutoCommit();

    void setAutoCommit(boolean autoCommit);

    int getIsolation();

    void setIsolationDefault(int level);

    void startPhasedTransaction();

    void prepareCommit();

    void commit(boolean chain);

    void rollback(boolean chain);

    void rollbackToSavepoint(String name);

    void savepoint(String name);

    void releaseSavepoint(String name);

    void addWarning(HsqlException warning);

    Object getAttribute(int id);

    void setAttribute(int id, Object value);

    long getId();

    void resetSession();

    String getInternalConnectionURL();

    BlobDataID createBlob(long length);

    ClobDataID createClob(long length);

    void allocateResultLob(ResultLob result, InputStream dataInput);

    Scanner getScanner();

    Calendar getCalendar();

    TimestampData getCurrentDate();

    int getZoneSeconds();

    int getStreamBlockSize();

    HsqlProperties getClientProperties();

    JDBCConnection getJDBCConnection();

    void setJDBCConnection(JDBCConnection connection);

    String getDatabaseUniqueName();
}
