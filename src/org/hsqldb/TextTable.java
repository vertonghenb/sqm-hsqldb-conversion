


package org.hsqldb;

import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.StringConverter;
import org.hsqldb.persist.PersistentStore;
import org.hsqldb.persist.TextCache;
import org.hsqldb.persist.TextFileReader;
import org.hsqldb.rowio.RowInputInterface;




public class TextTable extends org.hsqldb.Table {

    String  dataSource  = "";
    boolean isReversed  = false;
    boolean isConnected = false;



    
    TextTable(Database db, HsqlNameManager.HsqlName name, int type) {

        super(db, name, type);

        isWithDataSource = true;
    }

    public boolean isConnected() {
        return isConnected;
    }

    
    public void connect(Session session) {
        connect(session, isReadOnly);
    }

    
    private void connect(Session session, boolean withReadOnlyData) {

        
        if ((dataSource.length() == 0) || isConnected) {

            
            return;
        }

        PersistentStore store =
            database.persistentStoreCollection.getStore(this);

        this.store = store;

        TextCache      cache  = null;
        TextFileReader reader = null;

        try {
            cache = (TextCache) database.logger.openTextFilePersistence(this,
                    dataSource, withReadOnlyData, isReversed);

            store.setCache(cache);

            reader = cache.getTextFileReader();

            
            Row row     = null;
            int nextpos = 0;

            if (cache.isIgnoreFirstLine()) {
                nextpos += reader.readHeaderLine();
                cache.setHeaderInitialise(reader.getHeaderLine());
            }

            while (true) {
                RowInputInterface rowIn = reader.readObject(nextpos);

                if (rowIn == null) {
                    break;
                }

                row = (Row) store.get(rowIn);

                if (row == null) {
                    break;
                }

                Object[] data = row.getData();

                nextpos = (int) row.getPos() + row.getStorageSize();

                systemUpdateIdentityValue(data);
                enforceRowConstraints(session, data);
                store.indexRow(session, row);
            }
        } catch (Throwable t) {
            int linenumber = reader == null ? 0
                                            : reader.getLineNumber();

            clearAllData(session);

            if (cache != null) {
                database.logger.closeTextCache(this);
                store.release();
            }

            
            
            
            throw Error.error(t, ErrorCode.TEXT_FILE, 0, new Object[] {
                new Integer(linenumber), t.toString()
            });
        }

        isConnected = true;
        isReadOnly  = withReadOnlyData;
    }

    
    public void disconnect() {

        this.store = null;

        PersistentStore store =
            database.persistentStoreCollection.getStore(this);

        store.release();

        isConnected = false;
    }

    
    private void openCache(Session session, String dataSourceNew,
                           boolean isReversedNew, boolean isReadOnlyNew) {

        String  dataSourceOld = dataSource;
        boolean isReversedOld = isReversed;
        boolean isReadOnlyOld = isReadOnly;

        if (dataSourceNew == null) {
            dataSourceNew = "";
        }

        disconnect();

        dataSource = dataSourceNew;
        isReversed = (isReversedNew && dataSource.length() > 0);

        try {
            connect(session, isReadOnlyNew);
        } catch (HsqlException e) {
            dataSource = dataSourceOld;
            isReversed = isReversedOld;

            connect(session, isReadOnlyOld);

            throw e;
        }
    }

    
    protected void setDataSource(Session session, String dataSourceNew,
                                 boolean isReversedNew, boolean createFile) {

        if (getTableType() == Table.TEMP_TEXT_TABLE) {
            ;
        } else {
            session.getGrantee().checkSchemaUpdateOrGrantRights(
                getSchemaName().name);
        }

        dataSourceNew = dataSourceNew.trim();

        
        if (isReversedNew || (isReversedNew != isReversed)
                || !dataSource.equals(dataSourceNew) || !isConnected) {
            openCache(session, dataSourceNew, isReversedNew, isReadOnly);
        }

        if (isReversed) {
            isReadOnly = true;
        }
    }

    public String getDataSource() {
        return dataSource;
    }

    public boolean isDescDataSource() {
        return isReversed;
    }

    public void setHeader(String header) {

        PersistentStore store =
            database.persistentStoreCollection.getStore(this);
        TextCache cache = (TextCache) store.getCache();

        if (cache != null && cache.isIgnoreFirstLine()) {
            cache.setHeader(header);

            return;
        }

        throw Error.error(ErrorCode.TEXT_TABLE_HEADER);
    }

    public String getHeader() {

        PersistentStore store =
            database.persistentStoreCollection.getStore(this);
        TextCache cache  = (TextCache) store.getCache();
        String    header = cache == null ? null
                                         : cache.getHeader();

        return header == null ? null
                              : StringConverter.toQuotedString(header, '\'',
                              true);
    }

    
    void checkDataReadOnly() {

        if (dataSource.length() == 0) {
            throw Error.error(ErrorCode.TEXT_TABLE_UNKNOWN_DATA_SOURCE);
        }

        if (isReadOnly) {
            throw Error.error(ErrorCode.DATA_IS_READONLY);
        }
    }

    public boolean isDataReadOnly() {
        return !isConnected() || super.isDataReadOnly()
               || store.getCache().isDataReadOnly();
    }

    public void setDataReadOnly(boolean value) {

        if (!value) {
            if (isReversed) {
                throw Error.error(ErrorCode.DATA_IS_READONLY);
            }

            if (database.isFilesReadOnly()) {
                throw Error.error(ErrorCode.DATABASE_IS_READONLY);
            }

            if (isConnected()) {
                store.getCache().close(true);
                store.getCache().open(value);
            }
        }

        isReadOnly = value;
    }

    boolean isIndexCached() {
        return false;
    }

    void setIndexRoots(String s) {

        
    }

    String getDataSourceDDL() {

        String dataSource = getDataSource();

        if (dataSource == null) {
            return null;
        }

        StringBuffer sb = new StringBuffer(128);

        sb.append(Tokens.T_SET).append(' ').append(Tokens.T_TABLE).append(' ');
        sb.append(getName().getSchemaQualifiedStatementName());
        sb.append(' ').append(Tokens.T_SOURCE).append(' ').append('\'');
        sb.append(dataSource);
        sb.append('\'');

        return sb.toString();
    }

    
    String getDataSourceHeader() {

        String header = getHeader();

        if (header == null) {
            return null;
        }

        StringBuffer sb = new StringBuffer(128);

        sb.append(Tokens.T_SET).append(' ').append(Tokens.T_TABLE).append(' ');
        sb.append(getName().getSchemaQualifiedStatementName());
        sb.append(' ').append(Tokens.T_SOURCE).append(' ');
        sb.append(Tokens.T_HEADER).append(' ');
        sb.append(header);

        return sb.toString();
    }

    
    public void insertData(Session session, PersistentStore store,
                           Object[] data) {

        Row row = (Row) store.getNewCachedObject(session, data, false);

        store.indexRow(session, row);
        store.commitPersistence(row);
    }
}
