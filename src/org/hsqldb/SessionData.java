package org.hsqldb;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.CharArrayWriter;
import org.hsqldb.lib.CountdownInputStream;
import org.hsqldb.lib.HashMap;
import org.hsqldb.lib.HsqlByteArrayOutputStream;
import org.hsqldb.lib.Iterator;
import org.hsqldb.lib.LongDeque;
import org.hsqldb.lib.LongKeyHashMap;
import org.hsqldb.lib.LongKeyLongValueHashMap;
import org.hsqldb.lib.ReaderInputStream;
import org.hsqldb.navigator.RowSetNavigator;
import org.hsqldb.navigator.RowSetNavigatorClient;
import org.hsqldb.persist.PersistentStore;
import org.hsqldb.persist.PersistentStoreCollectionSession;
import org.hsqldb.result.Result;
import org.hsqldb.result.ResultLob;
import org.hsqldb.result.ResultProperties;
import org.hsqldb.types.BlobData;
import org.hsqldb.types.BlobDataID;
import org.hsqldb.types.ClobData;
import org.hsqldb.types.ClobDataID;
import org.hsqldb.types.LobData;
public class SessionData {
    private final Database                  database;
    private final Session                   session;
    public PersistentStoreCollectionSession persistentStoreCollection;
    LongKeyHashMap resultMap;
    Object currentValue;
    HashMap sequenceMap;
    HashMap sequenceUpdateMap;
    LongDeque newLobIDs;
    public SessionData(Database database, Session session) {
        this.database = database;
        this.session  = session;
        persistentStoreCollection =
            new PersistentStoreCollectionSession(session);
    }
    public PersistentStore getSubqueryRowStore(TableBase table) {
        PersistentStore store = persistentStoreCollection.getStore(table);
        store.removeAll();
        return store;
    }
    public PersistentStore getNewResultRowStore(TableBase table,
            boolean isCached) {
        try {
            PersistentStore store = session.database.logger.newStore(session,
                persistentStoreCollection, table);
            if (!isCached) {
                store.setMemory(true);
            }
            return store;
        } catch (HsqlException e) {}
        throw Error.runtimeError(ErrorCode.U_S0500, "SessionData");
    }
    void setResultSetProperties(Result command, Result result) {
        int required = command.rsProperties;
        int returned = result.rsProperties;
        if (required != returned) {
            if (ResultProperties.isReadOnly(required)) {
                returned = ResultProperties.addHoldable(returned,
                        ResultProperties.isHoldable(required));
            } else {
                if (ResultProperties.isUpdatable(returned)) {
                    if (ResultProperties.isHoldable(required)) {
                        session.addWarning(Error.error(ErrorCode.W_36503));
                    }
                } else {
                    returned = ResultProperties.addHoldable(returned,
                            ResultProperties.isHoldable(required));
                    session.addWarning(Error.error(ErrorCode.W_36502));
                }
            }
            if (ResultProperties.isSensitive(required)) {
                session.addWarning(Error.error(ErrorCode.W_36501));
            }
            returned = ResultProperties.addScrollable(returned,
                    ResultProperties.isScrollable(required));
            result.rsProperties = returned;
        }
    }
    Result getDataResultHead(Result command, Result result,
                             boolean isNetwork) {
        int fetchSize = command.getFetchSize();
        result.setResultId(session.actionTimestamp);
        int required = command.rsProperties;
        int returned = result.rsProperties;
        if (required != returned) {
            if (ResultProperties.isReadOnly(required)) {
                returned = ResultProperties.addHoldable(returned,
                        ResultProperties.isHoldable(required));
            } else {
                if (ResultProperties.isReadOnly(returned)) {
                    returned = ResultProperties.addHoldable(returned,
                            ResultProperties.isHoldable(required));
                } else {
                    if (session.isAutoCommit()) {
                        returned = ResultProperties.addHoldable(returned,
                                ResultProperties.isHoldable(required));
                    } else {
                        returned = ResultProperties.addHoldable(returned,
                                false);
                    }
                }
            }
            returned = ResultProperties.addScrollable(returned,
                    ResultProperties.isScrollable(required));
            result.rsProperties = returned;
        }
        boolean hold = false;
        boolean copy = false;
        if (ResultProperties.isUpdatable(result.rsProperties)) {
            hold = true;
        }
        if (isNetwork) {
            if (fetchSize != 0
                    && result.getNavigator().getSize() > fetchSize) {
                copy = true;
                hold = true;
            }
        } else {
            if (!result.getNavigator().isMemory()) {
                hold = true;
            }
        }
        if (hold) {
            if (resultMap == null) {
                resultMap = new LongKeyHashMap();
            }
            resultMap.put(result.getResultId(), result);
        }
        if (copy) {
            result = Result.newDataHeadResult(session, result, 0, fetchSize);
        }
        return result;
    }
    Result getDataResultSlice(long id, int offset, int count) {
        Result          result = (Result) resultMap.get(id);
        RowSetNavigator source = result.getNavigator();
        if (offset + count > source.getSize()) {
            count = source.getSize() - offset;
        }
        return Result.newDataRowsResult(result, offset, count);
    }
    Result getDataResult(long id) {
        Result result = (Result) resultMap.get(id);
        return result;
    }
    RowSetNavigatorClient getRowSetSlice(long id, int offset, int count) {
        Result          result = (Result) resultMap.get(id);
        RowSetNavigator source = result.getNavigator();
        if (offset + count > source.getSize()) {
            count = source.getSize() - offset;
        }
        return new RowSetNavigatorClient(source, offset, count);
    }
    public void closeNavigator(long id) {
        Result result = (Result) resultMap.remove(id);
        result.getNavigator().release();
    }
    public void closeAllNavigators() {
        if (resultMap == null) {
            return;
        }
        Iterator it = resultMap.values().iterator();
        while (it.hasNext()) {
            Result result = (Result) it.next();
            result.getNavigator().release();
        }
        resultMap.clear();
    }
    public void closeAllTransactionNavigators() {
        if (resultMap == null) {
            return;
        }
        Iterator it = resultMap.values().iterator();
        while (it.hasNext()) {
            Result result = (Result) it.next();
            if (!ResultProperties.isHoldable(result.rsProperties)) {
                result.getNavigator().release();
                it.remove();
            }
        }
    }
    public void registerNewLob(long lobID) {
        if (newLobIDs == null) {
            newLobIDs = new LongDeque();
        }
        newLobIDs.add(lobID);
        hasLobOps = true;
    }
    public LongDeque getNewLobIDs() {
        return newLobIDs;
    }
    public void clearNewLobIDs() {
        if (newLobIDs != null) {
            newLobIDs.clear();
        }
    }
    LongKeyLongValueHashMap resultLobs = new LongKeyLongValueHashMap();
    boolean hasLobOps;
    public void adjustLobUsageCount(Object value, int adjust) {
        if (session.isProcessingLog || session.isProcessingScript) {
            return;
        }
        if (value == null) {
            return;
        }
        database.lobManager.adjustUsageCount(session,
                                             ((LobData) value).getId(),
                                             adjust);
        hasLobOps = true;
    }
    public void adjustLobUsageCount(TableBase table, Object[] data,
                                    int adjust) {
        if (!table.hasLobColumn) {
            return;
        }
        if (table.isTemp) {
            return;
        }
        if (session.isProcessingLog || session.isProcessingScript) {
            return;
        }
        for (int j = 0; j < table.columnCount; j++) {
            if (table.colTypes[j].isLobType()) {
                Object value = data[j];
                if (value == null) {
                    continue;
                }
                database.lobManager.adjustUsageCount(session,
                                                     ((LobData) value).getId(),
                                                     adjust);
                hasLobOps = true;
            }
        }
    }
    public void allocateLobForResult(ResultLob result,
                                     InputStream inputStream) {
        try {
            CountdownInputStream countStream;
            switch (result.getSubType()) {
                case ResultLob.LobResultTypes.REQUEST_CREATE_BYTES : {
                    long blobId;
                    long blobLength = result.getBlockLength();
                    if (blobLength < 0) {
                        allocateBlobSegments(result, result.getInputStream());
                        break;
                    }
                    if (inputStream == null) {
                        blobId      = result.getLobID();
                        inputStream = result.getInputStream();
                    } else {
                        BlobData blob = session.createBlob(blobLength);
                        blobId = blob.getId();
                        resultLobs.put(result.getLobID(), blobId);
                    }
                    countStream = new CountdownInputStream(inputStream);
                    countStream.setCount(blobLength);
                    database.lobManager.setBytesForNewBlob(
                        blobId, countStream, result.getBlockLength());
                    break;
                }
                case ResultLob.LobResultTypes.REQUEST_CREATE_CHARS : {
                    long clobId;
                    long clobLength = result.getBlockLength();
                    if (clobLength < 0) {
                        allocateClobSegments(result, result.getReader());
                        break;
                    }
                    if (inputStream == null) {
                        clobId = result.getLobID();
                        if (result.getReader() != null) {
                            inputStream =
                                new ReaderInputStream(result.getReader());
                        } else {
                            inputStream = result.getInputStream();
                        }
                    } else {
                        ClobData clob = session.createClob(clobLength);
                        clobId = clob.getId();
                        resultLobs.put(result.getLobID(), clobId);
                    }
                    countStream = new CountdownInputStream(inputStream);
                    countStream.setCount(clobLength * 2);
                    database.lobManager.setCharsForNewClob(
                        clobId, countStream, result.getBlockLength(), false);
                    break;
                }
                case ResultLob.LobResultTypes.REQUEST_SET_BYTES : {
                    long   blobId     = resultLobs.get(result.getLobID());
                    long   dataLength = result.getBlockLength();
                    byte[] byteArray  = result.getByteArray();
                    Result actionResult = database.lobManager.setBytes(blobId,
                        result.getOffset(), byteArray, (int) dataLength);
                    break;
                }
                case ResultLob.LobResultTypes.REQUEST_SET_CHARS : {
                    long   clobId     = resultLobs.get(result.getLobID());
                    long   dataLength = result.getBlockLength();
                    char[] charArray  = result.getCharArray();
                    Result actionResult = database.lobManager.setChars(clobId,
                        result.getOffset(), charArray);
                    break;
                }
            }
        } catch (Throwable e) {
            resultLobs.clear();
            throw Error.error(ErrorCode.GENERAL_ERROR, e);
        }
    }
    private void allocateBlobSegments(ResultLob result,
                                      InputStream stream) throws IOException {
        long currentOffset = result.getOffset();
        int  bufferLength  = session.getStreamBlockSize();
        HsqlByteArrayOutputStream byteArrayOS =
            new HsqlByteArrayOutputStream(bufferLength);
        while (true) {
            byteArrayOS.reset();
            byteArrayOS.write(stream, bufferLength);
            byte[] byteArray = byteArrayOS.getBuffer();
            Result actionResult =
                database.lobManager.setBytes(result.getLobID(), currentOffset,
                                             byteArray, byteArrayOS.size());
            currentOffset += byteArrayOS.size();
            if (byteArrayOS.size() < bufferLength) {
                return;
            }
        }
    }
    private void allocateClobSegments(ResultLob result,
                                      Reader reader) throws IOException {
        long            currentOffset = result.getOffset();
        int             bufferLength  = session.getStreamBlockSize();
        CharArrayWriter charWriter    = new CharArrayWriter(bufferLength);
        while (true) {
            charWriter.reset();
            charWriter.write(reader, bufferLength);
            char[] charArray = charWriter.getBuffer();
            if (charWriter.size() < bufferLength) {
                charArray = charWriter.toCharArray();
            }
            Result actionResult =
                database.lobManager.setChars(result.getLobID(), currentOffset,
                                             charArray);
            currentOffset += charWriter.size();
            if (charWriter.size() < bufferLength) {
                return;
            }
        }
    }
    public void registerLobForResult(Result result) {
        RowSetNavigator navigator = result.getNavigator();
        if (navigator == null) {
            registerLobsForRow((Object[]) result.valueData);
        } else {
            while (navigator.next()) {
                Object[] data = navigator.getCurrent();
                registerLobsForRow(data);
            }
            navigator.reset();
        }
        resultLobs.clear();
    }
    private void registerLobsForRow(Object[] data) {
        for (int i = 0; i < data.length; i++) {
            if (data[i] instanceof BlobDataID) {
                BlobData blob = (BlobDataID) data[i];
                long     id   = blob.getId();
                if (id < 0) {
                    id = resultLobs.get(id);
                }
                data[i] = database.lobManager.getBlob(id);
            } else if (data[i] instanceof ClobDataID) {
                ClobData clob = (ClobDataID) data[i];
                long     id   = clob.getId();
                if (id < 0) {
                    id = resultLobs.get(id);
                }
                data[i] = database.lobManager.getClob(id);
            }
        }
    }
    ClobData createClobFromFile(String filename, String encoding) {
        session.checkAdmin();
        filename = database.logger.getSecurePath(filename);
        if (filename == null) {
            throw (Error.error(ErrorCode.ACCESS_IS_DENIED, filename));
        }
        File    file   = new File(filename);
        boolean exists = file.exists();
        if (!exists) {
            throw Error.error(ErrorCode.FILE_IO_ERROR);
        }
        long        fileLength = file.length();
        InputStream is         = null;
        try {
            ClobData clob = session.createClob(fileLength);
            is = new FileInputStream(file);
            Reader reader = new InputStreamReader(is, encoding);
            is = new ReaderInputStream(reader);
            database.lobManager.setCharsForNewClob(clob.getId(), is,
                                                   fileLength, true);
            return clob;
        } catch (IOException e) {
            throw Error.error(ErrorCode.FILE_IO_ERROR, e.toString());
        } finally {
            try {
                is.close();
            } catch (Exception e) {}
        }
    }
    BlobData createBlobFromFile(String filename) {
        session.checkAdmin();
        filename = database.logger.getSecurePath(filename);
        if (filename == null) {
            throw (Error.error(ErrorCode.ACCESS_IS_DENIED, filename));
        }
        File    file   = new File(filename);
        boolean exists = file.exists();
        if (!exists) {
            throw Error.error(ErrorCode.FILE_IO_ERROR);
        }
        long        fileLength = file.length();
        InputStream is         = null;
        try {
            BlobData blob = session.createBlob(fileLength);
            is = new FileInputStream(file);
            database.lobManager.setBytesForNewBlob(blob.getId(), is,
                                                   fileLength);
            return blob;
        } catch (IOException e) {
            throw Error.error(ErrorCode.FILE_IO_ERROR);
        } finally {
            try {
                is.close();
            } catch (Exception e) {}
        }
    }
    public void startRowProcessing() {
        if (sequenceMap != null) {
            sequenceMap.clear();
        }
    }
    public Object getSequenceValue(NumberSequence sequence) {
        if (sequenceMap == null) {
            sequenceMap       = new HashMap();
            sequenceUpdateMap = new HashMap();
        }
        HsqlName key   = sequence.getName();
        Object   value = sequenceMap.get(key);
        if (value == null) {
            value = sequence.getValueObject();
            sequenceMap.put(key, value);
            sequenceUpdateMap.put(sequence, value);
        }
        return value;
    }
    public Object getSequenceCurrent(NumberSequence sequence) {
        return sequenceUpdateMap == null ? null
                                         : sequenceUpdateMap.get(sequence);
    }
}