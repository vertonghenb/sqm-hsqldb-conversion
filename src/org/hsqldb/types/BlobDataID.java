package org.hsqldb.types;
import java.io.InputStream;
import java.io.OutputStream;
import org.hsqldb.SessionInterface;
import org.hsqldb.error.Error;
import org.hsqldb.result.Result;
import org.hsqldb.result.ResultLob;
public class BlobDataID implements BlobData {
    long id;
    long length = -1;
    public BlobDataID(long id) {
        this.id = id;
    }
    public BlobData duplicate(SessionInterface session) {
        ResultLob resultOut = ResultLob.newLobDuplicateRequest(id);
        Result    resultIn  = session.execute(resultOut);
        if (resultIn.isError()) {
            throw resultIn.getException();
        }
        long lobID = ((ResultLob) resultIn).getLobID();
        return new BlobDataID(lobID);
    }
    public void free() {}
    public InputStream getBinaryStream(SessionInterface session) {
        long length = length(session);
        return new BlobInputStream(session, this, 0, length);
    }
    public InputStream getBinaryStream(SessionInterface session, long pos,
                                       long length) {
        return new BlobInputStream(session, this, pos, length);
    }
    public byte[] getBytes() {
        return null;
    }
    public byte[] getBytes(SessionInterface session, long pos, int length) {
        ResultLob resultOut = ResultLob.newLobGetBytesRequest(id, pos, length);
        Result    resultIn  = session.execute(resultOut);
        if (resultIn.isError()) {
            throw Error.error(resultIn);
        }
        return ((ResultLob) resultIn).getByteArray();
    }
    public BlobData getBlob(SessionInterface session, long pos, long length) {
        ResultLob resultOut = ResultLob.newLobGetRequest(id, pos, length);
        Result    resultIn  = session.execute(resultOut);
        if (resultIn.isError()) {
            throw Error.error(resultIn);
        }
        long lobID = ((ResultLob) resultIn).getLobID();
        return new BlobDataID(lobID);
    }
    public long getId() {
        return id;
    }
    public void setId(long id) {
        this.id = id;
    }
    public int getStreamBlockSize() {
        return 0;
    }
    public boolean isClosed() {
        return false;
    }
    public long length(SessionInterface session) {
        if (length > -1) {
            return length;
        }
        ResultLob resultOut = ResultLob.newLobGetLengthRequest(id);
        Result    resultIn  = session.execute(resultOut);
        if (resultIn.isError()) {
            throw resultIn.getException();
        }
        length = ((ResultLob) resultIn).getBlockLength();
        return length;
    }
    public long bitLength(SessionInterface session) {
        return length(session) * 8;
    }
    public boolean isBits() {
        return false;
    }
    public long position(SessionInterface session, BlobData pattern,
                         long start) {
        ResultLob resultOut = ResultLob.newLobGetCharPatternPositionRequest(id,
            pattern.getId(), start);
        Result resultIn = session.execute(resultOut);
        if (resultIn.isError()) {
            throw resultIn.getException();
        }
        return ((ResultLob) resultIn).getOffset();
    }
    public long position(SessionInterface session, byte[] pattern,
                         long start) {
        ResultLob resultOut = ResultLob.newLobGetBytePatternPositionRequest(id,
            pattern, start);
        Result resultIn = session.execute(resultOut);
        if (resultIn.isError()) {
            throw resultIn.getException();
        }
        return ((ResultLob) resultIn).getOffset();
    }
    public long nonZeroLength(SessionInterface session) {
        ResultLob resultOut = ResultLob.newLobGetTruncateLength(id);
        Result    resultIn  = session.execute(resultOut);
        if (resultIn.isError()) {
            throw resultIn.getException();
        }
        return ((ResultLob) resultIn).getBlockLength();
    }
    public void setBytes(SessionInterface session, long pos, byte[] bytes,
                         int offset, int len) {
        if (offset != 0 || len != bytes.length) {
            if (!BinaryData.isInLimits(bytes.length, offset, len)) {
                throw new IndexOutOfBoundsException();
            }
            byte[] newbytes = new byte[len];
            System.arraycopy(bytes, offset, newbytes, 0, len);
            bytes = newbytes;
        }
        ResultLob resultOut = ResultLob.newLobSetBytesRequest(id, pos, bytes);
        Result    resultIn  = session.execute(resultOut);
        if (resultIn.isError()) {
            throw resultIn.getException();
        }
        length = ((ResultLob) resultIn).getBlockLength();
    }
    public void setBytes(SessionInterface session, long pos, byte[] bytes) {
        setBytes(session, pos, bytes, 0, bytes.length);
    }
    public void setBinaryStream(SessionInterface session, long pos,
                                InputStream in) {
    }
    public void setSession(SessionInterface session) {}
    public void truncate(SessionInterface session, long len) {
        ResultLob resultOut = ResultLob.newLobTruncateRequest(id, len);
        Result    resultIn  = session.execute(resultOut);
        if (resultIn.isError()) {
            throw resultIn.getException();
        }
    }
    public boolean isBinary() {
        return true;
    }
    public boolean equals(Object other) {
        if (other instanceof BlobDataID) {
            return id == ((BlobDataID) other).id;
        }
        return false;
    }
    public int hashCode() {
        return (int) id;
    }
}