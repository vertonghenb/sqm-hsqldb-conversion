package org.hsqldb.types;
import java.io.InputStream;
import java.io.OutputStream;
import org.hsqldb.SessionInterface;
public interface BlobData extends LobData {
    byte[] getBytes();
    byte[] getBytes(SessionInterface session, long pos, int length);
    BlobData getBlob(SessionInterface session, long pos, long length);
    InputStream getBinaryStream(SessionInterface session);
    InputStream getBinaryStream(SessionInterface session, long pos,
                                long length);
    long length(SessionInterface session);
    long bitLength(SessionInterface session);
    boolean isBits();
    void setBytes(SessionInterface session, long pos, byte[] bytes,
                  int offset, int len);
    void setBytes(SessionInterface session, long pos, byte[] bytes);
    void setBinaryStream(SessionInterface session, long pos, InputStream in);
    void truncate(SessionInterface session, long len);
    BlobData duplicate(SessionInterface session);
    long position(SessionInterface session, byte[] pattern, long start);
    long position(SessionInterface session, BlobData pattern, long start);
    long nonZeroLength(SessionInterface session);
    long getId();
    void setId(long id);
    void free();
    boolean isClosed();
    void setSession(SessionInterface session);
    int getStreamBlockSize();
}