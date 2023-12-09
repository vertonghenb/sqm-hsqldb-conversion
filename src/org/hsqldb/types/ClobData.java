


package org.hsqldb.types;

import java.io.Reader;

import org.hsqldb.SessionInterface;


public interface ClobData extends LobData {

    char[] getChars(SessionInterface session, final long position, int length);

    long length(SessionInterface session);

    String getSubString(SessionInterface session, final long pos,
                        final int length);

    ClobData getClob(SessionInterface session, final long pos,
                     final long length);

    ClobData duplicate(SessionInterface session);

    void truncate(SessionInterface session, long len);

    Reader getCharacterStream(SessionInterface session);

    void setString(SessionInterface session, long pos, String str);

    void setClob(SessionInterface session, long pos, ClobData clob, long offset,
                  long len);

    void setChars(SessionInterface session, long pos, char[] chars, int offset,
                 int len);

    void setCharacterStream(SessionInterface session, long pos,
                                   Reader in);

    long position(SessionInterface session, String searchstr, long start);

    long position(SessionInterface session, ClobData searchstr, long start);

    long nonSpaceLength(SessionInterface session);

    Reader getCharacterStream(SessionInterface session, long pos, long length);

    long getId();

    void setId(long id);
}
