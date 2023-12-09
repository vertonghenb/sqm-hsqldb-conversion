


package org.hsqldb.types;

import org.hsqldb.SessionInterface;


public interface LobData {

    long length(SessionInterface session);

    long getId();

    void setId(long id);

    void setSession(SessionInterface session);

    boolean isBinary();
}
