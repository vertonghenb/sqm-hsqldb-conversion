


package org.hsqldb.server;

import java.net.Socket;




public interface HsqlSocketRequestHandler {

    void handleConnection(Socket socket);

    void signalCloseAllServerConnections();
}
