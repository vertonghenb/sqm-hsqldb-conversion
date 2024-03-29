package org.hsqldb.test;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.SQLException;
import org.hsqldb.server.HsqlServerFactory;
import org.hsqldb.server.HsqlSocketRequestHandler;
public class TestInstantiation {
    public TestInstantiation() {
        try {
            ServerSocket             serversocket;
            Socket                   socket;
            String                   m_DatabaseName;
            HsqlSocketRequestHandler m_hsrh;
            m_DatabaseName = "mem:.";
            serversocket   = new ServerSocket(9999);
            while (true) {
                socket = serversocket.accept();
                m_hsrh = HsqlServerFactory.createHsqlServer(m_DatabaseName,
                        true, false);
                m_hsrh.handleConnection(socket);
            }
        } catch (IOException e1) {
            System.out.println(e1.getMessage());
        } catch (SQLException e2) {
            System.out.println(e2.getMessage());
        }
    }
    public static void main(String[] argv) {
        new TestInstantiation();
    }
}