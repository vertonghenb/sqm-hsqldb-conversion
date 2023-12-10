package org.hsqldb.server;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.Principal;
import java.security.Provider;
import java.security.PublicKey;
import java.security.Security;
import javax.net.ssl.HandshakeCompletedEvent;
import javax.net.ssl.HandshakeCompletedListener;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.security.cert.X509Certificate;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.StringConverter;
public final class HsqlSocketFactorySecure extends HsqlSocketFactory
implements HandshakeCompletedListener {
    protected Object socketFactory;
    protected Object serverSocketFactory;
    protected final Object socket_factory_mutex = new Object();
    protected final Object server_socket_factory_mutex = new Object();
    protected HsqlSocketFactorySecure() throws Exception {
        super();
        Provider p;
        String   cls;
        if (Security.getProvider("SunJSSE") == null) {
            try {
                p = (Provider) Class.forName(
                    "com.sun.net.ssl.internal.ssl.Provider").newInstance();
                Security.addProvider(p);
            } catch (Exception e) {}
        }
    }
    public void configureSocket(Socket socket) {
        SSLSocket s;
        super.configureSocket(socket);
        s = (SSLSocket) socket;
        s.addHandshakeCompletedListener(this);
    }
    public ServerSocket createServerSocket(int port) throws Exception {
        SSLServerSocket ss;
        ss = (SSLServerSocket) getServerSocketFactoryImpl()
            .createServerSocket(port);
        if (Error.TRACESYSTEMOUT) {
            Error.printSystemOut("[" + this + "]: createServerSocket()");
            Error.printSystemOut("capabilities for " + ss + ":");
            Error.printSystemOut("----------------------------");
            dump("supported cipher suites", ss.getSupportedCipherSuites());
            dump("enabled cipher suites", ss.getEnabledCipherSuites());
        }
        return ss;
    }
    public ServerSocket createServerSocket(int port,
                                           String address) throws Exception {
        SSLServerSocket ss;
        InetAddress     addr;
        addr = InetAddress.getByName(address);
        ss = (SSLServerSocket) getServerSocketFactoryImpl()
            .createServerSocket(port, 128, addr);
        if (Error.TRACESYSTEMOUT) {
            Error.printSystemOut("[" + this + "]: createServerSocket()");
            Error.printSystemOut("capabilities for " + ss + ":");
            Error.printSystemOut("----------------------------");
            dump("supported cipher suites", ss.getSupportedCipherSuites());
            dump("enabled cipher suites", ss.getEnabledCipherSuites());
        }
        return ss;
    }
    private static void dump(String title, String[] as) {
        Error.printSystemOut(title);
        Error.printSystemOut("----------------------------");
        for (int i = 0; i < as.length; i++) {
            Error.printSystemOut(String.valueOf(as[i]));
        }
        Error.printSystemOut("----------------------------");
    }
    public Socket createSocket(String host, int port) throws Exception {
        SSLSocket socket;
        socket = (SSLSocket) getSocketFactoryImpl().createSocket(host, port);
        socket.addHandshakeCompletedListener(this);
        socket.startHandshake();
        verify(host, socket.getSession());
        return socket;
    }
    public boolean isSecure() {
        return true;
    }
    protected SSLServerSocketFactory getServerSocketFactoryImpl()
    throws Exception {
        Object factory;
        synchronized (server_socket_factory_mutex) {
            factory = serverSocketFactory;
            if (factory == null) {
                factory             = SSLServerSocketFactory.getDefault();
                serverSocketFactory = factory;
            }
        }
        return (SSLServerSocketFactory) factory;
    }
    protected SSLSocketFactory getSocketFactoryImpl() throws Exception {
        Object factory;
        synchronized (socket_factory_mutex) {
            factory = socketFactory;
            if (factory == null) {
                factory       = SSLSocketFactory.getDefault();
                socketFactory = factory;
            }
        }
        return (SSLSocketFactory) factory;
    }
    protected void verify(String host, SSLSession session) throws Exception {
        X509Certificate[] chain;
        X509Certificate   certificate;
        Principal         principal;
        PublicKey         publicKey;
        String            DN;
        String            CN;
        int               start;
        int               end;
        String            emsg;
        chain       = session.getPeerCertificateChain();
        certificate = chain[0];
        principal   = certificate.getSubjectDN();
        DN          = String.valueOf(principal);
        start       = DN.indexOf("CN=");
        if (start < 0) {
            throw new UnknownHostException(
                Error.getMessage(ErrorCode.M_SERVER_SECURE_VERIFY_1));
        }
        start += 3;
        end   = DN.indexOf(',', start);
        CN    = DN.substring(start, (end > -1) ? end
                                               : DN.length());
        if (CN.length() < 1) {
            throw new UnknownHostException(
                Error.getMessage(ErrorCode.M_SERVER_SECURE_VERIFY_2));
        }
        if (!CN.equalsIgnoreCase(host)) {
            throw new UnknownHostException(
                Error.getMessage(
                    ErrorCode.M_SERVER_SECURE_VERIFY_3, 0,
                    new Object[] {
                CN, host
            }));
        }
    }
    public void handshakeCompleted(HandshakeCompletedEvent evt) {
        SSLSession session;
        String     sessionId;
        SSLSocket  socket;
        if (Error.TRACESYSTEMOUT) {
            socket  = evt.getSocket();
            session = evt.getSession();
            Error.printSystemOut("SSL handshake completed:");
            Error.printSystemOut(
                "------------------------------------------------");
            Error.printSystemOut("socket:      : " + socket);
            Error.printSystemOut("cipher suite : "
                                 + session.getCipherSuite());
            sessionId = StringConverter.byteArrayToHexString(session.getId());
            Error.printSystemOut("session id   : " + sessionId);
            Error.printSystemOut(
                "------------------------------------------------");
        }
    }
}