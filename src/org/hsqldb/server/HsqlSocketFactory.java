


package org.hsqldb.server;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;


public class HsqlSocketFactory {


    private static HsqlSocketFactory plainImpl;
    private static HsqlSocketFactory sslImpl;



    
    protected HsqlSocketFactory() throws Exception {}



    
    public static HsqlSocketFactory getInstance(boolean tls)
    throws Exception {
        return tls ? getSSLImpl()
                   : getPlainImpl();
    }


    public void configureSocket(Socket socket) {

        
    }

    
    public ServerSocket createServerSocket(int port) throws Exception {
        return new ServerSocket(port);
    }

    
    public ServerSocket createServerSocket(int port,
                                           String address) throws Exception {
        return new ServerSocket(port, 128, InetAddress.getByName(address));
    }

    
    public Socket createSocket(String host, int port) throws Exception {
        return new Socket(host, port);
    }

    
    public boolean isSecure() {
        return false;
    }


    private static HsqlSocketFactory getPlainImpl() throws Exception {

        synchronized (HsqlSocketFactory.class) {
            if (plainImpl == null) {
                plainImpl = new HsqlSocketFactory();
            }
        }

        return plainImpl;
    }

    private static HsqlSocketFactory getSSLImpl() throws Exception {

        synchronized (HsqlSocketFactory.class) {
            if (sslImpl == null) {
                sslImpl = newFactory("org.hsqldb.server.HsqlSocketFactorySecure");
            }
        }

        return sslImpl;
    }

    
    private static HsqlSocketFactory newFactory(String implClass)
    throws Exception {

        Class       clazz;
        Constructor ctor;
        Class[]     ctorParm;
        Object[]    ctorArg;
        Object      factory;

        clazz    = Class.forName(implClass);
        ctorParm = new Class[0];

        
        ctor    = clazz.getDeclaredConstructor(ctorParm);
        ctorArg = new Object[0];

        try {
            factory = ctor.newInstance(ctorArg);
        } catch (InvocationTargetException e) {
            Throwable t = e.getTargetException();

            throw (t instanceof Exception) ? ((Exception) t)
                                           : new RuntimeException(
                                               t.toString());
        }

        return (HsqlSocketFactory) factory;
    }


}
