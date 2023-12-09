


package org.hsqldb.server;

import java.net.InetAddress;

import org.hsqldb.lib.HashSet;
import org.hsqldb.lib.StringUtil;
import org.hsqldb.persist.HsqlProperties;




public final class ServerConfiguration implements ServerConstants {

    private ServerConfiguration() {}

    
    public static int getDefaultPort(int protocol, boolean isTls) {

        switch (protocol) {

            case SC_PROTOCOL_HSQL : {
                return isTls ? SC_DEFAULT_HSQLS_SERVER_PORT
                             : SC_DEFAULT_HSQL_SERVER_PORT;
            }
            case SC_PROTOCOL_HTTP : {
                return isTls ? SC_DEFAULT_HTTPS_SERVER_PORT
                             : SC_DEFAULT_HTTP_SERVER_PORT;
            }
            case SC_PROTOCOL_BER : {
                return isTls ? -1
                             : SC_DEFAULT_BER_SERVER_PORT;
            }
            default : {
                return -1;
            }
        }
    }

    
    public static ServerProperties getPropertiesFromFile(int protocol,
            String path, String extension) {

        boolean result;

        if (StringUtil.isEmpty(path)) {
            return null;
        }

        ServerProperties p = new ServerProperties(protocol, path, extension);

        try {
            result = p.load();
        } catch (Exception e) {
            return null;
        }

        return result ? p
                      : null;
    }

    
    public static String[] listLocalInetAddressNames() {

        InetAddress   addr;
        InetAddress[] addrs;
        HashSet       set;

        set = new HashSet();

        try {
            addr  = InetAddress.getLocalHost();
            addrs = InetAddress.getAllByName(addr.getHostAddress());

            for (int i = 0; i < addrs.length; i++) {
                set.add(addrs[i].getHostAddress());
                set.add(addrs[i].getHostName());
            }

            addrs = InetAddress.getAllByName(addr.getHostName());

            for (int i = 0; i < addrs.length; i++) {
                set.add(addrs[i].getHostAddress());
                set.add(addrs[i].getHostName());
            }
        } catch (Exception e) {}

        try {
            addr  = InetAddress.getByName(null);
            addrs = InetAddress.getAllByName(addr.getHostAddress());

            for (int i = 0; i < addrs.length; i++) {
                set.add(addrs[i].getHostAddress());
                set.add(addrs[i].getHostName());
            }

            addrs = InetAddress.getAllByName(addr.getHostName());

            for (int i = 0; i < addrs.length; i++) {
                set.add(addrs[i].getHostAddress());
                set.add(addrs[i].getHostName());
            }
        } catch (Exception e) {}

        try {
            set.add(InetAddress.getByName("loopback").getHostAddress());
            set.add(InetAddress.getByName("loopback").getHostName());
        } catch (Exception e) {}

        return (String[]) set.toArray(new String[set.size()]);
    }

    
    public static ServerProperties newDefaultProperties(int protocol) {

        ServerProperties p = new ServerProperties(protocol);

        p.setProperty(ServerProperties.sc_key_autorestart_server,
                      SC_DEFAULT_SERVER_AUTORESTART);
        p.setProperty(ServerProperties.sc_key_address, SC_DEFAULT_ADDRESS);
        p.setProperty(ServerProperties.sc_key_no_system_exit,
                      SC_DEFAULT_NO_SYSTEM_EXIT);
        p.setProperty(ServerProperties.sc_key_max_databases,
                      SC_DEFAULT_MAX_DATABASES);
        p.setProperty(ServerProperties.sc_key_silent, SC_DEFAULT_SILENT);
        p.setProperty(ServerProperties.sc_key_tls, SC_DEFAULT_TLS);
        p.setProperty(ServerProperties.sc_key_trace, SC_DEFAULT_TRACE);
        p.setProperty(ServerProperties.sc_key_web_default_page,
                      SC_DEFAULT_WEB_PAGE);
        p.setProperty(ServerProperties.sc_key_web_root, SC_DEFAULT_WEB_ROOT);

        
        
        
        
        return p;
    }

    
    public static void translateAddressProperty(HsqlProperties p) {

        if (p == null) {
            return;
        }

        String address = p.getProperty(ServerProperties.sc_key_address);

        if (StringUtil.isEmpty(address)) {
            p.setProperty(ServerProperties.sc_key_address, SC_DEFAULT_ADDRESS);
        }
    }

    
    public static void translateDefaultDatabaseProperty(HsqlProperties p) {

        if (p == null) {
            return;
        }

        if (!p.isPropertyTrue(ServerProperties.sc_key_remote_open_db)) {
            if (p.getProperty(ServerProperties.sc_key_database + "." + 0)
                    == null) {
                String defaultdb =
                    p.getProperty(ServerProperties.sc_key_database);

                if (defaultdb == null) {
                    defaultdb = SC_DEFAULT_DATABASE;
                } else {
                    p.removeProperty(ServerProperties.sc_key_database);
                }

                p.setProperty(ServerProperties.sc_key_database + ".0",
                              defaultdb);
                p.setProperty(ServerProperties.sc_key_dbname + ".0", "");
            }

            if (p.getProperty(ServerProperties.sc_key_dbname + "." + 0)
                    == null) {
                p.setProperty(ServerProperties.sc_key_dbname + ".0", "");
            }
        }
    }

    
    public static void translateDefaultNoSystemExitProperty(HsqlProperties p) {

        if (p == null) {
            return;
        }

        p.setPropertyIfNotExists(ServerProperties.sc_key_no_system_exit,
                                 "false");
    }
}
