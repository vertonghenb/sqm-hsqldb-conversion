package org.hsqldb.server;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Enumeration;
import org.hsqldb.lib.HashMap;
import org.hsqldb.lib.IntKeyHashMap;
import org.hsqldb.lib.Iterator;
import org.hsqldb.lib.OrderedHashSet;
import org.hsqldb.persist.HsqlProperties;
public class ServerProperties extends HsqlProperties {
    static final int SERVER_PROPERTY       = 0;
    static final int SERVER_MULTI_PROPERTY = 1;
    static final int SYSTEM_PROPERTY       = 2;
    static final String sc_key_prefix  = "server";
    static final String sc_key_address = "server.address";
    static final String sc_key_autorestart_server =
        "server.restart_on_shutdown";
    static final String sc_key_database         = "server.database";
    static final String sc_key_dbname           = "server.dbname";
    static final String sc_key_no_system_exit   = "server.no_system_exit";
    static final String sc_key_port             = "server.port";
    static final String sc_key_http_port        = "server.port";
    static final String sc_key_silent           = "server.silent";
    static final String sc_key_tls              = "server.tls";
    static final String sc_key_trace            = "server.trace";
    static final String sc_key_web_default_page = "server.default_page";
    static final String sc_key_web_root         = "server.root";
    static final String sc_key_max_connections  = "server.maxconnections";
    static final String sc_key_remote_open_db   = "server.remote_open";
    static final String sc_key_max_databases    = "server.maxdatabases";
    static final String sc_key_acl              = "server.acl";
    static final String sc_key_daemon           = "server.daemon";
    static final String sc_key_props            = "server.props";
    static final String sc_key_system           = "system";
    static final String sc_default_web_mime = "text/html";
    static final String sc_default_web_page = "index.html";
    static final String sc_default_web_root = ".";
    static final HashMap        meta     = new HashMap();
    static final OrderedHashSet prefixes = new OrderedHashSet();
    final int         protocol;
    protected boolean initialised = false;
    IntKeyHashMap idToAliasMap = new IntKeyHashMap();
    IntKeyHashMap idToPathMap  = new IntKeyHashMap();
    public ServerProperties(int protocol, File file) throws IOException {
        FileInputStream fis = null;
        try {
            fis = new FileInputStream(file);
            stringProps.load(fis);
        } finally {
            if (fis != null) {
                fis.close();
            }
        }
        this.protocol = protocol;
    }
    ServerProperties(int protocol) {
        this.protocol = protocol;
    }
    ServerProperties(int protocol, String path, String extension) {
        super(path, extension);
        this.protocol = protocol;
    }
    public void validate() {
        Enumeration en = stringProps.propertyNames();
        while (en.hasMoreElements()) {
            String   key      = (String) en.nextElement();
            Object[] metadata = (Object[]) meta.get(key);
            if (metadata == null) {
                metadata = getPrefixedMetadata(key);
            }
            if (metadata == null) {
                String error = "unsupported property: " + key;
                super.addError(ANY_ERROR, error);
                continue;
            }
            String error = null;
            if (((Integer) metadata[indexType]).intValue()
                    == SYSTEM_PROPERTY) {
                error = validateSystemProperty(key, metadata);
            } else if (((Integer) metadata[indexType]).intValue()
                       == SERVER_MULTI_PROPERTY) {
                error = validateMultiProperty(key, metadata);
            } else {
                String value = getProperty(key);
                if (value == null) {
                    if (metadata[indexDefaultValue] == null) {
                        error = "missing value for property: " + key;
                    } else {
                        setProperty(key,
                                    metadata[indexDefaultValue].toString());
                    }
                } else {
                    error = HsqlProperties.validateProperty(key, value,
                            metadata);
                }
            }
            if (error != null) {
                super.addError(ANY_ERROR, error);
            }
        }
        Iterator it = idToAliasMap.keySet().iterator();
        while (it.hasNext()) {
            int number = it.nextInt();
            if (!idToPathMap.containsKey(number)) {
                addError(ANY_ERROR, "no path for database id: " + number);
            }
        }
        it = idToPathMap.keySet().iterator();
        while (it.hasNext()) {
            int number = it.nextInt();
            if (!idToAliasMap.containsKey(number)) {
                addError(ANY_ERROR, "no alias for database id: " + number);
            }
        }
        initialised = true;
    }
    Object[] getPrefixedMetadata(String key) {
        for (int i = 0; i < prefixes.size(); i++) {
            String prefix = (String) prefixes.get(i);
            if (key.startsWith(prefix)) {
                return (Object[]) meta.get(prefix);
            }
        }
        return null;
    }
    String validateMultiProperty(String key, Object[] meta) {
        int    dbNumber;
        String prefix = (String) meta[indexName];
        if (meta[indexName].equals(sc_key_database)) {
            if (sc_key_database.equals(key)) {
                key = key + ".0";
            }
        }
        try {
            dbNumber = Integer.parseInt(key.substring(prefix.length() + 1));
        } catch (NumberFormatException e1) {
            return ("maformed database enumerator: " + key);
        }
        if (meta[indexName].equals(sc_key_dbname)) {
            String alias    = stringProps.getProperty(key).toLowerCase();
            Object existing = idToAliasMap.put(dbNumber, alias);
            if (existing != null) {
                return "duplicate database enumerator: " + key;
            }
        } else if (meta[indexName].equals(sc_key_database)) {
            String path     = stringProps.getProperty(key);
            Object existing = idToPathMap.put(dbNumber, path);
            if (existing != null) {
                return "duplicate database enumerator: " + key;
            }
        }
        return null;
    }
    String validateSystemProperty(String key, Object[] meta) {
        String prefix      = (String) meta[indexName];
        String specificKey = key.substring(prefix.length() + 1);
        String value       = stringProps.getProperty(key);
        if (value == null) {
            return "value required for property: " + key;
        }
        System.setProperty(specificKey, value);
        return null;
    }
    static {
        meta.put(sc_key_database,
                 getMeta(sc_key_database, SERVER_MULTI_PROPERTY, null));
        meta.put(sc_key_dbname,
                 getMeta(sc_key_dbname, SERVER_MULTI_PROPERTY, null));
        meta.put(sc_key_system, getMeta(sc_key_system, SYSTEM_PROPERTY, null));
        meta.put(sc_key_silent,
                 getMeta(sc_key_silent, SERVER_PROPERTY, false));
        meta.put(sc_key_trace, getMeta(sc_key_trace, SERVER_PROPERTY, false));
        meta.put(sc_key_tls, getMeta(sc_key_tls, SERVER_PROPERTY, false));
        meta.put(sc_key_acl, getMeta(sc_key_acl, SERVER_PROPERTY, null));
        meta.put(sc_key_autorestart_server,
                 getMeta(sc_key_autorestart_server, SERVER_PROPERTY, false));
        meta.put(sc_key_remote_open_db,
                 getMeta(sc_key_remote_open_db, SERVER_PROPERTY, false));
        meta.put(sc_key_no_system_exit,
                 getMeta(sc_key_no_system_exit, SERVER_PROPERTY, false));
        meta.put(sc_key_daemon,
                 getMeta(sc_key_daemon, SERVER_PROPERTY, false));
        meta.put(sc_key_address,
                 getMeta(sc_key_address, SERVER_PROPERTY, null));
        meta.put(sc_key_port, getMeta(sc_key_port, 0, 9001, 0, 65535));
        meta.put(sc_key_http_port, getMeta(sc_key_http_port, 0, 80, 0, 65535));
        meta.put(sc_key_max_connections,
                 getMeta(sc_key_max_connections, 0, 100, 1, 10000));
        meta.put(sc_key_max_databases,
                 getMeta(sc_key_max_databases, 0, 10, 1, 1000));
        prefixes.add(sc_key_database);
        prefixes.add(sc_key_dbname);
        prefixes.add(sc_key_system);
    }
}