package org.hsqldb.server;
public interface ServerConstants {
    int SERVER_STATE_ONLINE   = 1;
    int SERVER_STATE_OPENING  = 4;
    int SERVER_STATE_CLOSING  = 8;
    int SERVER_STATE_SHUTDOWN = 16;
    int SC_DATABASE_SHUTDOWN  = 0;
    String SC_DEFAULT_ADDRESS = "0.0.0.0";
    String SC_DEFAULT_DATABASE = "test";
    int SC_DEFAULT_HSQL_SERVER_PORT  = 9001;
    int SC_DEFAULT_HSQLS_SERVER_PORT = 554;
    int SC_DEFAULT_HTTP_SERVER_PORT  = 80;
    int SC_DEFAULT_HTTPS_SERVER_PORT = 443;
    int SC_DEFAULT_BER_SERVER_PORT   = 9101;
    boolean SC_DEFAULT_SERVER_AUTORESTART = false;
    boolean SC_DEFAULT_NO_SYSTEM_EXIT     = true;
    boolean SC_DEFAULT_SILENT             = true;
    boolean SC_DEFAULT_TLS                = false;
    boolean SC_DEFAULT_TRACE              = false;
    boolean SC_DEFAULT_REMOTE_OPEN_DB     = false;
    int     SC_DEFAULT_MAX_DATABASES      = 10;
    int SC_PROTOCOL_HTTP = 0;
    int SC_PROTOCOL_HSQL = 1;
    int SC_PROTOCOL_BER  = 2;
    String SC_DEFAULT_WEB_MIME = "text/html";
    String SC_DEFAULT_WEB_PAGE = "index.html";
    String SC_DEFAULT_WEB_ROOT = ".";
}