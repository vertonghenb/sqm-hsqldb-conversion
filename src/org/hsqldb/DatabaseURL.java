


package org.hsqldb;

import java.util.Locale;

import org.hsqldb.persist.HsqlProperties;
import org.hsqldb.server.ServerConstants;




public class DatabaseURL {

    static final String        S_DOT               = ".";
    public static final String S_MEM               = "mem:";
    public static final String S_FILE              = "file:";
    public static final String S_RES               = "res:";
    public static final String S_ALIAS             = "alias:";
    public static final String S_HSQL              = "hsql://";
    public static final String S_HSQLS             = "hsqls://";
    public static final String S_HTTP              = "http://";
    public static final String S_HTTPS             = "https://";
    public static final String S_URL_PREFIX        = "jdbc:hsqldb:";
    public static final String S_URL_INTERNAL      = "jdbc:default:connection";
    public static final String url_connection_type = "connection_type";
    public static final String url_database        = "database";

    
    public static boolean isFileBasedDatabaseType(String type) {

        if (type == S_FILE || type == S_RES) {
            return true;
        }

        return false;
    }

    
    public static boolean isInProcessDatabaseType(String type) {

        if (type == S_FILE || type == S_RES || type == S_MEM) {
            return true;
        }

        return false;
    }

    
    public static HsqlProperties parseURL(String url, boolean hasPrefix,
                                          boolean noPath) {

        String         urlImage   = url.toLowerCase(Locale.ENGLISH);
        HsqlProperties props      = new HsqlProperties();
        HsqlProperties extraProps = null;
        String         arguments  = null;
        int            pos        = 0;
        String         type       = null;
        int            port       = 0;
        String         database;
        String         path;
        boolean        isNetwork = false;

        if (hasPrefix) {
            if (urlImage.startsWith(S_URL_PREFIX)) {
                pos = S_URL_PREFIX.length();
            } else {
                return props;
            }
        }

        while (true) {
            int replacePos = url.indexOf("${");

            if (replacePos == -1) {
                break;
            }

            int endPos = url.indexOf("}", replacePos);

            if (endPos == -1) {
                break;
            }

            String varName  = url.substring(replacePos + 2, endPos);
            String varValue = null;

            try {
                varValue = System.getProperty(varName);
            } catch (SecurityException e) {}

            if (varValue == null) {
                break;
            }

            url = url.substring(0, replacePos) + varValue
                  + url.substring(endPos + 1);
            urlImage = url.toLowerCase(Locale.ENGLISH);
        }

        props.setProperty("url", url);

        int postUrlPos = url.length();

        
        
        
        
        int semiPos = url.indexOf(';', pos);

        if (semiPos > -1) {
            arguments  = url.substring(semiPos + 1, urlImage.length());
            postUrlPos = semiPos;
            extraProps = HsqlProperties.delimitedArgPairsToProps(arguments,
                    "=", ";", null);

            
            props.addProperties(extraProps);
        }

        if (postUrlPos == pos + 1 && urlImage.startsWith(S_DOT, pos)) {
            type = S_DOT;
        } else if (urlImage.startsWith(S_MEM, pos)) {
            type = S_MEM;
        } else if (urlImage.startsWith(S_FILE, pos)) {
            type = S_FILE;
        } else if (urlImage.startsWith(S_RES, pos)) {
            type = S_RES;
        } else if (urlImage.startsWith(S_ALIAS, pos)) {
            type = S_ALIAS;
        } else if (urlImage.startsWith(S_HSQL, pos)) {
            type      = S_HSQL;
            port      = ServerConstants.SC_DEFAULT_HSQL_SERVER_PORT;
            isNetwork = true;
        } else if (urlImage.startsWith(S_HSQLS, pos)) {
            type      = S_HSQLS;
            port      = ServerConstants.SC_DEFAULT_HSQLS_SERVER_PORT;
            isNetwork = true;
        } else if (urlImage.startsWith(S_HTTP, pos)) {
            type      = S_HTTP;
            port      = ServerConstants.SC_DEFAULT_HTTP_SERVER_PORT;
            isNetwork = true;
        } else if (urlImage.startsWith(S_HTTPS, pos)) {
            type      = S_HTTPS;
            port      = ServerConstants.SC_DEFAULT_HTTPS_SERVER_PORT;
            isNetwork = true;
        }

        if (type == null) {
            type = S_FILE;
        } else if (type == S_DOT) {
            type = S_MEM;

            
        } else {
            pos += type.length();
        }

        props.setProperty("connection_type", type);

        if (isNetwork) {

            
            String pathSeg = null;
            String hostSeg = null;
            String portSeg = null;
            int    endPos  = url.indexOf('/', pos);

            if (endPos > 0 && endPos < postUrlPos) {
                pathSeg = url.substring(endPos, postUrlPos);

                
            } else {
                endPos = postUrlPos;
            }

            
            if (url.charAt(pos) == '[') {

                
                int endIpv6 = url.indexOf(']', pos + 2);

                
                if (endIpv6 < 0 || endIpv6 >= endPos) {
                    return null;

                    
                    
                }

                hostSeg = urlImage.substring(pos + 1, endIpv6);

                if (endPos > endIpv6 + 1) {
                    portSeg = url.substring(endIpv6 + 1, endPos);
                }
            } else {

                
                int colPos = url.indexOf(':', pos + 1);

                if (colPos > -1 && colPos < endPos) {

                    
                    portSeg = url.substring(colPos, endPos);
                } else {
                    colPos = -1;
                }

                hostSeg = urlImage.substring(pos, (colPos > 0) ? colPos
                                                               : endPos);
            }

            
            
            if (portSeg != null) {
                if (portSeg.length() < 2 || portSeg.charAt(0) != ':') {

                    
                    
                    return null;
                }

                try {
                    port = Integer.parseInt(portSeg.substring(1));
                } catch (NumberFormatException e) {

                    
                    return null;
                }
            }

            if (noPath) {
                path     = "";
                database = pathSeg;
            } else if (pathSeg == null) {
                path     = "/";
                database = "";
            } else {
                int lastSlashPos = pathSeg.lastIndexOf('/');

                if (lastSlashPos < 1) {
                    path = "/";
                    database =
                        pathSeg.substring(1).toLowerCase(Locale.ENGLISH);
                } else {
                    path     = pathSeg.substring(0, lastSlashPos);
                    database = pathSeg.substring(lastSlashPos + 1);
                }
            }

            
            props.setProperty("port", port);
            props.setProperty("host", hostSeg);
            props.setProperty("path", path);

            if (!noPath && extraProps != null) {
                String filePath = extraProps.getProperty("filepath");

                if (filePath != null && database.length() != 0) {
                    database += ";" + filePath;
                } else {
                    if (url.indexOf(S_MEM) == postUrlPos + 1
                            || url.indexOf(S_FILE) == postUrlPos + 1) {
                        database += url.substring(postUrlPos);
                    }
                }
            }
        } else {
            if (type == S_MEM) {
                database = urlImage.substring(pos, postUrlPos);
            } else if (type == S_RES) {
                database = url.substring(pos, postUrlPos);

                if (database.indexOf('/') != 0) {
                    database = '/' + database;
                }
            } else {
                database = url.substring(pos, postUrlPos);

                if (database.startsWith("~")) {
                    String userHome = "~";

                    try {
                        userHome = System.getProperty("user.home");
                    } catch (SecurityException e) {}

                    database = userHome + database.substring(1);
                }
            }

            if (database.length() == 0) {
                return null;
            }
        }

        pos = database.indexOf("&password=");

        if (pos != -1) {
            String password = database.substring(pos + "&password=".length());

            props.setProperty("password", password);

            database = database.substring(0, pos);
        }

        pos = database.indexOf("?user=");

        if (pos != -1) {
            String user = database.substring(pos + "?user=".length());

            props.setProperty("user", user);

            database = database.substring(0, pos);
        }

        props.setProperty("database", database);

        return props;
    }
}
