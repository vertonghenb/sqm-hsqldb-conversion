package org.hsqldb.lib;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.MalformedURLException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.StringTokenizer;
public class RCData {
    public static final String DEFAULT_JDBC_DRIVER   = "org.hsqldb.jdbc.JDBCDriver";
    private String             defaultJdbcDriverName = DEFAULT_JDBC_DRIVER;
    public void setDefaultJdbcDriver(String defaultJdbcDriverName) {
        this.defaultJdbcDriverName = defaultJdbcDriverName;
    }
    public String getDefaultJdbcDriverName() {
        return defaultJdbcDriverName;
    }
    public RCData(File file, String dbKey) throws Exception {
        if (file == null) {
            throw new IllegalArgumentException("RC file name not specified");
        }
        if (!file.canRead()) {
            throw new IOException("Please set up authentication file '" + file
                                  + "'");
        }
        StringTokenizer tokenizer = null;
        boolean         thisone   = false;
        String          s;
        String          keyword, value;
        int             linenum = 0;
        BufferedReader  br      = new BufferedReader(new FileReader(file));
        try {
        while ((s = br.readLine()) != null) {
            ++linenum;
            s = s.trim();
            if (s.length() == 0) {
                continue;
            }
            if (s.charAt(0) == '#') {
                continue;
            }
            tokenizer = new StringTokenizer(s);
            if (tokenizer.countTokens() == 1) {
                keyword = tokenizer.nextToken();
                value   = "";
            } else if (tokenizer.countTokens() > 1) {
                keyword = tokenizer.nextToken();
                value   = tokenizer.nextToken("").trim();
            } else {
                try {
                    br.close();
                } catch (IOException e) {
                }
                throw new Exception("Corrupt line " + linenum + " in '" + file
                                    + "':  " + s);
            }
            if (dbKey == null) {
                if (keyword.equals("urlid")) {
                    System.out.println(value);
                }
                continue;
            }
            if (keyword.equals("urlid")) {
                if (value.equals(dbKey)) {
                    if (id == null) {
                        id      = dbKey;
                        thisone = true;
                    } else {
                        try {
                            br.close();
                        } catch (IOException e) {
                        }
                        throw new Exception("Key '" + dbKey + " redefined at"
                                            + " line " + linenum + " in '"
                                            + file);
                    }
                } else {
                    thisone = false;
                }
                continue;
            }
            if (thisone) {
                if (keyword.equals("url")) {
                    url = value;
                } else if (keyword.equals("username")) {
                    username = value;
                } else if (keyword.equals("driver")) {
                    driver = value;
                } else if (keyword.equals("charset")) {
                    charset = value;
                } else if (keyword.equals("truststore")) {
                    truststore = value;
                } else if (keyword.equals("password")) {
                    password = value;
                } else if (keyword.equals("transiso")) {
                    ti = value;
                } else if (keyword.equals("libpath")) {
                    libpath = value;
                } else {
                    try {
                        br.close();
                    } catch (IOException e) {
                    }
                    throw new Exception("Bad line " + linenum + " in '" + file
                                        + "':  " + s);
                }
            }
        }
        } finally {
            try  {
                br.close();
            } catch (IOException ioe) {
            }
            br = null;  
        }
        if (dbKey == null) {
            return;
        }
        if (url == null) {
            throw new Exception("url not set " + "for '" + dbKey
                                + "' in file '" + file + "'");
        }
        if (libpath != null) {
            throw new IllegalArgumentException(
                "Sorry, 'libpath' not supported yet");
        }
    }
    public RCData(String id, String url, String username, String password,
                  String driver, String charset,
                  String truststore) throws Exception {
        this(id, url, username, password, driver, charset, truststore, null);
    }
    public RCData(String id, String url, String username, String password,
                  String driver, String charset, String truststore,
                  String libpath) throws Exception {
        this(id, url, username, password, driver, charset, truststore,
                libpath, null);
    }
    public RCData(String id, String url, String username, String password,
                  String driver, String charset, String truststore,
                  String libpath, String ti) throws Exception {
        this.id         = id;
        this.url        = url;
        this.username   = username;
        this.password   = password;
        this.ti         = ti;
        this.driver     = driver;
        this.charset    = charset;
        this.truststore = truststore;
        this.libpath    = libpath;
        if (libpath != null) {
            throw new IllegalArgumentException(
                "Sorry, 'libpath' not supported yet");
        }
        if (id == null || url == null) {
            throw new Exception("id or url was not set");
        }
    }
    public String id;
    public String url;
    public String username;
    public String password;
    public String ti;
    public String driver;
    public String charset;
    public String truststore;
    public String libpath;
    public Connection getConnection()
    throws ClassNotFoundException, SQLException, MalformedURLException {
        return getConnection(null, null);
    }
    public Connection getConnection(String curDriverIn, String curTrustStoreIn)
                                    throws ClassNotFoundException,
                                           MalformedURLException,
                                           SQLException {
        String curDriver = null;
        String curTrustStore = null;
        Properties sysProps = System.getProperties();
        if (curDriverIn == null) {
            curDriver = ((driver == null) ? DEFAULT_JDBC_DRIVER
                                          : driver);
        } else {
            curDriver = expandSysPropVars(curDriverIn);
        }
        if (curTrustStoreIn == null) {
            if (truststore != null) {
                curTrustStore = expandSysPropVars(truststore);
            }
        } else {
            curTrustStore = expandSysPropVars(curTrustStoreIn);
        }
        if (curTrustStore == null) {
            sysProps.remove("javax.net.ssl.trustStore");
        } else {
            sysProps.put("javax.net.ssl.trustStore", curTrustStore);
        }
        String urlString = null;
        try {
            urlString = expandSysPropVars(url);
        } catch (IllegalArgumentException iae) {
            throw new MalformedURLException(iae.toString() + " for URL '"
                                            + url + "'");
        }
        String userString = null;
        if (username != null) try {
            userString = expandSysPropVars(username);
        } catch (IllegalArgumentException iae) {
            throw new MalformedURLException(iae.toString()
                                            + " for user name '" + username
                                            + "'");
        }
        String passwordString = null;
        if (password != null) try {
            passwordString = expandSysPropVars(password);
        } catch (IllegalArgumentException iae) {
            throw new MalformedURLException(iae.toString()
                                            + " for password");
        }
        Class.forName(curDriver);
        Connection c = (userString == null)
                     ? DriverManager.getConnection(urlString)
                     : DriverManager.getConnection(urlString, userString,
                                                   passwordString);
        if (ti != null) RCData.setTI(c, ti);
        return c;
    }
    public static String expandSysPropVars(String inString) {
        String outString = new String(inString);
        int    varOffset, varEnd;
        String varVal, varName;
        while (true) {
            varOffset = outString.indexOf("${");
            if (varOffset < 0) {
                break;
            }
            varEnd = outString.indexOf('}', varOffset + 2);
            if (varEnd < 0) {
                break;
            }
            varName = outString.substring(varOffset + 2, varEnd);
            if (varName.length() < 1) {
                throw new IllegalArgumentException("Bad variable setting");
            }
            varVal = System.getProperty(varName);
            if (varVal == null) {
                throw new IllegalArgumentException(
                    "No Java system property with name '" + varName + "'");
            }
            outString = outString.substring(0, varOffset) + varVal
                        + outString.substring(varEnd + 1);
        }
        return outString;
    }
    public static void setTI(Connection c, String tiString)
            throws SQLException {
        int i = -1;
        if (tiString.equals("TRANSACTION_READ_UNCOMMITTED"))
            i = Connection.TRANSACTION_READ_UNCOMMITTED;
        if (tiString.equals("TRANSACTION_READ_COMMITTED"))
            i = Connection.TRANSACTION_READ_COMMITTED;
        if (tiString.equals("TRANSACTION_REPEATABLE_READ"))
            i = Connection.TRANSACTION_REPEATABLE_READ;
        if (tiString.equals("TRANSACTION_SERIALIZABLE"))
            i = Connection.TRANSACTION_SERIALIZABLE;
        if (tiString.equals("TRANSACTION_NONE"))
            i = Connection.TRANSACTION_NONE;
        if (i < 0) {
            throw new SQLException(
                    "Trans. isol. value not supported by "
                    + RCData.class.getName() + ": " + tiString);
        }
        c.setTransactionIsolation(i);
    }
    public static String tiToString(int ti) {
        switch (ti) {
            case Connection.TRANSACTION_READ_UNCOMMITTED:
                return "TRANSACTION_READ_UNCOMMITTED";
            case Connection.TRANSACTION_READ_COMMITTED:
                return "TRANSACTION_READ_COMMITTED";
            case Connection.TRANSACTION_REPEATABLE_READ:
                return "TRANSACTION_REPEATABLE_READ";
            case Connection.TRANSACTION_SERIALIZABLE:
                return "TRANSACTION_SERIALIZABLE";
            case Connection.TRANSACTION_NONE:
                return "TRANSACTION_NONE";
        }
        return "Custom Transaction Isolation numerical value: " + ti;
    }
}