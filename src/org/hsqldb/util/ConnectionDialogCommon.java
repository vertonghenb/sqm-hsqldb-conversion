


package org.hsqldb.util;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Constructor;
import java.util.Enumeration;
import java.util.Hashtable;

import org.hsqldb.lib.java.JavaSystem;






class ConnectionDialogCommon {

    private static String[][]       connTypes;
    private static final String[][] sJDBCTypes = {
        {
            "HSQL Database Engine In-Memory", "org.hsqldb.jdbcDriver",
            "jdbc:hsqldb:mem:."
        }, {
            "HSQL Database Engine Standalone", "org.hsqldb.jdbcDriver",
            "jdbc:hsqldb:file:\u00ABdatabase/path?\u00BB"
        }, {
            "HSQL Database Engine Server", "org.hsqldb.jdbcDriver",
            "jdbc:hsqldb:hsql://localhost/"
        }, {
            "HSQL Database Engine WebServer", "org.hsqldb.jdbcDriver",
            "jdbc:hsqldb:http://\u00ABhostname/?\u00BB"
        }, {
            "JDBC-ODBC Bridge from Sun", "sun.jdbc.odbc.JdbcOdbcDriver",
            "jdbc:odbc:\u00ABdatabase?\u00BB"
        }, {
            "Cloudscape RMI", "RmiJdbc.RJDriver",
            "jdbc:rmi://\u00ABhost?\u00BB:1099/jdbc:cloudscape:"
            + "\u00ABdatabase?\u00BB;create=true"
        }, {
            "IBM DB2", "COM.ibm.db2.jdbc.app.DB2Driver",
            "jdbc:db2:\u00ABdatabase?\u00BB"
        }, {
            "IBM DB2 (thin)", "COM.ibm.db2.jdbc.net.DB2Driver",
            "jdbc:db2://\u00ABhost?\u00BB:6789/\u00ABdatabase?\u00BB"
        }, {
            "Informix", "com.informix.jdbc.IfxDriver",
            "jdbc:informix-sqli://\u00ABhost?\u00BB:1533/\u00ABdatabase?\u00BB:"
            + "INFORMIXSERVER=\u00ABserver?\u00BB"
        }, {
            "InstantDb", "jdbc.idbDriver", "jdbc:idb:\u00ABdatabase?\u00BB.prp"
        }, {
            "MySQL Connector/J", "com.mysql.jdbc.Driver",
            "jdbc:mysql://\u00ABhost?\u00BB/\u00ABdatabase?\u00BB"
        }, {
            "MM.MySQL", "org.gjt.mm.mysql.Driver",
            "jdbc:mysql://\u00ABhost?\u00BB/\u00ABdatabase?\u00BB"
        }, {
            "Oracle", "oracle.jdbc.driver.OracleDriver",
            "jdbc:oracle:oci8:@\u00ABdatabase?\u00BB"
        }, {
            "Oracle (thin)", "oracle.jdbc.driver.OracleDriver",
            "jdbc:oracle:thin:@\u00ABhost?\u00BB:1521:\u00ABdatabase?\u00BB"
        }, {
            "PointBase", "com.pointbase.jdbc.jdbcUniversalDriver",
            "jdbc:pointbase://\u00ABhost?\u00BB/\u00ABdatabase?\u00BB"
        }, {
            "PostgreSQL", "org.postgresql.Driver",
            "jdbc:postgresql://\u00ABhost?\u00BB/\u00ABdatabase?\u00BB"
        }, {
            "PostgreSQL v6.5", "postgresql.Driver",
            "jdbc:postgresql://\u00ABhost?\u00BB/\u00ABdatabase?\u00BB"
        }
    };

    static String[][] getTypes() {

        return sJDBCTypes;

    }

    private static final String fileName       = "hsqlprefs.dat";
    private static File         recentSettings = null;

    static synchronized Hashtable loadRecentConnectionSettings()
    throws IOException {

        Hashtable list = new Hashtable();

        try {
            if (recentSettings == null) {
                setHomeDir();

                if (homedir == null) {
                    return list;
                }

                recentSettings = new File(homedir, fileName);

                if (!recentSettings.exists()) {
                    JavaSystem.createNewFile(recentSettings);

                    return list;
                }
            }
        } catch (Throwable e) {
            return list;
        }

        FileInputStream   in        = null;
        ObjectInputStream objStream = null;

        try {
            in        = new FileInputStream(recentSettings);
            objStream = new ObjectInputStream(in);

            list.clear();

            while (true) {
                ConnectionSetting setting =
                    (ConnectionSetting) objStream.readObject();

                if (!emptySettingName.equals(setting.getName())) {
                    list.put(setting.getName(), setting);
                }
            }
        } catch (EOFException eof) {

            
        } catch (ClassNotFoundException cnfe) {
            throw (IOException) new IOException("Unrecognized class type "
                                                + cnfe.getMessage());
        } catch (ClassCastException cce) {
            throw (IOException) new IOException("Unrecognized class type "
                                                + cce.getMessage());
        } catch (Throwable t) {}
        finally {
            if (objStream != null) {
                objStream.close();
            }

            if (in != null) {
                in.close();
            }
        }

        return list;
    }

    static String emptySettingName = "Recent settings...";

    
    static void addToRecentConnectionSettings(Hashtable settings,
            ConnectionSetting newSetting) throws IOException {
        settings.put(newSetting.getName(), newSetting);
        ConnectionDialogCommon.storeRecentConnectionSettings(settings);
    }

    
    private static void storeRecentConnectionSettings(Hashtable settings) {

        try {
            if (recentSettings == null) {
                setHomeDir();

                if (homedir == null) {
                    return;
                }

                recentSettings = new File(homedir, fileName);

                if (!recentSettings.exists()) {


                }
            }

            if (settings == null || settings.size() == 0) {
                return;
            }

            
            FileOutputStream   out = new FileOutputStream(recentSettings);
            ObjectOutputStream objStream = new ObjectOutputStream(out);
            Enumeration        en        = settings.elements();

            while (en.hasMoreElements()) {
                objStream.writeObject(en.nextElement());
            }

            objStream.flush();
            objStream.close();
            out.close();
        } catch (Throwable t) {}
    }

    
    static void deleteRecentConnectionSettings() {

        try {
            if (recentSettings == null) {
                setHomeDir();

                if (homedir == null) {
                    return;
                }

                recentSettings = new File(homedir, fileName);
            }

            if (!recentSettings.exists()) {
                recentSettings = null;

                return;
            }

            recentSettings.delete();

            recentSettings = null;
        } catch (Throwable t) {}
    }

    private static String homedir = null;

    public static void setHomeDir() {


        if (homedir == null) {
            try {
                Class c =
                    Class.forName("sun.security.action.GetPropertyAction");
                Constructor constructor = c.getConstructor(new Class[]{
                    String.class });
                java.security.PrivilegedAction a =
                    (java.security.PrivilegedAction) constructor.newInstance(
                        new Object[]{ "user.home" });

                homedir =
                    (String) java.security.AccessController.doPrivileged(a);
            } catch (Exception e) {
                System.err.println(
                    "No access to home directory.  Continuing without...");
            }
        }


    }
}
