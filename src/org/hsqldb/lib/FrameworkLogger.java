package org.hsqldb.lib;
import java.util.Enumeration;
import java.util.logging.Level;
import java.util.logging.ConsoleHandler;
import java.util.logging.Logger;
import java.util.logging.LogManager;
import java.util.Map;
import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;
import java.io.IOException;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.lang.reflect.Method;
public class FrameworkLogger {
    public static String report() {
        return new StringBuilder().append(loggerInstances.size()).append(
            " logger instances:  ").append(
            loggerInstances.keySet()).toString();
    }
    static private Map    loggerInstances  = new HashMap();
    static private Map    jdkToLog4jLevels = new HashMap();
    static private Method log4jGetLogger;
    static private Method log4jLogMethod;
    private Object        log4jLogger;
    private Logger        jdkLogger;
    static private boolean noopMode;    
    static {
        try {
            reconfigure();
        } catch (java.lang.SecurityException e) {}
    }
    public static synchronized void clearLoggers(String prefixToZap) {
        Set                targetKeys = new HashSet();
        java.util.Iterator it         = loggerInstances.keySet().iterator();
        String             k;
        String             dottedPrefix = prefixToZap + '.';
        while (it.hasNext()) {
            k = (String) it.next();
            if (k.equals(prefixToZap) || k.startsWith(dottedPrefix)) {
                targetKeys.add(k);
            }
        }
        loggerInstances.keySet().removeAll(targetKeys);
    }
    static void reconfigure() {
        noopMode = false;
        Class log4jLoggerClass = null;
        loggerInstances.clear();
        log4jLoggerClass = null;
        log4jGetLogger   = null;
        log4jLogMethod   = null;
        try {
            log4jLoggerClass = Class.forName("org.apache.log4j.Logger");
        } catch (Exception e) {
        }
        if (log4jLoggerClass != null) {
            try {
                if (jdkToLog4jLevels.size() < 1) {
                    Method log4jToLevel = Class.forName(
                        "org.apache.log4j.Level").getMethod(
                        "toLevel", new Class[]{ String.class });
                    jdkToLog4jLevels.put(Level.ALL,
                                         log4jToLevel.invoke(null,
                                             new Object[]{ "ALL" }));
                    jdkToLog4jLevels.put(Level.FINER,
                                         log4jToLevel.invoke(null,
                                             new Object[]{ "DEBUG" }));
                    jdkToLog4jLevels.put(Level.WARNING,
                                         log4jToLevel.invoke(null,
                                             new Object[]{ "ERROR" }));
                    jdkToLog4jLevels.put(Level.SEVERE,
                                         log4jToLevel.invoke(null,
                                             new Object[]{ "FATAL" }));
                    jdkToLog4jLevels.put(Level.INFO,
                                         log4jToLevel.invoke(null,
                                             new Object[]{ "INFO" }));
                    jdkToLog4jLevels.put(Level.OFF,
                                         log4jToLevel.invoke(null,
                                             new Object[]{ "OFF" }));
                    jdkToLog4jLevels.put(Level.FINEST,
                                         log4jToLevel.invoke(null,
                                             new Object[]{ "TRACE" }));
                    jdkToLog4jLevels.put(Level.WARNING,
                                         log4jToLevel.invoke(null,
                                             new Object[]{ "WARN" }));
                }
                log4jLogMethod = log4jLoggerClass.getMethod("log",
                        new Class[] {
                    String.class, Class.forName("org.apache.log4j.Priority"),
                    Object.class, Throwable.class
                });
                log4jGetLogger = log4jLoggerClass.getMethod("getLogger",
                        new Class[]{ String.class });
                return;    
            } catch (Exception e) {
                try {
                    System.err.println(
                        "<clinit> failure "
                        + "instantiating configured Log4j system: " + e);
                } catch (Throwable t) {
                }
            }
        }
        log4jLoggerClass = null;
        log4jLogMethod   = null;
        log4jGetLogger   = null;
        String propVal = System.getProperty("hsqldb.reconfig_logging");
        if (propVal != null && propVal.equalsIgnoreCase("false")) {
            return;
        }
        try {
            LogManager lm = LogManager.getLogManager();
            if (isDefaultJdkConfig()) {
                lm.reset();
                ConsoleHandler consoleHandler = new ConsoleHandler();
                consoleHandler.setFormatter(
                    new BasicTextJdkLogFormatter(false));
                consoleHandler.setLevel(Level.INFO);
                lm.readConfiguration(
                    FrameworkLogger.class.getResourceAsStream(
                        "/org/hsqldb/resources/jdklogging-default.properties"));
                Logger cmdlineLogger = Logger.getLogger("org.hsqldb.cmdline");
                cmdlineLogger.addHandler(consoleHandler);
                cmdlineLogger.setUseParentHandlers(false);
            } else {
                lm.readConfiguration();
            }
        } catch (Exception e) {
            noopMode = true;
            System.err.println(
                "<clinit> failure initializing JDK logging system.  "
                + "Continuing without Application logging.");
            e.printStackTrace();
        }
    }
    private FrameworkLogger(String s) {
        if (!noopMode) {
            if (log4jGetLogger == null) {
                jdkLogger = Logger.getLogger(s);
            } else {
                try {
                    log4jLogger = log4jGetLogger.invoke(null,
                                                        new Object[]{ s });
                } catch (Exception e) {
                    throw new RuntimeException(
                        "Failed to instantiate Log4j Logger", e);
                }
            }
        }
        loggerInstances.put(s, this);
    }
    public static FrameworkLogger getLog(Class c) {
        return getLog(c.getName());
    }
    public static FrameworkLogger getLog(Class c, String contextId) {
        return (contextId == null) ? getLog(c)
                                   : getLog(contextId + '.' + c.getName());
    }
    public static FrameworkLogger getLog(String baseId, String contextId) {
        return (contextId == null) ? getLog(baseId)
                                   : getLog(contextId + '.' + baseId);
    }
    public static FrameworkLogger getLog(String s) {
        if (loggerInstances.containsKey(s)) {
            return (FrameworkLogger) loggerInstances.get(s);
        }
        return new FrameworkLogger(s);
    }
    public void log(Level level, String message, Throwable t) {
        privlog(level, message, t, 2, FrameworkLogger.class);
    }
    public void privlog(Level level, String message, Throwable t,
                        int revertMethods, Class skipClass) {
        if (noopMode) {
            return;
        }
        if (log4jLogger == null) {
            StackTraceElement elements[] = new Throwable().getStackTrace();
            String            c = elements[revertMethods].getClassName();
            String            m = elements[revertMethods].getMethodName();
            if (t == null) {
                jdkLogger.logp(level, c, m, message);
            } else {
                jdkLogger.logp(level, c, m, message, t);
            }
        } else {
            try {
                log4jLogMethod.invoke(log4jLogger, new Object[] {
                    skipClass.getName(), jdkToLog4jLevels.get(level), message,
                    t
                });
            } catch (Exception e) {
                throw new RuntimeException(
                    "Logging failed when attempting to log: " + message, e);
            }
        }
    }
    public void enduserlog(Level level, String message) {
        if (noopMode) {
            return;
        }
        if (log4jLogger == null) {
            String c = FrameworkLogger.class.getName();
            String m = "\\l";
            jdkLogger.logp(level, c, m, message);
        } else {
            try {
                log4jLogMethod.invoke(log4jLogger, new Object[] {
                    FrameworkLogger.class.getName(),
                    jdkToLog4jLevels.get(level), message, null
                });
            } catch (Exception e) {
                throw new RuntimeException(
                    "Logging failed when attempting to log: " + message, e);
            }
        }
    }
    public void log(Level level, String message) {
        privlog(level, message, null, 2, FrameworkLogger.class);
    }
    public void finer(String message) {
        privlog(Level.FINER, message, null, 2, FrameworkLogger.class);
    }
    public void warning(String message) {
        privlog(Level.WARNING, message, null, 2, FrameworkLogger.class);
    }
    public void severe(String message) {
        privlog(Level.SEVERE, message, null, 2, FrameworkLogger.class);
    }
    public void info(String message) {
        privlog(Level.INFO, message, null, 2, FrameworkLogger.class);
    }
    public void finest(String message) {
        privlog(Level.FINEST, message, null, 2, FrameworkLogger.class);
    }
    public void error(String message) {
        privlog(Level.WARNING, message, null, 2, FrameworkLogger.class);
    }
    public void finer(String message, Throwable t) {
        privlog(Level.FINER, message, t, 2, FrameworkLogger.class);
    }
    public void warning(String message, Throwable t) {
        privlog(Level.WARNING, message, t, 2, FrameworkLogger.class);
    }
    public void severe(String message, Throwable t) {
        privlog(Level.SEVERE, message, t, 2, FrameworkLogger.class);
    }
    public void info(String message, Throwable t) {
        privlog(Level.INFO, message, t, 2, FrameworkLogger.class);
    }
    public void finest(String message, Throwable t) {
        privlog(Level.FINEST, message, t, 2, FrameworkLogger.class);
    }
    public void error(String message, Throwable t) {
        privlog(Level.WARNING, message, t, 2, FrameworkLogger.class);
    }
    public static boolean isDefaultJdkConfig() {
        File globalCfgFile = new File(System.getProperty("java.home"),
                                      "lib/logging.properties");
        if (!globalCfgFile.isFile()) {
            return false;
        }
        FileInputStream fis = null;
        LogManager      lm  = LogManager.getLogManager();
        try {
            fis = new FileInputStream(globalCfgFile);
            Properties defaultProps = new Properties();
            defaultProps.load(fis);
            Enumeration names = defaultProps.propertyNames();
            int         i     = 0;
            String      name;
            String      liveVal;
            while (names.hasMoreElements()) {
                i++;
                name    = (String) names.nextElement();
                liveVal = lm.getProperty(name);
                if (liveVal == null) {
                    return false;
                }
                if (!lm.getProperty(name).equals(liveVal)) {
                    return false;
                }
            }
            return true;
        } catch (IOException ioe) {
            return false;
        } finally {
            if (fis != null) {
                try {
                    fis.close();
                } catch (IOException ioe) {
                }
            }
        }
    }
}