package org.hsqldb.sample;
import java.util.Properties;
public class DatabaseManagerSample extends org.hsqldb.util.DatabaseManager {
    static {
        Properties p = new Properties();
        p.put("org.hsqldb.util.ConnectionTypeClass",
              "org.hsqldb.sample.ConnectionTypesSample");
        System.setProperties(p);
    }
}