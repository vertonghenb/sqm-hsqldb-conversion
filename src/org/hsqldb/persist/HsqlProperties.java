package org.hsqldb.persist;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Enumeration;
import java.util.Properties;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.lib.FileAccess;
import org.hsqldb.lib.FileUtil;
import org.hsqldb.lib.HashMap;
import org.hsqldb.lib.java.JavaSystem;
import org.hsqldb.store.ValuePool;
public class HsqlProperties {
    public static final int ANY_ERROR        = 0;
    public static final int NO_VALUE_FOR_KEY = 1;
    protected String        fileName;
    protected String        fileExtension = "";
    protected Properties    stringProps;
    protected int[]         errorCodes = ValuePool.emptyIntArray;
    protected String[]      errorKeys  = ValuePool.emptyStringArray;
    protected boolean       resource   = false;
    protected FileAccess    fa;
    protected HashMap       metaData;
    public HsqlProperties() {
        stringProps = new Properties();
        fileName    = null;
    }
    public HsqlProperties(String fileName) {
        this(fileName, ".properties");
    }
    public HsqlProperties(String fileName, String fileExtension) {
        stringProps        = new Properties();
        this.fileName      = fileName;
        this.fileExtension = fileExtension;
        fa                 = FileUtil.getFileUtil();
    }
    public HsqlProperties(HashMap meta, String fileName, FileAccess accessor,
                          boolean b) {
        stringProps        = new Properties();
        this.fileName      = fileName;
        this.fileExtension = ".properties";
        fa                 = accessor;
        metaData           = meta;
    }
    public HsqlProperties(Properties props) {
        stringProps = props;
    }
    public void setFileName(String name) {
        fileName = name;
    }
    public String setProperty(String key, int value) {
        return setProperty(key, Integer.toString(value));
    }
    public String setProperty(String key, boolean value) {
        return setProperty(key, String.valueOf(value));
    }
    public String setProperty(String key, String value) {
        return (String) stringProps.put(key, value);
    }
    public String setPropertyIfNotExists(String key, String value) {
        value = getProperty(key, value);
        return setProperty(key, value);
    }
    public Properties getProperties() {
        return stringProps;
    }
    public String getProperty(String key) {
        return stringProps.getProperty(key);
    }
    public String getProperty(String key, String defaultValue) {
        return stringProps.getProperty(key, defaultValue);
    }
    public int getIntegerProperty(String key, int defaultValue) {
        String prop = getProperty(key);
        try {
            if (prop != null) {
                defaultValue = Integer.parseInt(prop);
            }
        } catch (NumberFormatException e) {}
        return defaultValue;
    }
    public boolean isPropertyTrue(String key) {
        return isPropertyTrue(key, false);
    }
    public boolean isPropertyTrue(String key, boolean defaultValue) {
        String value = stringProps.getProperty(key);
        if (value == null) {
            return defaultValue;
        }
        return value.toLowerCase().equals("true");
    }
    public void removeProperty(String key) {
        stringProps.remove(key);
    }
    public void addProperties(Properties props) {
        if (props == null) {
            return;
        }
        Enumeration keys = props.propertyNames();
        while (keys.hasMoreElements()) {
            String key   = (String) keys.nextElement();
            String value = props.getProperty(key);
            this.stringProps.put(key, value);
        }
    }
    public void addProperties(HsqlProperties props) {
        if (props == null) {
            return;
        }
        addProperties(props.stringProps);
    }
    public boolean propertiesFileExists() {
        if (fileName == null) {
            return false;
        }
        String propFilename = fileName + fileExtension;
        return fa.isStreamElement(propFilename);
    }
    public boolean load() throws Exception {
        if (fileName == null || fileName.length() == 0) {
            throw new FileNotFoundException(
                Error.getMessage(ErrorCode.M_HsqlProperties_load));
        }
        if (!propertiesFileExists()) {
            return false;
        }
        InputStream fis           = null;
        String      propsFilename = fileName + fileExtension;
        try {
            fis = fa.openInputStreamElement(propsFilename);
            stringProps.load(fis);
        } finally {
            if (fis != null) {
                fis.close();
            }
        }
        return true;
    }
    public void save() throws Exception {
        if (fileName == null || fileName.length() == 0) {
            throw new java.io.FileNotFoundException(
                Error.getMessage(ErrorCode.M_HsqlProperties_load));
        }
        String filestring = fileName + fileExtension;
        save(filestring);
    }
    public void save(String fileString) throws Exception {
        fa.createParentDirs(fileString);
        OutputStream        fos = fa.openOutputStreamElement(fileString);
        FileAccess.FileSync outDescriptor = fa.getFileSync(fos);
        JavaSystem.saveProperties(
            stringProps,
            HsqlDatabaseProperties.PRODUCT_NAME + " "
            + HsqlDatabaseProperties.THIS_FULL_VERSION, fos);
        fos.flush();
        outDescriptor.sync();
        fos.close();
        outDescriptor = null;
        fos           = null;
        return;
    }
    protected void addError(int code, String key) {
        errorCodes = (int[]) ArrayUtil.resizeArray(errorCodes,
                errorCodes.length + 1);
        errorKeys = (String[]) ArrayUtil.resizeArray(errorKeys,
                errorKeys.length + 1);
        errorCodes[errorCodes.length - 1] = code;
        errorKeys[errorKeys.length - 1]   = key;
    }
    public static HsqlProperties argArrayToProps(String[] arg, String type) {
        HsqlProperties props = new HsqlProperties();
        for (int i = 0; i < arg.length; i++) {
            String p = arg[i];
            if (p.equals("--help") || p.equals("-help")) {
                props.addError(NO_VALUE_FOR_KEY, p.substring(1));
            } else if (p.startsWith("--")) {
                String value = i + 1 < arg.length ? arg[i + 1]
                                                  : "";
                props.setProperty(type + "." + p.substring(2), value);
                i++;
            } else if (p.charAt(0) == '-') {
                String value = i + 1 < arg.length ? arg[i + 1]
                                                  : "";
                props.setProperty(type + "." + p.substring(1), value);
                i++;
            }
        }
        return props;
    }
    public static HsqlProperties delimitedArgPairsToProps(String s,
            String pairsep, String dlimiter, String type) {
        HsqlProperties props       = new HsqlProperties();
        int            currentpair = 0;
        while (true) {
            int nextpair = s.indexOf(dlimiter, currentpair);
            if (nextpair == -1) {
                nextpair = s.length();
            }
            int valindex = s.substring(0, nextpair).indexOf(pairsep,
                                       currentpair);
            if (valindex == -1) {
                props.addError(NO_VALUE_FOR_KEY,
                               s.substring(currentpair, nextpair).trim());
            } else {
                String key = s.substring(currentpair, valindex).trim();
                String value = s.substring(valindex + pairsep.length(),
                                           nextpair).trim();
                if (type != null) {
                    key = type + "." + key;
                }
                props.setProperty(key, value);
            }
            if (nextpair == s.length()) {
                break;
            }
            currentpair = nextpair + dlimiter.length();
        }
        return props;
    }
    public Enumeration propertyNames() {
        return stringProps.propertyNames();
    }
    public boolean isEmpty() {
        return stringProps.isEmpty();
    }
    public String[] getErrorKeys() {
        return errorKeys;
    }
    public void validate() {}
    public static final int indexName         = 0;
    public static final int indexType         = 1;
    public static final int indexClass        = 2;
    public static final int indexIsRange      = 3;
    public static final int indexDefaultValue = 4;
    public static final int indexRangeLow     = 5;
    public static final int indexRangeHigh    = 6;
    public static final int indexValues       = 7;
    public static final int indexLimit        = 9;
    public static Object[] getMeta(String name, int type,
                                   String defaultValue) {
        Object[] row = new Object[indexLimit];
        row[indexName]         = name;
        row[indexType]         = ValuePool.getInt(type);
        row[indexClass]        = "String";
        row[indexDefaultValue] = defaultValue;
        return row;
    }
    public static Object[] getMeta(String name, int type,
                                   boolean defaultValue) {
        Object[] row = new Object[indexLimit];
        row[indexName]         = name;
        row[indexType]         = ValuePool.getInt(type);
        row[indexClass]        = "Boolean";
        row[indexDefaultValue] = defaultValue ? Boolean.TRUE
                                              : Boolean.FALSE;
        return row;
    }
    public static Object[] getMeta(String name, int type, int defaultValue,
                                   int[] values) {
        Object[] row = new Object[indexLimit];
        row[indexName]         = name;
        row[indexType]         = ValuePool.getInt(type);
        row[indexClass]        = "Integer";
        row[indexDefaultValue] = ValuePool.getInt(defaultValue);
        row[indexValues]       = values;
        return row;
    }
    public static Object[] getMeta(String name, int type, int defaultValue,
                                   int rangeLow, int rangeHigh) {
        Object[] row = new Object[indexLimit];
        row[indexName]         = name;
        row[indexType]         = ValuePool.getInt(type);
        row[indexClass]        = "Integer";
        row[indexDefaultValue] = ValuePool.getInt(defaultValue);
        row[indexIsRange]      = Boolean.TRUE;
        row[indexRangeLow]     = ValuePool.getInt(rangeLow);
        row[indexRangeHigh]    = ValuePool.getInt(rangeHigh);
        return row;
    }
    public static String validateProperty(String key, String value,
                                          Object[] meta) {
        if (meta[indexClass].equals("Boolean")) {
            value = value.toLowerCase();
            if (value.equals("true") || value.equals("false")) {
                return null;
            }
            return "invalid boolean value for property: " + key;
        }
        if (meta[indexClass].equals("String")) {
            return null;
        }
        if (meta[indexClass].equals("Integer")) {
            int number = Integer.parseInt(value);
            if (Boolean.TRUE.equals(meta[indexIsRange])) {
                int low  = ((Integer) meta[indexRangeLow]).intValue();
                int high = ((Integer) meta[indexRangeHigh]).intValue();
                if (number < low || high < number) {
                    return "value outside range for property: " + key;
                }
            }
            if (meta[indexValues] != null) {
                int[] values = (int[]) meta[indexValues];
                if (ArrayUtil.find(values, number) == -1) {
                    return "value not supported for property: " + key;
                }
            }
            return null;
        }
        return null;
    }
    public boolean validateProperty(String name, int number) {
        Object[] meta = (Object[]) metaData.get(name);
        if (meta == null) {
            return false;
        }
        if (meta[indexClass].equals("Integer")) {
            if (Boolean.TRUE.equals(meta[indexIsRange])) {
                int low  = ((Integer) meta[indexRangeLow]).intValue();
                int high = ((Integer) meta[indexRangeHigh]).intValue();
                if (number < low || high < number) {
                    return false;
                }
            }
            if (meta[indexValues] != null) {
                int[] values = (int[]) meta[indexValues];
                if (ArrayUtil.find(values, number) == -1) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }
    public String toString() {
        StringBuffer sb;
        sb = new StringBuffer();
        sb.append('[');
        int         len = stringProps.size();
        Enumeration en  = stringProps.propertyNames();
        for (int i = 0; i < len; i++) {
            String key = (String) en.nextElement();
            sb.append(key);
            sb.append('=');
            sb.append(stringProps.get(key));
            if (i + 1 < len) {
                sb.append(',');
                sb.append(' ');
            }
            sb.append(']');
        }
        return sb.toString();
    }
}