package org.hsqldb.types;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.HashSet;
import org.hsqldb.lib.IntValueHashMap;
import org.hsqldb.persist.HsqlDatabaseProperties;
public class Types {
    public static final String DecimalClassName   = "java.math.BigDecimal";
    public static final String DateClassName      = "java.sql.Date";
    public static final String TimeClassName      = "java.sql.Time";
    public static final String TimestampClassName = "java.sql.Timestamp";
    public static final String BlobClassName      = "java.sql.Blob";
    public static final String ClobClassName      = "java.sql.Clob";
    public static final int SQL_CHAR              = 1;
    public static final int SQL_NUMERIC           = 2;
    public static final int SQL_DECIMAL           = 3;
    public static final int SQL_INTEGER           = 4;
    public static final int SQL_SMALLINT          = 5;
    public static final int SQL_FLOAT             = 6;
    public static final int SQL_REAL              = 7;
    public static final int SQL_DOUBLE            = 8;
    public static final int SQL_VARCHAR           = 12;
    public static final int SQL_BOOLEAN           = 16;
    public static final int SQL_USER_DEFINED_TYPE = 17;
    public static final int SQL_ROW               = 19;
    public static final int SQL_REF               = 20;
    public static final int SQL_BIGINT            = 25;             
    public static final int SQL_BLOB              = 30;             
    public static final int SQL_CLOB              = 40;             
    public static final int SQL_ARRAY             = 50;             
    public static final int SQL_MULTISET = 55;                      
    public static final int SQL_BINARY   = 60;                      
    public static final int SQL_VARBINARY = 61;                     
    public static final int SQL_DATE                      = 91;
    public static final int SQL_TIME                      = 92;
    public static final int SQL_TIMESTAMP                 = 93;     
    public static final int SQL_TIME_WITH_TIME_ZONE       = 94;
    public static final int SQL_TIMESTAMP_WITH_TIME_ZONE  = 95;     
    public static final int SQL_INTERVAL_YEAR             = 101;    
    public static final int SQL_INTERVAL_MONTH            = 102;
    public static final int SQL_INTERVAL_DAY              = 103;
    public static final int SQL_INTERVAL_HOUR             = 104;
    public static final int SQL_INTERVAL_MINUTE           = 105;
    public static final int SQL_INTERVAL_SECOND           = 106;
    public static final int SQL_INTERVAL_YEAR_TO_MONTH    = 107;
    public static final int SQL_INTERVAL_DAY_TO_HOUR      = 108;
    public static final int SQL_INTERVAL_DAY_TO_MINUTE    = 109;
    public static final int SQL_INTERVAL_DAY_TO_SECOND    = 110;
    public static final int SQL_INTERVAL_HOUR_TO_MINUTE   = 111;
    public static final int SQL_INTERVAL_HOUR_TO_SECOND   = 112;
    public static final int SQL_INTERVAL_MINUTE_TO_SECOND = 113;
    public static final int SQL_TYPE_NUMBER_LIMIT = 256;
    public static final int SQL_BIT         = 14;                   
    public static final int SQL_BIT_VARYING = 15;                   
    public static final int SQL_DATALINK         = 70;
    public static final int SQL_UDT              = 17;
    public static final int SQL_UDT_LOCATOR      = 18;
    public static final int SQL_BLOB_LOCATOR     = 31;
    public static final int SQL_CLOB_LOCATOR     = 41;
    public static final int SQL_ARRAY_LOCATOR    = 51;
    public static final int SQL_MULTISET_LOCATOR = 56;
    public static final int SQL_ALL_TYPES        = 0;
    public static final int SQL_DATETIME         = 9;               
    public static final int SQL_INTERVAL         = 10;              
    public static final int SQL_XML              = 137;
    public static final int SQL_NCHAR         = (-8);
    public static final int SQL_WCHAR         = (-8);
    public static final int SQL_WVARCHAR      = (-9);
    public static final int SQL_NVARCHAR      = (-9);
    public static final int SQL_WLONGVARCHAR  = (-10);
    public static final int SQL_NTEXT         = (-10);
    public static final int SQL_LONGVARBINARY = (-4);
    public static final int SQL_IMAGE         = (-4);
    public static final int SQL_GUID          = (-11);
    public static final int SQL_VARIANT       = (-150);
    public static final int SQL_SUB_DISTINCT   = 1;
    public static final int SQL_SUB_STRUCTURED = 2;
    public static final int VARCHAR_IGNORECASE = 100;
    public static final int ARRAY = 2003;
    public static final int BIGINT = -5;
    public static final int BINARY = -2;
    public static final int BIT = -7;
    public static final int BLOB = 2004;
    public static final int BOOLEAN = SQL_BOOLEAN;
    public static final int CHAR = SQL_CHAR;
    public static final int CLOB = 2005;
    public static final int DATALINK = 70;
    public static final int DATE = SQL_DATE;
    public static final int DECIMAL = SQL_DECIMAL;
    public static final int DISTINCT = 2001;
    public static final int DOUBLE = SQL_DOUBLE;
    public static final int FLOAT = SQL_FLOAT;
    public static final int INTEGER = SQL_INTEGER;
    public static final int JAVA_OBJECT = 2000;
    public static final int LONGVARBINARY = -4;
    public static final int LONGVARCHAR = -1;
    public static final int MULTISET = 0;                           
    public static final int NULL = 0;
    public static final int NUMERIC = SQL_NUMERIC;
    public static final int OTHER = 1111;
    public static final int REAL = SQL_REAL;
    public static final int REF = 2006;
    public static final int SMALLINT = SQL_SMALLINT;
    public static final int STRUCT = 2002;
    public static final int TIME = SQL_TIME;
    public static final int TIMESTAMP = SQL_TIMESTAMP;
    public static final int TINYINT = -6;
    public static final int VARBINARY = -3;
    public static final int VARCHAR = SQL_VARCHAR;
    public static final int ROWID = 2008;
    public static final int NCHAR = -8;
    public static final int NVARCHAR = -9;
    public static final int LONGNVARCHAR = -10;
    public static final int NCLOB = 2007;
    public static final int SQLXML = 2009;
    public static final int TYPE_SUB_DEFAULT = 1;
    public static final int TYPE_SUB_IGNORECASE = TYPE_SUB_DEFAULT << 2;
    public static final int[][] ALL_TYPES = {
        {
            SQL_ARRAY, TYPE_SUB_DEFAULT
        }, {
            SQL_BIGINT, TYPE_SUB_DEFAULT
        }, {
            SQL_BINARY, TYPE_SUB_DEFAULT
        }, {
            SQL_VARBINARY, TYPE_SUB_DEFAULT
        }, {
            SQL_BLOB, TYPE_SUB_DEFAULT
        }, {
            SQL_BOOLEAN, TYPE_SUB_DEFAULT
        }, {
            SQL_CHAR, TYPE_SUB_DEFAULT
        }, {
            SQL_CLOB, TYPE_SUB_DEFAULT
        }, {
            DATALINK, TYPE_SUB_DEFAULT
        }, {
            SQL_DATE, TYPE_SUB_DEFAULT
        }, {
            SQL_DECIMAL, TYPE_SUB_DEFAULT
        }, {
            DISTINCT, TYPE_SUB_DEFAULT
        }, {
            SQL_DOUBLE, TYPE_SUB_DEFAULT
        }, {
            SQL_FLOAT, TYPE_SUB_DEFAULT
        }, {
            SQL_INTEGER, TYPE_SUB_DEFAULT
        }, {
            JAVA_OBJECT, TYPE_SUB_DEFAULT
        }, {
            SQL_NCHAR, TYPE_SUB_DEFAULT
        }, {
            NCLOB, TYPE_SUB_DEFAULT
        }, {
            SQL_ALL_TYPES, TYPE_SUB_DEFAULT
        }, {
            SQL_NUMERIC, TYPE_SUB_DEFAULT
        }, {
            SQL_NVARCHAR, TYPE_SUB_DEFAULT
        }, {
            OTHER, TYPE_SUB_DEFAULT
        }, {
            SQL_REAL, TYPE_SUB_DEFAULT
        }, {
            SQL_REF, TYPE_SUB_DEFAULT
        }, {
            ROWID, TYPE_SUB_DEFAULT
        }, {
            SQL_SMALLINT, TYPE_SUB_DEFAULT
        }, {
            STRUCT, TYPE_SUB_DEFAULT
        }, {
            SQL_TIME, TYPE_SUB_DEFAULT
        }, {
            SQL_TIMESTAMP, TYPE_SUB_DEFAULT
        }, {
            TINYINT, TYPE_SUB_DEFAULT
        }, {
            SQL_VARCHAR, TYPE_SUB_DEFAULT
        }, {
            SQL_VARCHAR, TYPE_SUB_IGNORECASE
        }, {
            SQL_XML, TYPE_SUB_DEFAULT
        }
    };
    static final IntValueHashMap javaTypeNumbers;
    private static final HashSet illegalParameterClasses;
    static {
        javaTypeNumbers = new IntValueHashMap(32);
        javaTypeNumbers.put("int", Types.SQL_INTEGER);
        javaTypeNumbers.put("java.lang.Integer", Types.SQL_INTEGER);
        javaTypeNumbers.put("double", Types.SQL_DOUBLE);
        javaTypeNumbers.put("java.lang.Double", Types.SQL_DOUBLE);
        javaTypeNumbers.put("java.lang.String", Types.SQL_VARCHAR);
        javaTypeNumbers.put(DateClassName, Types.SQL_DATE);
        javaTypeNumbers.put(TimeClassName, Types.SQL_TIME);
        javaTypeNumbers.put(TimestampClassName, Types.SQL_TIMESTAMP);
        javaTypeNumbers.put(BlobClassName, Types.SQL_BLOB);
        javaTypeNumbers.put(ClobClassName, Types.SQL_CLOB);
        javaTypeNumbers.put("java.util.Date", Types.SQL_DATE);
        javaTypeNumbers.put(DecimalClassName, Types.SQL_DECIMAL);
        javaTypeNumbers.put("boolean", Types.SQL_BOOLEAN);
        javaTypeNumbers.put("java.lang.Boolean", Types.SQL_BOOLEAN);
        javaTypeNumbers.put("byte", Types.TINYINT);
        javaTypeNumbers.put("java.lang.Byte", Types.TINYINT);
        javaTypeNumbers.put("short", Types.SQL_SMALLINT);
        javaTypeNumbers.put("java.lang.Short", Types.SQL_SMALLINT);
        javaTypeNumbers.put("long", Types.SQL_BIGINT);
        javaTypeNumbers.put("java.lang.Long", Types.SQL_BIGINT);
        javaTypeNumbers.put("[B", Types.SQL_BINARY);
        javaTypeNumbers.put("java.lang.Object", Types.OTHER);
        javaTypeNumbers.put("java.lang.Void", Types.SQL_ALL_TYPES);
        illegalParameterClasses = new org.hsqldb.lib.HashSet();
        illegalParameterClasses.add(Byte.TYPE);
        illegalParameterClasses.add(Short.TYPE);
        illegalParameterClasses.add(Float.TYPE);
        illegalParameterClasses.add(Byte.class);
        illegalParameterClasses.add(Short.class);
        illegalParameterClasses.add(Float.class);
    }
    public static Type getParameterSQLType(Class c) {
        String  name;
        int     typeCode;
        boolean isArray;
        if (c == null) {
            throw Error.runtimeError(ErrorCode.U_S0500, "Types");
        }
        if (Void.TYPE.equals(c)) {
            return Type.SQL_ALL_TYPES;
        }
        name     = c.getName();
        typeCode = javaTypeNumbers.get(name, Integer.MIN_VALUE);
        if (typeCode != Integer.MIN_VALUE) {
            return Type.getDefaultTypeWithSize(typeCode);
        }
        if (c.isArray()) {
            Class c1 = c.getComponentType();
            name     = c1.getName();
            typeCode = javaTypeNumbers.get(name, Integer.MIN_VALUE);
            if (typeCode != Integer.MIN_VALUE) {
                return Type.getDefaultTypeWithSize(typeCode);
            }
            if (typeCode == Types.SQL_ALL_TYPES) {
                return null;
            }
            return Type.getDefaultTypeWithSize(typeCode);
        }
        if (name.equals("java.sql.Array")) {
            return Type.getDefaultArrayType(Types.SQL_ALL_TYPES);
        }
        return null;
    }
    public static boolean acceptsZeroPrecision(int type) {
        switch (type) {
            case Types.SQL_TIME :
            case Types.SQL_TIMESTAMP :
                return true;
            default :
                return false;
        }
    }
    public static boolean requiresPrecision(int type) {
        switch (type) {
            case Types.SQL_BIT_VARYING :
            case Types.SQL_VARBINARY :
            case Types.SQL_VARCHAR :
            case Types.SQL_NVARCHAR :
            case Types.VARCHAR_IGNORECASE :
                return true;
            default :
                return false;
        }
    }
    public static boolean acceptsPrecision(int type) {
        switch (type) {
            case Types.LONGVARCHAR :
            case Types.LONGVARBINARY :
            case Types.SQL_ARRAY :
            case Types.SQL_BINARY :
            case Types.SQL_BIT :
            case Types.SQL_BIT_VARYING :
            case Types.SQL_BLOB :
            case Types.SQL_CHAR :
            case Types.SQL_NCHAR :
            case Types.SQL_CLOB :
            case Types.NCLOB :
            case Types.SQL_VARBINARY :
            case Types.SQL_VARCHAR :
            case Types.SQL_NVARCHAR :
            case Types.VARCHAR_IGNORECASE :
            case Types.SQL_DECIMAL :
            case Types.SQL_NUMERIC :
            case Types.SQL_FLOAT :
            case Types.SQL_TIME :
            case Types.SQL_TIMESTAMP :
            case Types.SQL_INTERVAL_YEAR :
            case Types.SQL_INTERVAL_YEAR_TO_MONTH :
            case Types.SQL_INTERVAL_MONTH :
            case Types.SQL_INTERVAL_DAY :
            case Types.SQL_INTERVAL_DAY_TO_HOUR :
            case Types.SQL_INTERVAL_DAY_TO_MINUTE :
            case Types.SQL_INTERVAL_DAY_TO_SECOND :
            case Types.SQL_INTERVAL_HOUR :
            case Types.SQL_INTERVAL_HOUR_TO_MINUTE :
            case Types.SQL_INTERVAL_HOUR_TO_SECOND :
            case Types.SQL_INTERVAL_MINUTE :
            case Types.SQL_INTERVAL_MINUTE_TO_SECOND :
            case Types.SQL_INTERVAL_SECOND :
                return true;
            default :
                return false;
        }
    }
    public static boolean acceptsScaleCreateParam(int type) {
        switch (type) {
            case Types.SQL_INTERVAL_SECOND :
                return true;
            case Types.SQL_DECIMAL :
            case Types.SQL_NUMERIC :
                return true;
            default :
                return false;
        }
    }
    public static final int MAX_CHAR_OR_VARCHAR_DISPLAY_SIZE =
        MAX_CHAR_OR_VARCHAR_DISPLAY_SIZE();
    private static int MAX_CHAR_OR_VARCHAR_DISPLAY_SIZE() {
        try {
            return Integer.getInteger(
                HsqlDatabaseProperties.system_max_char_or_varchar_display_size,
                32766).intValue();
        } catch (SecurityException e) {
            return 32766;
        }
    }
    public static boolean isSearchable(int type) {
        switch (type) {
            case Types.SQL_BLOB :
            case Types.SQL_CLOB :
            case Types.NCLOB :
            case Types.JAVA_OBJECT :
            case Types.STRUCT :
            case Types.OTHER :
            case Types.ROWID :
                return false;
            case Types.SQL_ARRAY :
            default :
                return true;
        }
    }
}