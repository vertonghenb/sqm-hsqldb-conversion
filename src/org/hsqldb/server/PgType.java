




package org.hsqldb.server;

import java.sql.SQLException;

import org.hsqldb.HsqlException;
import org.hsqldb.Session;
import org.hsqldb.jdbc.Util;
import org.hsqldb.types.Type;
import org.hsqldb.types.Types;


public class PgType {
    private int oid;
    private int typeWidth = -1;
    private int lpConstraint = -1; 
    private Type hType;

    public int getOid() {
        return oid;
    }
    public int getTypeWidth() {
        return typeWidth;
    }
    public int getLPConstraint() {
        return lpConstraint;
    }

    
    protected PgType(Type hType, int oid) {
        this(hType, oid, null, null);
    }

    
    protected PgType(Type hType, int oid, int typeWidth) {
        this(hType, oid, new Integer(typeWidth), null);
    }

    
    protected PgType(Type hType, int oid, Integer dummy, long lpConstraint)
    throws RecoverableOdbcFailure {
        this(hType, oid, dummy, new Integer((int) lpConstraint));
        if (lpConstraint < 0) {
            throw new RecoverableOdbcFailure(
                "Length/Precision value is below minimum value of 0");
        }
        if (lpConstraint > Integer.MAX_VALUE) {
            throw new RecoverableOdbcFailure(
                "Length/Precision value is above maximum value of "
                + Integer.MAX_VALUE);
        }
    }

    
    protected PgType(Type hType,
        int oid, Integer typeWidthObject, Integer lpConstraintObject) {
        this.hType = hType;
        this.oid = oid;
        this.typeWidth = (typeWidthObject == null)
                       ? -1 : typeWidthObject.intValue();
        this.lpConstraint = (lpConstraintObject == null)
                       ? -1 : lpConstraintObject.intValue();
    }

    public static PgType getPgType(Type hType, boolean directColumn)
    throws RecoverableOdbcFailure {
        switch (hType.typeCode) {
            case Types.TINYINT:
                return tinyIntSingleton;
            case Types.SQL_SMALLINT:
                return int2singleton;
            case Types.SQL_INTEGER:
                return int4singleton;
            case Types.SQL_BIGINT:
                return int8singleton;

            case Types.SQL_NUMERIC:
            case Types.SQL_DECIMAL:
                return new PgType(hType, TYPE_NUMERIC, null,
                        (hType.precision << 16) + hType.scale + 4);

            case Types.SQL_FLOAT:
                
                
                
            case Types.SQL_DOUBLE:
            case Types.SQL_REAL:
                return doubleSingleton;

            case Types.BOOLEAN:
                return boolSingleton;

            case Types.SQL_CHAR: 
                if (directColumn) {
                    return new PgType(hType,
                        TYPE_BPCHAR, null, hType.precision + 4);
                }
                return unknownSingleton;  

            case Types.SQL_VARCHAR: 
            case Types.VARCHAR_IGNORECASE: 
                if (hType.precision < 0) {
                    throw new RecoverableOdbcFailure (
                        "Length/Precision value is below minimum value of 0");
                }
                if (hType.precision > Integer.MAX_VALUE) {
                    throw new RecoverableOdbcFailure (
                        "Length/Precision value is above maximum value of "
                        + Integer.MAX_VALUE);
                }
                return (hType.precision != 0 && directColumn)
                    ? new PgType(hType, TYPE_VARCHAR, null, hType.precision + 4)
                    : textSingleton;
                
                
            case Types.SQL_CLOB: 
                throw new RecoverableOdbcFailure (
                    "Driver doesn't support type 'CLOB' yet");

            case Types.SQL_BLOB: 
                return new PgType(hType, TYPE_BLOB, null, hType.precision);
            case Types.SQL_BINARY:
            case Types.SQL_VARBINARY: 
                return new PgType(hType, TYPE_BYTEA, null, hType.precision);
                
                
                
                
                
                

            case Types.OTHER:
                throw new RecoverableOdbcFailure (
                    "Driver doesn't support type 'OTHER' yet");

            case Types.SQL_BIT:
                return bitSingleton;
            case Types.SQL_BIT_VARYING:
                return bitVaryingSingleton;
                
                

            case Types.SQL_DATE:
                return dateSingleton;

            
            case Types.SQL_TIME :
                return new PgType(hType, TYPE_TIME, new Integer(8),
                                  hType.precision);

            case Types.SQL_TIME_WITH_TIME_ZONE :
                return new PgType(hType, TYPE_TIME_WITH_TMZONE,
                                  new Integer(12), hType.precision);

            case Types.SQL_TIMESTAMP :
                return new PgType(hType, TYPE_TIMESTAMP_NO_TMZONE,
                                  new Integer(8), hType.precision);

            case Types.SQL_TIMESTAMP_WITH_TIME_ZONE :
                return new PgType(hType, TYPE_TIMESTAMP, new Integer(8),
                                  hType.precision);

            
            
            
            case Types.SQL_INTERVAL_YEAR:
            case Types.SQL_INTERVAL_YEAR_TO_MONTH:
            case Types.SQL_INTERVAL_MONTH:
                
                
                throw new RecoverableOdbcFailure (
                    "Driver doesn't support month-resolution 'INTERVAL's yet");
            case Types.SQL_INTERVAL_DAY:
            case Types.SQL_INTERVAL_DAY_TO_HOUR:
            case Types.SQL_INTERVAL_DAY_TO_MINUTE:
            case Types.SQL_INTERVAL_HOUR:
            case Types.SQL_INTERVAL_HOUR_TO_MINUTE:
            case Types.SQL_INTERVAL_MINUTE:
                
                
                
                
                
                throw new RecoverableOdbcFailure (
                    "Driver doesn't support non-second-resolution 'INTERVAL's "
                    + "yet");
            case Types.SQL_INTERVAL_DAY_TO_SECOND:
                PgType.ignoredConstraintWarning(hType);
                return daySecIntervalSingleton;
            case Types.SQL_INTERVAL_HOUR_TO_SECOND:
                PgType.ignoredConstraintWarning(hType);
                return hourSecIntervalSingleton;
            case Types.SQL_INTERVAL_MINUTE_TO_SECOND:
                PgType.ignoredConstraintWarning(hType);
                return minSecIntervalSingleton;
            case Types.SQL_INTERVAL_SECOND:
                PgType.ignoredConstraintWarning(hType);
                return secIntervalSingleton;

            default:
                throw new RecoverableOdbcFailure (
                    "Unsupported type: " + hType.getNameString());
        }
    }

    
    public Object getParameter(String inString, Session session)
    throws SQLException, RecoverableOdbcFailure {
        if (inString == null) {
            return null;
        }
        Object o = inString;

        switch (hType.typeCode) {
            case Types.SQL_BOOLEAN :
                if (inString.length() == 1) switch (inString.charAt(0)) {
                    case 'T':
                    case 't':
                    case 'Y':
                    case 'y':
                    case '1':
                        return Boolean.TRUE;
                    default:
                        return Boolean.FALSE;
                }
                return Boolean.valueOf(inString);

            case Types.SQL_BINARY :
            case Types.SQL_VARBINARY :
            case Types.SQL_BLOB :
                throw new RecoverableOdbcFailure(
                    "This data type should be transmitted to server in binary "
                    + "format: " + hType.getNameString());

            case Types.OTHER :
            case Types.SQL_CLOB :
                throw new RecoverableOdbcFailure(
                        "Type not supported yet: " + hType.getNameString());
                
            case Types.SQL_DATE :
            case Types.SQL_TIME_WITH_TIME_ZONE :
            case Types.SQL_TIMESTAMP_WITH_TIME_ZONE :
            case Types.SQL_TIME :
            case Types.SQL_TIMESTAMP : {
                try {
                    o = hType.convertToType(session, o, Type.SQL_VARCHAR);
                } catch (HsqlException e) {
                    PgType.throwError(e);
                }
                break;
            }

            case Types.TINYINT :
            case Types.SQL_SMALLINT :
            case Types.SQL_INTEGER :
            case Types.SQL_BIGINT :
            case Types.SQL_REAL :
            case Types.SQL_FLOAT :
            case Types.SQL_DOUBLE :
            case Types.SQL_NUMERIC :
            case Types.SQL_DECIMAL :
                try {
                    o = hType.convertToType(session, o, Type.SQL_VARCHAR);
                } catch (HsqlException e) {
                    PgType.throwError(e);
                }
                break;
            default :
                
                try {
                    o = hType.convertToDefaultType(session, o);
                    
                } catch (HsqlException e) {
                    PgType.throwError(e);
                }
                break;
        }
        return o;
    }

    public String valueString(Object datum) {
        String dataString = hType.convertToString(datum);
        switch (hType.typeCode) {
            case Types.SQL_BOOLEAN :
                return String.valueOf(((Boolean) datum).booleanValue()
                    ? 't' : 'f');
                
                
                
                
            case Types.SQL_VARBINARY :
            case Types.SQL_BINARY :
                dataString = OdbcUtil.hexCharsToOctalOctets(dataString);
                break;
        }
        return dataString;
    }

    
    public static final int TYPE_BOOL         =  16;
    public static final int TYPE_BYTEA        =  17;
    public static final int TYPE_CHAR         =  18;
    public static final int TYPE_NAME         =  19;
    public static final int TYPE_INT8         =  20;
    public static final int TYPE_INT2         =  21;
    public static final int TYPE_INT2VECTOR   =  22;
    public static final int TYPE_INT4         =  23;
    public static final int TYPE_REGPROC      =  24;
    public static final int TYPE_TEXT         =  25;
    public static final int TYPE_OID          =  26;
    public static final int TYPE_TID          =  27;
    public static final int TYPE_XID          =  28;
    public static final int TYPE_CID          =  29;
    public static final int TYPE_OIDVECTOR    =  30;
    public static final int TYPE_SET          =  32;
    public static final int TYPE_XML          = 142;
    public static final int TYPE_XMLARRAY     = 143;
    public static final int TYPE_CHAR2        = 409;
    public static final int TYPE_CHAR4        = 410;
    public static final int TYPE_CHAR8        = 411;
    public static final int TYPE_POINT        = 600;
    public static final int TYPE_LSEG         = 601;
    public static final int TYPE_PATH         = 602;
    public static final int TYPE_BOX          = 603;
    public static final int TYPE_POLYGON      = 604;
    public static final int TYPE_FILENAME     = 605;
    public static final int TYPE_CIDR         = 650;
    public static final int TYPE_FLOAT4       = 700;
    public static final int TYPE_FLOAT8       = 701;
    public static final int TYPE_ABSTIME      = 702;
    public static final int TYPE_RELTIME      = 703;
    public static final int TYPE_TINTERVAL    = 704;
    public static final int TYPE_UNKNOWN      = 705;
    public static final int TYPE_MONEY        = 790;
    public static final int TYPE_OIDINT2      = 810;
    public static final int TYPE_MACADDR      = 829;
    public static final int TYPE_INET         = 869;
    public static final int TYPE_OIDINT4      = 910;
    public static final int TYPE_OIDNAME      = 911;
    public static final int TYPE_TEXTARRAY    = 1009;
    public static final int TYPE_BPCHARARRAY  = 1014;
    public static final int TYPE_VARCHARARRAY = 1015;
    public static final int TYPE_BPCHAR       = 1042;
    public static final int TYPE_VARCHAR      = 1043;
    public static final int TYPE_DATE         = 1082;
    public static final int TYPE_TIME         = 1083;
    public static final int TYPE_TIMESTAMP_NO_TMZONE = 1114; 
    public static final int TYPE_DATETIME     = 1184;
    public static final int TYPE_TIME_WITH_TMZONE   = 1266;   
    public static final int TYPE_TIMESTAMP    = 1296; 
    public static final int TYPE_NUMERIC      = 1700;
    public static final int TYPE_RECORD       = 2249;
    public static final int TYPE_VOID         = 2278;
    public static final int TYPE_UUID         = 2950;

    
    
    public static final int TYPE_BLOB         = 9998;
    public static final int TYPE_TINYINT      = 9999;

    
    public static final int TYPE_BIT          = 1560;
    
    public static final int TYPE_VARBIT       = 1562;
    

    
    static final void throwError(HsqlException e) throws SQLException {


        throw Util.sqlException(e.getMessage(), e.getSQLState(),
            e.getErrorCode(), e);





    }

    static protected final PgType tinyIntSingleton =
        new PgType(Type.TINYINT, TYPE_TINYINT, 1);
    static protected final PgType int2singleton =
        new PgType(Type.SQL_SMALLINT, TYPE_INT2, 2);
    static protected final PgType int4singleton =
        new PgType(Type.SQL_INTEGER, TYPE_INT4, 4);
    static protected final PgType int8singleton =
        new PgType(Type.SQL_BIGINT, TYPE_INT8, 8);
    static protected final PgType doubleSingleton =
        new PgType(Type.SQL_DOUBLE, TYPE_FLOAT8, 8);
    static protected final PgType boolSingleton =
        new PgType(Type.SQL_BOOLEAN, TYPE_BOOL, 1);
    static protected final PgType textSingleton =
        new PgType(Type.SQL_VARCHAR, TYPE_TEXT);
    static protected final PgType dateSingleton =
        new PgType(Type.SQL_DATE, TYPE_DATE, 4);
    static protected final PgType unknownSingleton =
        new PgType(Type.SQL_CHAR_DEFAULT, TYPE_UNKNOWN, -2);
    static protected final PgType bitSingleton =
        new PgType(Type.SQL_BIT, TYPE_BIT);
    static protected final PgType bitVaryingSingleton =
        new PgType(Type.SQL_BIT_VARYING, TYPE_VARBIT);
    static protected final PgType daySecIntervalSingleton =
        new PgType(Type.SQL_INTERVAL_DAY_TO_SECOND, TYPE_TINTERVAL, 16);
    static protected final PgType hourSecIntervalSingleton =
        new PgType(Type.SQL_INTERVAL_HOUR_TO_SECOND, TYPE_TINTERVAL, 16);
    static protected final PgType minSecIntervalSingleton =
        new PgType(Type.SQL_INTERVAL_MINUTE_TO_SECOND, TYPE_TINTERVAL, 16);
    static protected final PgType secIntervalSingleton =
        new PgType(Type.SQL_INTERVAL_SECOND, TYPE_TINTERVAL, 16);

    static private void ignoredConstraintWarning(Type hsqldbType) {
        if (hsqldbType.precision == 0 && hsqldbType.scale == 0) {
            return;
        }
        
        
    }
}
