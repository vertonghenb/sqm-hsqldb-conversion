


package org.hsqldb.types;

import org.hsqldb.Session;
import org.hsqldb.SessionInterface;
import org.hsqldb.Tokens;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.jdbc.JDBCBlobClient;
import org.hsqldb.lib.StringConverter;


public final class BlobType extends BinaryType {

    public static final long maxBlobPrecision = 1024L * 1024 * 1024 * 1024;
    public static final int  defaultBlobSize  = 1024 * 1024 * 16;

    public BlobType(long precision) {
        super(Types.SQL_BLOB, precision);
    }

    public int displaySize() {
        return precision > Integer.MAX_VALUE ? Integer.MAX_VALUE
                                             : (int) precision;
    }

    public int getJDBCTypeCode() {
        return Types.BLOB;
    }

    public Class getJDBCClass() {
        return java.sql.Blob.class;
    }

    public String getJDBCClassName() {
        return "java.sql.Blob";
    }

    public String getNameString() {
        return Tokens.T_BLOB;
    }

    public String getFullNameString() {
        return "BINARY LARGE OBJECT";
    }

    public String getDefinition() {

        long   factor     = precision;
        String multiplier = null;

        if (precision % (1024 * 1024 * 1024) == 0) {
            factor     = precision / (1024 * 1024 * 1024);
            multiplier = Tokens.T_G_FACTOR;
        } else if (precision % (1024 * 1024) == 0) {
            factor     = precision / (1024 * 1024);
            multiplier = Tokens.T_M_FACTOR;
        } else if (precision % (1024) == 0) {
            factor     = precision / (1024);
            multiplier = Tokens.T_K_FACTOR;
        }

        StringBuffer sb = new StringBuffer(16);

        sb.append(getNameString());
        sb.append('(');
        sb.append(factor);

        if (multiplier != null) {
            sb.append(multiplier);
        }

        sb.append(')');

        return sb.toString();
    }

    public boolean acceptsPrecision() {
        return true;
    }

    public boolean requiresPrecision() {
        return false;
    }

    public long getMaxPrecision() {
        return maxBlobPrecision;
    }

    public boolean isBinaryType() {
        return true;
    }

    public boolean isLobType() {
        return true;
    }

    public int compare(Session session, Object a, Object b) {

        if (a == b) {
            return 0;
        }

        if (a == null) {
            return -1;
        }

        if (b == null) {
            return 1;
        }

        if (b instanceof BinaryData) {
            return session.database.lobManager.compare((BlobData) a,
                    ((BlobData) b).getBytes());
        }

        return session.database.lobManager.compare((BlobData) a, (BlobData) b);
    }

    
    public Object convertToTypeLimits(SessionInterface session, Object a) {
        return a;
    }

    public Object convertToType(SessionInterface session, Object a,
                                Type otherType) {

        if (a == null) {
            return null;
        }

        if (otherType.typeCode == Types.SQL_BLOB) {
            return a;
        }

        if (otherType.typeCode == Types.SQL_BINARY
                || otherType.typeCode == Types.SQL_VARBINARY) {
            BlobData b    = (BlobData) a;
            BlobData blob = session.createBlob(b.length(session));

            blob.setBytes(session, 0, b.getBytes());

            return blob;
        }

        throw Error.error(ErrorCode.X_42561);
    }

    public Object convertToDefaultType(SessionInterface session, Object a) {

        if (a == null) {
            return a;
        }

        
        if (a instanceof byte[]) {
            return new BinaryData((byte[]) a, false);
        }

        throw Error.error(ErrorCode.X_42561);
    }

    public String convertToString(Object a) {

        if (a == null) {
            return null;
        }

        byte[] bytes = ((BlobData) a).getBytes();

        return StringConverter.byteArrayToHexString(bytes);
    }

    public String convertToSQLString(Object a) {

        if (a == null) {
            return Tokens.T_NULL;
        }

        byte[] bytes = ((BlobData) a).getBytes();

        return StringConverter.byteArrayToSQLHexString(bytes);
    }

    public Object convertJavaToSQL(SessionInterface session, Object a) {

        if (a == null) {
            return null;
        }

        if (a instanceof JDBCBlobClient) {
            return ((JDBCBlobClient) a).getBlob();
        }

        throw Error.error(ErrorCode.X_42561);
    }

    public Object convertSQLToJava(SessionInterface session, Object a) {

        if (a == null) {
            return null;
        }

        if (a instanceof BlobDataID) {
            BlobDataID blob = (BlobDataID) a;

            return new JDBCBlobClient(session, blob);
        }

        throw Error.error(ErrorCode.X_42561);
    }
}
