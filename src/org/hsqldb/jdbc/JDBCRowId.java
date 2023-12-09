


package org.hsqldb.jdbc;

import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.StringConverter;

import java.io.IOException;
import java.sql.RowId;
import java.sql.SQLException;
import java.util.Arrays;






public final class JDBCRowId implements RowId {

    private int hash;

    
    private final byte[] id;

    
    public JDBCRowId(final byte[] id) throws SQLException {

        if (id == null) {
            throw Util.nullArgument("id");
        }
        this.id = id;
    }

    
    public JDBCRowId(RowId id) throws SQLException {
        this(id.getBytes());
    }

    
    public JDBCRowId(final String hex) throws SQLException {

        if (hex == null) {
            throw Util.nullArgument("hex");
        }

        try {
            this.id = StringConverter.hexStringToByteArray(hex);
        } catch (IOException e) {
            throw Util.sqlException(ErrorCode.JDBC_INVALID_ARGUMENT,
                                    "hex: " + e);

            
        }
    }

    
    public boolean equals(Object obj) {
        return (obj instanceof JDBCRowId)
               && Arrays.equals(this.id, ((JDBCRowId) obj).id);
    }

    
    public byte[] getBytes() {
        return id.clone();
    }

    
    public String toString() {
        return StringConverter.byteArrayToHexString(id);
    }

    
    public int hashCode() {

        if (hash == 0) {
            hash = Arrays.hashCode(id);
        }

        return hash;
    }

    
    Object id() {
        return id;
    }
}
