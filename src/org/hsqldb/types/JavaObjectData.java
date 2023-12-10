package org.hsqldb.types;
import java.io.Serializable;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.InOutUtil;
public class JavaObjectData {
    private byte[] data;
    JavaObjectData() {}
    public JavaObjectData(byte[] data) {
        this.data = data;
    }
    public JavaObjectData(Serializable o) {
        try {
            data = InOutUtil.serialize(o);
        } catch (Exception e) {
            throw Error.error(ErrorCode.X_22521, e.toString());
        }
    }
    public byte[] getBytes() {
        return data;
    }
    public int getBytesLength() {
        return data.length;
    }
    public Serializable getObject() {
        try {
            return InOutUtil.deserialize(data);
        } catch (Exception e) {
            throw Error.error(ErrorCode.X_22521, e.toString());
        }
    }
    public String toString() {
        return super.toString();
    }
    public boolean equals(Object other) {
        return other instanceof JavaObjectData;
    }
    public int hashCode() {
        return 1;
    }
}