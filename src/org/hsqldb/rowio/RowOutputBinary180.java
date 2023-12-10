package org.hsqldb.rowio;
import org.hsqldb.HsqlDateTime;
import org.hsqldb.types.TimeData;
import org.hsqldb.types.TimestampData;
import org.hsqldb.types.Type;
import org.hsqldb.types.Types;
public class RowOutputBinary180 extends RowOutputBinary {
    public RowOutputBinary180(int initialSize, int scale) {
        super(initialSize, scale);
    }
    protected void writeDate(TimestampData o, Type type) {
        long millis = o.getSeconds() * 1000L;
        millis =
            HsqlDateTime.convertMillisToCalendar(HsqlDateTime.tempCalDefault,
                millis);
        writeLong(millis);
        writeLong(o.getSeconds() * 1000L);
    }
    protected void writeTime(TimeData o, Type type) {
        if (type.typeCode == Types.SQL_TIME) {
            long millis = o.getSeconds() * 1000L;
            millis = HsqlDateTime.convertMillisToCalendar(
                HsqlDateTime.tempCalDefault, millis);
            writeLong(millis);
        } else {
            writeInt(o.getSeconds());
            writeInt(o.getNanos());
            writeInt(o.getZone());
        }
    }
    protected void writeTimestamp(TimestampData o, Type type) {
        if (type.typeCode == Types.SQL_TIMESTAMP) {
            long millis = o.getSeconds() * 1000L;
            millis = HsqlDateTime.convertMillisToCalendar(
                HsqlDateTime.tempCalDefault, millis);
            writeLong(millis);
            writeInt(o.getNanos());
        } else {
            writeLong(o.getSeconds());
            writeInt(o.getNanos());
            writeInt(o.getZone());
        }
    }
}