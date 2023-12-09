


package org.hsqldb.rowio;

import java.io.IOException;

import org.hsqldb.HsqlDateTime;
import org.hsqldb.types.TimeData;
import org.hsqldb.types.TimestampData;
import org.hsqldb.types.Type;
import org.hsqldb.types.Types;


public class RowInputBinary180 extends RowInputBinary {

    public RowInputBinary180(byte[] buf) {
        super(buf);
    }

    protected TimeData readTime(Type type) throws IOException {

        if (type.typeCode == Types.SQL_TIME) {
            long millis = readLong();

            millis = HsqlDateTime.convertMillisFromCalendar(
                HsqlDateTime.tempCalDefault, millis);
            millis = HsqlDateTime.getNormalisedTime(millis);

            return new TimeData((int) (millis / 1000), 0, 0);
        } else {
            return new TimeData(readInt(), readInt(), readInt());
        }
    }

    protected TimestampData readDate(Type type) throws IOException {

        long millis = readLong();

        millis = HsqlDateTime.convertMillisFromCalendar(HsqlDateTime.tempCalDefault,
                                               millis);

        millis = HsqlDateTime.getNormalisedDate(millis);

        return new TimestampData(millis / 1000);
    }

    protected TimestampData readTimestamp(Type type) throws IOException {

        if (type.typeCode == Types.SQL_TIMESTAMP) {
            long millis = readLong();
            int  nanos  = readInt();

            millis = HsqlDateTime.convertMillisFromCalendar(HsqlDateTime.tempCalDefault,
                                                   millis);

            return new TimestampData(millis / 1000, nanos);
        } else {
            return new TimestampData(readLong(), readInt(), readInt());
        }
    }
}
