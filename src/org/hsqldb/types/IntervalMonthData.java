package org.hsqldb.types;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
public class IntervalMonthData {
    public final long units;
    public static IntervalMonthData newIntervalYear(long years,
            IntervalType type) {
        return new IntervalMonthData(years * 12, type);
    }
    public static IntervalMonthData newIntervalMonth(long months,
            IntervalType type) {
        return new IntervalMonthData(months, type);
    }
    public IntervalMonthData(long months, IntervalType type) {
        if (months >= type.getIntervalValueLimit()) {
            throw Error.error(ErrorCode.X_22006);
        }
        if (type.typeCode == Types.SQL_INTERVAL_YEAR) {
            months -= (months % 12);
        }
        this.units = months;
    }
    public IntervalMonthData(long months) {
        this.units = months;
    }
    public boolean equals(Object other) {
        if (other instanceof IntervalMonthData) {
            return units == ((IntervalMonthData) other).units;
        }
        return false;
    }
    public int hashCode() {
        return (int) units;
    }
    public int compareTo(IntervalMonthData b) {
        long diff = units - b.units;
        if (diff == 0) {
            return 0;
        } else {
            return diff > 0 ? 1
                            : -1;
        }
    }
    public String toString() {
        return Type.SQL_INTERVAL_MONTH_MAX_PRECISION.convertToString(this);
    }
}