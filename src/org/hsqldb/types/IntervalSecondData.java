


package org.hsqldb.types;

import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;


public class IntervalSecondData {

    public final static IntervalSecondData oneDay = newIntervalDay(1,
        Type.SQL_INTERVAL_DAY);

    
    final long units;
    final int  nanos;

    public static IntervalSecondData newIntervalDay(long days,
            IntervalType type) {
        return new IntervalSecondData(days * 24 * 60 * 60, 0, type);
    }

    public static IntervalSecondData newIntervalHour(long hours,
            IntervalType type) {
        return new IntervalSecondData(hours * 60 * 60, 0, type);
    }

    public static IntervalSecondData newIntervalMinute(long minutes,
            IntervalType type) {
        return new IntervalSecondData(minutes * 60, 0, type);
    }

    public static IntervalSecondData newIntervalSeconds(long seconds,
            IntervalType type) {
        return new IntervalSecondData(seconds, 0, type);
    }

    public IntervalSecondData(long seconds, int nanos, IntervalType type) {

        if (seconds >= type.getIntervalValueLimit()) {
            throw Error.error(ErrorCode.X_22015);
        }

        this.units = seconds;
        this.nanos = nanos;
    }

    public IntervalSecondData(long seconds, int nanos) {
        this.units = seconds;
        this.nanos = nanos;
    }

    
    public IntervalSecondData(long seconds, long nanos, IntervalType type,
                              boolean normalise) {

        if (nanos >= DTIType.limitNanoseconds) {
            long carry = nanos / DTIType.limitNanoseconds;

            nanos   = nanos % DTIType.limitNanoseconds;
            seconds += carry;
        } else if (nanos <= -DTIType.limitNanoseconds) {
            long carry = -nanos / DTIType.limitNanoseconds;

            nanos   = -(-nanos % DTIType.limitNanoseconds);
            seconds -= carry;
        }

        int scaleFactor = DTIType.nanoScaleFactors[type.scale];

        nanos /= scaleFactor;
        nanos *= scaleFactor;

        if (seconds > 0 && nanos < 0) {
            nanos += DTIType.limitNanoseconds;

            seconds--;
        } else if (seconds < 0 && nanos > 0) {
            nanos -= DTIType.limitNanoseconds;

            seconds++;
        }

        scaleFactor = DTIType.yearToSecondFactors[type.endPartIndex];
        seconds     /= scaleFactor;
        seconds     *= scaleFactor;

        if (seconds >= type.getIntervalValueLimit()) {
            throw Error.error(ErrorCode.X_22015);
        }

        this.units = seconds;
        this.nanos = (int) nanos;
    }

    public boolean equals(Object other) {

        if (other instanceof IntervalSecondData) {
            return units == ((IntervalSecondData) other).units
                   && nanos == ((IntervalSecondData) other).nanos;
        }

        return false;
    }

    public int hashCode() {
        return (int) units ^ nanos;
    }

    public int compareTo(IntervalSecondData b) {

        long diff = units - b.units;

        if (diff == 0) {
            diff = nanos - b.nanos;

            if (diff == 0) {
                return 0;
            }
        }

        return diff > 0 ? 1
                        : -1;
    }

    public long getSeconds() {
        return units;
    }

    public int getNanos() {
        return nanos;
    }

    public String toString() {
        return Type.SQL_INTERVAL_SECOND_MAX_FRACTION_MAX_PRECISION
            .convertToString(this);
    }
}
