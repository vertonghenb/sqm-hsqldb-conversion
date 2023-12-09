


package org.hsqldb.types;


public class TimeData {

    final int zone;
    final int seconds;
    final int nanos;

    public TimeData(int seconds, int nanos, int zoneSeconds) {

        while (seconds < 0) {
            seconds += 24 * 60 * 60;
        }

        if (seconds > 24 * 60 * 60) {
            seconds %= 24 * 60 * 60;
        }

        this.zone    = zoneSeconds;
        this.seconds = seconds;
        this.nanos   = nanos;
    }

    public TimeData(int seconds, int nanos) {
        this (seconds, nanos, 0);
    }

    public int getSeconds() {
        return seconds;
    }

    public int getNanos() {
        return nanos;
    }

    public int getZone() {
        return zone;
    }

    public boolean equals(Object other) {

        if (other instanceof TimeData) {
            return seconds == ((TimeData) other).seconds
                   && nanos == ((TimeData) other).nanos
                   && zone ==  ((TimeData) other).zone ;
        }

        return false;
    }

    public int hashCode() {
        return seconds ^ nanos;
    }

    public int compareTo(TimeData b) {

        long diff = seconds - b.seconds;

        if (diff == 0) {
            diff = nanos - b.nanos;

            if (diff == 0) {
                return 0;
            }
        }

        return diff > 0 ? 1
                        : -1;
    }
}
