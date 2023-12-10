package org.hsqldb.types;
public class TimestampData {
    final long seconds;
    final int  nanos;
    final int  zone;
    public TimestampData(long seconds) {
        this.seconds = seconds;
        this.nanos   = 0;
        this.zone    = 0;
    }
    public TimestampData(long seconds, int nanos) {
        this.seconds = seconds;
        this.nanos   = nanos;
        this.zone    = 0;
    }
    public TimestampData(long seconds, int nanos, int zoneSeconds) {
        this.seconds = seconds;
        this.nanos   = nanos;
        this.zone    = zoneSeconds;
    }
    public long getSeconds() {
        return seconds;
    }
    public int getNanos() {
        return nanos;
    }
    public int getZone() {
        return zone;
    }
    public boolean equals(Object other) {
        if (other instanceof TimestampData) {
            return seconds == ((TimestampData) other).seconds
                   && nanos == ((TimestampData) other).nanos
                   && zone == ((TimestampData) other).zone;
        }
        return false;
    }
    public int hashCode() {
        return (int) seconds ^ nanos;
    }
    public int compareTo(TimestampData b) {
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