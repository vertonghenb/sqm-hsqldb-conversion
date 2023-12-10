package org.hsqldb.lib;
public class StopWatch {
    private long startTime;
    private long lastStart;
    private long total;
    boolean running = false;
    public StopWatch() {
        this(true);
    }
    public StopWatch(boolean start) {
        if (start) {
            start();
        }
    }
    public long elapsedTime() {
        if (running) {
            return total + System.currentTimeMillis() - startTime;
        } else {
            return total;
        }
    }
    public long currentElapsedTime() {
        if (running) {
            return System.currentTimeMillis() - startTime;
        } else {
            return 0;
        }
    }
    public void zero() {
        total = 0;
        start();
    }
    public void start() {
        startTime = System.currentTimeMillis();
        running   = true;
    }
    public void stop() {
        if (running) {
            total   += System.currentTimeMillis() - startTime;
            running = false;
        }
    }
    public void mark() {
        stop();
        start();
    }
    public String elapsedTimeToMessage(String prefix) {
        return prefix + " in " + elapsedTime() + " ms.";
    }
    public String currentElapsedTimeToMessage(String prefix) {
        return prefix + " in " + currentElapsedTime() + " ms.";
    }
    public String toString() {
        return super.toString() + "[running=" + running + ", startTime="
               + startTime + ", total=" + total + "]";
    }
}