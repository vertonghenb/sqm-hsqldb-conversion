


package org.hsqldb.test;

import java.util.Map;
import java.util.HashMap;


public class Waiter {
    
    static private Map map = new HashMap();
    private String key;
    private boolean notified = false; 
    private boolean waiting = false;  
    private boolean abort = false;  

    public boolean isNotified() { return notified; }
    public boolean isWaiting() { return waiting; }

    private Waiter(String key) {
        this.key = key;
        map.put(key, this);
    }

    
    public synchronized void waitFor(boolean enforceSequence) {
        if (abort)
            throw new RuntimeException("Notifier side failed previously");
        if (notified) {
            if (enforceSequence)
                throw new RuntimeException(
                        "Request to wait on '" + key
                        + "', but this object has already been notified");
            return;
        }
        waiting = true;
        try {
            wait();
        } catch (InterruptedException ie) {
            throw new RuntimeException(
                    "Unexpected interrupted while waiting for '"
                    + key + "'", ie);
        } finally {
            waiting = false;
        }
        map.remove(this);
        if (!notified)
            throw new RuntimeException(
                    "Exiting waitFor() on '" + key
                    + "' even though not 'notified'");
    }

    
    public synchronized void resume(boolean enforceSequence) {
        if (enforceSequence && !waiting) {
            abort = true;
            throw new RuntimeException("Requested to resume on '"
                    + key + " ', but nothing is waiting for it");
        }
        notified = true;
        notify();
    }

    
    public synchronized static Waiter getWaiter(String key) {
        Waiter waiter = (Waiter) map.get(key);
        if (waiter == null) waiter = new Waiter(key);
        return waiter;
    }
}
