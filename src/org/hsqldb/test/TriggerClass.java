package org.hsqldb.test;
import org.hsqldb.Trigger;
public class TriggerClass implements Trigger {
    static int   callCount;
    static int[] callCounts = new int[12];
    public void fire(int type, String trigName, String tabName,
                     Object[] oldRow, Object[] newRow) {
        callCounts[type]++;
        callCount++;
    }
}