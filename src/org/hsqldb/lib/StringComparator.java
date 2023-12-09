


package org.hsqldb.lib;

import java.util.Comparator;

public class StringComparator implements Comparator {

    
    public int compare(Object a, Object b) {

        
        if (a == b) {
            return 0;
        }

        if (a == null) {
            return -1;
        }

        if (b == null) {
            return 1;
        }

        return ((String) a).compareTo((String) b);
    }
}
