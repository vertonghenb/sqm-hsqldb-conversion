


package org.hsqldb.lib;

import java.util.NoSuchElementException;


public interface IntLookup {

    int add(int key, int value);

    int lookup(int key) throws NoSuchElementException;

    int lookup(int key, int def);
}
