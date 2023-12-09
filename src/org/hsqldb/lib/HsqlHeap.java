


package org.hsqldb.lib;


public interface HsqlHeap {

    
    void clear();

    
    boolean isEmpty();

    
    boolean isFull();

    
    void add(Object o) throws IllegalArgumentException, RuntimeException;

    
    Object peek();

    
    Object remove();

    
    int size();
}
