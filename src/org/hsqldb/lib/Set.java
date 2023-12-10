package org.hsqldb.lib;
public interface Set extends Collection {
    int size();
    boolean isEmpty();
    boolean contains(Object o);
    Iterator iterator();
    boolean add(Object o);
    Object get(Object o);
    boolean remove(Object o);
    void clear();
    boolean equals(Object o);
    int hashCode();
}