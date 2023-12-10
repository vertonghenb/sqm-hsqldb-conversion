package org.hsqldb.lib;
public interface Collection {
    int size();
    boolean isEmpty();
    boolean contains(Object o);
    Iterator iterator();
    boolean add(Object o);
    boolean remove(Object o);
    boolean addAll(Collection c);
    void clear();
    int hashCode();
}