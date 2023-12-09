


package org.hsqldb.lib;


public interface HsqlList extends Collection {

    void add(int index, Object element);

    boolean add(Object element);

    Object get(int index);

    Object remove(int index);

    Object set(int index, Object element);

    boolean isEmpty();

    int size();

    Iterator iterator();
}
