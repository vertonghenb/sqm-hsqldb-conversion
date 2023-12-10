package org.hsqldb.lib;
import java.util.NoSuchElementException;
public interface Iterator {
    boolean hasNext();
    Object next() throws NoSuchElementException;
    int nextInt() throws NoSuchElementException;
    long nextLong() throws NoSuchElementException;
    void remove() throws NoSuchElementException;
    void setValue(Object value);
}