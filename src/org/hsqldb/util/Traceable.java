package org.hsqldb.util;
interface Traceable {
    boolean TRACE = Boolean.getBoolean("hsqldb.util.trace");
    void trace(String s);
}