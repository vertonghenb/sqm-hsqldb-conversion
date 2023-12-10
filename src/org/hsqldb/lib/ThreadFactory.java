package org.hsqldb.lib;
public interface ThreadFactory {
    Thread newThread(Runnable r);
}