


package org.hsqldb.lib;


class HsqlThreadFactory implements ThreadFactory {

    
    protected ThreadFactory factory;

    
    public HsqlThreadFactory() {
        this(null);
    }

    
    public HsqlThreadFactory(ThreadFactory f) {
        setImpl(f);
    }

    
    public Thread newThread(Runnable r) {
        return factory == this ? new Thread(r)
                               : factory.newThread(r);
    }

    
    public synchronized ThreadFactory setImpl(ThreadFactory f) {

        ThreadFactory old;

        old     = factory;
        factory = (f == null) ? this
                              : f;

        return old;
    }

    
    public synchronized ThreadFactory getImpl() {
        return factory;
    }
}
