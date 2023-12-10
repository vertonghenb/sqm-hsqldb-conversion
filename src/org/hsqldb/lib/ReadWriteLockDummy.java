package org.hsqldb.lib;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
public class ReadWriteLockDummy implements ReadWriteLock {
    public Lock readLock() {
        return new LockDummy();
    }
    public Lock writeLock() {
        return new LockDummy();
    }
    public static class LockDummy implements Lock {
        public void lock() {}
        public void lockInterruptibly() throws InterruptedException {}
        public boolean tryLock() {
            return false;
        }
        public boolean tryLock(long time,
                               TimeUnit unit) throws InterruptedException {
            return false;
        }
        public void unlock() {}
        public Condition newCondition() {
            return null;
        }
    }
}