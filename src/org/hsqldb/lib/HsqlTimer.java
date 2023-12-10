package org.hsqldb.lib;
import java.util.Comparator;
import java.util.Date;
public final class HsqlTimer implements Comparator, ThreadFactory {
    protected final TaskQueue taskQueue = new TaskQueue(16,
        (Comparator) this);
    protected final TaskRunner taskRunner = new TaskRunner();
    protected Thread taskRunnerThread;
    protected final ThreadFactory threadFactory;
    protected volatile boolean isShutdown;
    public HsqlTimer() {
        this(null);
    }
    public HsqlTimer(final ThreadFactory threadFactory) {
        this.threadFactory = (threadFactory == null) ? this
                                                     : threadFactory;
    }
    public int compare(final Object a, final Object b) {
        final long awhen = ((Task) (a)).getNextScheduled();
        final long bwhen = ((Task) (b)).getNextScheduled();
        return (awhen < bwhen) ? -1
                               : (awhen == bwhen) ? 0
                                                  : 1;
    }
    public Thread newThread(final Runnable runnable) {
        final Thread thread = new Thread(runnable);
        thread.setName("HSQLDB Timer @" + Integer.toHexString(hashCode()));
        thread.setDaemon(true);
        return thread;
    }
    public synchronized Thread getThread() {
        return this.taskRunnerThread;
    }
    public synchronized void restart() throws IllegalStateException {
        if (this.isShutdown) {
            throw new IllegalStateException("isShutdown==true");
        } else if (this.taskRunnerThread == null) {
            this.taskRunnerThread =
                this.threadFactory.newThread(this.taskRunner);
            this.taskRunnerThread.start();
        } else {
            this.taskQueue.unpark();
        }
    }
    public Object scheduleAfter(final long delay,
                                final Runnable runnable)
                                throws IllegalArgumentException {
        if (runnable == null) {
            throw new IllegalArgumentException("runnable == null");
        }
        return this.addTask(now() + delay, runnable, 0, false);
    }
    public Object scheduleAt(final Date date,
                             final Runnable runnable)
                             throws IllegalArgumentException {
        if (date == null) {
            throw new IllegalArgumentException("date == null");
        } else if (runnable == null) {
            throw new IllegalArgumentException("runnable == null");
        }
        return this.addTask(date.getTime(), runnable, 0, false);
    }
    public Object schedulePeriodicallyAt(final Date date, final long period,
                                         final Runnable runnable,
                                         final boolean relative)
                                         throws IllegalArgumentException {
        if (date == null) {
            throw new IllegalArgumentException("date == null");
        } else if (period <= 0) {
            throw new IllegalArgumentException("period <= 0");
        } else if (runnable == null) {
            throw new IllegalArgumentException("runnable == null");
        }
        return addTask(date.getTime(), runnable, period, relative);
    }
    public Object schedulePeriodicallyAfter(final long delay,
            final long period, final Runnable runnable,
            final boolean relative) throws IllegalArgumentException {
        if (period <= 0) {
            throw new IllegalArgumentException("period <= 0");
        } else if (runnable == null) {
            throw new IllegalArgumentException("runnable == null");
        }
        return addTask(now() + delay, runnable, period, relative);
    }
    public synchronized void shutdown() {
        if (!this.isShutdown) {
            this.isShutdown = true;
            this.taskQueue.cancelAllTasks();
        }
    }
    public synchronized void shutDown() {
        shutdown();
    }
    public synchronized void shutdownImmediately() {
        if (!this.isShutdown) {
            final Thread runner = this.taskRunnerThread;
            this.isShutdown = true;
            if (runner != null && runner.isAlive()) {
                runner.interrupt();
            }
            this.taskQueue.cancelAllTasks();
        }
    }
    public static void cancel(final Object task) {
        if (task instanceof Task) {
            ((Task) task).cancel();
        }
    }
    public static boolean isCancelled(final Object task) {
        return (task instanceof Task) ? ((Task) task).isCancelled()
                                      : true;
    }
    public static boolean isFixedRate(final Object task) {
        if (task instanceof Task) {
            final Task ltask = (Task) task;
            return (ltask.relative && ltask.period > 0);
        } else {
            return false;
        }
    }
    public static boolean isFixedDelay(final Object task) {
        if (task instanceof Task) {
            final Task ltask = (Task) task;
            return (!ltask.relative && ltask.period > 0);
        } else {
            return false;
        }
    }
    public static boolean isPeriodic(final Object task) {
        return (task instanceof Task) ? (((Task) task).period > 0)
                                      : false;
    }
    public static Date getLastScheduled(Object task) {
        if (task instanceof Task) {
            final Task ltask = (Task) task;
            final long last  = ltask.getLastScheduled();
            return (last == 0) ? null
                               : new Date(last);
        } else {
            return null;
        }
    }
    public static Object setPeriod(final Object task, final long period) {
        return (task instanceof Task) ? ((Task) task).setPeriod(period)
                                      : task;
    }
    public static Date getNextScheduled(Object task) {
        if (task instanceof Task) {
            final Task ltask = (Task) task;
            final long next  = ltask.isCancelled() ? 0
                                                   : ltask.getNextScheduled();
            return next == 0 ? null
                             : new Date(next);
        } else {
            return null;
        }
    }
    protected Task addTask(final long first, final Runnable runnable,
                           final long period, boolean relative) {
        if (this.isShutdown) {
            throw new IllegalStateException("shutdown");
        }
        final Task task = new Task(first, runnable, period, relative);
        this.taskQueue.addTask(task);
        this.restart();
        return task;
    }
    protected synchronized void clearThread() {
        try {
            taskRunnerThread.setContextClassLoader(null);
        } catch (Throwable t) {}
        taskRunnerThread = null;
    }
    protected Task nextTask() {
        try {
            while (!this.isShutdown || Thread.interrupted()) {
                long now;
                long next;
                long wait;
                Task task;
                synchronized (this.taskQueue) {
                    task = this.taskQueue.peekTask();
                    if (task == null) {
                        break;
                    }
                    now  = System.currentTimeMillis();
                    next = task.next;
                    wait = (next - now);
                    if (wait > 0) {
                        this.taskQueue.park(wait);
                        continue;           
                    } else {
                        this.taskQueue.removeTask();
                    }
                }
                long period = task.period;
                if (period > 0) {           
                    if (task.relative) {    
                        final long late = (now - next);
                        if (late > period) {
                            period = 0;     
                        } else if (late > 0) {
                            period -= late;
                        }
                    }
                    task.updateSchedule(now, now + period);
                    this.taskQueue.addTask(task);
                }
                return task;
            }
        } catch (InterruptedException e) {
        }
        return null;
    }
    static int nowCount = 0;
    static long now() {
        nowCount++;
        return System.currentTimeMillis();
    }
    protected class TaskRunner implements Runnable {
        public void run() {
            try {
                do {
                    final Task task = HsqlTimer.this.nextTask();
                    if (task == null) {
                        break;
                    }
                    task.runnable.run();
                } while (true);
            } finally {
                HsqlTimer.this.clearThread();
            }
        }
    }
    protected class Task {
        Runnable runnable;
        long period;
        long last;
        long next;
        boolean cancelled = false;
        private Object cancel_mutex = new Object();
        final boolean relative;
        Task(final long first, final Runnable runnable, final long period,
                final boolean relative) {
            this.next     = first;
            this.runnable = runnable;
            this.period   = period;
            this.relative = relative;
        }
        void cancel() {
            boolean signalCancelled = false;
            synchronized (cancel_mutex) {
                if (!cancelled) {
                    cancelled = signalCancelled = true;
                }
            }
            if (signalCancelled) {
                HsqlTimer.this.taskQueue.signalTaskCancelled(this);
            }
        }
        boolean isCancelled() {
            synchronized (cancel_mutex) {
                return cancelled;
            }
        }
        synchronized long getLastScheduled() {
            return last;
        }
        synchronized long getNextScheduled() {
            return next;
        }
        synchronized void updateSchedule(final long last, final long next) {
            this.last = last;
            this.next = next;
        }
        synchronized Object setPeriod(final long newPeriod) {
            if (this.period == newPeriod || this.isCancelled()) {
                return this;
            } else if (newPeriod > this.period) {
                this.period = newPeriod;
                return this;
            } else {
                this.cancel();
                return HsqlTimer.this.addTask(now(), this.runnable, newPeriod,
                                              this.relative);
            }
        }
    }
    protected static class TaskQueue extends HsqlArrayHeap {
        TaskQueue(final int capacity, final Comparator oc) {
            super(capacity, oc);
        }
        void addTask(final Task task) {
            super.add(task);
        }
        void cancelAllTasks() {
            Object[] oldHeap;
            int      oldCount;
            synchronized (this) {
                oldHeap  = this.heap;
                oldCount = this.count;
                this.heap  = new Object[1];
                this.count = 0;
            }
            for (int i = 0; i < oldCount; i++) {
                ((Task) oldHeap[i]).cancelled = true;
            }
        }
        synchronized void park(final long timeout)
        throws InterruptedException {
            this.wait(timeout);
        }
        synchronized Task peekTask() {
            while (super.heap[0] != null
                    && ((Task) super.heap[0]).isCancelled()) {
                super.remove();
            }
            return (Task) super.heap[0];
        }
        synchronized void signalTaskCancelled(Task task) {
            if (task == super.heap[0]) {
                super.remove();
                this.notify();
            }
        }
        Task removeTask() {
            return (Task) super.remove();
        }
        synchronized void unpark() {
            this.notify();
        }
    }
}