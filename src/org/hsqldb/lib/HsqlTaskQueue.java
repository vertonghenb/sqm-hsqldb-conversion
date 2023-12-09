


package org.hsqldb.lib;


public class HsqlTaskQueue {

    
    protected Thread taskRunnerThread;

    
    protected static final Runnable SHUTDOWNTASK = new Runnable() {
        public void run() {}
    };

    
    protected volatile boolean isShutdown;

    public synchronized Thread getTaskRunnerThread() {
        return taskRunnerThread;
    }

    protected synchronized void clearThread() {

        try {
            taskRunnerThread.setContextClassLoader(null);
        } catch (Throwable t) {}

        taskRunnerThread = null;
    }

    protected final HsqlDeque queue = new HsqlDeque();

    protected class TaskRunner implements Runnable {

        public void run() {

            Runnable task;

            try {
                while (!isShutdown) {
                    synchronized (queue) {
                        task = (Runnable) queue.getFirst();
                    }

                    if (task == SHUTDOWNTASK) {
                        isShutdown = true;

                        synchronized (queue) {
                            queue.clear();
                        }

                        break;
                    } else if (task != null) {
                        task.run();

                        task = null;
                    } else {
                        break;
                    }
                }
            } finally {
                clearThread();
            }
        }
    }

    protected final TaskRunner taskRunner = new TaskRunner();

    public HsqlTaskQueue() {}

    public boolean isShutdown() {
        return isShutdown;
    }

    public synchronized void restart() {

        if (taskRunnerThread == null && !isShutdown) {
            taskRunnerThread = new Thread(taskRunner);

            taskRunnerThread.start();
        }
    }

    public void execute(Runnable command) throws RuntimeException {

        if (!isShutdown) {
            synchronized (queue) {
                queue.addLast(command);
            }

            restart();
        }
    }

    public synchronized void shutdownAfterQueued() {

        if (!isShutdown) {
            synchronized (queue) {
                queue.addLast(SHUTDOWNTASK);
            }
        }
    }

    public synchronized void shutdownAfterCurrent() {

        isShutdown = true;

        synchronized (queue) {
            queue.clear();
            queue.addLast(SHUTDOWNTASK);
        }
    }

    public synchronized void shutdownImmediately() {

        isShutdown = true;

        if (taskRunnerThread != null) {
            taskRunnerThread.interrupt();
        }

        synchronized (queue) {
            queue.clear();
            queue.addLast(SHUTDOWNTASK);
        }
    }
}
