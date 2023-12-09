


package org.hsqldb.lib;

import java.util.concurrent.CountDownLatch;


public class CountUpDownLatch {

    volatile CountDownLatch latch;
    volatile int            count;

    public CountUpDownLatch() {
        latch = new CountDownLatch(1);
    }

    public void await() throws InterruptedException {

        if (count == 0) {
            return;
        }

        latch.await();
    }

    public void countDown() {

        count--;

        if (count == 0) {
            latch.countDown();
        }
    }

    public long getCount() {
        return count;
    }

    public void countUp() {

        if (latch.getCount() == 0) {
            latch = new CountDownLatch(1);
        }

        count++;
    }

    public void setCount(int count) {

        if (count == 0) {
            if (latch.getCount() != 0) {
                latch.countDown();
            }
        } else if (latch.getCount() == 0) {
            latch = new CountDownLatch(1);
        }

        this.count = count;
    }
}
