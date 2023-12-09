


package org.hsqldb.lib;

import java.util.Comparator;


public class HsqlArrayHeap implements HsqlHeap {


    protected Comparator oc;
    protected int        count;
    protected Object[]   heap;



    
    public HsqlArrayHeap(int capacity,
                         Comparator comparator)
                         throws IllegalArgumentException {

        if (capacity <= 0) {
            throw new IllegalArgumentException("" + capacity);
        }

        if (comparator == null) {
            throw new IllegalArgumentException("null comparator");
        }

        heap = new Object[capacity];
        oc   = comparator;
    }









    public synchronized void clear() {

        for (int i = 0; i < count; ++i) {
            heap[i] = null;
        }

        count = 0;
    }

    public synchronized void add(Object o)
    throws IllegalArgumentException, RuntimeException {

        int ci;    
        int pi;    

        if (o == null) {
            throw new IllegalArgumentException("null element");
        }

        if (isFull()) {
            throw new RuntimeException("full");
        }

        if (count >= heap.length) {
            increaseCapacity();
        }

        ci = count;

        count++;

        do {
            if (ci <= 0) {
                break;
            }

            pi = (ci - 1) >> 1;

            try {
                if (oc.compare(o, heap[pi]) >= 0) {
                    break;
                }
            } catch (Exception e) {
                throw new IllegalArgumentException(e.toString());
            }

            heap[ci] = heap[pi];
            ci       = pi;
        } while (true);

        heap[ci] = o;
    }

    public synchronized boolean isEmpty() {
        return count == 0;
    }

    public synchronized boolean isFull() {

        
        return count == Integer.MAX_VALUE;
    }

    public synchronized Object peek() {
        return heap[0];
    }

    public synchronized Object remove() {

        int    ci;     
        int    li;     
        int    ri;     
        int    chi;    
        Object co;
        Object ro;

        if (count == 0) {
            return null;
        }

        ci = 0;
        ro = heap[ci];

        count--;

        if (count == 0) {
            heap[0] = null;

            return ro;
        }

        co          = heap[count];
        heap[count] = null;

        do {
            li = (ci << 1) + 1;

            if (li >= count) {
                break;
            }

            ri  = (ci << 1) + 2;
            chi = (ri >= count || oc.compare(heap[li], heap[ri]) < 0) ? li
                                                                      : ri;

            if (oc.compare(co, heap[chi]) <= 0) {
                break;
            }

            heap[ci] = heap[chi];
            ci       = chi;
        } while (true);

        heap[ci] = co;

        return ro;
    }

    public synchronized int size() {
        return count;
    }










































































    public synchronized String toString() {

        StringBuffer sb = new StringBuffer();

        sb.append(super.toString());
        sb.append(" : size=");
        sb.append(count);
        sb.append(' ');
        sb.append('[');

        for (int i = 0; i < count; i++) {
            sb.append(heap[i]);

            if (i + 1 < count) {
                sb.append(',');
                sb.append(' ');
            }
        }

        sb.append(']');

        return sb.toString();
    }













    private void increaseCapacity() {

        Object[] oldheap;

        
        
        
        
        oldheap = heap;

        
        heap = new Object[3 * heap.length / 2 + 1];

        System.arraycopy(oldheap, 0, heap, 0, count);
    }
}
