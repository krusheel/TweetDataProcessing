package org.freethinking.dataprocessing;

public class SynchronizedCounter {
    
    int c;
    
    synchronized public void increment() {
        c++;
    }

    synchronized public int getCount() {
        return c;
    }
}
