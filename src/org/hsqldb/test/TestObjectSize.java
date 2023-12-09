


package org.hsqldb.test;

import java.sql.Timestamp;

import org.hsqldb.lib.StopWatch;

public class TestObjectSize {

    public TestObjectSize() {

        StopWatch sw        = new StopWatch();
        int       testCount = 2350000;

        System.out.println("Fill Memory with Objects ");

        Object[] objectArray = new Object[testCount];

        for (int j = 0; j < objectArray.length; j++) {
            objectArray[j] = new Timestamp(0);
        }

        System.out.println("Array Filled " + sw.elapsedTime());

        for (int j = 0; j < objectArray.length; j++) {
            objectArray[j] = null;
        }

        Object[] objectArray2 = new Object[testCount];
        Object[] objectArray3 = new Object[testCount];
        Object[] objectArray4 = new Object[testCount];
        Object[] objectArray5 = new Object[testCount];
        Object[] objectArray6 = new Object[testCount];


        short[] shortArray = new short[testCount];
        byte[]  byteArray  = new byte[testCount];

        System.out.println("Fill with Empty Arrays " + sw.elapsedTime());
        sw.zero();
    }

    public static void main(String[] argv) {
        TestObjectSize ls = new TestObjectSize();
    }
}
