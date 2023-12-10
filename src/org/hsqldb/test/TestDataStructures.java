package org.hsqldb.test;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Random;
import java.util.Vector;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.HsqlDeque;
import org.hsqldb.lib.HsqlList;
import org.hsqldb.lib.StopWatch;
import junit.framework.TestCase;
public class TestDataStructures extends TestCase {
    private static final int NUMBER_OF_TEST_RUNS          = 100000;
    private static final int NUMBER_OF_ITERATIONS_PER_RUN = 80;
    private Random           randomGenerator;
    private static final int ADD        = 1;
    private static final int ADD_AT     = 2;
    private static final int GET        = 3;
    private static final int REMOVE     = 4;
    private static final int SET        = 5;
    private static final int OPTIMIZE   = 6;
    private static final int REMOVE_ALL = 7;
    private Vector           listCommandsCalled;
    public TestDataStructures(String s) {
        super(s);
        randomGenerator    = new Random(System.currentTimeMillis());
        listCommandsCalled = new Vector(NUMBER_OF_ITERATIONS_PER_RUN);
    }
    public void testLists() {
        HsqlArrayList arrayList   = new HsqlArrayList();
        HsqlDeque     deque       = new HsqlDeque();
        Vector        vector      = new Vector();
        Vector listCommandsCalled = new Vector(NUMBER_OF_ITERATIONS_PER_RUN);
        Integer       tempInt     = null;
        int           tempCommandCode;
        int           tempPosition;
        boolean       arrayListException = false;
        boolean       dequeException     = false;
        boolean       vectorException    = false;
        Object        arrayListObject    = null;
        Object        linkedListObject   = null;
        Object        vectorObject       = null;
        for (int i = 0; i < getRandomInt(3, 12); i++) {    
            tempInt = getRandomInteger();
            arrayList.add(tempInt);
            deque.add(tempInt);
            vector.addElement(tempInt);
            listCommandsCalled.addElement("Add");
        }
        compareLists(arrayList, deque, vector);
        for (int j = 0; j < NUMBER_OF_ITERATIONS_PER_RUN; j++) {
            tempCommandCode = getRandomInt(0, 15);    
            switch (tempCommandCode) {
                case ADD :
                    tempInt = getRandomInteger();
                    listCommandsCalled.addElement("Add");
                    arrayList.add(tempInt);
                    deque.add(tempInt);
                    vector.addElement(tempInt);
                    break;
                case ADD_AT :
                    tempInt      = getRandomInteger();
                    tempPosition = getRandomInt(0, vector.size());
                    listCommandsCalled.addElement("Add at " + tempPosition);
                    try {
                        arrayList.add(tempPosition, tempInt);
                    } catch (Exception ex) {
                        arrayListException = true;
                    }
                    try {
                        deque.add(tempPosition, tempInt);
                    } catch (Exception ex) {
                        dequeException = true;
                    }
                    try {
                        vector.insertElementAt(tempInt, tempPosition);
                    } catch (Exception ex) {
                        vectorException = true;
                    }
                    break;
                case GET :
                    tempPosition = getRandomInt(0, vector.size() - 1);
                    listCommandsCalled.addElement("Get " + tempPosition);
                    try {
                        arrayListObject = arrayList.get(tempPosition);
                    } catch (Exception ex) {
                        arrayListException = true;
                    }
                    try {
                        linkedListObject = deque.get(tempPosition);
                    } catch (Exception ex) {
                        dequeException = true;
                    }
                    try {
                        vectorObject = vector.elementAt(tempPosition);
                    } catch (Exception ex) {
                        vectorException = true;
                    }
                    break;
                case REMOVE :
                    tempPosition = getRandomInt(0, vector.size() - 1);
                    listCommandsCalled.addElement("Remove " + tempPosition);
                    try {
                        arrayListObject = arrayList.remove(tempPosition);
                    } catch (Exception ex) {
                        arrayListException = true;
                    }
                    try {
                        linkedListObject = deque.remove(tempPosition);
                    } catch (Exception ex) {
                        dequeException = true;
                    }
                    try {
                        vectorObject = vector.elementAt(tempPosition);
                        vector.removeElementAt(tempPosition);
                    } catch (Exception ex) {
                        vectorException = true;
                    }
                    break;
                case SET :
                    tempInt      = getRandomInteger();
                    tempPosition = getRandomInt(0, vector.size() - 1);
                    listCommandsCalled.addElement("Set " + tempPosition);
                    try {
                        arrayList.set(tempPosition, tempInt);
                    } catch (Exception ex) {
                        arrayListException = true;
                    }
                    try {
                        deque.set(tempPosition, tempInt);
                    } catch (Exception ex) {
                        dequeException = true;
                    }
                    try {
                        vector.setElementAt(tempInt, tempPosition);
                    } catch (Exception ex) {
                        vectorException = true;
                    }
                    break;
                case OPTIMIZE :
                    listCommandsCalled.addElement("Optimize");
                    arrayList.trim();
                    vector.trimToSize();
                    break;
                case REMOVE_ALL :
                    if (getRandomInt(0, 5) == 4) {    
                        listCommandsCalled.addElement("Remove all");
                        if (vector.size() == 0) {
                            break;
                        }
                        for (int k = arrayList.size() - 1; k >= 0; k--) {
                            arrayList.remove(k);
                            deque.remove(k);
                        }
                        vector.removeAllElements();
                    }
                    break;
                default :
            }
            if (arrayListException || dequeException || vectorException) {
                if (!(arrayListException && dequeException
                        && vectorException)) {
                    if (!(arrayListException && vectorException)) {
                        System.out.println(
                            "Exception discrepancy with vector and arraylist");
                    } else if (!(dequeException && vectorException)) {
                        System.out.println(
                            "Exception discrepancy with vector and linkedlist");
                    } else {
                        System.out.println("Error in TestDataStructures");
                    }
                    this.printListCommandsCalled(listCommandsCalled);
                    fail("test failed");
                }
                return;
            }
            if (!objectEquals(linkedListObject, arrayListObject,
                              vectorObject)) {
                System.out.println("Objects returned inconsistent");
                this.printListCommandsCalled(listCommandsCalled);
                fail("test failed");
            }
            compareLists(arrayList, deque, vector);
        }
    }
    public void compareLists(HsqlArrayList arrayList, HsqlDeque linkedList,
                             Vector vector) {
        boolean arrayListError  = false;
        boolean linkedListError = false;
        if (!equalsVector(arrayList, vector)) {
            System.out.println("Error in array list implementation");
            arrayListError = true;
        }
        if (!equalsVector(linkedList, vector)) {
            System.out.println("Error in linked list implementation");
            linkedListError = true;
        }
        if (arrayListError || linkedListError) {
            this.printListCommandsCalled(listCommandsCalled);
            System.out.flush();
            fail("test failed");
        }
    }
    public void printListCommandsCalled(Vector commands) {
        int commandCode = 0;
        for (int i = 0; i < commands.size(); i++) {
            System.out.println((String) commands.elementAt(i));
        }
        System.out.flush();
    }
    private boolean objectEquals(Object lObject, Object aObject,
                                 Object vObject) {
        if (lObject == null && aObject == null && vObject == null) {
            return true;
        }
        try {
            if (!lObject.equals(vObject)) {
                System.out.println("LinkList object returned inconsistent");
                return false;
            } else if (!aObject.equals(vObject)) {
                System.out.println("ArrayList object returned inconsistent");
                return false;
            } else {
                return true;
            }
        } catch (NullPointerException ex) {
            return false;
        }
    }
    private int getRandomInt(int lowBound, int highBound) {
        double random = randomGenerator.nextDouble();
        return ((int) (((highBound - lowBound) * random) + .5)) + lowBound;
    }
    private Integer getRandomInteger() {
        return new Integer(getRandomInt(0, (int) (Integer.MAX_VALUE / 100.0)));
    }
    private boolean equalsVector(HsqlList list, Vector vector) {
        if (list.size() != vector.size()) {
            return false;
        }
        org.hsqldb.lib.Iterator listElements   = list.iterator();
        Enumeration             vectorElements = vector.elements();
        Object                  listObj        = null;
        Object                  vectorObj      = null;
        while (listElements.hasNext()) {
            listObj   = listElements.next();
            vectorObj = vectorElements.nextElement();
            if (!listObj.equals(vectorObj)) {
                return false;
            }
        }
        return true;
    }
    public void testGrowth() {
        HsqlArrayList d = new HsqlArrayList();
        for (int i = 0; i < 12; i++) {
            d.add(new Integer(i));
        }
        for (int i = 0; i < d.size(); i++) {
            System.out.println(d.get(i));
        }
        d = new HsqlArrayList();
        for (int i = 0; i < 12; i++) {
            d.add(new Integer(i));
        }
        d.set(11, new Integer(11));
        for (int i = 0; i < d.size(); i++) {
            System.out.println(d.get(i));
        }
        org.hsqldb.lib.Iterator it = d.iterator();
        for (int i = 0; it.hasNext(); i++) {
            Integer value = (Integer) it.next();
            System.out.println(value);
            assertEquals(i, value.intValue());
        }
        assertEquals(12, d.size());
    }
    public void testSpeed() {
        randomGenerator = new Random(System.currentTimeMillis());
        int           TEST_RUNS     = 100000;
        int           LOOP_COUNT    = 1000;
        HsqlArrayList arrayList     = new HsqlArrayList(TEST_RUNS);
        ArrayList     utilArrayList = new ArrayList(TEST_RUNS);
        Vector        vector        = new Vector(TEST_RUNS);
        Integer       value         = new Integer(randomGenerator.nextInt());
        Integer       INT_0         = new Integer(0);
        StopWatch     sw            = new StopWatch();
        System.out.println(sw.currentElapsedTimeToMessage("time"));
        for (int i = 0; i < TEST_RUNS; i++) {
            arrayList.add(INT_0);
        }
        for (int i = 0; i < TEST_RUNS; i++) {
            for (int j = 0; j < LOOP_COUNT; j++) {
                arrayList.set(i, INT_0);
            }
        }
        System.out.println(
            sw.currentElapsedTimeToMessage("time HsqlArrayLsit"));
        sw.zero();
        for (int i = 0; i < TEST_RUNS; i++) {
            utilArrayList.add(INT_0);
        }
        for (int i = 0; i < TEST_RUNS; i++) {
            for (int j = 0; j < LOOP_COUNT; j++) {
                utilArrayList.set(i, INT_0);
            }
        }
        System.out.println(sw.currentElapsedTimeToMessage("time ArrayList"));
        sw.zero();
        for (int i = 0; i < TEST_RUNS; i++) {
            vector.addElement(INT_0);
        }
        for (int i = 0; i < TEST_RUNS; i++) {
            for (int j = 0; j < LOOP_COUNT; j++) {
                vector.setElementAt(INT_0, i);
            }
        }
        System.out.println(sw.currentElapsedTimeToMessage("time Vector"));
    }
    public static void main(String[] args) {
        TestDataStructures test = new TestDataStructures("testLists");
        for (int i = 0; i < NUMBER_OF_TEST_RUNS; i++) {
            test.testLists();
            if (i % 1000 == 0) {
                System.out.println("Finished " + i + " tests");
                System.out.flush();
            }
        }
        System.out.println(
            "After " + NUMBER_OF_TEST_RUNS + " tests of a maximum of "
            + NUMBER_OF_ITERATIONS_PER_RUN
            + " list commands each test, the list tests passed");
        test.testGrowth();
    }
}