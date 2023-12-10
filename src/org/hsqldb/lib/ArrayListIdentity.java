package org.hsqldb.lib;
public class ArrayListIdentity extends HsqlArrayList implements HsqlList {
    public int indexOf(Object o) {
        for (int i = 0; i < elementCount; i++) {
            if (elementData[i] == o) {
                return i;
            }
        }
        return -1;
    }
}