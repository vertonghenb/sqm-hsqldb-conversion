package org.hsqldb.navigator;
import java.io.IOException;
import org.hsqldb.Row;
import org.hsqldb.Session;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.lib.OrderedLongKeyHashMap;
import org.hsqldb.result.ResultMetaData;
import org.hsqldb.rowio.RowInputInterface;
import org.hsqldb.rowio.RowOutputInterface;
import org.hsqldb.types.Type;
public class RowSetNavigatorDataChangeMemory
implements RowSetNavigatorDataChange {
    public static RowSetNavigatorDataChangeMemory emptyRowSet =
        new RowSetNavigatorDataChangeMemory(null);
    int                   size;
    int                   currentPos = -1;
    OrderedLongKeyHashMap list;
    Session               session;
    public RowSetNavigatorDataChangeMemory(Session session) {
        this.session = session;
        list         = new OrderedLongKeyHashMap(64, true);
    }
    public void release() {
        beforeFirst();
        list.clear();
        size = 0;
    }
    public int getSize() {
        return size;
    }
    public int getRowPosition() {
        return currentPos;
    }
    public boolean next() {
        if (currentPos < size - 1) {
            currentPos++;
            return true;
        }
        currentPos = size - 1;
        return false;
    }
    public boolean beforeFirst() {
        currentPos = -1;
        return true;
    }
    public Row getCurrentRow() {
        return (Row) list.getValueByIndex(currentPos);
    }
    public Object[] getCurrentChangedData() {
        return (Object[]) list.getSecondValueByIndex(currentPos);
    }
    public int[] getCurrentChangedColumns() {
        return (int[]) list.getThirdValueByIndex(currentPos);
    }
    public void write(RowOutputInterface out,
                      ResultMetaData meta) throws IOException {}
    public void read(RowInputInterface in,
                     ResultMetaData meta) throws IOException {}
    public void endMainDataSet() {}
    public boolean addRow(Row row) {
        int lookup = list.getLookup(row.getId());
        if (lookup == -1) {
            list.put(row.getId(), row, null);
            size++;
            return true;
        } else {
            if (list.getSecondValueByIndex(lookup) != null) {
                if (session.database.sqlEnforceTDCD) {
                    throw Error.error(ErrorCode.X_27000);
                }
                list.setSecondValueByIndex(lookup, null);
                list.setThirdValueByIndex(lookup, null);
                return true;
            }
            return false;
        }
    }
    public Object[] addRow(Session session, Row row, Object[] data,
                           Type[] types, int[] columnMap) {
        long rowId  = row.getId();
        int  lookup = list.getLookup(rowId);
        if (lookup == -1) {
            list.put(rowId, row, data);
            list.setThirdValueByIndex(size, columnMap);
            size++;
            return data;
        } else {
            Object[] rowData = ((Row) list.getFirstByLookup(lookup)).getData();
            Object[] currentData =
                (Object[]) list.getSecondValueByIndex(lookup);
            if (currentData == null) {
                if (session.database.sqlEnforceTDCD) {
                    throw Error.error(ErrorCode.X_27000);
                } else {
                    return null;
                }
            }
            for (int i = 0; i < columnMap.length; i++) {
                int j = columnMap[i];
                if (types[j].compare(session, data[j], currentData[j]) != 0) {
                    if (types[j].compare(session, rowData[j], currentData[j])
                            != 0) {
                        if (session.database.sqlEnforceTDCU) {
                            throw Error.error(ErrorCode.X_27000);
                        }
                    } else {
                        currentData[j] = data[j];
                    }
                }
            }
            int[] currentMap = (int[]) list.getThirdValueByIndex(lookup);
            currentMap = ArrayUtil.union(currentMap, columnMap);
            list.setThirdValueByIndex(lookup, currentMap);
            return currentData;
        }
    }
    public boolean containsDeletedRow(Row row) {
        int lookup = list.getLookup(row.getId());
        if (lookup == -1) {
            return false;
        }
        Object[] currentData = (Object[]) list.getSecondValueByIndex(lookup);
        return currentData == null;
    }
}