


package org.hsqldb;

import java.io.IOException;

import org.hsqldb.persist.PersistentStore;
import org.hsqldb.rowio.RowInputBinary;
import org.hsqldb.rowio.RowInputInterface;
import org.hsqldb.rowio.RowOutputBinary;
import org.hsqldb.rowio.RowOutputInterface;
import org.hsqldb.types.ArrayType;
import org.hsqldb.types.Type;


public class RowDiskDataChange extends RowAVLDisk {

    public final static int COL_POS_ROW_NUM     = 0;
    public final static int COL_POS_ROW_ID      = 1;
    public final static int COL_POS_TABLE_ID    = 2;
    public final static int COL_POS_SCHEMA_NAME = 3;
    public final static int COL_POS_TABLE_NAME  = 4;
    public final static int COL_POS_IS_UPDATE   = 5;

    
    final static Type[] arrayType = new Type[]{
        new ArrayType(Type.SQL_INTEGER, Integer.MAX_VALUE) };
    final Table targetTable;
    Object[]    updateData;
    int[]       updateColMap;

    
    public RowDiskDataChange(TableBase t, Object[] data,
                             PersistentStore store, Table targetTable) {

        super(t, data, store);

        this.targetTable = targetTable;
    }

    
    public RowDiskDataChange(Session session, TableBase t,
                             RowInputInterface in) throws IOException {

        super(t, in);

        targetTable = t.database.schemaManager.getTable(
            session,
            (String) rowData[COL_POS_TABLE_NAME],
            (String) rowData[COL_POS_SCHEMA_NAME]);

        if ((Boolean) rowData[COL_POS_IS_UPDATE]) {
            updateData = in.readData(targetTable.colTypes);

            RowInputBinary bin = (RowInputBinary) in;

            if (bin.readNull()) {
                updateColMap = null;
            } else {
                updateColMap = bin.readIntArray();
            }
        } else {
            updateData   = null;
            updateColMap = null;
        }
    }

    public void write(RowOutputInterface out) {

        try {
            writeNodes(out);

            if (hasDataChanged) {
                out.writeData(this, table.colTypes);

                if (updateData != null) {
                    Type[] targetTypes = targetTable.colTypes;

                    out.writeData(targetTypes.length, targetTypes, updateData,
                                  null, null);

                    RowOutputBinary bout = (RowOutputBinary) out;

                    if (updateColMap == null) {
                        bout.writeNull(Type.SQL_ARRAY_ALL_TYPES);
                    } else {
                        bout.writeArray(updateColMap);
                    }
                }

                out.writeEnd();

                hasDataChanged = false;
            }
        } catch (IOException e) {}
    }

    public Object[] getUpdateData() {
        return updateData;
    }

    public int[] getUpdateColumnMap() {
        return updateColMap;
    }

    public void setUpdateData(Object[] data) {
        updateData = data;
    }

    public void setUpdateColumnMap(int[] colMap) {
        updateColMap = colMap;
    }

    public int getRealSize(RowOutputInterface out) {

        RowOutputBinary bout = (RowOutputBinary) out;
        int             size = out.getSize(this);

        if (updateData != null) {
            size += bout.getSize(updateData, targetTable.getColumnCount(),
                                 targetTable.getColumnTypes());

            if (updateColMap != null) {
                size += bout.getSize(updateColMap);
            }
        }

        return size;
    }
}
