


package org.hsqldb.navigator;

import java.io.IOException;

import org.hsqldb.Row;
import org.hsqldb.Session;
import org.hsqldb.result.ResultMetaData;
import org.hsqldb.rowio.RowInputInterface;
import org.hsqldb.rowio.RowOutputInterface;
import org.hsqldb.types.Type;


public interface RowSetNavigatorDataChange {

    public void release();

    public int getSize();

    public int getRowPosition();

    public boolean next();

    public boolean beforeFirst();

    public Row getCurrentRow();

    public Object[] getCurrentChangedData();

    public int[] getCurrentChangedColumns();

    public void write(RowOutputInterface out,
                      ResultMetaData meta) throws IOException;

    public void read(RowInputInterface in,
                     ResultMetaData meta) throws IOException;

    public void endMainDataSet();

    public boolean addRow(Row row);

    public Object[] addRow(Session session, Row row, Object[] data,
                           Type[] types, int[] columnMap);

    public boolean containsDeletedRow(Row row);
}
