package org.hsqldb.index;
import org.hsqldb.Row;
import org.hsqldb.SchemaObject;
import org.hsqldb.Session;
import org.hsqldb.TableBase;
import org.hsqldb.navigator.RowIterator;
import org.hsqldb.persist.PersistentStore;
import org.hsqldb.types.Type;
public interface Index extends SchemaObject {
    int INDEX_NONE       = 0;
    int INDEX_NON_UNIQUE = 1;
    int INDEX_UNIQUE     = 2;
    double minimumSelectivity = 16;
    double cachedFactor       = 8;
    int    probeDepth         = 4;
    Index[] emptyArray = new Index[]{};
    RowIterator emptyIterator();
    public int getPosition();
    public void setPosition(int position);
    public long getPersistenceId();
    public int getVisibleColumns();
    public int getColumnCount();
    public boolean isUnique();
    public boolean isConstraint();
    public int[] getColumns();
    public Type[] getColumnTypes();
    public boolean[] getColumnDesc();
    public int[] getDefaultColumnMap();
    public int getIndexOrderValue();
    public boolean isForward();
    public void setTable(TableBase table);
    public void setClustered(boolean clustered);
    public boolean isClustered();
    public int size(Session session, PersistentStore store);
    public int sizeUnique(PersistentStore store);
    public boolean isEmpty(PersistentStore store);
    public void checkIndex(PersistentStore store);
    public void insert(Session session, PersistentStore store, Row row);
    public void delete(Session session, PersistentStore store, Row row);
    public boolean existsParent(Session session, PersistentStore store,
                                Object[] rowdata, int[] rowColMap);
    public RowIterator findFirstRow(Session session, PersistentStore store,
                                    Object[] rowdata, int matchCount,
                                    int distinctCount, int compareType,
                                    boolean reversed, boolean[] map);
    public RowIterator findFirstRow(Session session, PersistentStore store,
                                    Object[] rowdata);
    public RowIterator findFirstRow(Session session, PersistentStore store,
                                    Object[] rowdata, int[] rowColMap);
    public RowIterator findFirstRowNotNull(Session session,
                                           PersistentStore store);
    public RowIterator firstRow(PersistentStore store);
    public RowIterator firstRow(Session session, PersistentStore store);
    public RowIterator lastRow(Session session, PersistentStore store);
    public int compareRowNonUnique(Session session, Object[] a, Object[] b,
                                   int[] rowColMap);
    public int compareRowNonUnique(Session session, Object[] a, Object[] b,
                                   int[] rowColMap, int fieldCount);
    public int compareRowNonUnique(Session session, Object[] a, Object[] b,
                                   int fieldcount);
    public int compareRow(Session session, Object[] a, Object[] b);
}