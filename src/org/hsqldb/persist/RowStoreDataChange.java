package org.hsqldb.persist;
import java.io.IOException;
import org.hsqldb.HsqlException;
import org.hsqldb.RowDiskDataChange;
import org.hsqldb.Session;
import org.hsqldb.TableBase;
import org.hsqldb.rowio.RowInputInterface;
public class RowStoreDataChange extends RowStoreAVLHybrid {
    public RowStoreDataChange(Session session,
                             PersistentStoreCollection manager,
                             TableBase table) {
        super(session, manager, table, true);
        super.changeToDiskTable(session);
    }
    public CachedObject get(RowInputInterface in) {
        try {
            return new RowDiskDataChange(session, table, in);
        } catch (HsqlException e) {
            return null;
        } catch (IOException e1) {
            return null;
        }
    }
}