package org.hsqldb.index;
import org.hsqldb.Constraint;
import org.hsqldb.HsqlNameManager;
import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.Row;
import org.hsqldb.RowAVL;
import org.hsqldb.Session;
import org.hsqldb.Table;
import org.hsqldb.TableBase;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.persist.PersistentStore;
import org.hsqldb.types.Type;
public class IndexAVLMemory extends IndexAVL {
    public IndexAVLMemory(HsqlName name, long id, TableBase table,
                          int[] columns, boolean[] descending,
                          boolean[] nullsLast, Type[] colTypes, boolean pk,
                          boolean unique, boolean constraint,
                          boolean forward) {
        super(name, id, table, columns, descending, nullsLast, colTypes, pk,
              unique, constraint, forward);
    }
    public void checkIndex(PersistentStore store) {
        readLock.lock();
        try {
            NodeAVL p = getAccessor(store);
            NodeAVL f = null;
            while (p != null) {
                f = p;
                checkNodes(store, p);
                p = p.nLeft;
            }
            p = f;
            while (f != null) {
                checkNodes(store, f);
                f = next(store, f);
            }
        } finally {
            readLock.unlock();
        }
    }
    void checkNodes(PersistentStore store, NodeAVL p) {
        NodeAVL l = p.nLeft;
        NodeAVL r = p.nRight;
        if (l != null && l.getBalance(store) == -2) {
            System.out.print("broken index - deleted");
        }
        if (r != null && r.getBalance(store) == -2) {
            System.out.print("broken index -deleted");
        }
        if (l != null && !p.equals(l.getParent(store))) {
            System.out.print("broken index - no parent");
        }
        if (r != null && !p.equals(r.getParent(store))) {
            System.out.print("broken index - no parent");
        }
    }
    public void insert(Session session, PersistentStore store, Row row) {
        NodeAVL        n;
        NodeAVL        x;
        boolean        isleft        = true;
        int            compare       = -1;
        final Object[] rowData       = row.getData();
        boolean        compareRowId  = !isUnique || hasNulls(session, rowData);
        boolean        compareSimple = isSimple;
        writeLock.lock();
        try {
            n = getAccessor(store);
            x = n;
            if (n == null) {
                store.setAccessor(this, ((RowAVL) row).getNode(position));
                return;
            }
            while (true) {
                Row currentRow = n.row;
                compare = 0;
                if (compareSimple) {
                    compare =
                        colTypes[0].compare(session, rowData[colIndex[0]],
                                            currentRow.getData()[colIndex[0]]);
                    if (compare == 0 && compareRowId) {
                        compare = compareRowForInsertOrDelete(session, row,
                                                              currentRow,
                                                              compareRowId, 1);
                    }
                } else {
                    compare = compareRowForInsertOrDelete(session, row,
                                                          currentRow,
                                                          compareRowId, 0);
                }
                if (compare == 0 && session != null && !compareRowId
                        && session.database.txManager.isMVRows()) {
                    if (!isEqualReadable(session, store, n)) {
                        compareRowId = true;
                        compare = compareRowForInsertOrDelete(session, row,
                                                              currentRow,
                                                              compareRowId,
                                                              colIndex.length);
                    }
                }
                if (compare == 0) {
                    if (isConstraint) {
                        Constraint c =
                            ((Table) table).getUniqueConstraintForIndex(this);
                        throw c.getException(row.getData());
                    } else {
                        throw Error.error(ErrorCode.X_23505,
                                          name.statementName);
                    }
                }
                isleft = compare < 0;
                x      = n;
                n      = isleft ? x.nLeft
                                : x.nRight;
                if (n == null) {
                    break;
                }
            }
            x = x.set(store, isleft, ((RowAVL) row).getNode(position));
            balance(store, x, isleft);
        } finally {
            writeLock.unlock();
        }
    }
    void delete(PersistentStore store, NodeAVL x) {
        if (x == null) {
            return;
        }
        NodeAVL n;
        writeLock.lock();
        try {
            if (x.nLeft == null) {
                n = x.nRight;
            } else if (x.nRight == null) {
                n = x.nLeft;
            } else {
                NodeAVL d = x;
                x = x.nLeft;
                while (true) {
                    NodeAVL temp = x.nRight;
                    if (temp == null) {
                        break;
                    }
                    x = temp;
                }
                n = x.nLeft;
                int b = x.iBalance;
                x.iBalance = d.iBalance;
                d.iBalance = b;
                NodeAVL xp = x.nParent;
                NodeAVL dp = d.nParent;
                if (d.isRoot(store)) {
                    store.setAccessor(this, x);
                }
                x.nParent = dp;
                if (dp != null) {
                    if (dp.nRight == d) {
                        dp.nRight = x;
                    } else {
                        dp.nLeft = x;
                    }
                }
                if (d == xp) {
                    d.nParent = x;
                    if (d.nLeft == x) {
                        x.nLeft = d;
                        NodeAVL dr = d.nRight;
                        x.nRight = dr;
                    } else {
                        x.nRight = d;
                        NodeAVL dl = d.nLeft;
                        x.nLeft = dl;
                    }
                } else {
                    d.nParent = xp;
                    xp.nRight = d;
                    NodeAVL dl = d.nLeft;
                    NodeAVL dr = d.nRight;
                    x.nLeft  = dl;
                    x.nRight = dr;
                }
                x.nRight.nParent = x;
                x.nLeft.nParent  = x;
                d.nLeft = n;
                if (n != null) {
                    n.nParent = d;
                }
                d.nRight = null;
                x        = d;
            }
            boolean isleft = x.isFromLeft(store);
            x.replace(store, this, n);
            n = x.nParent;
            x.delete();
            while (n != null) {
                x = n;
                int sign = isleft ? 1
                                  : -1;
                switch (x.iBalance * sign) {
                    case -1 :
                        x.iBalance = 0;
                        break;
                    case 0 :
                        x.iBalance = sign;
                        return;
                    case 1 :
                        NodeAVL r = x.child(store, !isleft);
                        int     b = r.iBalance;
                        if (b * sign >= 0) {
                            x.replace(store, this, r);
                            NodeAVL child = r.child(store, isleft);
                            x.set(store, !isleft, child);
                            r.set(store, isleft, x);
                            if (b == 0) {
                                x.iBalance = sign;
                                r.iBalance = -sign;
                                return;
                            }
                            x.iBalance = 0;
                            r.iBalance = 0;
                            x          = r;
                        } else {
                            NodeAVL l = r.child(store, isleft);
                            x.replace(store, this, l);
                            b = l.iBalance;
                            r.set(store, isleft, l.child(store, !isleft));
                            l.set(store, !isleft, r);
                            x.set(store, !isleft, l.child(store, isleft));
                            l.set(store, isleft, x);
                            x.iBalance = (b == sign) ? -sign
                                                     : 0;
                            r.iBalance = (b == -sign) ? sign
                                                      : 0;
                            l.iBalance = 0;
                            x          = l;
                        }
                }
                isleft = x.isFromLeft(store);
                n      = x.nParent;
            }
        } finally {
            writeLock.unlock();
        }
    }
    NodeAVL next(PersistentStore store, NodeAVL x) {
        NodeAVL r = x.nRight;
        if (r != null) {
            x = r;
            NodeAVL l = x.nLeft;
            while (l != null) {
                x = l;
                l = x.nLeft;
            }
            return x;
        }
        NodeAVL ch = x;
        x = x.nParent;
        while (x != null && ch == x.nRight) {
            ch = x;
            x  = x.nParent;
        }
        return x;
    }
    NodeAVL last(PersistentStore store, NodeAVL x) {
        if (x == null) {
            return null;
        }
        NodeAVL left = x.nLeft;
        if (left != null) {
            x = left;
            NodeAVL right = x.nRight;
            while (right != null) {
                x     = right;
                right = x.nRight;
            }
            return x;
        }
        NodeAVL ch = x;
        x = x.nParent;
        while (x != null && ch.equals(x.nLeft)) {
            ch = x;
            x  = x.nParent;
        }
        return x;
    }
    void balance(PersistentStore store, NodeAVL x, boolean isleft) {
        while (true) {
            int sign = isleft ? 1
                              : -1;
            switch (x.iBalance * sign) {
                case 1 :
                    x.iBalance = 0;
                    return;
                case 0 :
                    x.iBalance = -sign;
                    break;
                case -1 :
                    NodeAVL l = isleft ? x.nLeft
                                       : x.nRight;
                    if (l.iBalance == -sign) {
                        x.replace(store, this, l);
                        x.set(store, isleft, l.child(store, !isleft));
                        l.set(store, !isleft, x);
                        x.iBalance = 0;
                        l.iBalance = 0;
                    } else {
                        NodeAVL r = !isleft ? l.nLeft
                                            : l.nRight;
                        x.replace(store, this, r);
                        l.set(store, !isleft, r.child(store, isleft));
                        r.set(store, isleft, l);
                        x.set(store, isleft, r.child(store, !isleft));
                        r.set(store, !isleft, x);
                        int rb = r.iBalance;
                        x.iBalance = (rb == -sign) ? sign
                                                   : 0;
                        l.iBalance = (rb == sign) ? -sign
                                                  : 0;
                        r.iBalance = 0;
                    }
                    return;
            }
            if (x.nParent == null) {
                return;
            }
            isleft = x.nParent == null || x == x.nParent.nLeft;
            x      = x.nParent;
        }
    }
}