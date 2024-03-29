package org.hsqldb.rights;
import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.SchemaObject;
import org.hsqldb.Table;
import org.hsqldb.Tokens;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.HashSet;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.OrderedHashSet;
public final class Right {
    boolean        isFull;
    boolean        isFullSelect;
    boolean        isFullInsert;
    boolean        isFullUpdate;
    boolean        isFullReferences;
    boolean        isFullTrigger;
    boolean        isFullDelete;
    OrderedHashSet selectColumnSet;
    OrderedHashSet insertColumnSet;
    OrderedHashSet updateColumnSet;
    OrderedHashSet referencesColumnSet;
    OrderedHashSet triggerColumnSet;
    Right   grantableRights;
    Grantee grantor;
    Grantee grantee;
    public static final OrderedHashSet emptySet      = new OrderedHashSet();
    public static final Right          fullRights    = new Right();
    public static final Right          noRights      = new Right();
    static final OrderedHashSet        fullRightsSet = new OrderedHashSet();
    static {
        fullRights.grantor = GranteeManager.systemAuthorisation;
        fullRights.isFull  = true;
        fullRightsSet.add(fullRights);
    }
    public static final String[] privilegeNames = {
        Tokens.T_SELECT, Tokens.T_INSERT, Tokens.T_UPDATE, Tokens.T_DELETE,
        Tokens.T_REFERENCES, Tokens.T_TRIGGER
    };
    public static final int[] privilegeTypes = {
        GrantConstants.SELECT, GrantConstants.INSERT, GrantConstants.UPDATE,
        GrantConstants.DELETE, GrantConstants.REFERENCES,
        GrantConstants.TRIGGER
    };
    public Right() {
        this.isFull = false;
    }
    Right(Table table) {
        isFull              = false;
        isFullDelete        = true;
        selectColumnSet     = table.getColumnNameSet();
        insertColumnSet     = table.getColumnNameSet();
        updateColumnSet     = table.getColumnNameSet();
        referencesColumnSet = table.getColumnNameSet();
        triggerColumnSet    = table.getColumnNameSet();
    }
    public boolean isFull() {
        return isFull;
    }
    public Grantee getGrantor() {
        return grantor;
    }
    public Grantee getGrantee() {
        return grantee;
    }
    public Right getGrantableRights() {
        return grantableRights == null ? noRights
                                       : grantableRights;
    }
    public Right duplicate() {
        Right right = new Right();
        right.add(this);
        return right;
    }
    public void add(Right right) {
        if (isFull) {
            return;
        }
        if (right.isFull) {
            clear();
            isFull = true;
            return;
        }
        isFullSelect     |= right.isFullSelect;
        isFullInsert     |= right.isFullInsert;
        isFullUpdate     |= right.isFullUpdate;
        isFullReferences |= right.isFullReferences;
        isFullDelete     |= right.isFullDelete;
        if (isFullSelect) {
            selectColumnSet = null;
        } else if (right.selectColumnSet != null) {
            if (selectColumnSet == null) {
                selectColumnSet = new OrderedHashSet();
            }
            selectColumnSet.addAll(right.selectColumnSet);
        }
        if (isFullInsert) {
            insertColumnSet = null;
        } else if (right.insertColumnSet != null) {
            if (insertColumnSet == null) {
                insertColumnSet = new OrderedHashSet();
            }
            insertColumnSet.addAll(right.insertColumnSet);
        }
        if (isFullUpdate) {
            updateColumnSet = null;
        } else if (right.updateColumnSet != null) {
            if (updateColumnSet == null) {
                updateColumnSet = new OrderedHashSet();
            }
            updateColumnSet.addAll(right.updateColumnSet);
        }
        if (isFullReferences) {
            referencesColumnSet = null;
        } else if (right.referencesColumnSet != null) {
            if (referencesColumnSet == null) {
                referencesColumnSet = new OrderedHashSet();
            }
            referencesColumnSet.addAll(right.referencesColumnSet);
        }
        if (isFullTrigger) {
            triggerColumnSet = null;
        } else if (right.triggerColumnSet != null) {
            if (triggerColumnSet == null) {
                triggerColumnSet = new OrderedHashSet();
            }
            triggerColumnSet.addAll(right.triggerColumnSet);
        }
    }
    public void remove(SchemaObject object, Right right) {
        if (right.isFull) {
            clear();
            return;
        }
        if (isFull) {
            isFull = false;
            isFullSelect = isFullInsert = isFullUpdate = isFullReferences =
                isFullDelete = true;
        }
        if (right.isFullDelete) {
            isFullDelete = false;
        }
        if (!isFullSelect && selectColumnSet == null) {}
        else if (right.isFullSelect) {
            isFullSelect    = false;
            selectColumnSet = null;
        } else if (right.selectColumnSet != null) {
            if (isFullSelect) {
                isFullSelect    = false;
                selectColumnSet = ((Table) object).getColumnNameSet();
            }
            selectColumnSet.removeAll(right.selectColumnSet);
            if (selectColumnSet.isEmpty()) {
                selectColumnSet = null;
            }
        }
        if (!isFullInsert && insertColumnSet == null) {}
        else if (right.isFullInsert) {
            isFullInsert    = false;
            insertColumnSet = null;
        } else if (right.insertColumnSet != null) {
            if (isFullInsert) {
                isFullInsert    = false;
                insertColumnSet = ((Table) object).getColumnNameSet();
            }
            insertColumnSet.removeAll(right.insertColumnSet);
            if (insertColumnSet.isEmpty()) {
                insertColumnSet = null;
            }
        }
        if (!isFullUpdate && updateColumnSet == null) {}
        else if (right.isFullUpdate) {
            isFullUpdate    = false;
            updateColumnSet = null;
        } else if (right.updateColumnSet != null) {
            if (isFullUpdate) {
                isFullUpdate    = false;
                updateColumnSet = ((Table) object).getColumnNameSet();
            }
            updateColumnSet.removeAll(right.updateColumnSet);
            if (updateColumnSet.isEmpty()) {
                updateColumnSet = null;
            }
        }
        if (!isFullReferences && referencesColumnSet == null) {}
        else if (right.isFullReferences) {
            isFullReferences    = false;
            referencesColumnSet = null;
        } else if (right.referencesColumnSet != null) {
            if (isFullReferences) {
                isFullReferences    = false;
                referencesColumnSet = ((Table) object).getColumnNameSet();
            }
            referencesColumnSet.removeAll(right.referencesColumnSet);
            if (referencesColumnSet.isEmpty()) {
                referencesColumnSet = null;
            }
        }
        if (!isFullTrigger && triggerColumnSet == null) {}
        else if (right.isFullTrigger) {
            isFullTrigger    = false;
            triggerColumnSet = null;
        } else if (right.triggerColumnSet != null) {
            if (isFullTrigger) {
                isFullTrigger    = false;
                triggerColumnSet = ((Table) object).getColumnNameSet();
            }
            triggerColumnSet.removeAll(right.triggerColumnSet);
            if (triggerColumnSet.isEmpty()) {
                triggerColumnSet = null;
            }
        }
    }
    void clear() {
        isFull = isFullSelect = isFullInsert = isFullUpdate =
            isFullReferences = isFullDelete = false;
        selectColumnSet = insertColumnSet = updateColumnSet =
            referencesColumnSet = triggerColumnSet = null;
    }
    public boolean isEmpty() {
        if (isFull || isFullSelect || isFullInsert || isFullUpdate
                || isFullReferences || isFullDelete) {
            return false;
        }
        if (selectColumnSet != null && !selectColumnSet.isEmpty()) {
            return false;
        }
        if (insertColumnSet != null && !insertColumnSet.isEmpty()) {
            return false;
        }
        if (updateColumnSet != null && !updateColumnSet.isEmpty()) {
            return false;
        }
        if (referencesColumnSet != null && !referencesColumnSet.isEmpty()) {
            return false;
        }
        if (triggerColumnSet != null && !triggerColumnSet.isEmpty()) {
            return false;
        }
        return true;
    }
    OrderedHashSet getColumnsForAllRights(Table table) {
        if (isFull) {
            return table.getColumnNameSet();
        }
        if (isFullSelect || isFullInsert || isFullUpdate || isFullReferences) {
            return table.getColumnNameSet();
        }
        OrderedHashSet set = new OrderedHashSet();
        if (selectColumnSet != null) {
            set.addAll(selectColumnSet);
        }
        if (insertColumnSet != null) {
            set.addAll(insertColumnSet);
        }
        if (updateColumnSet != null) {
            set.addAll(updateColumnSet);
        }
        if (referencesColumnSet != null) {
            set.addAll(referencesColumnSet);
        }
        return set;
    }
    public boolean contains(Right right) {
        if (isFull) {
            return true;
        }
        if (right.isFull) {
            return false;
        }
        if (!containsRights(isFullSelect, selectColumnSet,
                            right.selectColumnSet, right.isFullSelect)) {
            return false;
        }
        if (!containsRights(isFullInsert, insertColumnSet,
                            right.insertColumnSet, right.isFullInsert)) {
            return false;
        }
        if (!containsRights(isFullUpdate, updateColumnSet,
                            right.updateColumnSet, right.isFullUpdate)) {
            return false;
        }
        if (!containsRights(isFullReferences, referencesColumnSet,
                            right.referencesColumnSet,
                            right.isFullReferences)) {
            return false;
        }
        if (!containsRights(isFullTrigger, triggerColumnSet,
                            right.triggerColumnSet, right.isFullTrigger)) {
            return false;
        }
        if (!isFullDelete && right.isFullDelete) {
            return false;
        }
        return true;
    }
    void removeDroppedColumns(OrderedHashSet columnSet, Table table) {
        for (int i = 0; i < columnSet.size(); i++) {
            HsqlName name = (HsqlName) columnSet.get(i);
            if (table.findColumn(name.name) >= 0) {
                columnSet.remove(i);
                i--;
            }
        }
    }
    public OrderedHashSet getColumnsForPrivilege(Table table, int type) {
        if (isFull) {
            return table.getColumnNameSet();
        }
        switch (type) {
            case GrantConstants.SELECT :
                return isFullSelect ? table.getColumnNameSet()
                                    : selectColumnSet == null ? emptySet
                                                              : selectColumnSet;
            case GrantConstants.INSERT :
                return isFullInsert ? table.getColumnNameSet()
                                    : insertColumnSet == null ? emptySet
                                                              : insertColumnSet;
            case GrantConstants.UPDATE :
                return isFullUpdate ? table.getColumnNameSet()
                                    : updateColumnSet == null ? emptySet
                                                              : updateColumnSet;
            case GrantConstants.REFERENCES :
                return isFullReferences ? table.getColumnNameSet()
                                        : referencesColumnSet == null
                                          ? emptySet
                                          : referencesColumnSet;
            case GrantConstants.TRIGGER :
                return isFullTrigger ? table.getColumnNameSet()
                                     : triggerColumnSet == null ? emptySet
                                                                : triggerColumnSet;
        }
        return emptySet;
    }
    static boolean containsAllColumns(OrderedHashSet columnSet, Table table,
                                      boolean[] columnCheckList) {
        for (int i = 0; i < columnCheckList.length; i++) {
            if (columnCheckList[i]) {
                if (columnSet == null) {
                    return false;
                }
                if (columnSet.contains(table.getColumn(i).getName())) {
                    continue;
                }
                return false;
            }
        }
        return true;
    }
    static boolean containsRights(boolean isFull, OrderedHashSet columnSet,
                                  OrderedHashSet otherColumnSet,
                                  boolean otherIsFull) {
        if (isFull) {
            return true;
        }
        if (otherIsFull) {
            return false;
        }
        if (otherColumnSet != null
                && (columnSet == null
                    || !columnSet.containsAll(otherColumnSet))) {
            return false;
        }
        return true;
    }
    boolean canSelect(Table table, boolean[] columnCheckList) {
        if (isFull || isFullSelect) {
            return true;
        }
        return containsAllColumns(selectColumnSet, table, columnCheckList);
    }
    boolean canInsert(Table table, boolean[] columnCheckList) {
        if (isFull || isFullInsert) {
            return true;
        }
        return containsAllColumns(insertColumnSet, table, columnCheckList);
    }
    boolean canUpdate(Table table, boolean[] columnCheckList) {
        if (isFull || isFullUpdate) {
            return true;
        }
        return containsAllColumns(updateColumnSet, table, columnCheckList);
    }
    boolean canReference(Table table, boolean[] columnCheckList) {
        if (isFull || isFullReferences) {
            return true;
        }
        return containsAllColumns(referencesColumnSet, table, columnCheckList);
    }
    boolean canTrigger(Table table, boolean[] columnCheckList) {
        if (isFull || isFullTrigger) {
            return true;
        }
        return containsAllColumns(triggerColumnSet, table, columnCheckList);
    }
    boolean canDelete() {
        return isFull || isFullDelete;
    }
    public boolean canAccessFully(int privilegeType) {
        if (isFull) {
            return true;
        }
        switch (privilegeType) {
            case GrantConstants.DELETE :
                return isFullDelete;
            case GrantConstants.SELECT :
                return isFullSelect;
            case GrantConstants.INSERT :
                return isFullInsert;
            case GrantConstants.UPDATE :
                return isFullUpdate;
            case GrantConstants.REFERENCES :
                return isFullReferences;
            case GrantConstants.TRIGGER :
                return isFullTrigger;
            case GrantConstants.EXECUTE :
                return isFull;
            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "Right");
        }
    }
    public boolean canAcesssNonSelect() {
        if (isFull) {
            return true;
        }
        if (isFullInsert || isFullUpdate || isFullDelete || isFullReferences
                || isFullTrigger) {
            return true;
        }
        boolean result = false;
        result |= (insertColumnSet != null && !insertColumnSet.isEmpty());
        result |= (updateColumnSet != null && !updateColumnSet.isEmpty());
        result |= referencesColumnSet != null
                  && !referencesColumnSet.isEmpty();
        result |= triggerColumnSet != null && !triggerColumnSet.isEmpty();
        return result;
    }
    public boolean canAccess(int privilegeType) {
        if (isFull) {
            return true;
        }
        switch (privilegeType) {
            case GrantConstants.DELETE :
                return isFullDelete;
            case GrantConstants.SELECT :
                if (isFullSelect) {
                    return true;
                }
                return selectColumnSet != null && !selectColumnSet.isEmpty();
            case GrantConstants.INSERT :
                if (isFullInsert) {
                    return true;
                }
                return insertColumnSet != null && !insertColumnSet.isEmpty();
            case GrantConstants.UPDATE :
                if (isFullUpdate) {
                    return true;
                }
                return updateColumnSet != null && !updateColumnSet.isEmpty();
            case GrantConstants.REFERENCES :
                if (isFullReferences) {
                    return true;
                }
                return referencesColumnSet != null
                       && !referencesColumnSet.isEmpty();
            case GrantConstants.TRIGGER :
                if (isFullTrigger) {
                    return true;
                }
                return triggerColumnSet != null && !triggerColumnSet.isEmpty();
            case GrantConstants.EXECUTE :
                return isFull;
            default :
                throw Error.runtimeError(ErrorCode.U_S0500, "Right");
        }
    }
    public boolean canAccess(Table table, int[] columnMap) {
        if (isFull) {
            return true;
        }
        if (isFullSelect || isFullInsert || isFullUpdate || isFullDelete
                || isFullReferences || isFullTrigger) {
            return true;
        }
        boolean result = false;
        result |= (selectColumnSet != null && insertColumnSet.isEmpty());
        result |= (insertColumnSet != null && insertColumnSet.isEmpty());
        result |= (updateColumnSet != null && !updateColumnSet.isEmpty());
        result |= referencesColumnSet != null
                  && !referencesColumnSet.isEmpty();
        result |= triggerColumnSet != null && !triggerColumnSet.isEmpty();
        if (!result) {
            return false;
        }
        HashSet set = new HashSet();
        set.addAll(selectColumnSet);
        set.addAll(insertColumnSet);
        set.addAll(updateColumnSet);
        set.addAll(referencesColumnSet);
        set.addAll(triggerColumnSet);
        for (int i = 0; i < columnMap.length; i++) {
            if (!set.contains(table.getColumn(i).getName())) {
                return false;
            }
        }
        return result;
    }
    String getTableRightsSQL(Table table) {
        StringBuffer sb = new StringBuffer();
        if (isFull) {
            return Tokens.T_ALL;
        }
        if (isFullSelect) {
            sb.append(Tokens.T_SELECT);
            sb.append(',');
        } else if (selectColumnSet != null) {
            sb.append(Tokens.T_SELECT);
            getColumnList(table, selectColumnSet, sb);
            sb.append(',');
        }
        if (isFullInsert) {
            sb.append(Tokens.T_INSERT);
            sb.append(',');
        } else if (insertColumnSet != null) {
            sb.append(Tokens.T_INSERT);
            getColumnList(table, insertColumnSet, sb);
            sb.append(',');
        }
        if (isFullUpdate) {
            sb.append(Tokens.T_UPDATE);
            sb.append(',');
        } else if (updateColumnSet != null) {
            sb.append(Tokens.T_UPDATE);
            getColumnList(table, updateColumnSet, sb);
            sb.append(',');
        }
        if (isFullDelete) {
            sb.append(Tokens.T_DELETE);
            sb.append(',');
        }
        if (isFullReferences) {
            sb.append(Tokens.T_REFERENCES);
            sb.append(',');
        } else if (referencesColumnSet != null) {
            sb.append(Tokens.T_REFERENCES);
            sb.append(',');
        }
        if (isFullTrigger) {
            sb.append(Tokens.T_TRIGGER);
            sb.append(',');
        } else if (triggerColumnSet != null) {
            sb.append(Tokens.T_TRIGGER);
            sb.append(',');
        }
        return sb.toString().substring(0, sb.length() - 1);
    }
    private static void getColumnList(Table t, OrderedHashSet set,
                                      StringBuffer buf) {
        int       count        = 0;
        boolean[] colCheckList = t.getNewColumnCheckList();
        for (int i = 0; i < set.size(); i++) {
            HsqlName name     = (HsqlName) set.get(i);
            int      colIndex = t.findColumn(name.name);
            if (colIndex == -1) {
                continue;
            }
            colCheckList[colIndex] = true;
            count++;
        }
        if (count == 0) {
            throw Error.runtimeError(ErrorCode.U_S0500, "Right");
        }
        buf.append('(');
        for (int i = 0, colCount = 0; i < colCheckList.length; i++) {
            if (colCheckList[i]) {
                colCount++;
                buf.append(t.getColumn(i).getName().statementName);
                if (colCount < count) {
                    buf.append(',');
                }
            }
        }
        buf.append(')');
    }
    public void setColumns(Table table) {
        if (selectColumnSet != null) {
            setColumns(table, selectColumnSet);
        }
        if (insertColumnSet != null) {
            setColumns(table, insertColumnSet);
        }
        if (updateColumnSet != null) {
            setColumns(table, updateColumnSet);
        }
        if (referencesColumnSet != null) {
            setColumns(table, referencesColumnSet);
        }
        if (triggerColumnSet != null) {
            setColumns(table, triggerColumnSet);
        }
    }
    private static void setColumns(Table t, OrderedHashSet set) {
        int       count        = 0;
        boolean[] colCheckList = t.getNewColumnCheckList();
        for (int i = 0; i < set.size(); i++) {
            String name     = (String) set.get(i);
            int    colIndex = t.findColumn(name);
            if (colIndex == -1) {
                throw Error.error(ErrorCode.X_42501, name);
            }
            colCheckList[colIndex] = true;
            count++;
        }
        if (count == 0) {
            throw Error.error(ErrorCode.X_42501);
        }
        set.clear();
        for (int i = 0; i < colCheckList.length; i++) {
            if (colCheckList[i]) {
                set.add(t.getColumn(i).getName());
            }
        }
    }
    public void set(int type, OrderedHashSet set) {
        switch (type) {
            case GrantConstants.SELECT :
                if (set == null) {
                    isFullSelect = true;
                }
                selectColumnSet = set;
                break;
            case GrantConstants.DELETE :
                if (set == null) {
                    isFullDelete = true;
                }
                break;
            case GrantConstants.INSERT :
                if (set == null) {
                    isFullInsert = true;
                }
                insertColumnSet = set;
                break;
            case GrantConstants.UPDATE :
                if (set == null) {
                    isFullUpdate = true;
                }
                updateColumnSet = set;
                break;
            case GrantConstants.REFERENCES :
                if (set == null) {
                    isFullReferences = true;
                }
                referencesColumnSet = set;
                break;
            case GrantConstants.TRIGGER :
                if (set == null) {
                    isFullTrigger = true;
                }
                triggerColumnSet = set;
                break;
        }
    }
    String[] getTableRightsArray() {
        if (isFull) {
            return new String[] {
                Tokens.T_SELECT, Tokens.T_INSERT, Tokens.T_UPDATE,
                Tokens.T_DELETE, Tokens.T_REFERENCES
            };
        }
        HsqlArrayList list  = new HsqlArrayList();
        String[]      array = new String[list.size()];
        if (isFullSelect) {
            list.add(Tokens.T_SELECT);
        }
        if (isFullInsert) {
            list.add(Tokens.T_INSERT);
        }
        if (isFullUpdate) {
            list.add(Tokens.T_UPDATE);
        }
        if (isFullDelete) {
            list.add(Tokens.T_DELETE);
        }
        if (isFullReferences) {
            list.add(Tokens.T_REFERENCES);
        }
        if (isFullTrigger) {
            list.add(Tokens.T_TRIGGER);
        }
        list.toArray(array);
        return array;
    }
}