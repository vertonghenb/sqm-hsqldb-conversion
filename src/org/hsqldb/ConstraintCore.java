


package org.hsqldb;

import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.index.Index;


class ConstraintCore {

    
    HsqlName refName;
    HsqlName mainName;
    HsqlName uniqueName;
    HsqlName refTableName;
    HsqlName mainTableName;

    
    
    Table mainTable;
    int[] mainCols;
    Index mainIndex;

    
    Table   refTable;
    int[]   refCols;
    Index   refIndex;
    int     deleteAction;
    int     updateAction;
    boolean hasUpdateAction;
    boolean hasDeleteAction;
    int     matchType;

    
    ConstraintCore duplicate() {

        ConstraintCore copy = new ConstraintCore();

        copy.refName      = refName;
        copy.mainName     = mainName;
        copy.uniqueName   = uniqueName;
        copy.mainTable    = mainTable;
        copy.mainCols     = mainCols;
        copy.mainIndex    = mainIndex;
        copy.refTable     = refTable;
        copy.refCols      = refCols;
        copy.refIndex     = refIndex;
        copy.deleteAction = deleteAction;
        copy.updateAction = updateAction;
        copy.matchType    = matchType;

        return copy;
    }
}
