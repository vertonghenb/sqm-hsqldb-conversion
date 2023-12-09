


package org.hsqldb.util;

import java.io.Serializable;


class SQLStatements implements Serializable {

    String  sSchema, sType;
    String  sDatabaseToConvert;
    String  sSourceTable, sDestTable;
    String  sDestDrop, sDestCreate, sDestInsert, sDestDelete;
    String  sDestDropIndex, sDestCreateIndex, sDestAlter, sSourceSelect;
    boolean bTransfer    = true;
    boolean bCreate      = true;
    boolean bDelete      = true;
    boolean bDrop        = true;
    boolean bCreateIndex = true;
    boolean bDropIndex   = true;
    boolean bInsert      = true;
    boolean bAlter       = true;
    boolean bFKForced    = false;
    boolean bIdxForced   = false;
}
