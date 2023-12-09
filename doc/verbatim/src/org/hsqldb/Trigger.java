


package org.hsqldb;





public interface Trigger {

    
    int INSERT_AFTER      = 0;
    int DELETE_AFTER      = 1;
    int UPDATE_AFTER      = 2;
    int INSERT_AFTER_ROW  = 3;
    int DELETE_AFTER_ROW  = 4;
    int UPDATE_AFTER_ROW  = 5;
    int INSERT_BEFORE_ROW = 6;
    int DELETE_BEFORE_ROW = 7;
    int UPDATE_BEFORE_ROW = 8;

    
    void fire(int type, String trigName, String tabName, Object[] oldRow,
              Object[] newRow) throws HsqlException;
}
