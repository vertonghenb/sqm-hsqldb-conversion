


package org.hsqldb;




/**
 * The interface an HSQLDB TRIGGER must implement. The user-supplied class that
 * implements this must have a default constructor.
 *
 * @author Peter Hudson
 * @version 1.9.0
 * @since 1.7.0
 */
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

    /**
     * The method invoked upon each triggered action.
     *
     * <p> type contains the integer index id for trigger type, e.g.
     * TriggerDef.INSERT_AFTER
     *
     * <p> For all triggers defined as default FOR EACH STATEMENT both
     *  oldRow and newRow are null.
     *
     * <p> For triggers defined as FOR EACH ROW, the following will apply:
     *
     * <p> When UPDATE triggers are fired, oldRow contains the existing values
     * of the table row and newRow contains the new values.
     *
     * <p> For INSERT triggers, oldRow is null and newRow contains the table row
     * to be inserted. For DELETE triggers, newRow is null and oldRow contains
     * the table row to be deleted.
     *
     * <p> For error conditions, users can construct an HsqlException using one
     * of the static methods of org.hsqldb.error.Error with a predefined
     * SQL State from org.hsqldb.error.ErrorCode.
     *
     * @param type the type as one of the int values defined in the interface
     * @param trigName the name of the trigger
     * @param tabName the name of the table upon which the triggered action is
     *   occuring
     * @param oldRow the old row
     * @param newRow the new row
     * @throws HsqlException
     */
    void fire(int type, String trigName, String tabName, Object[] oldRow,
              Object[] newRow) throws HsqlException;
}
