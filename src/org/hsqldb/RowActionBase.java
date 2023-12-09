


package org.hsqldb;


public class RowActionBase {

    public static final byte ACTION_NONE          = 0;
    public static final byte ACTION_INSERT        = 1;
    public static final byte ACTION_DELETE        = 2;
    public static final byte ACTION_DELETE_FINAL  = 3;
    public static final byte ACTION_INSERT_DELETE = 4;
    public static final byte ACTION_REF           = 5;
    public static final byte ACTION_CHECK         = 6;
    public static final byte ACTION_DEBUG         = 7;

    
    RowActionBase            next;
    Session                  session;
    long                     actionTimestamp;
    long                     commitTimestamp;
    byte                     type;
    boolean                  deleteComplete;
    boolean                  rolledback;
    boolean                  prepared;
    int[]                    changeColumnMap;

    RowActionBase() {}

    
    RowActionBase(Session session, byte type) {

        this.session    = session;
        this.type       = type;
        actionTimestamp = session.actionTimestamp;
    }

    void setAsAction(RowActionBase action) {

        next            = action.next;
        session         = action.session;
        actionTimestamp = action.actionTimestamp;
        commitTimestamp = action.commitTimestamp;
        type            = action.type;
        deleteComplete  = action.deleteComplete;
        rolledback      = action.rolledback;
        prepared        = action.prepared;
        changeColumnMap = action.changeColumnMap;
    }
}
