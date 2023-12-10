package org.hsqldb.result;
public class ResultProperties {
    static final int idx_returnable = 0;
    static final int idx_holdable   = 1;
    static final int idx_scrollable = 2;
    static final int idx_updatable  = 3;
    static final int idx_sensitive  = 4;
    public static final int defaultPropsValue   = 0;
    public static final int updatablePropsValue = 1 << idx_updatable;
    public static int getProperties(int sensitive, int updatable,
                                    int scrollable, int holdable,
                                    int returnable) {
        int combined = (sensitive << idx_sensitive)
                       | (updatable << idx_updatable)
                       | (scrollable << idx_scrollable)
                       | (holdable << idx_holdable)
                       | (returnable << idx_returnable);
        return combined;
    }
    public static int getJDBCHoldability(int props) {
        return isHoldable(props) ? ResultConstants.HOLD_CURSORS_OVER_COMMIT
                                 : ResultConstants.CLOSE_CURSORS_AT_COMMIT;
    }
    public static int getJDBCConcurrency(int props) {
        return isReadOnly(props) ? ResultConstants.CONCUR_READ_ONLY
                                 : ResultConstants.CONCUR_UPDATABLE;
    }
    public static int getJDBCScrollability(int props) {
        return isScrollable(props) ? ResultConstants.TYPE_SCROLL_INSENSITIVE
                                   : ResultConstants.TYPE_FORWARD_ONLY;
    }
    public static int getValueForJDBC(int type, int concurrency,
                                      int holdability) {
        int scrollable = type == ResultConstants.TYPE_FORWARD_ONLY ? 0
                                                                   : 1;
        int updatable  = concurrency == ResultConstants.CONCUR_UPDATABLE ? 1
                                                                         : 0;
        int holdable = holdability == ResultConstants.HOLD_CURSORS_OVER_COMMIT
                       ? 1
                       : 0;
        int prop = (updatable << idx_updatable)
                   | (scrollable << idx_scrollable)
                   | (holdable << idx_holdable);
        return prop;
    }
    public static boolean isUpdatable(int props) {
        return (props & (1 << idx_updatable)) == 0 ? false
                                                   : true;
    }
    public static boolean isScrollable(int props) {
        return (props & (1 << idx_scrollable)) == 0 ? false
                                                    : true;
    }
    public static boolean isHoldable(int props) {
        return (props & (1 << idx_holdable)) == 0 ? false
                                                  : true;
    }
    public static boolean isSensitive(int props) {
        return (props & (1 << idx_sensitive)) == 0 ? false
                                                   : true;
    }
    public static boolean isReadOnly(int props) {
        return (props & (1 << idx_updatable)) == 0 ? true
                                                   : false;
    }
    public static int addUpdatable(int props, boolean flag) {
        return flag ? props | ((1) << idx_updatable)
                    : props & (~(1 << idx_updatable));
    }
    public static int addHoldable(int props, boolean flag) {
        return flag ? props | ((1) << idx_holdable)
                    : props & (~(1 << idx_holdable));
    }
    public static int addScrollable(int props, boolean flag) {
        return flag ? props | ((1) << idx_scrollable)
                    : props & (~(1 << idx_scrollable));
    }
}