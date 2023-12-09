


package org.hsqldb.result;

import org.hsqldb.StatementTypes;






public interface ResultConstants {

    
    int HSQL_API_BASE = 0;

    
    int NONE = HSQL_API_BASE + 0;

    
    int UPDATECOUNT = HSQL_API_BASE + 1;

    
    int ERROR = HSQL_API_BASE + 2;

    
    int DATA = HSQL_API_BASE + 3;

    
    int PREPARE_ACK = HSQL_API_BASE + 4;

    
    int SETSESSIONATTR = HSQL_API_BASE + 6;

    
    int GETSESSIONATTR = HSQL_API_BASE + 7;

    
    int BATCHEXECDIRECT = HSQL_API_BASE + 8;

    
    int BATCHEXECUTE = HSQL_API_BASE + 9;

    
    int RESETSESSION = HSQL_API_BASE + 10;

    
    int CONNECTACKNOWLEDGE = HSQL_API_BASE + 11;

    
    int PREPARECOMMIT = HSQL_API_BASE + 12;

    
    int REQUESTDATA = HSQL_API_BASE + 13;

    
    int DATAROWS = HSQL_API_BASE + 14;

    
    int DATAHEAD = HSQL_API_BASE + 15;

    
    int BATCHEXECRESPONSE = HSQL_API_BASE + 16;

    
    int PARAM_METADATA = HSQL_API_BASE + 17;

    
    int LARGE_OBJECT_OP = HSQL_API_BASE + 18;

    
    int WARNING = HSQL_API_BASE + 19;

    
    int GENERATED = HSQL_API_BASE + 20;

    
    int EXECUTE_INVALID = HSQL_API_BASE + 21;

    
    int CONNECT = HSQL_API_BASE + 31;

    
    int DISCONNECT = HSQL_API_BASE + 32;

    
    int ENDTRAN = HSQL_API_BASE + 33;

    
    int EXECDIRECT = HSQL_API_BASE + 34;

    
    int EXECUTE = HSQL_API_BASE + 35;

    
    int FREESTMT = HSQL_API_BASE + 36;

    
    int PREPARE = HSQL_API_BASE + 37;

    
    int SETCONNECTATTR = HSQL_API_BASE + 38;

    
    int STARTTRAN = HSQL_API_BASE + 39;

    
    int CLOSE_RESULT = HSQL_API_BASE + 40;

    
    int UPDATE_RESULT = HSQL_API_BASE + 41;

    
    int VALUE = HSQL_API_BASE + 42;

    
    int CALL_RESPONSE = HSQL_API_BASE + 43;

    
    int CHANGE_SET = HSQL_API_BASE + 44;

    
    int MODE_UPPER_LIMIT = HSQL_API_BASE + 48;







































































































































































































































































































































































































    int TX_COMMIT                  = 0;
    int TX_ROLLBACK                = 1;
    int TX_SAVEPOINT_NAME_ROLLBACK = 2;
    int TX_SAVEPOINT_NAME_RELEASE  = 4;
    int TX_COMMIT_AND_CHAIN        = 6;
    int TX_ROLLBACK_AND_CHAIN      = 7;


    int UPDATE_CURSOR = StatementTypes.UPDATE_CURSOR;
    int DELETE_CURSOR = StatementTypes.DELETE_CURSOR;
    int INSERT_CURSOR = StatementTypes.INSERT;









    int SQL_ATTR_SAVEPOINT_NAME = 10027;



    
    int EXECUTE_FAILED = -3;

    
    int SUCCESS_NO_INFO = -2;

    int SQL_ASENSITIVE    = 0;
    int SQL_INSENSITIVE   = 1;
    int SQL_SENSITIVE     = 2;
    int SQL_NONSCROLLABLE = 0;
    int SQL_SCROLLABLE    = 1;
    int SQL_NONHOLDABLE   = 0;
    int SQL_HOLDABLE      = 1;


    int SQL_WITHOUT_RETURN = 0;
    int SQL_WITH_RETURN    = 1;
    int SQL_NOT_UPDATABLE  = 0;
    int SQL_UPDATABLE      = 1;


    int TYPE_FORWARD_ONLY       = 1003;
    int TYPE_SCROLL_INSENSITIVE = 1004;
    int TYPE_SCROLL_SENSITIVE   = 1005;

    
    int CONCUR_READ_ONLY = 1007;
    int CONCUR_UPDATABLE = 1008;

    
    int HOLD_CURSORS_OVER_COMMIT = 1;
    int CLOSE_CURSORS_AT_COMMIT  = 2;

    
    int RETURN_GENERATED_KEYS             = 1;     
    int RETURN_NO_GENERATED_KEYS          = 2;     
    int RETURN_GENERATED_KEYS_COL_NAMES   = 11;    
    int RETURN_GENERATED_KEYS_COL_INDEXES = 21;    
}
