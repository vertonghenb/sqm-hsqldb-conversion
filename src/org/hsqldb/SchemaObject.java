


package org.hsqldb;

import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.rights.Grantee;
import org.hsqldb.lib.OrderedHashSet;


public interface SchemaObject {

    int CATALOG          = 1;
    int SCHEMA           = 2;
    int TABLE            = 3;
    int VIEW             = 4;
    int CONSTRAINT       = 5;
    int ASSERTION        = 6;
    int SEQUENCE         = 7;
    int TRIGGER          = 8;
    int COLUMN           = 9;
    int TRANSITION       = 10;
    int GRANTEE          = 11;
    int TYPE             = 12;
    int DOMAIN           = 13;
    int CHARSET          = 14;
    int COLLATION        = 15;
    int FUNCTION         = 16;
    int PROCEDURE        = 17;
    int ROUTINE          = 18;
    int CURSOR           = 19;
    int INDEX            = 20;
    int LABEL            = 21;
    int VARIABLE         = 22;
    int PARAMETER        = 23;
    int SPECIFIC_ROUTINE = 24;
    int WRAPPER          = 25;
    int SERVER           = 26;
    int SUBQUERY         = 27;

    
    SchemaObject[] emptyArray = new SchemaObject[]{};

    int getType();

    HsqlName getName();

    HsqlName getSchemaName();

    HsqlName getCatalogName();

    Grantee getOwner();

    OrderedHashSet getReferences();

    OrderedHashSet getComponents();

    void compile(Session session, SchemaObject parentObject);

    String getSQL();

    long getChangeTimestamp();

    interface ConstraintTypes {

        int FOREIGN_KEY = 0;
        int MAIN        = 1;
        int UNIQUE      = 2;
        int CHECK       = 3;
        int PRIMARY_KEY = 4;
        int TEMP        = 5;
    }

    
    interface ReferentialAction {

        int CASCADE     = 0;
        int RESTRICT    = 1;
        int SET_NULL    = 2;
        int NO_ACTION   = 3;
        int SET_DEFAULT = 4;
    }

    interface Deferable {

        int NOT_DEFERRABLE = 0;
        int INIT_DEFERRED  = 1;
        int INIT_IMMEDIATE = 2;
    }

    interface ViewCheckModes {

        int CHECK_NONE    = 0;
        int CHECK_LOCAL   = 1;
        int CHECK_CASCADE = 2;
    }

    interface ParameterModes {

        byte PARAM_UNKNOWN = 0;    
        byte PARAM_IN    = 1;      
        byte PARAM_OUT   = 4;      
        byte PARAM_INOUT = 2;      
    }

    interface Nullability {

        byte NO_NULLS         = 0;    
        byte NULLABLE         = 1;    
        byte NULLABLE_UNKNOWN = 2;    
    }
}
