package org.hsqldb.types;
import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.SchemaObject;
import org.hsqldb.Session;
import org.hsqldb.TypeInvariants;
import org.hsqldb.Tokens;
import org.hsqldb.lib.OrderedHashSet;
import org.hsqldb.rights.Grantee;
public class Charset implements SchemaObject {
    public static final int[][] uppercaseLetters   = new int[][] {
        {
            'A', 'Z'
        }
    };
    public static final int[][] unquotedIdentifier = new int[][] {
        {
            '0', '9'
        }, {
            'A', 'Z'
        }, {
            '_', '_'
        }
    };
    public static final int[][] basicIdentifier    = new int[][] {
        {
            '0', '9'
        }, {
            'A', 'Z'
        }, {
            '_', '_'
        }, {
            'a', 'z'
        }
    };
    HsqlName                    name;
    public HsqlName             base;
    int[][] ranges;
    public Charset(HsqlName name) {
        this.name = name;
    }
    public int getType() {
        return SchemaObject.CHARSET;
    }
    public HsqlName getName() {
        return name;
    }
    public HsqlName getCatalogName() {
        return name.schema.schema;
    }
    public HsqlName getSchemaName() {
        return name.schema;
    }
    public Grantee getOwner() {
        return name.schema.owner;
    }
    public OrderedHashSet getReferences() {
        OrderedHashSet set = new OrderedHashSet();
        set.add(base);
        return set;
    }
    public OrderedHashSet getComponents() {
        return null;
    }
    public void compile(Session session, SchemaObject parentObject) {}
    public String getSQL() {
        StringBuffer sb = new StringBuffer();
        sb.append(Tokens.T_CREATE).append(' ').append(
            Tokens.T_CHARACTER).append(' ').append(Tokens.T_SET).append(' ');
        sb.append(name.getSchemaQualifiedStatementName());
        sb.append(' ').append(Tokens.T_AS).append(' ').append(Tokens.T_GET);
        sb.append(' ').append(base.getSchemaQualifiedStatementName());
        return sb.toString();
    }
    public long getChangeTimestamp() {
        return 0;
    }
    public static boolean isInSet(String value, int[][] ranges) {
        int length = value.length();
        mainLoop:
        for (int index = 0; index < length; index++) {
            int ch = value.charAt(index);
            for (int i = 0; i < ranges.length; i++) {
                if (ch > ranges[i][1]) {
                    continue;
                }
                if (ch < ranges[i][0]) {
                    return false;
                }
                continue mainLoop;
            }
            return false;
        }
        return true;
    }
    public static boolean startsWith(String value, int[][] ranges) {
        int ch = value.charAt(0);
        for (int i = 0; i < ranges.length; i++) {
            if (ch > ranges[i][1]) {
                continue;
            }
            if (ch < ranges[i][0]) {
                return false;
            }
            return true;
        }
        return false;
    }
    public static Charset getDefaultInstance() {
        return TypeInvariants.UTF16;
    }
}