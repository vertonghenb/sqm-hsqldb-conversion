


package org.hsqldb.cmdline.sqltool;

import java.util.regex.Pattern;



public class Token {
    public static final int SQL_TYPE = 0;
    public static final int SPECIAL_TYPE = 1;
    public static final int PL_TYPE = 2;
    public static final int EDIT_TYPE = 3;
    public static final int RAW_TYPE = 4;
    public static final int RAWEXEC_TYPE = 5;
    public static final int SYNTAX_ERR_TYPE = 6;
    public static final int UNTERM_TYPE = 7;
    public static final int BUFFER_TYPE = 8;
    public static final int MACRO_TYPE = 9;
    public static final Pattern leadingWhitePattern = Pattern.compile("^\\s+");
    public int line;
    public TokenList nestedBlock = null;

    public String[] typeString = {
        "SQL", "SPECIAL", "PL", "EDIT", "RAW", "RAWEXEC", "SYNTAX",
        "UNTERM", "BUFFER", "MACRO"
    };
    public char[] typeChar = {
        'S', '\\', '*', 'E', 'R', 'X', '!', '<', '>', '/'
    };

    public String getTypeString() {
        return typeString[type];
    }
    public char getTypeChar() {
        return typeChar[type];
    }

    public String val;
    public int type;
    public Token(int inType, String inVal, int inLine) {
        val = inVal; type = inType; line = inLine + 1;
        switch (inType) {
            case SPECIAL_TYPE:
            case EDIT_TYPE:
            case PL_TYPE:
            case MACRO_TYPE:
                
                
                if (val == null) throw new IllegalArgumentException(
                        "Null String value for scanner token");
                
                
                
                val = leadingWhitePattern.matcher(val).replaceFirst("");
                break;

            case SYNTAX_ERR_TYPE:
            case BUFFER_TYPE:
            case RAW_TYPE:
            case RAWEXEC_TYPE:
            case UNTERM_TYPE:
                
                
                if (val == null) throw new IllegalArgumentException(
                        "Null String value for scanner token");
                break;

            case SQL_TYPE:
                
                
                break;

            default: throw new IllegalArgumentException(
                "Internal error.  Unexpected scanner token type: " + inType);
        }
    }

    public Token(int inType, StringBuffer inBuf, int inLine) {
        this(inType, inBuf.toString(), inLine);
    }

    public Token(int inType, int inLine) {
        this(inType, (String) null, inLine);
    }

    public String toString() { return "@" + line
            + " TYPE=" + getTypeString() + ", VALUE=(" + val + ')';
    }

    
    public boolean equals(Token otherToken) {
        if (type != otherToken.type) return false;
        if (val == null && otherToken.val != null) return false;
        if (val != null && otherToken.val == null) return false;
        if (val != null && !val.equals(otherToken.val)) return false;
        return true;
    }

    
    public String reconstitute() {
        return reconstitute(false);
    }

    
    public String reconstitute(boolean semify) {
        if (val == null) return "";
        switch (type) {
            case Token.SPECIAL_TYPE:
            case Token.PL_TYPE:
                return Character.toString(getTypeChar()) + val;
            case Token.SQL_TYPE:
                return val + (semify ? ";" : "");
        }
        return "? " + val;
    }
}
