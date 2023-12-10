package org.hsqldb.util.preprocessor;
final class Token {
    static final int EOI     = -1;
    static final int UNKNOWN = 0;
    static final int IDENT   = 1;
    static final int NUMBER  = 2;
    static final int STRING  = 3;
    static final int AND     = '&';
    static final int OR      = '|';
    static final int XOR     = '^';
    static final int NOT     = '!';
    static final int GT      = '>';
    static final int GTE     = '>' + '=';
    static final int LT      = '<';
    static final int LTE     = '<' + '=';
    static final int ASSIGN  = '=';
    static final int EQ      = '=' + '=';
    static final int LPAREN  = '(';
    static final int RPAREN  = ')';
    static boolean isAssignmentOperator(final int type) {
        return (type == ASSIGN);
    }
    static boolean isComparisonOperator(final int type) {
        switch(type) {
            case EQ :
            case LT :
            case GT :
            case LTE :
            case GTE : {
                return true;
            }
            default : {
                return false;
            }
        }
    }
    static boolean isLogicalOperator(final int type) {
        switch(type) {
            case AND :
            case OR :
            case XOR :
            case NOT : {
                return true;
            }
            default : {
                return false;
            }
        }
    }
    static boolean isValue(final int type) {
        switch (type) {
            case IDENT :
            case STRING :
            case NUMBER : {
                return true;
            }
            default : {
                return false;
            }
        }
    }
}