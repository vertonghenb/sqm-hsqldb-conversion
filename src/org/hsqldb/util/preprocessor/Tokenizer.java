package org.hsqldb.util.preprocessor;
final class Tokenizer {
    private final String command;
    private final int    commandLength;
    private int          tokenType;
    private int          startIndex;
    private int          currentIndex;
    Tokenizer(final String cmd) {
        this.command       = cmd + " ";
        this.commandLength = command.length();
        this.startIndex    = 0;
        this.currentIndex  = 0;
        this.tokenType     = Token.UNKNOWN;
    }
    void skipBlanks() {
        final String cmd = this.command;
        final int    len = this.commandLength;
        top:
            while (currentIndex < len) {
            switch(cmd.charAt(currentIndex)) {
                case ' '  :
                case '\t' : {
                    currentIndex++;
                    continue top;
                }
            }
            break;
            }
    }
    int next() throws PreprocessorException {
        skipBlanks();
        startIndex = currentIndex;
        final String cmd = this.command;
        final int    len = this.commandLength;
        if (currentIndex >= len) {
            tokenType = Token.EOI;
            return tokenType;
        }
        char ch = cmd.charAt(currentIndex);
        if (Character.isJavaIdentifierStart(ch)) {
            tokenType = Token.IDENT;
            currentIndex++;
            while (currentIndex < len &&
                    Character.isJavaIdentifierPart(cmd.charAt(currentIndex))) {
                currentIndex++;
            }
            return tokenType;
        } else if (Character.isDigit(ch)) {
            tokenType = Token.NUMBER;
            currentIndex++;
            while(currentIndex < len &&
                    Character.isDigit(cmd.charAt(currentIndex))) {
                currentIndex++;
            }
            if (currentIndex < len && cmd.charAt(currentIndex) == '.') {
                currentIndex++;
            }
            while(currentIndex < len &&
                    Character.isDigit(cmd.charAt(currentIndex))) {
                currentIndex++;
            }
            return tokenType;
        } else if (ch == '"') {
            tokenType = Token.STRING;
            currentIndex++;
            int pos = cmd.indexOf('"', currentIndex);
            if (pos == -1) {
                throw new PreprocessorException("Unclosed string literal: " +
                        cmd.substring(startIndex)); 
            }
            currentIndex = pos + 1;
            return tokenType;
        }
        switch(ch) {
            case Token.LPAREN :
            case Token.RPAREN :
            case Token.XOR :
            case Token.NOT : {
                currentIndex++;
                return (tokenType = ch);
            }
            case Token.ASSIGN : {
                currentIndex++;
                if(currentIndex < len &&
                        cmd.charAt(currentIndex) == Token.ASSIGN) {
                    currentIndex++;
                    tokenType = Token.EQ;
                } else {
                    tokenType = Token.ASSIGN;
                }
                return tokenType;
            }
            case Token.LT : {
                currentIndex++;
                if (currentIndex < len &&
                        cmd.charAt(currentIndex) == Token.ASSIGN) {
                    currentIndex++;
                    tokenType = Token.LTE;
                } else {
                    tokenType = Token.LT;
                }
                return tokenType;
            }
            case Token.GT : {
                currentIndex++;
                if (currentIndex < len &&
                        cmd.charAt(currentIndex) == Token.ASSIGN) {
                    currentIndex++;
                    tokenType = Token.GTE;
                } else {
                    tokenType = Token.GT;
                }
                return tokenType;
            }
            case Token.AND :
            case Token.OR : {
                currentIndex++;
                if (currentIndex < len && cmd.charAt(currentIndex) == ch) {
                    currentIndex++;
                }
                return (tokenType = ch);
            }
            default : {
                throw new PreprocessorException("Syntax error: " +
                        cmd.substring(currentIndex)); 
            }
        }
    }
    int getTokenType() {
        return tokenType;
    }
    boolean isToken(final int type) {
        return (this.tokenType == type);
    }
    String getIdent() {
        return isToken(Token.EOI) ? null
                : this.command.substring(startIndex, currentIndex);
    }
    Number getNumber() {
        return (isToken(Token.EOI)) ? null
                : new Double(Double.parseDouble(this.command.
                substring(startIndex, currentIndex)));
    }
    String getString() {
        return isToken(Token.EOI) ? null
                : this.command.substring(startIndex + 1, currentIndex - 1);
    }
    int getStartIndex() {
        return this.startIndex;
    }
    int currentIndex() {
        return this.currentIndex;
    }
    String getSource() {
        return this.command;
    }
}