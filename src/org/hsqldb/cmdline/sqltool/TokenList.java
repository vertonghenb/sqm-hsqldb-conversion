package org.hsqldb.cmdline.sqltool;
import java.util.ArrayList;
public class TokenList extends ArrayList<Token> implements TokenSource {
    static final long serialVersionUID = 5441418591320947274L;
    public TokenList() {
        super();
    }
    public TokenList(TokenList inList) {
        super(inList);
    }
    public Token yylex() {
        if (size() < 1) return null;
        return remove(0);
    }
    public TokenList dup() {
        return new TokenList(this);
    }
}