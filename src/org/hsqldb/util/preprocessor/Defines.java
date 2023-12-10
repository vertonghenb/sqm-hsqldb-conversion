package org.hsqldb.util.preprocessor;
import java.util.Hashtable;
class Defines {
    private Hashtable symbols = new Hashtable();
    public Defines() {}
    public Defines(String csvExpressions) throws PreprocessorException {
        defineCSV(csvExpressions);
    }
    public void clear() {
        this.symbols.clear();
    }
    public void defineCSV(String csvExpressions)
    throws PreprocessorException {
        if (csvExpressions != null) {
            csvExpressions = csvExpressions + ',';
            int start = 0;
            int len   = csvExpressions.length();
            while (start < len) {
                int    end  = csvExpressions.indexOf(',', start);
                String expr = csvExpressions.substring(start, end).trim();
                if (expr.length() > 0) {
                    defineSingle(expr);
                }
                start = end + 1;
            }
        }
    }
    public void defineSingle(String expression) throws PreprocessorException {
        Tokenizer tokenizer = new Tokenizer(expression);
        tokenizer.next();
        if (!tokenizer.isToken(Token.IDENT)) {
            throw new PreprocessorException("IDENT token required at position: "
                    + tokenizer.getStartIndex()
                    + " in ["
                    + expression +
                    "]"); 
        }
        String ident = tokenizer.getIdent();
        int tokenType = tokenizer.next();
        switch(tokenType) {
            case Token.EOI : {
                this.symbols.put(ident, ident);
                return;
            }
            case Token.ASSIGN : {
                tokenType = tokenizer.next();
                break;
            }
            default : {
                break;
            }
        }
        switch(tokenType) {
            case Token.NUMBER : {
                Number number = tokenizer.getNumber();
                this.symbols.put(ident, number);
                break;
            }
            case Token.STRING : {
                String string = tokenizer.getString();
                this.symbols.put(ident, string);
                break;
            }
            case Token.IDENT : {
                String rhsIdent = tokenizer.getIdent();
                if (!isDefined(rhsIdent)) {
                    throw new PreprocessorException("Right hand side" +
                            "IDENT token [" + rhsIdent + "] at position: "
                            + tokenizer.getStartIndex()
                            + " is undefined in ["
                            + expression
                            + "]"); 
                }
                Object value = this.symbols.get(rhsIdent);
                symbols.put(ident, value);
                break;
            }
            default : {
                throw new PreprocessorException("Right hand side NUMBER,"
                        + "STRING or IDENT token required at position: " +
                        + tokenizer.getStartIndex()
                        + " in ["
                        + expression
                        + "]"); 
            }
        }
        tokenizer.next();
        if (!tokenizer.isToken(Token.EOI)) {
            throw new PreprocessorException("Illegal trailing "
                    + "characters at position: "
                    + tokenizer.getStartIndex()
                    + " in ["
                    + expression
                    + "]"); 
        }
        return;
    }
    public void undefine(String symbol) {
        this.symbols.remove(symbol);
    }
    public boolean isDefined(String symbol) {
        return this.symbols.containsKey(symbol);
    }
    public Object getDefintion(String symbol) {
        return this.symbols.get(symbol);
    }
    public boolean evaluate(String expression) throws PreprocessorException {
        Tokenizer tokenizer = new Tokenizer(expression);
        tokenizer.next();
        Parser parser = new Parser(this, tokenizer);
        boolean result = parser.parseExpression();
        if (!tokenizer.isToken(Token.EOI)) {
            throw new PreprocessorException("Illegal trailing "
                    + "characters at position: "
                    + tokenizer.getStartIndex()
                    + " in ["
                    + expression
                    + "]"); 
        }
        return result;
    }
    public String toString() {
        return super.toString() + this.symbols.toString();
    }
}