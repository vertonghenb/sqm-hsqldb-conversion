


package org.hsqldb.util.preprocessor;




class Parser  {

    Defines   defines;
    Tokenizer tokenizer;

    Parser(Defines defines, Tokenizer tokenizer) {
        this.defines   = defines;
        this.tokenizer = tokenizer;
    }

    boolean parseExpression() throws PreprocessorException {
        boolean result = parseTerm();

        while (true) {
            switch(this.tokenizer.getTokenType()) {
                case Token.OR : {
                    this.tokenizer.next();

                    result = result | parseTerm();

                    break;
                }
                case Token.XOR : {
                    this.tokenizer.next();

                    result = result ^ parseTerm();

                    break;
                }

                default : {
                    return result;
                }
            }
        }
    }

    boolean parseTerm() throws PreprocessorException {
        boolean result = parseFactor();

        while (this.tokenizer.isToken(Token.AND)) {
            this.tokenizer.next();

            result = result & parseFactor();
        }

        return result;
    }

    boolean parseFactor() throws PreprocessorException {
        boolean result;

        switch(this.tokenizer.getTokenType()) {
            case Token.IDENT : {
                String ident = this.tokenizer.getIdent();
                int    type  = this.tokenizer.next();

                if ((type == Token.EOI) || (type == Token.RPAREN) ||
                        Token.isLogicalOperator(type)) {
                    result = this.defines.isDefined(ident);
                } else if (Token.isComparisonOperator(type)) {
                    result = parseComparison(ident, type);
                } else {
                    throw new PreprocessorException("Logical or comparison "
                            + "operator token required at position "
                            + this.tokenizer.getStartIndex()
                            + " in ["
                            + this.tokenizer.getSource()
                            + "]"); 
                }

                break;
            }
            case Token.NOT :{
                this.tokenizer.next();

                result = !parseFactor();

                break;
            }
            case Token.LPAREN : {
                this.tokenizer.next();

                result = parseExpression();

                if (!this.tokenizer.isToken(Token.RPAREN)) {
                    throw new PreprocessorException("RPAREN token required at "
                            + "position "
                            + this.tokenizer.getStartIndex()
                            + " in ["
                            + this.tokenizer.getSource()
                            + "]"); 
                }

                this.tokenizer.next();

                break;
            }
            default : {
                throw new PreprocessorException("IDENT, NOT or LPAREN "
                        + "token required at position "
                        + this.tokenizer.getStartIndex()
                        + " in ["
                        + this.tokenizer.getSource()
                        + "]"); 
            }
        }

        return result;
    }

    boolean parseComparison(String ident, int opType)
    throws PreprocessorException {


        boolean result;
        Object  lhs    = this.defines.getDefintion(ident);
        int     pos    = this.tokenizer.getStartIndex();
        Object  rhs    = parseValue();

        if (lhs == null) {
            throw new PreprocessorException("IDENT " + ident
                    + " is not defined at position"
                    + pos
                    + "in ["
                    + this.tokenizer.getSource()
                    + "]"); 
        }

        switch(opType) {
            case Token.EQ :{
                result = (compare(lhs, rhs) == 0);

                break;
            }
            case Token.LT : {
                result = (compare(lhs, rhs) < 0);

                break;
            }
            case Token.LTE : {
                result = (compare(lhs, rhs) <= 0);

                break;
            }
            case Token.GT : {
                result = (compare(lhs, rhs) > 0);

                break;
            }
            case Token.GTE : {
                result = (compare(lhs, rhs) >= 0);

                break;
            }
            default : {
                
                
                
                
                throw new PreprocessorException("Internal error"); 
            }
        }

        this.tokenizer.next();

        return result;
    }












    static int compare(Object o1, Object o2) {
        
        
        if (o1 instanceof Comparable) {
            return (o1.getClass().isAssignableFrom(o2.getClass()))
            ? ((Comparable)o1).compareTo(o2)
            : String.valueOf(o1).compareTo(String.valueOf(o2));
        } else {
            return o1.toString().compareTo(o2.toString());
        }
    }

    Object parseValue() throws PreprocessorException {
        Object value;

        switch(this.tokenizer.next()) {
            case Token.IDENT : {
                String ident = this.tokenizer.getIdent();

                value = this.defines.getDefintion(ident);

                if (value == null) {
                    throw new PreprocessorException("IDENT " + ident
                            + " is not defined at position"
                            + this.tokenizer.getStartIndex()
                            + "in ["
                            + this.tokenizer.getSource()
                            + "]"); 
                }

                break;
            }
            case Token.STRING : {
                value = this.tokenizer.getString();

                break;
            }
            case Token.NUMBER : {
                value = this.tokenizer.getNumber();

                break;
            }
            default :{
                throw new PreprocessorException("IDENT, STRING"
                        + "or NUMBER token required at position "
                        + this.tokenizer.getStartIndex()
                        + " in: ["
                        + this.tokenizer.getSource()
                        + "]"); 
            }
        }

        return value;
    }
}
