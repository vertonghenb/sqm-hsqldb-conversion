package org.hsqldb.util.preprocessor;
class Line  {
    static final String DIRECTIVE_PREFIX = "//#";
    static final String SPACE_CHARS = " \t";
    static final int    DIRECTIVE_PREFIX_LENGTH = DIRECTIVE_PREFIX.length();
    static final int    DIRECTIVE_PREFIX_LENGTH_PLUS_ONE =
            DIRECTIVE_PREFIX_LENGTH + 1;
    static final String HIDE_DIRECTIVE = DIRECTIVE_PREFIX + ' ';
    int    type;
    String sourceText;
    String indent;
    String text;
    String arguments;
    static int indexOfNonTabOrSpace(String line) {
        int pos = 0;
        int len = line.length();
        while (pos < len) {
            char ch = line.charAt(pos);
            if ((ch == ' ') || (ch == '\t')) {
                pos++;
                continue;
            }
            break;
        }
        return pos;
    }
    static int indexOfTabOrSpace(String s, int fromIndex) {
        int spos = s.indexOf(' ', fromIndex);
        int tpos = s.indexOf('\t', fromIndex);
        return (((tpos != -1) && (tpos < spos)) || (spos == -1)) ? tpos : spos;
    }
    Line(String line) throws PreprocessorException {
        setSourceText(line);
    }
    void setSourceText(String line) throws PreprocessorException {
        this.sourceText = line;
        int pos         = indexOfNonTabOrSpace(line);
        this.indent     = line.substring(0, pos);
        line            = line.substring(pos);
        if (!line.startsWith(DIRECTIVE_PREFIX)) {
            this.text      = line;
            this.arguments = null;
            this.type      = LineType.VISIBLE;
        } else if (line.length() == DIRECTIVE_PREFIX_LENGTH){
            this.text      = "";
            this.arguments = null;
            this.type       = LineType.HIDDEN;
        } else  if (SPACE_CHARS.indexOf(line.
                charAt(DIRECTIVE_PREFIX_LENGTH)) != -1) {
            this.text      = line.substring(DIRECTIVE_PREFIX_LENGTH_PLUS_ONE);
            this.arguments = null;
            this.type      = LineType.HIDDEN;
        } else {
            pos = indexOfTabOrSpace(line, DIRECTIVE_PREFIX_LENGTH_PLUS_ONE);
            if (pos == -1) {
                this.text      = line;
                this.arguments = null;
            } else {
                this.text      = line.substring(0, pos);
                this.arguments = line.substring(pos + 1).trim();
            }
            Integer oType = (Integer) LineType.directives().get(text);
            if (oType == null) {
                throw new PreprocessorException("Unknown directive ["
                        + text + "] in [" + line + "]"); 
            }
            this.type = oType.intValue();
        }
    }
    String getArguments() throws PreprocessorException {
        if (arguments == null || arguments.length() == 0) {
            throw new PreprocessorException("["+ text
                    + "]: has no argument(s)"); 
        }
        return arguments;
    }
    String getSourceText() {
        return sourceText;
    }
    String getIndent() {
        return indent;
    }
    String getText() {
        return text;
    }
    int getType() {
        return type;
    }
    boolean isType(int lineType) {
        return (this.type == lineType);
    }
    public String toString() {
        return LineType.labels()[this.type] + "(" + this.type + "): indent ["
                + this.indent + "] text [" + this.text
                + ((this.arguments == null) ? "]" : ("] args ["
                + this.arguments + "]")) ;
    }
}