


package org.hsqldb.cmdline.sqltool;

import java.io.IOException;



public interface TokenSource {
    public Token yylex() throws IOException;
}
