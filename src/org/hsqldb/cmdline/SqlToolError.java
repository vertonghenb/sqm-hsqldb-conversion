package org.hsqldb.cmdline;
import org.hsqldb.lib.AppendableException;
public class SqlToolError extends AppendableException {
    static final long serialVersionUID = 1792522673702223649L;
    public SqlToolError(Throwable cause) {
        super(null, cause);
    }
    public SqlToolError() {
    }
    public SqlToolError(String s) {
        super(s);
    }
    public SqlToolError(String string, Throwable cause) {
        super(string, cause);
    }
}