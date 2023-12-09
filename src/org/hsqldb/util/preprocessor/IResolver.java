


package org.hsqldb.util.preprocessor;

import java.io.File;




public interface IResolver {
    public String resolveProperties(String expression);
    public File   resolveFile(String path);
}
