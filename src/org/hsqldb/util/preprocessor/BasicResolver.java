package org.hsqldb.util.preprocessor;
import java.io.File;
import java.io.IOException;
class BasicResolver implements IResolver {
    File parentDir;
    public BasicResolver(File parentDir) {
        this.parentDir = parentDir;
    }
    public String resolveProperties(String expression) {
        return expression;
    }
    public File resolveFile(String path) {
        File file = new File(path);
        if (parentDir != null && !file.isAbsolute()) {
            try {
                path = this.parentDir.getCanonicalPath()
                       + File.separatorChar
                       + path;
                file = new File(path);
            } catch (IOException ex) {
                path = this.parentDir.getAbsolutePath()
                       + File.separatorChar
                       + path;
                file = new File(path);
            }
        }
        try {
            return file.getCanonicalFile();
        } catch (Exception e) {
            return file.getAbsoluteFile();
        }
    }
}