


package org.hsqldb.util.preprocessor;

import java.io.File;
import org.apache.tools.ant.Project;




class AntResolver implements IResolver {

    private Project project;

    
    public AntResolver(Project project) {
        this.project = project;
    }

    public String resolveProperties(String expression) {
        return this.project.replaceProperties(expression);
    }

    public File resolveFile(String path) {
        return this.project.resolveFile(path);
    }
}
