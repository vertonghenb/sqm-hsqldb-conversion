package org.hsqldb.util.preprocessor;
import java.io.File;
import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.taskdefs.MatchingTask;
public class PreprocessorAntTask extends MatchingTask {
    private String ifExpr;
    private String unlessExpr;
    private File   sourceDir;
    private File   targetDir;
    private String defines;
    private String altExt;
    private String encoding;
    private int    options = Option.INDENT;
    public void init() {
        super.init();
    }
    public void setSrcdir(final File value) {
        sourceDir = value;
    }
    public void setTargetdir(final File value) {
        targetDir = value;
    }
    public void setSymbols(final String value) {
        defines = value;
    }
    public void setVerbose(final boolean verbose) {
        options = Option.setVerbose(options, verbose);
    }
    public void setBackup(final boolean backup) {
        options = Option.setBackup(options, backup);
    }
    public void setIndent(final boolean indent) {
        options = Option.setIndent(options, indent);
    }
    public void setTestonly(final boolean testOnly) {
        options = Option.setTestOnly(options, testOnly);
    }
    public void setFilter(final boolean filter) {
        options = Option.setFilter(options, filter);
    }
    public void setAltext(final String ext) {
        this.altExt = ext;
    }
    public void setEncoding(final String encoding) {
          this.encoding = encoding;
    }
    public void setIf(final String expr) {
        this.ifExpr = expr;
    }
    public void setUnless(final String expr) {
        this.unlessExpr = expr;
    }
    public boolean isActive() {
        return (this.ifExpr == null
                || getProject().getProperty(this.ifExpr) != null
                || this.unlessExpr == null
                || getProject().getProperty(this.unlessExpr) == null);
    }
    public void execute() throws BuildException {
        if (!isActive()) {
            return;
        }
        checkTargetDir();
        this.sourceDir = getProject().resolveFile("" + this.sourceDir);
        IResolver resolver = new AntResolver(getProject());
        String[]  files    = getFiles();
        log("Preprocessing " + files.length + " file(s)");
        try {
            Preprocessor.preprocessBatch(this.sourceDir, this.targetDir, files,
                    this.altExt, this.encoding, this.options, this.defines,
                    resolver);
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new BuildException("Preprocessing failed: " + ex,
                    ex);
        }
    }
    private String[] getFiles() {
        return getDirectoryScanner(sourceDir).getIncludedFiles();
    }
    private void checkTargetDir() throws BuildException {
        if (targetDir == null) {
            throw new BuildException("Target directory required.");
        }
    }
}