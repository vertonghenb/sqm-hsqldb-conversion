package org.hsqldb.util.preprocessor;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Stack;
public class Preprocessor {
    public static void preprocessBatch(File sourceDir, File targetDir,
            String[] fileNames, String altExt, String encoding, int options,
            String defines, IResolver resolver) throws PreprocessorException {
        for (int i = 0; i < fileNames.length; i++) {
            String fileName = fileNames[i];
            try {
                preprocessFile(sourceDir, targetDir, fileName, altExt, encoding,
                        options, defines, resolver);
            } catch (PreprocessorException ppe) {
                if (!Option.isVerbose(options)) {
                    log(fileName + " ... not modified, " + ppe.getMessage());
                }
                throw ppe;
            }
        }
    }
    public static void preprocessFile(File sourceDir, File targetDir,
            String fileName, String altExt, String encoding, int options,
            String defines, IResolver resolver) throws PreprocessorException {
        String       sourcePath   = translatePath(sourceDir, fileName, null);
        String       targetPath   = translatePath(targetDir, fileName, altExt);
        File         targetFile   = new File(targetPath);
        File         backupFile   = new File(targetPath + "~");
        boolean      sameDir      = sourceDir.equals(targetDir);
        boolean      sameExt      = (altExt ==  null);
        boolean      verbose      = Option.isVerbose(options);
        boolean      testOnly     = Option.isTestOnly(options);
        boolean      backup       = Option.isBackup(options);
        Preprocessor preprocessor = new Preprocessor(sourcePath,
                encoding, options, resolver, defines);
        if (verbose) {
            log("Reading \"" + sourcePath + "\"");
        }
        preprocessor.loadDocument();
        boolean modified = preprocessor.preprocess();
        boolean rewrite  = modified || !sameDir || !sameExt;
        if (!rewrite) {
            if (verbose) {
                log(fileName + " ... not modified");
            }
            return;
        } else if (verbose) {
            log(fileName + " ... modified");
        }
        if (testOnly) {
            return;
        }
        try {
            targetFile.getParentFile().mkdirs();
        } catch (Exception e) {
            throw new PreprocessorException("mkdirs failed \"" + targetFile
                    + "\": " + e); 
        }
        backupFile.delete();
        if (targetFile.exists() && !targetFile.renameTo(backupFile)) {
            throw new PreprocessorException("Rename failed: \""
                    + targetFile
                    + "\" => \""
                    + backupFile
                    +"\"" ); 
        }
        if (verbose) {
            log("Writing \"" + targetPath + "\"");
        }
        preprocessor.saveDocument(targetPath);
        if (!backup) {
            backupFile.delete();
        }
    }
    static final int CONDITION_NONE      = 0;
    static final int CONDITION_ARMED     = 1;
    static final int CONDITION_IN_TRUE   = 2;
    static final int CONDITION_TRIGGERED = 3;
    static final Integer[] STATES = new Integer[] {
        new Integer(CONDITION_NONE),
        new Integer(CONDITION_ARMED),
        new Integer(CONDITION_IN_TRUE),
        new Integer(CONDITION_TRIGGERED)
    };
    private String    documentPath;
    private String    encoding;
    private int       options;
    private IResolver resolver;
    private Document  document;
    private Defines   defines;
    private Stack     stack;
    private int       state;
    private Preprocessor(String documentPath,
            String encoding, int options, IResolver resolver,
            String predefined) throws PreprocessorException {
        if (resolver == null) {
            File parentDir = new File(documentPath).getParentFile();
            this.resolver = new BasicResolver(parentDir);
        } else {
            this.resolver = resolver;
        }
        if (predefined == null || predefined.trim().length() == 0) {
            this.defines = new Defines();
        } else {
            predefined   = this.resolver.resolveProperties(predefined);
            this.defines = new Defines(predefined);
        }
        this.documentPath = documentPath;
        this.encoding     = encoding;
        this.options      = options;
        this.document     = new Document();
        this.stack        = new Stack();
        this.state        = CONDITION_NONE;
    }
    private Preprocessor(Preprocessor other, Document include) {
        this.document     = include;
        this.encoding     = other.encoding;
        this.stack        = new Stack();
        this.state        = CONDITION_NONE;
        this.options      = other.options;
        this.documentPath = other.documentPath;
        this.resolver     = other.resolver;
        this.defines      = other.defines;
    }
    private boolean preprocess() throws PreprocessorException {
        this.stack.clear();
        this.state = CONDITION_NONE;
        if (!this.document.contains(Line.DIRECTIVE_PREFIX)) {
            return false;
        }
        Document originalDocument = new Document(this.document);
        preprocessImpl();
        if (this.state != CONDITION_NONE) {
            throw new PreprocessorException("Missing final #endif"); 
        }
        if (Option.isFilter(options)) {
            for (int i = this.document.size() - 1; i >= 0; i--) {
                Line line = resolveLine(this.document.getSourceLine(i));
                if (!line.isType(LineType.VISIBLE)) {
                    this.document.deleteSourceLine(i);
                }
            }
        }
        return (!this.document.equals(originalDocument));
    }
    private void preprocessImpl() throws PreprocessorException {
        int     includeCount = 0;
        int     lineCount    = 0;
        while (lineCount < this.document.size()) {
            try {
                Line line = resolveLine(this.document.getSourceLine(lineCount));
                switch(line.getType()) {
                    case LineType.INCLUDE : {
                        lineCount = processInclude(lineCount, line);
                        break;
                    }
                    case LineType.VISIBLE :
                    case LineType.HIDDEN : {
                        this.document.setSourceLine(lineCount,
                                toSourceLine(line));
                        if (Option.isVerbose(options)) {
                                log((isHidingLines() ? "Commented: "
                                                     : "Uncommented: ") + line);
                        }
                        lineCount++;
                        break;
                    }
                    default : {
                        processDirective(line);
                        lineCount++;
                    }
                }
            } catch (PreprocessorException ex) {
                throw new PreprocessorException(ex.getMessage() + " at line "
                        + (lineCount + 1)
                        + " in \""
                        + this.documentPath
                        + "\""); 
            }
        }
    }
    private void processIf(boolean condition) {
        statePush();
        this.state = isHidingLines() ? CONDITION_TRIGGERED
                                     : (condition) ? CONDITION_IN_TRUE
                                                   : CONDITION_ARMED;
    }
    private void processElseIf(boolean condition) throws PreprocessorException {
        switch(state) {
            case CONDITION_NONE : {
                throw new PreprocessorException("Unexpected #elif"); 
            }
            case CONDITION_ARMED : {
                if (condition) {
                    this.state = CONDITION_IN_TRUE;
                }
                break;
            }
            case CONDITION_IN_TRUE : {
                this.state = CONDITION_TRIGGERED;
                break;
            }
        }
    }
    private void processElse() throws PreprocessorException {
        switch(state) {
            case CONDITION_NONE : {
                throw new PreprocessorException("Unexpected #else"); 
            }
            case CONDITION_ARMED : {
                this.state = CONDITION_IN_TRUE;
                break;
            }
            case CONDITION_IN_TRUE : {
                this.state = CONDITION_TRIGGERED;
                break;
            }
        }
    }
    private void processEndIf() throws PreprocessorException {
        if (state == CONDITION_NONE) {
            throw new PreprocessorException("Unexpected #endif"); 
        } else {
            statePop();
        }
    }
    private void processDirective(Line line) throws PreprocessorException {
        switch(line.getType()) {
            case LineType.DEFINE : {
                if (!isHidingLines()) {
                    this.defines.defineSingle(line.getArguments());
                }
                break;
            }
            case LineType.UNDEFINE : {
                if (!isHidingLines()) {
                    this.defines.undefine(line.getArguments());
                }
                break;
            }
            case LineType.IF : {
                processIf(this.defines.evaluate(line.getArguments()));
                break;
            }
            case LineType.IFDEF : {
                processIf(this.defines.isDefined(line.getArguments()));
                break;
            }
            case LineType.IFNDEF : {
                processIf(!this.defines.isDefined(line.getArguments()));
                break;
            }
            case LineType.ELIF : {
                processElseIf(this.defines.evaluate(line.getArguments()));
                break;
            }
            case LineType.ELIFDEF : {
                processElseIf(this.defines.isDefined(line.getArguments()));
                break;
            }
            case LineType.ELIFNDEF : {
                processElseIf(!this.defines.isDefined(line.getArguments()));
                break;
            }
            case LineType.ELSE : {
                processElse();
                break;
            }
            case LineType.ENDIF : {
                processEndIf();
                break;
            }
            default : {
                throw new PreprocessorException("Unhandled line type: "
                        + line); 
            }
        }
    }
    private int processInclude(int lineCount, Line line)
    throws PreprocessorException {
        String    path   = resolvePath(line.getArguments());
        boolean   hidden = isHidingLines();
        lineCount++;
        while (lineCount < this.document.size()) {
            line = resolveLine(this.document.getSourceLine(lineCount));
            if (line.isType(LineType.ENDINCLUDE)) {
                break;
            }
            this.document.deleteSourceLine(lineCount);
        }
        if (!line.isType(LineType.ENDINCLUDE)) {
            throw new PreprocessorException("Missing #endinclude"); 
        }
        if (!hidden) {
            Document     include      = loadInclude(path);
            Preprocessor preprocessor = new Preprocessor(this, include);
            preprocessor.preprocess();
            int count = include.size();
            for (int i = 0; i < count; i++) {
                String sourceLine = include.getSourceLine(i);
                if (resolveLine(sourceLine).isType(LineType.VISIBLE)) {
                    this.document.insertSourceLine(lineCount++, sourceLine);
                }
            }
        }
        lineCount++;
        return lineCount;
    }
    private boolean isHidingLines() {
        switch(state) {
            case CONDITION_ARMED :
            case CONDITION_TRIGGERED: {
                return true;
            }
            default : {
                return false;
            }
        }
    }
    private void statePush() {
        this.stack.push(STATES[this.state]);
    }
    private void statePop() {
        this.state = ((Integer) stack.pop()).intValue();
    }
    private Line resolveLine(String line) throws PreprocessorException {
        return new Line(this.resolver.resolveProperties(line));
    }
    private String resolvePath(String path) {
        if (path == null) {
            throw new IllegalArgumentException("path: null");
        }
        String value = this.resolver.resolveProperties(path);
        File   file  = this.resolver.resolveFile(value);
        try {
            return file.getCanonicalPath();
        } catch (IOException ex) {
            return file.getAbsolutePath();
        }
    }
    private String toSourceLine(Line line) {
        return (isHidingLines())
            ? Option.isIndent(this.options)
                ? line.indent + Line.HIDE_DIRECTIVE + line.text
                : Line.HIDE_DIRECTIVE + line.indent + line.text
            : line.indent + line.text;
    }
    private File toCanonicalOrAbsoluteFile(String path) {
        File file = new File(path);
        if (!file.isAbsolute()) {
            path = (new File(this.documentPath)).getParent()
                    + File.separatorChar
                    + path;
            file = new File(path);
        }
        try {
            return file.getCanonicalFile();
        } catch (Exception e) {
            return file.getAbsoluteFile();
        }
    }
    private static String translatePath(File dir, String fileName, String ext) {
        return new StringBuffer(dir.getPath()).append(File.separatorChar).
                append(translateFileExtension(fileName,ext)).toString();
    }
    private static String translateFileExtension(String fileName, String ext) {
        if (ext != null) {
            int pos = fileName.lastIndexOf('.');
            fileName = (pos < 0) ? fileName + ext
                                 : fileName.substring(0, pos) + ext;
        }
        return fileName;
    }
    private Document loadInclude(String path) throws PreprocessorException {
        Document include = new Document();
        File     file    = toCanonicalOrAbsoluteFile(path);
        try {
            return include.load(file, this.encoding);
        } catch (UnsupportedEncodingException uee) {
            throw new PreprocessorException("Unsupported encoding \""
                    + this.encoding + "\" loading include \"" + file
                    + "\""); 
        } catch (IOException ioe) {
            throw new PreprocessorException("Unable to load include \""
                    + file + "\": " + ioe); 
        }
    }
    private void loadDocument() throws PreprocessorException {
        try {
            this.document.load(this.documentPath, this.encoding);
        } catch (UnsupportedEncodingException uee) {
            throw new PreprocessorException("Unsupported encoding \""
                    + this.encoding + "\" reading file \"" + this.documentPath
                    + "\""); 
        } catch (IOException ioe) {
            throw new PreprocessorException("Unable to read file \""
                    + this.documentPath + "\": " + ioe); 
        }
    }
    private void saveDocument(Object target) throws PreprocessorException {
        try {
            if (this.document.size() > 0) {
                this.document.save(target, this.encoding);
            }
        } catch (UnsupportedEncodingException uee) {
            throw new PreprocessorException("Unsupported encoding \""
                    + this.encoding + "\" writing \"" + target
                    + "\""); 
        } catch (IOException ioe) {
            throw new PreprocessorException("Unable to write to \""
                    + target + "\": " + ioe); 
        }
    }
    private static void log(Object toLog) {
        System.out.println(toLog);
    }
}