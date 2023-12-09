


package org.hsqldb.cmdline;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.logging.Level;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.lang.reflect.Method;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.Charset;
import org.hsqldb.lib.AppendableException;
import org.hsqldb.lib.RCData;
import org.hsqldb.lib.StringUtil;
import org.hsqldb.lib.FrameworkLogger;
import org.hsqldb.cmdline.sqltool.Token;
import org.hsqldb.cmdline.sqltool.TokenList;
import org.hsqldb.cmdline.sqltool.TokenSource;
import org.hsqldb.cmdline.sqltool.SqlFileScanner;
import org.hsqldb.cmdline.sqltool.Calculator;
import org.hsqldb.cmdline.sqltool.FileRecordReader;





public class SqlFile {
    private enum Recursion { FILE, IF, WHILE, FOREACH, FOR, FORROWS }
    private static FrameworkLogger logger =
            FrameworkLogger.getLog(SqlFile.class);
    private static final int DEFAULT_HISTORY_SIZE = 40;
    private boolean          executing;
    private boolean          permitEmptySqlStatements;
    private boolean          interactive;
    private String           primaryPrompt    = "sql> ";
    private static String    rawPrompt;
    private static Method    createArrayOfMethod;
    private String           contPrompt       = "  +> ";
    private boolean          htmlMode;
    private TokenList        history;
    
    public static String     LS = System.getProperty("line.separator");
    private int              maxHistoryLength = 1;
    
    
    private boolean          reportTimes;
    private Reader           reader;
    
    
    private String           inputStreamLabel;
    private File             baseDir;
    private boolean          dsvTrimAll;
    private boolean          ignoreBangStatus;
    private boolean          allQuoted;
    private boolean          doPrepare;
    private static String    DSV_X_SYNTAX_MSG;
    private static String    DSV_M_SYNTAX_MSG;
    private static String    nobufferYetString;
    private String           prepareVar;
    private int              dsvRecordsPerCommit = 0;

    static String            DEFAULT_FILE_ENCODING =
                             System.getProperty("file.encoding");
    static Set<String>       hiddenVars = new HashSet<String>(
                             Arrays.asList("?", "*START_TIME", "*VERSION"));

    
    private String nullRepToken;   
    private String nullRepHtml;   
    private String dsvColDelim;    
    private String dsvColSplitter; 
    private String dsvRowDelim;    
    private String dsvRowSplitter; 
    private String dsvConstCols;   
    private String dsvSkipPrefix;  

    
    private String dsvSkipCols;
    private String dsvTargetFile;
    private String dsvTargetTable;
    private String dsvRejectFile;
    private String dsvRejectReport;
    private String topHtmlFile;
    private String bottomHtmlFile;
    private SimpleDateFormat timestampFormat;

    
    private static Pattern   varPattern = Pattern.compile("\\*?[a-zA-Z]\\w*");
    private static Pattern   wordAndDotPattern = Pattern.compile("[\\w.]+");
    private static Pattern   specialPattern =
            Pattern.compile("(\\S+)(?:(\\s+.*\\S))?\\s*");
    private static Pattern  plPattern = Pattern.compile("(.*\\S)?\\s*");
    private static Pattern  mathAsgnPattern = Pattern.compile(
        "\\(\\(\\s*([a-zA-Z]\\w*)\\s*([-+*/%][-+=])\\s*(.+?)?\\s*\\)\\)\\s*");
    private static Pattern  mathPattern = Pattern.compile(
            "\\(\\(\\s*([a-zA-Z]\\w*)\\s*=\\s*(.+?)?\\s*\\)\\)\\s*");
    private static Pattern   foreachPattern =
            Pattern.compile("foreach\\s+(\\S+)\\s*\\(([^)]+)\\)\\s*");
    private static Pattern   forrowsPattern =
            Pattern.compile("forrows((?:\\s+[a-zA-Z]\\w*)*)\\s*");
    private static Pattern   forPattern = Pattern.compile(
        "for\\s+(\\(\\(.+\\)\\))?\\s*(\\([^)]+\\))\\s*(\\(\\(.+\\)\\))\\s*");
    private static Pattern   ifwhilePattern =
            Pattern.compile("\\S+\\s*\\(([^)]*)\\)\\s*");
    private static Pattern   inlineifPattern =
            Pattern.compile("(if\\s*\\([^)]*\\))(.*\\S.*)");
    private static Pattern   varsetPattern =
            Pattern.compile("(\\S+)\\s*([=_~:])(.*)?");
    private static Pattern   substitutionPattern =
            Pattern.compile("(\\S)(.+?)\\1(.*?)\\1(.+?)?\\s*");
            
    private static Pattern   slashHistoryPattern =
            Pattern.compile("\\s*/([^/]+)/\\s*(\\S.*)?");
    private static Pattern   historyPattern =
            Pattern.compile("\\s*(-?\\d+)?\\s*(\\S.*)?");
            
    private static Pattern wincmdPattern;
    private static Pattern useMacroPattern =
            Pattern.compile("(\\w+)(\\s.*[^;])?(;?)");
    private static Pattern useFnPattern =
            Pattern.compile("(\\w+\\()\\s*([^;)]*?)\\s*\\)(.*)");
    private static Pattern legacyEditMacroPattern =
            Pattern.compile("(\\w+(?:\\(\\))?)\\s*:(.*)");
    private static Pattern editMacroPattern =
            Pattern.compile(":\\s(\\w+(?:\\(\\))?)(?:\\s(.*))?");
    private static Pattern spMacroPattern =
            Pattern.compile("(\\w+(?:\\(\\))?)\\s+([*\\\\])(.*\\S)");
    private static Pattern sqlMacroPattern =
            Pattern.compile("(\\w+(?:\\(\\))?)\\s+(.*\\S)");
    private static Pattern integerPattern = Pattern.compile("\\d+");
    private static Pattern nameValPairPattern =
            Pattern.compile("\\s*(\\w+)\\s*=(.*)");
            
    private static Pattern dotPattern = Pattern.compile("(\\w*)\\.(\\w*)");
    private static Pattern commitOccursPattern =
            Pattern.compile("(?is)(?:set\\s+autocommit.*)|(commit\\s*)");
    private static Pattern logPattern =
        Pattern.compile("(?i)(FINER|WARNING|SEVERE|INFO|FINEST)\\s+(.*\\S)");
    private static Pattern   arrayPattern =
            Pattern.compile("ARRAY\\s*\\[\\s*(.*\\S)?\\s*\\]");
    private static Pattern fnParamPat = Pattern.compile("\\*\\{(:)?(\\d+)\\}");

    private static Map<String, Pattern> nestingPLCommands =
            new HashMap<String, Pattern>();
    private static Map<String, Pattern> inlineNestPLCommands =
            new HashMap<String, Pattern>();
    static {
        nestingPLCommands.put("if", ifwhilePattern);
        nestingPLCommands.put("while", ifwhilePattern);
        nestingPLCommands.put("foreach", foreachPattern);
        nestingPLCommands.put("forrows", forrowsPattern);
        nestingPLCommands.put("for", forPattern);
        inlineNestPLCommands.put("if", inlineifPattern);

        if (System.getProperty("os.name").startsWith("Windows"))
            wincmdPattern = Pattern.compile("([^\"]+)?(\"[^\"]*\")?");

        rawPrompt = SqltoolRB.rawmode_prompt.getString() + "> ";
        DSV_OPTIONS_TEXT = SqltoolRB.dsv_options.getString();
        D_OPTIONS_TEXT = SqltoolRB.d_options.getString();
        DSV_X_SYNTAX_MSG = SqltoolRB.dsv_x_syntax.getString();
        DSV_M_SYNTAX_MSG = SqltoolRB.dsv_m_syntax.getString();
        nobufferYetString = SqltoolRB.nobuffer_yet.getString();
        try {
            SqlFile.createArrayOfMethod = Connection.class.getDeclaredMethod(
                    "createArrayOf", String.class, Object[].class);
        } catch (Exception expectedException) {
            
        }
    }
    
    
    

    private boolean removeEmptyVars() {
        String sysP = System.getProperty("sqltool.REMOVE_EMPTY_VARS");
        return sysP != null && Boolean.parseBoolean(sysP);
    }

    
    private void updateUserSettings() {
        
        String varVal;
        if (shared.userVars.containsKey("NULL")
                 || shared.userVars.containsKey("*NULL")) {
                errprintln(SqltoolRB.null_assignment.getString());
             shared.userVars.remove("NULL");
             shared.userVars.remove("*NULL");
         }
        for (String noEmpty : new String[] {
            "DSV_SKIP_COLS", "DSV_COL_DELIM", "TIMESTAMP_FORMAT",
            "DSV_COL_SPLITTER", "DSV_ROW_DELIM", "DSV_ROW_SPLITTER",
            "DSV_TARGET_FILE", "DSV_TARGET_TABLE", "DSV_CONST_COLS",
            "DSV_REJECT_FILE", "DSV_REJECT_REPORT", "DSV_RECORDS_PER_COMMIT",
        }) {
            varVal = shared.userVars.get('*' + noEmpty);
            if (varVal == null || varVal.length() > 0) {
                continue;
            }
            if (!removeEmptyVars()) {
                errprintln(SqltoolRB.auto_unset_warning.getString(noEmpty));
            }
            shared.userVars.remove('*' + noEmpty);
        }

        
        
        
        
        
        dsvSkipPrefix = SqlFile.convertEscapes(
                shared.userVars.get("*DSV_SKIP_PREFIX"));
        if (dsvSkipPrefix == null) {
            dsvSkipPrefix = DEFAULT_SKIP_PREFIX;
        } else if (dsvSkipPrefix.length() < 1) {
            dsvSkipPrefix = null;
        }
        dsvSkipCols = shared.userVars.get("*DSV_SKIP_COLS");
        dsvTrimAll = Boolean.parseBoolean(
                shared.userVars.get("*DSV_TRIM_ALL"));
        ignoreBangStatus = Boolean.parseBoolean(
                shared.userVars.get("*IGNORE_BANG_STATUS"));
        allQuoted = Boolean.parseBoolean(
                shared.userVars.get("*ALL_QUOTED"));
        dsvColDelim = SqlFile.convertEscapes(
                shared.userVars.get("*DSV_COL_DELIM"));
        if (dsvColDelim == null) dsvColDelim = DEFAULT_COL_DELIM;
        dsvColSplitter = shared.userVars.get("*DSV_COL_SPLITTER");
        if (dsvColSplitter == null) dsvColSplitter = DEFAULT_COL_SPLITTER;

        dsvRowDelim = SqlFile.convertEscapes(
                shared.userVars.get("*DSV_ROW_DELIM"));
        if (dsvRowDelim == null) dsvRowDelim = DEFAULT_ROW_DELIM;
        dsvRowSplitter = shared.userVars.get("*DSV_ROW_SPLITTER");
        if (dsvRowSplitter == null) dsvRowSplitter = DEFAULT_ROW_SPLITTER;

        dsvTargetFile = shared.userVars.get("*DSV_TARGET_FILE");
        dsvTargetTable = shared.userVars.get("*DSV_TARGET_TABLE");

        dsvConstCols = shared.userVars.get("*DSV_CONST_COLS");
        dsvRejectFile = shared.userVars.get("*DSV_REJECT_FILE");
        dsvRejectReport = shared.userVars.get("*DSV_REJECT_REPORT");
        topHtmlFile = shared.userVars.get("*TOP_HTMLFRAG_FILE");
        bottomHtmlFile = shared.userVars.get("*BOTTOM_HTMLFRAG_FILE");
        dsvRecordsPerCommit = 0;
        if (shared.userVars.get("*DSV_RECORDS_PER_COMMIT") != null) try {
            dsvRecordsPerCommit = Integer.parseInt(
                    shared.userVars.get("*DSV_RECORDS_PER_COMMIT"));
        } catch (NumberFormatException nfe) {
            errprintln(SqltoolRB.reject_rpc.getString(
                    shared.userVars.get("*DSV_RECORDS_PER_COMMIT")));
            shared.userVars.remove("*DSV_RECORDS_PER_COMMIT");
        }

        nullRepToken = convertEscapes(shared.userVars.get("*NULL_REP_TOKEN"));
        if (nullRepToken == null) nullRepToken = DEFAULT_NULL_REP;
        nullRepHtml = shared.userVars.get("*NULL_REP_HTML");
        if (nullRepHtml == null) nullRepHtml = DEFAULT_NULL_HTML;
        timestampFormat = null;
        String formatString = shared.userVars.get("*TIMESTAMP_FORMAT");
        if (formatString != null) try {
            timestampFormat = new SimpleDateFormat(formatString);
        } catch (IllegalArgumentException iae) {
            errprintln(SqltoolRB.bad_time_format.getString(
                    formatString, iae.getMessage()));
            shared.userVars.remove("*TIMESTAMP_FORMAT");
        }
    }

    
    private static class SharedFields {
        
        boolean possiblyUncommitteds;

        Connection jdbcConn;

        
        
        Map<String, String> userVars = new HashMap<String, String>();

        Map<String, Token> macros = new HashMap<String, Token>();

        PrintStream psStd;

        SharedFields(PrintStream psStd) {
            this.psStd = psStd;
        }

        String encoding;
    }

    private SharedFields shared;

    private static final String DIVIDER =
        "-----------------------------------------------------------------"
        + "-----------------------------------------------------------------";
    
    private static final String revString = "$Revision: 4863 $";
    private static final int revStringLength = revString.length();
    private static final String  revnum =
            (revStringLength - " $".length() > "$Revision: ".length())
            ?  revString.substring("$Revision: ".length(),
                    revStringLength - " $".length())
            : "<UNTRACKED>";

    private static String DSV_OPTIONS_TEXT;
    private static String D_OPTIONS_TEXT;

    
    public SqlFile(File inputFile) throws IOException {
        this(inputFile, null);
    }

    
    public SqlFile(File inputFile, String encoding) throws IOException {
        this(inputFile, encoding, false);
    }

    
    public SqlFile(File inputFile, String encoding, boolean interactive)
            throws IOException {
        this(new InputStreamReader(new FileInputStream(inputFile),
                (encoding == null) ? DEFAULT_FILE_ENCODING : encoding),
                inputFile.toString(), System.out, encoding, interactive,
                inputFile.getParentFile());
    }

    
    public SqlFile(String encoding, boolean interactive) throws IOException {
        this((encoding == null)
                ? new InputStreamReader(System.in)
                : new InputStreamReader(System.in, encoding),
                "<stdin>", System.out, encoding, interactive, null);
    }

    
    public SqlFile(Reader reader, String inputStreamLabel,
            PrintStream psStd, String encoding, boolean interactive,
            File baseDir) throws IOException {
        this(reader, inputStreamLabel, baseDir);
        try {
            shared = new SharedFields(psStd);
            shared.userVars.put(
                    "*START_TIME", (new java.util.Date()).toString());
            shared.userVars.put("*REVISION", revnum);
            shared.userVars.put("?", "");
            setEncoding(encoding);
            this.interactive = interactive;
            continueOnError = this.interactive;

            if (interactive) {
                history = new TokenList();
                maxHistoryLength = DEFAULT_HISTORY_SIZE;
            }
            
            
        } catch (IOException ioe) {
            closeReader();
            throw ioe;
        } catch (RuntimeException re) {
            closeReader();
            throw re;
        }
    }

    
    private SqlFile(SqlFile parentSqlFile, File inputFile) throws IOException {
        this(parentSqlFile,
                new InputStreamReader(new FileInputStream(inputFile),
                (parentSqlFile.shared.encoding == null)
                ? DEFAULT_FILE_ENCODING : parentSqlFile.shared.encoding),
                inputFile.toString(), inputFile.getParentFile());
    }

    
    private SqlFile(SqlFile parentSqlFile, Reader reader,
            String inputStreamLabel, File baseDir) {
        this(reader, inputStreamLabel, baseDir);
        try {
            recursed = Recursion.FILE;
            shared = parentSqlFile.shared;
            
            interactive = false;
            continueOnError = parentSqlFile.continueOnError;
            
            
            
            
        } catch (RuntimeException re) {
            closeReader();
            throw re;
        }
    }

    
    private SqlFile(Reader reader, String inputStreamLabel, File baseDir) {
        logger.privlog(Level.FINER, "<init>ting SqlFile instance",
                null, 2, FrameworkLogger.class);
        if (reader == null)
            throw new IllegalArgumentException("'reader' may not be null");
        if (inputStreamLabel == null)
            throw new IllegalArgumentException(
                    "'inputStreamLabel' may not be null");

        
        
        
        this.reader = reader;
        this.inputStreamLabel = inputStreamLabel;
        this.baseDir = (baseDir == null) ? new File(".") : baseDir;
    }

    public void setConnection(Connection jdbcConn) {
        if (jdbcConn == null)
            throw new IllegalArgumentException(
                    "We don't yet support unsetting the JDBC Connection");
        shared.jdbcConn = jdbcConn;
    }

    public Connection getConnection() {
        return shared.jdbcConn;
    }

    public void setContinueOnError(boolean continueOnError) {
        this.continueOnError = continueOnError;
    }

    public void setMaxHistoryLength(int maxHistoryLength) {
        if (executing)
            throw new IllegalStateException(
                "Can't set maxHistoryLength after execute() has been called");
        if (reader == null)
            throw new IllegalStateException(
                "Can't set maxHistoryLength execute() has run");
        this.maxHistoryLength = maxHistoryLength;
    }

    public void addMacros(Map<String, Token> newMacros) {
        shared.macros.putAll(newMacros);
    }

    public void addUserVars(Map<String, String> newUserVars) {
        for (String val : newUserVars.values()) {
            if (val == null)
                throw new IllegalArgumentException(
                        "Null mapping values not allowed");
        }
        shared.userVars.putAll(newUserVars);
        List<String> strangeVars = new ArrayList<String>();
        for (String name : newUserVars.keySet())
            if (!name.equals("?") && !varPattern.matcher(name).matches())
                strangeVars.add(name);
        if (strangeVars.size() > 0)
            errprintln(SqltoolRB.varname_warning.getString(
                    strangeVars.toString()));
        sqlExpandMode = null;
    }

    
    public Map<String, String> getUserVars() {
        return shared.userVars;
    }

    public Map<String, Token> getMacros() {
        
        return shared.macros;
    }

    
    private void setEncoding(String newEncoding)
            throws UnsupportedEncodingException {
        if (newEncoding == null || newEncoding.length() < 1) {
            shared.encoding = null;
            shared.userVars.remove("*ENCODING");
            return;
        }
        if (!Charset.isSupported(newEncoding))
            throw new UnsupportedEncodingException(newEncoding);
        shared.userVars.put("*ENCODING", newEncoding);
        shared.encoding = newEncoding;
    }

    
    private Recursion   recursed;
    private PrintWriter pwQuery;
    private PrintWriter pwDsv;
    private boolean     continueOnError;
    
    private SqlFileScanner scanner;
    private Token          buffer, prevToken;
    private boolean        preempt;
    private String         lastSqlStatement;
    private boolean        autoClose = true;
    private boolean        csvStyleQuoting;

    
    public void setAutoClose(boolean autoClose) {
        this.autoClose = autoClose;
    }

    
    synchronized public void execute() throws SqlToolError, SQLException {
        if (reader == null)
            throw new IllegalStateException("Can't call execute() "
                    + "more than once for a single SqlFile instance");
        updateUserSettings();

        try {
            scanner = new SqlFileScanner(reader);
            scanner.setStdPrintStream(shared.psStd);
            scanner.setRawLeadinPrompt(SqltoolRB.raw_leadin.getString());
            if (interactive) {
                stdprintln(SqltoolRB.SqlFile_banner.getString(revnum));
                scanner.setRawPrompt(rawPrompt);
                scanner.setSqlPrompt(contPrompt);
                scanner.setSqltoolPrompt(primaryPrompt);
                scanner.setInteractive(true);
                if (shared.jdbcConn == null)
                    stdprintln(SqltoolRB.suggest_j.getString());
                stdprint(primaryPrompt);
            }
            scanpass(scanner);
        } finally {
            try {
                closeQueryOutputStream();
                if (autoClose) closeReader();
            } finally {
                reader = null; 
            }
        }
    }

    
    public void closeReader() {
        if (reader == null) return;
        try {
            if (scanner != null) try {
                scanner.yyclose();
            } catch (IOException ioe) {
                errprintln(SqltoolRB.pipeclose_failure.getString(ioe));
            }
            try {
                reader.close();
            } catch (IOException ioe) {
                
                
            }
        } finally {
            reader = null; 
        }
    }

    
    private Matcher inlineNestMatcher(Token token) throws BadSpecial {
        if (token.type != Token.PL_TYPE) return null;
        
        String commandWord = token.val.replaceFirst("\\s.*", "");
        if (!inlineNestPLCommands.containsKey(commandWord)) return null;
        Pattern pattern = inlineNestPLCommands.get(commandWord);
        Matcher m = pattern.matcher(token.val);
        return m.matches() ? m : null;
    }

    
    private String nestingCommand(Token token) throws BadSpecial {
        if (token.type != Token.PL_TYPE) return null;
        
        String commandWord = token.val.replaceFirst("\\s.*", "");
        if (!nestingPLCommands.containsKey(commandWord)) return null;
        Pattern pattern = nestingPLCommands.get(commandWord);
        if (pattern.matcher(token.val).matches()) return commandWord;
        throw new BadSpecial(SqltoolRB.pl_malformat.getString());
    }

    
    private void setSqlExpandMode() {
        for (String key : shared.userVars.keySet()) {
            if (key.charAt(0) != '*' && !key.equals("?")) {
                sqlExpandMode = Boolean.TRUE;
                return;
            }
        }
        sqlExpandMode = Boolean.FALSE;
    }

    synchronized protected void scanpass(TokenSource ts)
                                     throws SqlToolError, SQLException {
        boolean rollbackUncoms = true;
        String nestingCommand;
        Matcher inlineNestMatcher;
        Token token = null;
        sqlExpandMode = null;

        try {
            while (true) try {
                if (preempt) {
                    token = buffer;
                    preempt = false;
                } else {
                    token = ts.yylex();
                    logger.finest("SqlFile got new token:  " + token);
                }
                if (token == null) break;

                inlineNestMatcher = inlineNestMatcher(token);
                if (inlineNestMatcher != null) {
                    processInlineBlock(token,
                            inlineNestMatcher.group(1),
                            inlineNestMatcher.group(2));
                    processBlock(token);
                    continue;
                }

                nestingCommand = nestingCommand(token);
                if (nestingCommand != null) {
                    if (token.nestedBlock == null)
                        token.nestedBlock = seekTokenSource(nestingCommand);
                        
                    processBlock(token);
                        
                    continue;
                }

                switch (token.type) {
                    case Token.SYNTAX_ERR_TYPE:
                        throw new SqlToolError(
                                SqltoolRB.input_malformat.getString());
                        
                        
                        
                        
                    case Token.UNTERM_TYPE:
                        throw new SqlToolError(
                                SqltoolRB.input_unterminated.getString(
                                token.val));
                    case Token.RAW_TYPE:
                    case Token.RAWEXEC_TYPE:
                        
                        if (token.val == null) token.val = "";
                        
                        if (token.val.trim().length() < 1)
                            throw new SqlToolError(
                                    SqltoolRB.raw_empty.getString());
                        int receivedType = token.type;
                        token.type = Token.SQL_TYPE;
                        if (setBuf(token) && receivedType == Token.RAW_TYPE
                                && interactive) {
                            stdprintln("");
                            stdprintln(SqltoolRB.raw_movedtobuffer.getString());
                            stdprint(primaryPrompt);
                            
                            
                            
                            
                        }
                        if (receivedType == Token.RAWEXEC_TYPE) {
                            historize();
                            Statement statement = processSQL();
                            ResultSet rs = null;
                            if (statement != null) {
                                try {
                                    rs = statement.getResultSet();
                                } catch (SQLException se) {
                                    try {
                                        if (statement != null)
                                            statement.close();
                                    } catch (SQLException nse) {
                                        
                                    } finally {
                                        statement = null;
                                    }
                                    throw se;  
                                }
                                displaySqlResults(
                                        statement, rs, null, null, true);
                            }
                        }
                        continue;
                    case Token.MACRO_TYPE:
                        processMacro(token);
                        continue;
                    case Token.PL_TYPE:
                        
                        prevToken = buffer;
                        setBuf(token);
                        historize();
                        processPL();
                        continue;
                    case Token.SPECIAL_TYPE:
                        
                        prevToken = buffer;
                        setBuf(token);
                        historize();
                        processSpecial(null);
                        continue;
                    case Token.EDIT_TYPE:
                        
                        processBuffHist(token);
                        continue;
                    case Token.BUFFER_TYPE:
                        token.type = Token.SQL_TYPE;
                        if (setBuf(token))
                            stdprintln(
                                    SqltoolRB.input_movedtobuffer.getString());
                        continue;
                    case Token.SQL_TYPE:
                        if (token.val == null) token.val = "";
                        setBuf(token);
                        historize();
                        Statement statement = processSQL();
                        ResultSet rs = null;
                        if (statement != null) {
                            try {
                                rs = statement.getResultSet();
                            } catch (SQLException se) {
                                try {
                                    if (statement != null) statement.close();
                                } catch (SQLException nse) {
                                    
                                } finally {
                                    statement = null;
                                }
                                throw se;  
                            }
                            displaySqlResults(
                                    statement, rs, null, null, true);
                        }
                        continue;
                    default:
                        assert false :
                            "Internal assertion failed. Unexpected token type: "
                            + token.getTypeString();
                }
            } catch (BadSpecial bs) {
                
                if (token == null) {
                    errprintln(SqltoolRB.errorat.getString(
                            inputStreamLabel, "?", "?", bs.getMessage()));
                } else {
                    errprintln(SqltoolRB.errorat.getString(
                            inputStreamLabel,
                            Integer.toString(token.line),
                            token.reconstitute(),
                            bs.getMessage(), bs.getMessage()));
                }
                Throwable cause = bs.getCause();
                if (cause != null)
                    errprintln(SqltoolRB.causereport.getString(cause));

                if (!continueOnError) throw new SqlToolError(bs);
            } catch (SQLException se) {
                
                errprintln("SQL " + SqltoolRB.errorat.getString(
                        inputStreamLabel,
                        ((token == null) ? "?"
                                         : Integer.toString(token.line)),
                        lastSqlStatement,
                        se.getMessage()));
                
                
                

                if (!continueOnError) throw se;
            } catch (BreakException be) {
                String msg = be.getMessage();

                if (recursed != null) {
                    rollbackUncoms = false;
                    
                    
                    
                    
                } else if (msg == null || msg.equals("file")) {
                    break;
                } else {
                    errprintln(SqltoolRB.break_unsatisfied.getString(msg));
                }

                if (recursed  != null || !continueOnError) throw be;
            } catch (ContinueException ce) {
                String msg = ce.getMessage();

                if (recursed != null) {
                    rollbackUncoms = false;
                } else {
                    errprintln(SqltoolRB.continue_unsatisfied.getString(msg));
                }

                if (recursed != null || !continueOnError) throw ce;
            } catch (QuitNow qn) {
                throw qn;
            } catch (SqlToolError ste) {
                StringBuffer sb = new StringBuffer(SqltoolRB.errorat.getString(
                    
                    ((token == null)
                            ? (new String[] {
                                inputStreamLabel, "?", "?",
                                ((ste.getMessage() == null)
                                        ? "" : ste.getMessage())
                              })
                            : (new String[] {
                                inputStreamLabel, Integer.toString(token.line),
                                ((token.val == null) ? "" : token.reconstitute()),
                                ((ste.getMessage() == null)
                                        ? "" : ste.getMessage())
                              }))
                ));
                Throwable cause = ste.getCause();
                errprintln((cause == null) ? sb.toString()
                        : SqltoolRB.causereport.getString(cause));
                if (!continueOnError) throw ste;
            }

            rollbackUncoms = false;
            
        } catch (IOException ioe) {
            throw new SqlToolError(
                    SqltoolRB.primaryinput_accessfail.getString(), ioe);
        } catch (QuitNow qn) {
            if (recursed != null)
                throw qn;
                
                
            rollbackUncoms = (qn.getMessage() != null);

            if (rollbackUncoms) {
                errprintln(SqltoolRB.aborting.getString(qn.getMessage()));
                throw new SqlToolError(qn.getMessage());
            }

            return;
        } finally {
            if (fetchingVar != null) {
                errprintln(SqltoolRB.plvar_set_incomplete.getString(
                        fetchingVar));
                fetchingVar = null;
                rollbackUncoms = true;
            }
            if (shared.jdbcConn != null) {
                if (shared.jdbcConn.getAutoCommit())
                    shared.possiblyUncommitteds = false;
                if (rollbackUncoms && shared.possiblyUncommitteds) {
                    errprintln(SqltoolRB.rollingback.getString());
                    shared.jdbcConn.rollback();
                    shared.possiblyUncommitteds = false;
                }
            }
        }
    }

    
    private static class BadSpecial extends AppendableException {
        static final long serialVersionUID = 7162440064026570590L;

        BadSpecial(String s) {
            super(s);
            assert s != null:
                "Must construct BadSpecials with non-null message";
        }
        BadSpecial(String s, Throwable t) {
            super(s, t);
            assert s != null:
                "Must construct BadSpecials with non-null message";
        }
    }

    
    private class QuitNow extends SqlToolError {
        static final long serialVersionUID = 1811094258670900488L;

        public QuitNow(String s) {
            super(s);
        }

        public QuitNow() {
            super();
        }
    }

    
    private class BreakException extends SqlToolError {
        static final long serialVersionUID = 351150072817675994L;

        public BreakException() {
            super();
        }

        public BreakException(String s) {
            super(s);
        }
    }

    
    private class ContinueException extends SqlToolError {
        static final long serialVersionUID = 5064604160827106014L;

        public ContinueException() {
            super();
        }

        public ContinueException(String s) {
            super(s);
        }
    }

    
    private class BadSubst extends Exception {
        static final long serialVersionUID = 7325933736897253269L;

        BadSubst(String s) {
            super(s);
        }
    }

    
    private class RowError extends AppendableException {
        static final long serialVersionUID = 754346434606022750L;

        RowError(String s) {
            super(s);
        }

        

        RowError(String s, Throwable t) {
            super(s, t);
        }
    }

    
    private void processBuffHist(Token token)
    throws BadSpecial, SQLException, SqlToolError {
        if (token.val.length() < 1)
            throw new BadSpecial(SqltoolRB.bufhist_unspecified.getString());

        
        
        char commandChar = token.val.charAt(0);
        String other       = token.val.substring(1);
        if (other.trim().length() == 0) other = null;
        switch (commandChar) {
            case 'l' :
            case 'b' :
                if (other != null)
                    throw new BadSpecial(
                            SqltoolRB.special_extrachars.getString("l", other));
                if (buffer == null) {
                    stdprintln(nobufferYetString);
                } else {
                    stdprintln(SqltoolRB.editbuffer_contents.getString(
                            buffer.reconstitute()));
                }

                return;

            case 'h' :
                if (other != null)
                    throw new BadSpecial(
                            SqltoolRB.special_extrachars.getString("h", other));
                showHistory();

                return;

            case '?' :
                stdprintln(SqltoolRB.buffer_help.getString());

                return;
        }

        Integer histNum = null;
        Matcher hm = slashHistoryPattern.matcher(token.val);
        if (hm.matches()) {
            histNum = historySearch(hm.group(1));
            if (histNum == null) {
                stdprintln(SqltoolRB.substitution_nomatch.getString());
                return;
            }
        } else {
            hm = historyPattern.matcher(token.val);
            if (!hm.matches())
                throw new BadSpecial(SqltoolRB.edit_malformat.getString());
                
                
            histNum = ((hm.group(1) == null || hm.group(1).length() < 1)
                    ? null : Integer.valueOf(hm.group(1)));
        }
        if (hm.groupCount() != 2)
            throw new BadSpecial(SqltoolRB.edit_malformat.getString());
            
            
        commandChar = ((hm.group(2) == null || hm.group(2).length() < 1)
                ? '\0' : hm.group(2).charAt(0));
        other = ((commandChar == '\0') ? null : hm.group(2).substring(1));
        if (other != null && other.trim().length() < 1) other = null;
        Token targetCommand = ((histNum == null)
                ? null : commandFromHistory(histNum.intValue()));
        

        switch (commandChar) {
            case '\0' :  
                setBuf(targetCommand);
                stdprintln(SqltoolRB.buffer_restored.getString(
                        buffer.reconstitute()));
                return;

            case ';' :
                if (other != null)
                    throw new BadSpecial(
                            SqltoolRB.special_extrachars.getString(";", other));

                if (targetCommand != null) setBuf(targetCommand);
                if (buffer == null) throw new BadSpecial(
                        SqltoolRB.nobuffer_yet.getString());
                stdprintln(SqltoolRB.buffer_executing.getString(
                        buffer.reconstitute()));
                preempt = true;
                return;

            case 'a' :
                if (targetCommand == null) targetCommand = buffer;
                if (targetCommand == null) throw new BadSpecial(
                        SqltoolRB.nobuffer_yet.getString());
                boolean doExec = false;

                if (other != null) {
                    if (other.trim().charAt(other.trim().length() - 1) == ';') {
                        other = other.substring(0, other.lastIndexOf(';'));
                        if (other.trim().length() < 1)
                            throw new BadSpecial(
                                    SqltoolRB.append_empty.getString());
                        doExec = true;
                    }
                }
                Token newToken = new Token(targetCommand.type,
                        targetCommand.val, targetCommand.line);
                if (other != null) newToken.val += other;
                setBuf(newToken);
                if (doExec) {
                    stdprintln(SqltoolRB.buffer_executing.getString(
                            buffer.reconstitute()));
                    preempt = true;
                    return;
                }

                if (interactive) scanner.setMagicPrefix(
                        newToken.reconstitute());

                switch (newToken.type) {
                    case Token.SQL_TYPE:
                        scanner.setRequestedState(SqlFileScanner.SQL);
                        break;
                    case Token.SPECIAL_TYPE:
                        scanner.setRequestedState(SqlFileScanner.SPECIAL);
                        break;
                    case Token.PL_TYPE:
                        scanner.setRequestedState(SqlFileScanner.PL);
                        break;
                    default:
                        assert false: "Internal assertion failed.  "
                            + "Appending to unexpected type: "
                            + newToken.getTypeString();
                }
                scanner.setCommandBuffer(newToken.val);

                return;

            case 'w' :
                if (targetCommand == null) targetCommand = buffer;
                if (targetCommand == null) throw new BadSpecial(
                        SqltoolRB.nobuffer_yet.getString());
                if (other == null)
                    throw new BadSpecial(SqltoolRB.destfile_demand.getString());
                String targetFile =
                        dereferenceAt(dereference(other.trim(), false));
                
                

                PrintWriter pw = null;
                try {
                    pw = new PrintWriter(
                            new OutputStreamWriter(
                            new FileOutputStream(targetFile, true),
                            (shared.encoding == null)
                            ? DEFAULT_FILE_ENCODING : shared.encoding)
                            
                    );

                    pw.println(targetCommand.reconstitute(true));
                    pw.flush();
                } catch (Exception e) {
                    throw new BadSpecial(SqltoolRB.file_appendfail.getString(
                            targetFile), e);
                } finally {
                    if (pw != null) try {
                        pw.close();
                    } finally {
                        pw = null; 
                    }
                }

                return;

            case 's' :
                boolean modeExecute = false;
                boolean modeGlobal = false;
                if (targetCommand == null) targetCommand = buffer;
                if (targetCommand == null) throw new BadSpecial(
                        SqltoolRB.nobuffer_yet.getString());

                try {
                    if (other == null || other.length() < 3)
                        throw new BadSubst(
                                SqltoolRB.substitution_malformat.getString());
                    Matcher m = substitutionPattern.matcher(other);
                    if (!m.matches())
                        throw new BadSubst(
                                SqltoolRB.substitution_malformat.getString());

                    
                    assert m.groupCount() > 2 && m.groupCount() < 5 :
                        "Internal assertion failed.  "
                        + "Matched substitution pattern, but captured "
                        + m.groupCount() + " groups";
                    String optionGroup = (
                            (m.groupCount() > 3 && m.group(4) != null)
                            ? (new String(m.group(4))) : null);

                    if (optionGroup != null) {
                        if (optionGroup.indexOf(';') > -1) {
                            modeExecute = true;
                            optionGroup = optionGroup.replaceFirst(";", "");
                        }
                        if (optionGroup.indexOf('g') > -1) {
                            modeGlobal = true;
                            optionGroup = optionGroup.replaceFirst("g", "");
                        }
                    }

                    Matcher bufferMatcher = Pattern.compile("(?s"
                            + ((optionGroup == null) ? "" : optionGroup)
                            + ')' + m.group(2)).matcher(targetCommand.val);
                    Token newBuffer = new Token(targetCommand.type,
                            (modeGlobal
                                ? bufferMatcher.replaceAll(m.group(3))
                                : bufferMatcher.replaceFirst(m.group(3))),
                                targetCommand.line);
                    if (newBuffer.val.equals(targetCommand.val)) {
                        stdprintln(SqltoolRB.substitution_nomatch.getString());
                        return;
                    }

                    setBuf(newBuffer);
                    stdprintln(modeExecute
                            ? SqltoolRB.buffer_executing.getString(
                                    buffer.reconstitute())
                            : SqltoolRB.editbuffer_contents.getString(
                                    buffer.reconstitute())
                    );
                } catch (PatternSyntaxException pse) {
                    throw new BadSpecial(
                            SqltoolRB.substitution_syntax.getString(), pse);
                } catch (BadSubst badswitch) {
                    throw new BadSpecial(
                            SqltoolRB.substitution_syntax.getString());
                }
                if (modeExecute) preempt = true;

                return;
        }

        throw new BadSpecial(SqltoolRB.buffer_unknown.getString(
                Character.toString(commandChar)));
    }

    private void enforce1charSpecial(String tokenString, char command)
            throws BadSpecial {
        if (tokenString.length() != 1)
            throw new BadSpecial(SqltoolRB.special_extrachars.getString(
                     Character.toString(command), tokenString.substring(1)));
    }

    
    private void processSpecial(String inString)
    throws BadSpecial, QuitNow, SQLException, SqlToolError {
        String string = (inString == null) ? buffer.val : inString;
        if (string.length() < 1)
            throw new BadSpecial(SqltoolRB.special_unspecified.getString());
        Matcher m = specialPattern.matcher(dereference(string, false));
        if (!m.matches())
            throw new BadSpecial(SqltoolRB.special_malformat.getString());
            
            
        assert m.groupCount() > 0 && m.groupCount() < 3:
            "Internal assertion failed.  Pattern matched, yet captured "
            + m.groupCount() + " groups";

        String arg1 = m.group(1);
        
        String other = ((m.groupCount() > 1) ? m.group(2) : null);

        switch (arg1.charAt(0)) {
            case 'q' :
                enforce1charSpecial(arg1, 'q');
                if (other != null) throw new QuitNow(other.trim());

                throw new QuitNow();
            case 'H' :
            case 'h' :
                enforce1charSpecial(arg1, 'h');
                htmlMode = (other == null) ? (!htmlMode)
                        : Boolean.parseBoolean(other.trim());

                shared.psStd.println(SqltoolRB.html_mode.getString(
                        Boolean.toString(htmlMode)));

                return;

            case 'm' :
                if (arg1.equals("m?") || arg1.equals("mq?")
                        || (other != null && other.trim().equals("?")
                        && (arg1.equals("m") || arg1.equals("mq")))) {
                    stdprintln(DSV_OPTIONS_TEXT + LS + DSV_M_SYNTAX_MSG);
                    return;
                }
                shared.userVars.remove("?");
                requireConnection();
                if ((!arg1.equals("mq") && arg1.length() != 1)
                        || other == null)
                    throw new BadSpecial(DSV_M_SYNTAX_MSG);
                other = other.trim();
                String skipPrefix = dsvSkipPrefix;

                if (other.charAt(other.length() - 1) == '*') {
                    other = other.substring(0, other.length()-1).trim();
                    if (other.length() < 1) 
                        throw new BadSpecial(DSV_M_SYNTAX_MSG);
                    skipPrefix = null;
                }

                csvStyleQuoting = arg1.equals("mq");
                try {
                    importDsv(dereferenceAt(other), skipPrefix);
                } finally {
                    csvStyleQuoting = false;
                }
                shared.userVars.put("?", "");

                return;

            case 'x' :
                if (arg1.equals("x?") || arg1.equals("xq?")
                        || (other != null && other.trim().equals("?")
                        && (arg1.equals("x") || arg1.equals("xq")))) {
                    stdprintln(DSV_OPTIONS_TEXT + LS + DSV_X_SYNTAX_MSG);
                    return;
                }
                shared.userVars.remove("?");
                requireConnection();
                try {
                    if ((!arg1.equals("xq") && arg1.length() != 1)
                            || other == null)
                        throw new BadSpecial(DSV_X_SYNTAX_MSG);
                    String tableName = null;
                    StringBuilder query = new StringBuilder();

                    
                    if (other.trim().charAt(0) == ':') {
                        
                        if (prevToken == null)
                            throw new BadSpecial(nobufferYetString);
                        query.append(prevToken.val)
                                .append(other.substring(other.indexOf(':')+1));
                    } else if (wordAndDotPattern.matcher(
                            other.trim()).matches()) {
                        
                        tableName = other.trim();
                        query.append("SELECT * FROM ").append(tableName);
                    } else {
                        
                        query.append(other.trim());
                    }

                    if (dsvTargetFile == null && tableName == null)
                        throw new BadSpecial(
                                SqltoolRB.dsv_targetfile_demand.getString());
                    ResultSet rs = null;
                    Statement st = null;
                    File dsvFile = null;
                    csvStyleQuoting = arg1.equals("xq");
                    try {
                        dsvFile = new File((dsvTargetFile == null)
                                ? (tableName
                                        + (csvStyleQuoting ? ".csv" : ".dsv"))
                                : dereferenceAt(dsvTargetFile));

                        pwDsv = new PrintWriter(new OutputStreamWriter(
                                new FileOutputStream(dsvFile),
                                (shared.encoding == null)
                                ? DEFAULT_FILE_ENCODING : shared.encoding));

                        st = shared.jdbcConn.createStatement();
                        rs = st.executeQuery(query.toString());
                        List<Integer> colList = new ArrayList<Integer>();
                        int[] incCols = null;
                        if (dsvSkipCols != null) {
                            Set<String> skipCols = new HashSet<String>();
                            for (String s : dsvSkipCols.split(
                                    "\\Q" + dsvColDelim, -1)) {
                            
                            
                            
                            
                                skipCols.add(s.trim().toLowerCase());
                            }
                            ResultSetMetaData rsmd = rs.getMetaData();
                            for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                                if (!skipCols.remove(rsmd.getColumnName(i)
                                        .toLowerCase()))
                                    colList.add(Integer.valueOf(i));
                            }
                            if (colList.size() < 1)
                                throw new BadSpecial(
                                        SqltoolRB.dsv_nocolsleft.getString(
                                        dsvSkipCols));
                            if (skipCols.size() > 0)
                                throw new BadSpecial(
                                        SqltoolRB.dsv_skipcols_missing.getString(
                                        skipCols.toString()));
                            incCols = new int[colList.size()];
                            for (int i = 0; i < incCols.length; i++)
                                incCols[i] = colList.get(i).intValue();
                        }
                        displaySqlResults(st, rs, incCols, null, true);
                    } finally {
                        csvStyleQuoting = false;
                        if (rs != null) try {
                            rs.close();
                        } catch (SQLException se) {
                            
                        } finally {
                            rs = null;
                        }
                        if (st != null) try {
                            st.close();
                        } catch (SQLException se) {
                            
                        } finally {
                            st = null;
                        }
                    }
                    pwDsv.flush();
                    stdprintln(SqltoolRB.file_wrotechars.getString(
                            Long.toString(dsvFile.length()),
                            dsvFile.toString()));
                } catch (FileNotFoundException e) {
                    throw new BadSpecial(SqltoolRB.file_writefail.getString(
                            other), e);
                } catch (UnsupportedEncodingException e) {
                    throw new BadSpecial(SqltoolRB.file_writefail.getString(
                            other), e);
                } finally {
                    
                    if (pwDsv != null) try {
                        pwDsv.close();
                    } finally {
                        pwDsv = null; 
                    }
                }

                return;

            case 'd' :
                if (arg1.equals("d?") ||
                        (arg1.equals("d") && other != null
                                 && other.trim().equals("?"))) {
                    stdprintln(D_OPTIONS_TEXT);
                    return;
                }
                requireConnection();
                if (arg1.length() == 2) {
                    listTables(arg1.charAt(1),
                            (other == null) ? null : other.trim());

                    return;
                }

                if (arg1.length() == 1 && other != null) try {
                    other = other.trim();
                    int space = other.indexOf(' ');

                    if (space < 0) {
                        describe(other, null);
                    } else {
                        describe(other.substring(0, space),
                                 other.substring(space + 1).trim());
                    }

                    return;
                } catch (SQLException se) {
                    throw new BadSpecial(
                            SqltoolRB.metadata_fetch_fail.getString(), se);
                }

                throw new BadSpecial(SqltoolRB.special_d_like.getString());
            case 'o' :
                boolean addFooter = arg1.equals("oc");
                if (addFooter) arg1 = "o";
                enforce1charSpecial(arg1, 'o');
                if (other == null) {
                    if (pwQuery == null)
                        throw new BadSpecial(
                                SqltoolRB.outputfile_nonetoclose.getString());

                    if (addFooter)
                        writeFooter(pwQuery, "(the HTML report file)");
                    closeQueryOutputStream();

                    return;
                }

                other = other.trim();
                if (pwQuery != null) {
                    shared.psStd.println(
                            SqltoolRB.outputfile_reopening.getString());
                    closeQueryOutputStream();
                }

                String filePath = dereferenceAt(other);
                boolean preExists = new File(filePath).exists();
                try {
                    pwQuery = new PrintWriter(new OutputStreamWriter(
                            new FileOutputStream(filePath, true),
                            (shared.encoding == null)
                            ? DEFAULT_FILE_ENCODING : shared.encoding));
                } catch (Exception e) {
                    throw new BadSpecial(SqltoolRB.file_writefail.getString(
                            filePath), e);
                }

                
                if (htmlMode && !preExists) writeHeader(pwQuery, filePath);
                pwQuery.flush();

                return;

            case 'i' :
                enforce1charSpecial(arg1, 'i');
                if (other == null)
                    throw new BadSpecial(
                            SqltoolRB.sqlfile_name_demand.getString());
                other = other.trim();

                sqlExpandMode = null;
                try {
                    new SqlFile(this, new File(dereferenceAt(other))).execute();
                } catch (ContinueException ce) {
                    throw ce;
                } catch (BreakException be) {
                    String beMessage = be.getMessage();

                    
                    if (beMessage != null && !beMessage.equals("file")) throw be;
                } catch (QuitNow qn) {
                    throw qn;
                } catch (Exception e) {
                    throw new BadSpecial(
                            SqltoolRB.sqlfile_execute_fail.getString(other), e);
                }
                updateUserSettings();

                return;

            case 'p' :
                if (arg1.equals("pr")) {
                    if (other == null) {
                        if (shared.psStd != null) shared.psStd.println();

                        if (pwQuery != null) {
                            pwQuery.println();
                            pwQuery.flush();
                        }
                    } else {
                        shared.psStd.println(other);
                        if (pwQuery != null) {
                            pwQuery.println(other);
                            pwQuery.flush();
                        }
                    }
                    return;
                }
                enforce1charSpecial(arg1, 'p');
                if (other == null) {
                    stdprintln(true);
                } else {
                    stdprintln(other.trim(), true);
                }

                return;

            case 'l' :
                if ((arg1.equals("l?") && other == null)
                        || (arg1.equals("l") && other != null
                                && other.equals("?"))) {
                    stdprintln(SqltoolRB.log_syntax.getString());
                } else {
                    enforce1charSpecial(arg1, 'l');
                    Matcher logMatcher = ((other == null) ? null
                            : logPattern.matcher(other.trim()));
                    if (logMatcher == null || (!logMatcher.matches()))
                        throw new BadSpecial(
                                SqltoolRB.log_syntax_error.getString());
                    String levelString = logMatcher.group(1);
                    Level level = null;
                    if (levelString.equalsIgnoreCase("FINER"))
                        level = Level.FINER;
                    else if (levelString.equalsIgnoreCase("WARNING"))
                        level = Level.WARNING;
                    else if (levelString.equalsIgnoreCase("SEVERE"))
                        level = Level.SEVERE;
                    else if (levelString.equalsIgnoreCase("INFO"))
                        level = Level.INFO;
                    else if (levelString.equalsIgnoreCase("FINEST"))
                        level = Level.FINEST;
                    assert level != null:
                        "Internal assertion failed.  "
                        + " Unexpected Level string: " + levelString;
                    logger.enduserlog(level, logMatcher.group(2));
                }

                return;

            case 'a' :
                requireConnection();
                enforce1charSpecial(arg1, 'a');
                if (other != null) {
                    other = other.trim();
                    shared.jdbcConn.setAutoCommit(
                        Boolean.parseBoolean(other));
                    shared.possiblyUncommitteds = false;
                }

                stdprintln(SqltoolRB.a_setting.getString(
                        Boolean.toString(shared.jdbcConn.getAutoCommit())));

                return;
            case 'j' : try {
                enforce1charSpecial(arg1, 'j');
                String urlid = null;
                String acct = null;
                String pwd = null;
                String url = null;
                boolean goalAutoCommit = false;
                String[] tokens = (other == null)
                        ? (new String[0]) : other.trim().split("\\s+", 3);
                switch (tokens.length) {
                    case 0:
                        break;
                    case 1:
                        urlid = tokens[0];
                        break;
                    case 2:
                        acct = tokens[0];
                        pwd = "";  
                        url = tokens[1];
                        break;
                    case 3:
                        acct = tokens[0];
                        pwd = tokens[1];
                        url = tokens[2];
                        break;
                }
                if (tokens.length > 0) {
                    
                    if (shared.jdbcConn != null) try {
                        goalAutoCommit = shared.jdbcConn.getAutoCommit();
                        shared.jdbcConn.close();
                        shared.possiblyUncommitteds = false;
                        shared.jdbcConn = null;
                        stdprintln(SqltoolRB.disconnect_success.getString());
                    } catch (SQLException se) {
                        throw new BadSpecial(
                                SqltoolRB.disconnect_failure.getString(), se);
                    }
                }
                if (urlid != null || acct != null) try {
                    if (urlid != null) {
                        shared.jdbcConn = new RCData(new File(
                            SqlTool.DEFAULT_RCFILE), urlid).getConnection();
                    } else if (acct != null) {
                        shared.jdbcConn =
                                DriverManager.getConnection(url, acct, pwd);
                    }
                    shared.possiblyUncommitteds = false;
                    shared.jdbcConn.setAutoCommit(goalAutoCommit);
                } catch (Exception e) {
                    throw new BadSpecial(
                            SqltoolRB.connect_failure.getString(), e);
                }
                displayConnBanner();
            } catch (Throwable t) {
                t.printStackTrace();
                return;
            }
                return;
            case 'v' :
                requireConnection();
                enforce1charSpecial(arg1, 'v');
                if (other != null) {
                    other = other.trim();
                    if (integerPattern.matcher(other).matches()) {
                        shared.jdbcConn.setTransactionIsolation(
                                Integer.parseInt(other));
                    } else {
                        RCData.setTI(shared.jdbcConn, other);
                    }
                }

                stdprintln(SqltoolRB.transiso_report.getString(
                        (shared.jdbcConn.isReadOnly() ? "R/O " : "R/W "),
                        RCData.tiToString(
                                shared.jdbcConn.getTransactionIsolation())));

                return;
            case '=' :
                requireConnection();
                enforce1charSpecial(arg1, '=');
                shared.jdbcConn.commit();
                shared.possiblyUncommitteds = false;
                stdprintln(SqltoolRB.committed.getString());

                return;

            case 'b' :
                if (arg1.length() == 1) {
                    if (other != null)
                        throw new BadSpecial(
                                SqltoolRB.special_b_malformat.getString());
                    fetchBinary = true;

                    return;
                }

                if (arg1.charAt(1) == 'p') {
                    if (other != null)
                        throw new BadSpecial(
                                SqltoolRB.special_b_malformat.getString());
                    doPrepare = true;

                    return;
                }

                if ((arg1.charAt(1) != 'd' && arg1.charAt(1) != 'l')
                        || other == null)
                    throw new BadSpecial(
                            SqltoolRB.special_b_malformat.getString());
                other = other.trim();

                File otherFile = new File(dereferenceAt(other));

                try {
                    if (arg1.charAt(1) == 'd') {
                        dump(otherFile);
                    } else {
                        binBuffer = SqlFile.loadBinary(otherFile);
                        stdprintln(SqltoolRB.binary_loadedbytesinto.getString(
                                binBuffer.length));
                    }
                } catch (BadSpecial bs) {
                    throw bs;
                } catch (IOException ioe) {
                    throw new BadSpecial(SqltoolRB.binary_filefail.getString(
                            other), ioe);
                }

                return;

            case 't' :
                enforce1charSpecial(arg1, '=');
                if (other != null)
                    
                    reportTimes = Boolean.parseBoolean(other.trim());

                stdprintln(SqltoolRB.exectime_reporting.getString(
                        Boolean.toString(reportTimes)));
                return;

            case 'c' :
                enforce1charSpecial(arg1, '=');
                if (other != null)
                    
                    continueOnError = Boolean.parseBoolean(other.trim());

                stdprintln(SqltoolRB.c_setting.getString(
                        Boolean.toString(continueOnError)));

                return;

            case '?' :
                stdprintln(SqltoolRB.special_help.getString());

                return;

            case '!' :
                

                InputStream stream;
                byte[]      ba         = new byte[1024];
                String      extCommand = ((arg1.length() == 1)
                        ? "" : arg1.substring(1))
                    + ((arg1.length() > 1 && other != null)
                       ? " " : "") + ((other == null) ? "" : other.trim());
                if (extCommand.trim().length() < 1)
                    throw new BadSpecial(SqltoolRB.bang_incomplete.getString());

                Process proc = null;
                try {
                    Runtime runtime = Runtime.getRuntime();
                    proc = ((wincmdPattern == null)
                            ? runtime.exec(extCommand)
                            : runtime.exec(genWinArgs(extCommand))
                    );

                    proc.getOutputStream().close();

                    int i;

                    stream = proc.getInputStream();

                    while ((i = stream.read(ba)) > 0) 
                        stdprint(new String(ba, 0, i));

                    stream.close();

                    stream = proc.getErrorStream();

                    String s;
                    while ((i = stream.read(ba)) > 0) {
                        s = new String(ba, 0, i);
                        if (s.endsWith(LS)) {
                            
                            
                            if (s.length() == LS.length()) continue;
                            s = s.substring(0, s.length() - LS.length());
                        }
                        logger.severe(s);
                    }

                    stream.close();
                    stream = null;  

                    if (proc.waitFor() != 0 && !ignoreBangStatus)
                        throw new BadSpecial(
                                SqltoolRB.bang_command_fail.getString(
                                extCommand));
                } catch (BadSpecial bs) {
                    throw bs;
                } catch (Exception e) {
                    throw new BadSpecial(SqltoolRB.bang_command_fail.getString(
                            extCommand), e);
                } finally {
                    if (proc != null) proc.destroy();
                }

                return;
        }

        throw new BadSpecial(SqltoolRB.special_unknown.getString(
                Character.toString(arg1.charAt(0))));
    }

    private static final char[] nonVarChars = {
        ' ', '\t', '=', '}', '\n', '\r', '\f'
    };

    
    static int pastName(String inString, int startIndex) {
        String workString = inString.substring(startIndex);
        int    e          = inString.length();  
        int    nonVarIndex;

        for (char nonVarChar : nonVarChars) {
            nonVarIndex = workString.indexOf(nonVarChar);

            if (nonVarIndex > -1 && nonVarIndex < e) e = nonVarIndex;
        }

        return startIndex + e;
    }

    
    private String dereference(String inString,
                               boolean permitAlias) throws SqlToolError {
        if (inString.length() < 1) return inString;

        
        String       varName, varValue;
        StringBuffer expandBuffer = new StringBuffer(inString);
        int          b, e;    
        int iterations;

        if (permitAlias && inString.trim().charAt(0) == '/') {
            int slashIndex = inString.indexOf('/');

            e = SqlFile.pastName(inString.substring(slashIndex + 1), 0);

            
            if (e < 1) 
                throw new SqlToolError(SqltoolRB.plalias_malformat.getString());

            varName  = inString.substring(slashIndex + 1, slashIndex + 1 + e);
            varValue = shared.userVars.get(varName);

            if (varValue == null) 
                throw new SqlToolError(
                        SqltoolRB.plvar_undefined.getString(varName));

            expandBuffer.replace(slashIndex, slashIndex + 1 + e,
                                 shared.userVars.get(varName));
        }

        String s;
        boolean permitUnset;
        
        

        iterations = 0;
        while (true) {
            s = expandBuffer.toString();
            b = s.indexOf("${");

            if (b < 0) break; 

            e = s.indexOf('}', b + 2);

            if (e == b + 2)
                throw new SqlToolError(SqltoolRB.sysprop_empty.getString());

            if (e < 0)
                throw new SqlToolError(
                        SqltoolRB.sysprop_unterminated.getString());

            permitUnset = (s.charAt(b + 2) == ':');

            varName = s.substring(b + (permitUnset ? 3 : 2), e);
            if (iterations++ > 10000)
                throw new
                    SqlToolError(SqltoolRB.var_infinite.getString(varName));

            varValue = System.getProperty(varName);
            if (varValue == null) {
                if (permitUnset) {
                    varValue = "";
                } else {
                    throw new SqlToolError(
                            SqltoolRB.sysprop_undefined.getString(varName));
                }
            }

            expandBuffer.replace(b, e + 1, varValue);
        }

        iterations = 0;
        while (true) {
            s = expandBuffer.toString();
            b = s.indexOf("*{");

            if (b < 0) break; 

            e = s.indexOf('}', b + 2);

            if (e == b + 2)
                throw new SqlToolError(SqltoolRB.plvar_nameempty.getString());

            if (e < 0)
                throw new SqlToolError(
                        SqltoolRB.plvar_unterminated.getString());

            permitUnset = (s.charAt(b + 2) == ':');

            varName = s.substring(b + (permitUnset ? 3 : 2), e);
            if (iterations++ > 100000)
                throw new SqlToolError(
                        SqltoolRB.var_infinite.getString(varName));
            
            

            varValue = shared.userVars.get(varName);
            if (varValue == null) {  
                if (varName.equals("*TIMESTAMP")) {
                    if (timestampFormat == null)
                        throw new SqlToolError(
                                SqltoolRB.no_timestamp_format.getString());
                    varValue = timestampFormat.format(new java.util.Date());
                } else {
                    if (!permitUnset)
                        throw new SqlToolError(
                                SqltoolRB.plvar_undefined.getString(varName));
                    varValue = "";
                }
            }

            expandBuffer.replace(b, e + 1, varValue);
        }

        return expandBuffer.toString();
    }

    private Boolean sqlExpandMode;  

    
    private String  fetchingVar;
    private boolean silentFetch;
    private boolean fetchBinary;

    
    private void processBlock(Token token)
            throws BadSpecial, SqlToolError, SQLException {
        Matcher m = plPattern.matcher(dereference(token.val, false));
        if (!m.matches())
            throw new BadSpecial(SqltoolRB.pl_malformat.getString());
            
            
        if (m.groupCount() < 1 || m.group(1) == null) {
            stdprintln(SqltoolRB.deprecated_noop.getString("*"));
            return;
        }

        String[] tokens = m.group(1).split("\\s+", -1);

        if (tokens[0].equals("for")) {
            Matcher forM = forPattern.matcher(
                    dereference(token.val, false));
            if (!forM.matches())
                throw new BadSpecial(
                        SqltoolRB.pl_malformat_specific.getString("for"));
            assert forM.groupCount() == 2 || forM.groupCount() == 3:
                "Internal assertion failed.  "
                + "forh pattern matched, but captured "
                + forM.groupCount() + " groups";
            String iterableAssignmentStr = forM.group(forM.groupCount());
            String logicalExprStr = forM.group(forM.groupCount() - 1);
            String initAssignmentStr = (forM.groupCount() < 3
                    || forM.group(1) == null
                    || forM.group(1).trim().length() < 1)
                ? null : forM.group(1);

            if (initAssignmentStr != null) try {
                Matcher mathMatcher =
                        mathAsgnPattern.matcher(initAssignmentStr);
                if (mathMatcher.matches()) {
                    shared.userVars.put(mathMatcher.group(1), Long.toString(
                            Calculator.reassignValue(mathMatcher.group(1),
                            shared.userVars, mathMatcher.group(2),
                            (mathMatcher.groupCount() < 3)
                            ? null : mathMatcher.group(3))));
                } else {
                    mathMatcher = mathPattern.matcher(initAssignmentStr);
                    if (mathMatcher.matches())
                        shared.userVars.put(mathMatcher.group(1), Long.toString(
                                new Calculator(((mathMatcher.groupCount() > 1
                                && mathMatcher.group(2) != null)
                                ? mathMatcher.group(2)
                                : ""), shared.userVars).reduce(0, false)));
                }
                sqlExpandMode = null;
            } catch (RuntimeException re) {
                throw new BadSpecial(SqltoolRB.math_expr_fail.getString(re));
            }
            String[] values =
                    logicalExprStr.substring(1, logicalExprStr.length() - 1)
                    .replaceAll("!([a-zA-Z0-9*])", "! $1").
                    replaceAll("([a-zA-Z0-9*])!", "$1 !").split("\\s+", -1);

            try {
                while (eval(values)) {
                    Recursion origRecursed = recursed;
                    recursed = Recursion.FOR;
                    try {
                        scanpass(token.nestedBlock.dup());
                    } catch (ContinueException ce) {
                        String ceMessage = ce.getMessage();

                        if (ceMessage != null && !ceMessage.equals("for"))
                            throw ce;
                    } finally {
                        recursed = origRecursed;
                    }
                    try {
                        Matcher mathMatcher =
                                mathAsgnPattern.matcher(iterableAssignmentStr);
                        if (mathMatcher.matches()) {
                            shared.userVars.put(
                                    mathMatcher.group(1), Long.toString(
                                    Calculator.reassignValue(
                                    mathMatcher.group(1),
                                    shared.userVars, mathMatcher.group(2),
                                    (mathMatcher.groupCount() < 3)
                                    ? null : mathMatcher.group(3))));
                        } else {
                            mathMatcher =
                                    mathPattern.matcher(iterableAssignmentStr);
                            if (mathMatcher.matches())
                                shared.userVars.put(
                                        mathMatcher.group(1), Long.toString(
                                        new Calculator(
                                        ((mathMatcher.groupCount() > 1
                                        && mathMatcher.group(2) != null)
                                        ? mathMatcher.group(2)
                                        : ""),
                                        shared.userVars).reduce(0, false)));
                        }
                    } catch (RuntimeException re) {
                        throw new BadSpecial(
                                SqltoolRB.math_expr_fail.getString(re));
                    }
                    
                    sqlExpandMode = null;
                }
            } catch (BreakException be) {
                String beMessage = be.getMessage();

                
                if (beMessage != null && !beMessage.equals("for")) throw be;
            } catch (QuitNow qn) {
                throw qn;
            } catch (RuntimeException re) {
                throw re;  
            } catch (Exception e) {
                throw new BadSpecial(SqltoolRB.pl_block_fail.getString(), e);
            } finally {
                
                
                updateUserSettings();
                sqlExpandMode = null;
            }
            return;
        }

        if (tokens[0].equals("forrows")) {
            Matcher forrowsM = forrowsPattern.matcher(
                    dereference(token.val, false));
            if (!forrowsM.matches())
                throw new BadSpecial(
                        SqltoolRB.pl_malformat_specific.getString("forrows"));

            String[] vars = (forrowsM.groupCount() > 0
                    && forrowsM.group(1) != null
                    && forrowsM.group(1).length() > 0)
                    ? forrowsM.group(1).trim().split("\\s+") : null;
            String[] origVals = (vars == null) ? null : new String[vars.length];
            if (origVals != null) for (int i = 0; i < vars.length; i++)
                origVals[i] = shared.userVars.get(vars[i]);
            TokenList dupNesteds = token.nestedBlock.dup();
            if (dupNesteds.size() < 2)
                
                throw new BadSpecial("Empty forrows loop");
            Token queryToken = dupNesteds.remove(0);
            if (queryToken.type != Token.SQL_TYPE)
                
                throw new BadSpecial("*forrows command not followed "
                        + "immediately by an SQL statement");
            setBuf(queryToken);
            List<String[]> rowData = new ArrayList<String[]>();
            Statement statement = processSQL();
            ResultSet rs = null;
            if (statement == null)
                
                throw new BadSpecial("Failed to prepare SQL for loop");
            int colCount = 0;
            try {
                rs = statement.getResultSet();
                ResultSetMetaData rsmd = rs.getMetaData();
                colCount = rsmd.getColumnCount();
                if (vars != null && vars.length > colCount)
                    
                    throw new BadSpecial("*forrows command specifies "
                            + vars.length
                            + " variables, but query pulled only "
                            + colCount + " columns");
                if (colCount < 1) return;
                String[] rowCells;
                while (rs.next()) {
                    rowCells = new String[colCount];
                    rowData.add(rowCells);
                    for (int i = 1; i <= colCount; i++)
                        rowCells[i-1] = rs.getString(i);
                }
            } finally {
                try {
                    if (rs != null) rs.close();
                } catch (SQLException nse) {
                    
                } finally {
                    rs = null;
                }
                try {
                    if (statement != null) statement.close();
                } catch (SQLException nse) {
                    
                } finally {
                    statement = null;
                }
            }
            lastSqlStatement = null;
            

            if (rowData.size() > 0) {
                String firstVal = rowData.get(0)[0];
                String lastVal = rowData.get(rowData.size()-1)[colCount - 1];
                shared.userVars.put("?",
                        (lastVal == null) ? nullRepToken : lastVal);
                if (fetchingVar != null) {
                    if (firstVal == null)
                        shared.userVars.remove(fetchingVar);
                    else
                        shared.userVars.put(fetchingVar, firstVal);
                    updateUserSettings();
                    sqlExpandMode = null;
                    fetchingVar = null;
                }
            } else {
                shared.userVars.put("?", "");
            }
            StringBuilder rowBuilder = new StringBuilder();
            String rowVal;
            try {
                for (String[] cells : rowData) {
                    if (cells.length == 1) {
                        rowVal = (cells[0] == null) ? nullRepToken : cells[0];
                    } else {
                        rowBuilder.setLength(0);
                        for (String s : cells) {
                            if (rowBuilder.length() > 0)
                                rowBuilder.append(dsvColDelim);
                            rowBuilder.append((s == null) ? nullRepToken : s);
                        }
                        rowVal = rowBuilder.toString();
                    }
                    shared.userVars.put("*ROW", rowVal);

                    if (vars != null) for (int i = 0; i < vars.length; i++)
                        if (cells[i] == null)
                            shared.userVars.remove(vars[i]);
                        else
                            shared.userVars.put(vars[i], cells[i]);
                    updateUserSettings();

                    Recursion origRecursed = recursed;
                    recursed = Recursion.FORROWS;
                    try {
                        scanpass(dupNesteds.dup());
                    } catch (ContinueException ce) {
                        String ceMessage = ce.getMessage();

                        if (ceMessage != null
                                && !ceMessage.equals("forrows")) throw ce;
                    } finally {
                        recursed = origRecursed;
                    }
                }
            } catch (BreakException be) {
                String beMessage = be.getMessage();

                
                if (beMessage != null && !beMessage.equals("forrows")) throw be;
            } catch (QuitNow qn) {
                throw qn;
            } catch (RuntimeException re) {
                throw re;  
            } catch (Exception e) {
                throw new BadSpecial(SqltoolRB.pl_block_fail.getString(), e);
            } finally {
                shared.userVars.remove("*ROW");
                if (origVals != null) for (int i = 1; i < origVals.length; i++)
                    if (origVals[i] == null)
                        shared.userVars.remove(vars[i]);
                    else
                        shared.userVars.put(vars[i], origVals[i]);
                updateUserSettings();
                sqlExpandMode = null;
            }
            return;
        }

        if (tokens[0].equals("foreach")) {
            Matcher foreachM = foreachPattern.matcher(
                    dereference(token.val, false));
            if (!foreachM.matches())
                throw new BadSpecial(
                        SqltoolRB.pl_malformat_specific.getString("foreach"));
            if (foreachM.groupCount() != 2)
            assert foreachM.groupCount() == 2:
                "Internal assertion failed.  "
                + "foreach pattern matched, but captured "
                + foreachM.groupCount() + " groups";

            String varName   = foreachM.group(1);
            if (varName.indexOf(':') > -1)
                throw new BadSpecial(SqltoolRB.plvar_nocolon.getString());
            if (!varPattern.matcher(varName).matches())
                errprintln(SqltoolRB.varname_warning.getString(varName));
            String[] values = foreachM.group(2).split("\\s+", -1);

            String origval = shared.userVars.get(varName);

            try {
                for (String val : values) {
                    
                    shared.userVars.put(varName, val);
                    updateUserSettings();

                    Recursion origRecursed = recursed;
                    recursed = Recursion.FOREACH;
                    try {
                        scanpass(token.nestedBlock.dup());
                    } catch (ContinueException ce) {
                        String ceMessage = ce.getMessage();

                        if (ceMessage != null
                                && !ceMessage.equals("foreach")) throw ce;
                    } finally {
                        recursed = origRecursed;
                    }
                }
            } catch (BreakException be) {
                String beMessage = be.getMessage();

                
                if (beMessage != null && !beMessage.equals("foreach")) throw be;
            } catch (QuitNow qn) {
                throw qn;
            } catch (RuntimeException re) {
                throw re;  
            } catch (Exception e) {
                throw new BadSpecial(SqltoolRB.pl_block_fail.getString(), e);
            } finally {
                if (origval == null) {
                    shared.userVars.remove(varName);
                } else {
                    shared.userVars.put(varName, origval);
                }
                updateUserSettings();
                sqlExpandMode = null;
            }

            return;
        }

        if (tokens[0].equals("if") || tokens[0].equals("while")) {
            Matcher ifwhileM= ifwhilePattern.matcher(
                    dereference(token.val, false));
            if (!ifwhileM.matches())
                throw new BadSpecial(SqltoolRB.ifwhile_malformat.getString());
            assert ifwhileM.groupCount() == 1:
                "Internal assertion failed.  "
                + "if/while pattern matched, but captured "
                + ifwhileM.groupCount() + " groups";

            String[] values =
                    ifwhileM.group(1).replaceAll("!([a-zA-Z0-9*])", "! $1").
                        replaceAll("([a-zA-Z0-9*])!", "$1 !").split("\\s+", -1);

            if (tokens[0].equals("if")) {
                try {
                    
                    Token elseToken = (token.nestedBlock.size() < 1)
                            ? null
                            : token.nestedBlock.get(
                                    token.nestedBlock.size() - 1);
                    if (elseToken != null && (elseToken.type != Token.PL_TYPE ||
                            !elseToken.val.equals("else"))) elseToken = null;
                    
                        
                    Token recurseToken = eval(values) ? token : elseToken;
                    if (recurseToken != null) {
                        Recursion origRecursed = recursed;
                        recursed = Recursion.IF;
                        try {
                            scanpass(recurseToken.nestedBlock.dup());
                        } finally {
                            recursed = origRecursed;
                        }
                    }
                } catch (BreakException be) {
                    String beMessage = be.getMessage();

                    
                    if (beMessage == null || !beMessage.equals("if")) throw be;
                } catch (ContinueException ce) {
                    throw ce;
                } catch (QuitNow qn) {
                    throw qn;
                } catch (BadSpecial bs) {
                    bs.appendMessage(
                            SqltoolRB.pl_malformat_specific.getString("if"));
                    throw bs;
                } catch (RuntimeException re) {
                    throw re;  
                } catch (Exception e) {
                    throw new BadSpecial(
                        SqltoolRB.pl_block_fail.getString(), e);
                }
            } else if (tokens[0].equals("while")) {
                try {

                    while (eval(values)) {
                        Recursion origRecursed = recursed;
                        recursed = Recursion.WHILE;
                        try {
                            scanpass(token.nestedBlock.dup());
                        } catch (ContinueException ce) {
                            String ceMessage = ce.getMessage();

                            if (ceMessage != null && !ceMessage.equals("while"))
                                throw ce;
                        } finally {
                            recursed = origRecursed;
                        }
                    }
                } catch (BreakException be) {
                    String beMessage = be.getMessage();

                    
                    if (beMessage != null && !beMessage.equals("while")) 
                        throw be;
                } catch (QuitNow qn) {
                    throw qn;
                } catch (BadSpecial bs) {
                    bs.appendMessage(
                            SqltoolRB.pl_malformat_specific.getString("while"));
                    throw bs;
                } catch (RuntimeException re) {
                    throw re;  
                } catch (Exception e) {
                    throw new BadSpecial(
                            SqltoolRB.pl_block_fail.getString(), e);
                }
            } else {
                assert false: SqltoolRB.pl_unknown.getString(tokens[0]);
            }

            return;
        }

        throw new BadSpecial(SqltoolRB.pl_unknown.getString(tokens[0]));
    }

    
    private void processPL() throws BadSpecial, SqlToolError {
        String string = buffer.val;
        String dereffed = dereference(string, false);

        Matcher mathMatcher = mathAsgnPattern.matcher(dereffed);
        if (mathMatcher.matches()) try {
            shared.userVars.put(mathMatcher.group(1), Long.toString(
                    Calculator.reassignValue(mathMatcher.group(1),
                    shared.userVars, mathMatcher.group(2),
                    (mathMatcher.groupCount() < 3)
                    ? null : mathMatcher.group(3))));
            
            sqlExpandMode = null;
            return;
        } catch (RuntimeException re) {
            throw new BadSpecial(SqltoolRB.math_expr_fail.getString(re));
        }
        mathMatcher = mathPattern.matcher(dereffed);
        if (mathMatcher.matches()) try {
            shared.userVars.put(mathMatcher.group(1), Long.toString(
                    new Calculator(((mathMatcher.groupCount() > 1
                    && mathMatcher.group(2) != null) ? mathMatcher.group(2)
                    : ""), shared.userVars).reduce(0, false)));
            
            sqlExpandMode = null;
            return;
        } catch (RuntimeException re) {
            throw new BadSpecial(SqltoolRB.math_expr_fail.getString(re));
        }

        Matcher m = plPattern.matcher(dereffed);
        if (!m.matches())
            throw new BadSpecial(SqltoolRB.pl_malformat.getString());
            
            
        if (m.groupCount() < 1 || m.group(1) == null) {
            stdprintln(SqltoolRB.deprecated_noop.getString("*"));
            return;
        }
        String[] tokens = m.group(1).split("\\s+", -1);

        if (tokens[0].charAt(0) == '?') {
            String remainder = tokens[0].substring(1);
            String msg = null;
            if (remainder.startsWith("assign") ||
                    (tokens.length > 1 && tokens[1].startsWith("assign")))
                msg = SqltoolRB.pl_assign.getString();
            else if (remainder.equals("control") ||
                    (tokens.length > 1 && tokens[1].equals("control")))
                msg = SqltoolRB.pl_control.getString();
            else
                msg = SqltoolRB.pl_help.getString();

            stdprintln(msg);

            return;
        }

        if (tokens[0].equals("else")) {
            if (recursed != Recursion.IF)
                throw new BadSpecial(SqltoolRB.else_without_if.getString());
            return;
        }

        if (tokens[0].equals("end"))
            throw new BadSpecial(SqltoolRB.end_noblock.getString());

        if (tokens[0].equals("continue")) {
            if (tokens.length > 1) {
                if (tokens.length == 2 &&
                        (tokens[1].equals("foreach")
                        || tokens[1].equals("forrows")
                        || tokens[1].equals("for")
                        || tokens[1].equals("while")))
                    throw new ContinueException(tokens[1]);
                throw new BadSpecial(SqltoolRB.continue_syntax.getString());
            }

            throw new ContinueException();
        }

        if (tokens[0].equals("return")) {
            if (tokens.length > 1)
                throw new BadSpecial(SqltoolRB.break_syntax.getString());
            throw new BreakException("file");
        }

        if (tokens[0].equals("break")) {
            if (tokens.length > 1) {
                if (tokens.length == 2 &&
                        (tokens[1].equals("foreach")
                        || tokens[1].equals("forrows")
                        || tokens[1].equals("while")
                        || tokens[1].equals("for")
                        || tokens[1].equals("file")))
                    throw new BreakException(tokens[1]);
                throw new BadSpecial(SqltoolRB.break_syntax.getString());
            }

            throw new BreakException();
        }

        if (tokens[0].equals("list") || tokens[0].equals("listvalues")
                || tokens[0].equals("listsysprops")) {
            boolean sysProps =tokens[0].equals("listsysprops");
            String  s;
            boolean doValues = (tokens[0].equals("listvalues") || sysProps);
            
            

            if (tokens.length == 1) {
                stdprint(formatNicely(
                        (sysProps ? System.getProperties() : shared.userVars),
                        doValues));
            } else {
                if (doValues) {
                    stdprintln(SqltoolRB.pl_list_parens.getString());
                } else {
                    stdprintln(SqltoolRB.pl_list_lengths.getString());
                }

                for (String token : tokens) {
                    s = (String) (sysProps ? System.getProperties()
                                           : shared.userVars).get(token);
                    if (s == null) continue;
                    stdprintln("    " + token + ": "
                               + (doValues ? ("(" + s + ')')
                                           : Integer.toString(s.length())));
                }
            }

            return;
        }

        if (tokens[0].equals("dump") || tokens[0].equals("load")) {
            if (tokens.length != 3)
                throw new BadSpecial(SqltoolRB.dumpload_malformat.getString());

            String varName = tokens[1];

            if (varName.indexOf(':') > -1)
                throw new BadSpecial(SqltoolRB.plvar_nocolon.getString());
            File   dlFile    = new File(dereferenceAt(tokens[2]));

            try {
                if (tokens[0].equals("dump")) {
                    dump(varName, dlFile);
                } else {
                    load(varName, dlFile, shared.encoding);
                }
            } catch (IOException ioe) {
                throw new BadSpecial(SqltoolRB.dumpload_fail.getString(
                        varName, dlFile.toString()), ioe);
            }

            return;
        }

        if (tokens[0].equals("prepare")) {
            if (tokens.length != 2)
                throw new BadSpecial(
                        SqltoolRB.pl_malformat_specific.getString("prepare"));

            if (shared.userVars.get(tokens[1]) == null)
                throw new BadSpecial(
                        SqltoolRB.plvar_undefined.getString(tokens[1]));

            prepareVar = tokens[1];
            doPrepare  = true;

            return;
        }

        if (tokens[0].equals("-")) {
            
            
            if (tokens.length != 2)
                throw new BadSpecial(
                        SqltoolRB.pl_unset_nomoreargs.getString());
            if (fetchingVar != null && fetchingVar.equals(tokens[1]))
                fetchingVar = null;

            if (tokens[1].equals("*ENCODING")) try {
                
                
                
                setEncoding(m.group(3));
                return;
            } catch (UnsupportedEncodingException use) {
                
                throw new BadSpecial(
                        SqltoolRB.encode_fail.getString(m.group(3)));
            }
            shared.userVars.remove(tokens[1]);
            updateUserSettings();
            sqlExpandMode = null;

            return;
        }

        String derefed = dereference(string, false);
        m = varsetPattern.matcher(derefed);
        if (!m.matches())
            throw new BadSpecial(SqltoolRB.pl_unknown.getString(tokens[0]));
        assert m.groupCount() > 1 && m.groupCount() < 4:
            "varset pattern matched but captured " + m.groupCount() + " groups";

        String varName  = m.group(1);
        
        
        if (derefed.trim().equals(varName + '_'))
            throw new BadSpecial(SqltoolRB.pl_unknown.getString(tokens[0]));

        if (varName.indexOf(':') > -1)
            throw new BadSpecial(SqltoolRB.plvar_nocolon.getString());
        if (!varPattern.matcher(varName).matches())
            errprintln(SqltoolRB.varname_warning.getString(varName));

        switch (m.group(2).charAt(0)) {
            case ':' :
                if (prevToken == null) throw new BadSpecial(nobufferYetString);
                StringBuilder sb = new StringBuilder();
                switch (prevToken.type) {
                    case Token.PL_TYPE:
                        sb.append('*');
                        break;
                    case Token.SPECIAL_TYPE:
                        sb.append('\\');
                        break;
                    default:
                        
                }
                sb.append(prevToken.val);
                if (m.groupCount() > 2 && m.group(3) != null)
                    sb.append(m.group(3));
                shared.userVars.put(varName, sb.toString());
                updateUserSettings();
                sqlExpandMode = null;

                return;

            case '_' :
                silentFetch = true;
            case '~' :
                
                
                
                if (m.groupCount() > 2 && m.group(3) != null
                            && m.group(3).trim().length() > 0) {
                    throw new BadSpecial(
                            SqltoolRB.plvar_tildedash_nomoreargs.getString(
                            m.group(3).trim()));
                }

                shared.userVars.remove(varName);
                updateUserSettings();
                sqlExpandMode = null;

                fetchingVar = varName;

                return;

            case '=' :
                if (fetchingVar != null && fetchingVar.equals(varName))
                    fetchingVar = null;
                String varVal = (m.groupCount() > 2 && m.group(3) != null)
                        ? m.group(3).replaceFirst("^\\s+", "") : null;
                if (varVal != null && varVal.length() < 1) varVal = null;

                if (varName.equals("*ENCODING")) try {
                    
                    
                    
                    setEncoding(varVal.trim());
                    return;
                } catch (UnsupportedEncodingException use) {
                    throw new BadSpecial(
                            SqltoolRB.encode_fail.getString(varVal));
                }
                if (varVal == null) {
                    if (removeEmptyVars()) {
                        stdprintln(SqltoolRB.
                                remove_empty_vars_suggestset.getString());
                        shared.userVars.remove(varName);
                    } else {
                        shared.userVars.put(varName, "");
                    }
                } else {
                    shared.userVars.put(varName, varVal);
                }
                updateUserSettings();
                sqlExpandMode = null;

                return;

        }

        throw new BadSpecial(SqltoolRB.pl_unknown.getString(tokens[0]));
        
    }

    
    

    private void stdprint(String s) {
        stdprint(s, false);
    }

    private void stdprintln(String s) {
        stdprintln(s, false);
    }

    
    private void stdprintln(boolean queryOutput) {
        if (shared.psStd != null) {
            if (htmlMode) {
                shared.psStd.println("<BR>");
            } else {
                shared.psStd.println();
            }
        }

        if (queryOutput && pwQuery != null) {
            if (htmlMode) {
                pwQuery.println("<BR>");
            } else {
                pwQuery.println();
            }

            pwQuery.flush();
        }
    }

    
    private void errprintln(String s) {
        if (pwQuery != null && htmlMode) {
            pwQuery.println("<DIV class=\"sqltool-error\"><CODE>"
                    + SqlFile.escapeHtml(s) + "</CODE></DIV>");
            pwQuery.flush();
        }
        if (shared.psStd != null && htmlMode) {
            shared.psStd.println("<DIV class=\"sqltool-error\"><CODE>"
                    + SqlFile.escapeHtml(s) + "</CODE></DIV>");
        } else {
            logger.privlog(Level.SEVERE, s, null, 4, SqlFile.class);
            
        }
    }

    
    private void stdprint(String s, boolean queryOutput) {
        if (shared.psStd != null)
            shared.psStd.print(
                    htmlMode ? ("<P>" + SqlFile.escapeHtml(s) + "</P>") : s);

        if (queryOutput && pwQuery != null) {
            pwQuery.print(
                    htmlMode ? ("<P>" + SqlFile.escapeHtml(s) + "</P>") : s);
            pwQuery.flush();
        }
    }

    
    private void stdprintln(String s, boolean queryOutput) {
        shared.psStd.println(
                htmlMode ? ("<P>" + SqlFile.escapeHtml(s) + "</P>") : s);

        if (queryOutput && pwQuery != null) {
            pwQuery.println(
                    htmlMode ? ("<P>" + SqlFile.escapeHtml(s) + "</P>") : s);
            pwQuery.flush();
        }
    }

    
    
    
    private static final String DEFAULT_NULL_REP = "[null]";
    private static final String DEFAULT_NULL_HTML = "&Oslash;";
    private static final String DEFAULT_ROW_DELIM = LS;
    private static final String DEFAULT_ROW_SPLITTER = "\\r\\n|\\r|\\n";
    private static final String DEFAULT_COL_DELIM = "|";
    private static final String DEFAULT_COL_SPLITTER = "\\|";
    private static final String DEFAULT_SKIP_PREFIX = "#";
    private static final int    DEFAULT_ELEMENT   = 0,
                                HSQLDB_ELEMENT    = 1,
                                ORACLE_ELEMENT    = 2
    ;

    
    private static final int[] listMDSchemaCols = { 1 };
    private static final int[] listMDIndexCols  = {
        2, 6, 3, 9, 4, 10, 11
    };

    
    private static final int[][] listMDTableCols = {
        {
            2, 3
        },    
        {
            2, 3
        },    
        {
            2, 3
        },    
    };

    
    private static final String[] oracleSysSchemas = {
        "SYS", "SYSTEM", "OUTLN", "DBSNMP", "OUTLN", "MDSYS", "ORDSYS",
        "ORDPLUGINS", "CTXSYS", "DSSYS", "PERFSTAT", "WKPROXY", "WKSYS",
        "WMSYS", "XDB", "ANONYMOUS", "ODM", "ODM_MTR", "OLAPSYS", "TRACESVR",
        "REPADMIN"
    };

    public String getCurrentSchema() throws BadSpecial, SqlToolError {
        requireConnection();
        Statement st = null;
        ResultSet rs = null;
        try {
            st = shared.jdbcConn.createStatement();
            rs = st.executeQuery("VALUES CURRENT_SCHEMA");
            if (!rs.next())
                throw new BadSpecial(SqltoolRB.no_vendor_schemaspt.getString());
            String currentSchema = rs.getString(1);
            if (currentSchema == null)
                throw new BadSpecial(
                        SqltoolRB.schemaname_retrieval_fail.getString());
            return currentSchema;
        } catch (SQLException se) {
            throw new BadSpecial(SqltoolRB.no_vendor_schemaspt.getString());
        } finally {
            if (rs != null) try {
                rs.close();
            } catch (SQLException se) {
                
            } finally {
                rs = null;
            }
            if (st != null) try {
                st.close();
            } catch (SQLException se) {
                
            } finally {
                st = null;
            }
        }
    }

    
    private void listTables(char c, String inFilter) throws BadSpecial,
            SqlToolError {
        requireConnection();
        String   schema  = null;
        int[]    listSet = null;
        String[] types   = null;

        
        String[] additionalSchemas = null;

        
        Statement statement = null;
        ResultSet rs        = null;
        String    narrower  = "";
        
        String filter = inFilter;

        try {
            DatabaseMetaData md            = shared.jdbcConn.getMetaData();
            String           dbProductName = md.getDatabaseProductName();
            int              majorVersion  = 0;
            int              minorVersion  = 0;

            
            
            if (dbProductName.indexOf("HSQL") > -1) try {
                majorVersion  = md.getDatabaseMajorVersion();
                minorVersion  = md.getDatabaseMinorVersion();
            } catch (UnsupportedOperationException uoe) {
                
                majorVersion = 2;
                minorVersion = 0;
            }

            
            

            
            types = new String[1];

            switch (c) {
                case '*' :
                    types = null;
                    break;

                case 'S' :
                    if (dbProductName.indexOf("Oracle") > -1) {
                        errprintln(SqltoolRB.vendor_oracle_dS.getString());

                        types[0]          = "TABLE";
                        schema            = "SYS";
                        additionalSchemas = oracleSysSchemas;
                    } else {
                        types[0] = "SYSTEM TABLE";
                    }
                    break;

                case 's' :
                    if (dbProductName.indexOf("HSQL") > -1) {
                        
                        
                        
                        if (filter != null) {
                            Matcher matcher = dotPattern.matcher(filter);
                            if (matcher.matches()) {
                                filter = (matcher.group(2).length() > 0)
                                        ? matcher.group(2) : null;
                                narrower = "\nWHERE sequence_schema = '"
                                        + ((matcher.group(1).length() > 0)
                                                ? matcher.group(1)
                                                : getCurrentSchema()) + "'";
                            }
                        }

                        statement = shared.jdbcConn.createStatement();

                        statement.execute(
                            "SELECT sequence_schema, sequence_name FROM "
                            + "information_schema."
                            + ((minorVersion> 8 || majorVersion > 1)
                            ? "sequences" : "system_sequences") + narrower);
                    } else {
                        types[0] = "SEQUENCE";
                    }
                    break;

                case 'r' :
                    if (dbProductName.indexOf("HSQL") > -1) {
                        statement = shared.jdbcConn.createStatement();

                        statement.execute(
                            "SELECT authorization_name FROM information_schema."
                            + ((minorVersion> 8 || majorVersion > 1)
                            ? "authorizations" : "system_authorizations")
                            + "\nWHERE authorization_type = 'ROLE'\n"
                            + "ORDER BY authorization_name");
                    } else if (dbProductName.indexOf(
                            "Adaptive Server Enterprise") > -1) {
                        
                        
                        
                        
                        statement = shared.jdbcConn.createStatement();

                        statement.execute(
                            "SELECT name FROM syssrvroles ORDER BY name");
                    } else if (dbProductName.indexOf(
                            "Apache Derby") > -1) {
                        throw new BadSpecial(
                            SqltoolRB.vendor_derby_dr.getString());
                    } else {
                        throw new BadSpecial(
                            SqltoolRB.vendor_nosup_d.getString("r"));
                    }
                    break;

                case 'u' :
                    if (dbProductName.indexOf("HSQL") > -1) {
                        statement = shared.jdbcConn.createStatement();

                        statement.execute("SELECT "
                            + ((minorVersion> 8 || majorVersion > 1)
                            ? "user_name" : "user") + ", admin FROM "
                            + "information_schema.system_users\n"
                            + "ORDER BY user_name");
                    } else if (dbProductName.indexOf("Oracle") > -1) {
                        statement = shared.jdbcConn.createStatement();

                        statement.execute(
                            "SELECT username, created FROM all_users "
                            + "ORDER BY username");
                    } else if (dbProductName.indexOf("PostgreSQL") > -1) {
                        statement = shared.jdbcConn.createStatement();

                        statement.execute(
                            "SELECT usename, usesuper FROM pg_catalog.pg_user "
                            + "ORDER BY usename");
                    } else if (dbProductName.indexOf(
                            "Adaptive Server Enterprise") > -1) {
                        
                        
                        
                        
                        statement = shared.jdbcConn.createStatement();

                        statement.execute(
                            "SELECT name, accdate, fullname FROM syslogins "
                            + "ORDER BY name");
                    } else if (dbProductName.indexOf(
                            "Apache Derby") > -1) {
                        throw new BadSpecial(
                            SqltoolRB.vendor_derby_du.getString());
                    } else {
                        throw new BadSpecial(
                            SqltoolRB.vendor_nosup_d.getString("u"));
                    }
                    break;

                case 'a' :
                    if (dbProductName.indexOf("HSQL") > -1
                        && (minorVersion < 9 && majorVersion < 2)) {
                        
                        
                        
                        if (filter != null) {
                            Matcher matcher = dotPattern.matcher(filter);
                            if (matcher.matches()) {
                                filter = (matcher.group(2).length() > 0)
                                        ? matcher.group(2) : null;
                                narrower = "\nWHERE alias_schema = '"
                                        + ((matcher.group(1).length() > 0)
                                                ? matcher.group(1)
                                                : getCurrentSchema()) + "'";
                            }
                        }

                        statement = shared.jdbcConn.createStatement();

                        statement.execute(
                            "SELECT alias_schem, alias FROM "
                            + "information_schema.system_aliases" + narrower);
                    } else {
                        types[0] = "ALIAS";
                    }
                    break;

                case 't' :
                    excludeSysSchemas = (dbProductName.indexOf("Oracle")
                                         > -1);
                    types[0] = "TABLE";
                    break;

                case 'v' :
                    types[0] = "VIEW";
                    break;

                case 'c' :
                    rs = md.getCatalogs();

                    if (rs == null)
                        throw new BadSpecial(
                            SqltoolRB.metadata_fetch_fail.getString());

                    displaySqlResults(null, rs, listMDSchemaCols, filter, false);

                    return;

                case 'n' :
                    rs = md.getSchemas();

                    if (rs == null)
                        throw new BadSpecial(
                            SqltoolRB.metadata_fetch_fail.getString());

                    displaySqlResults(null, rs, listMDSchemaCols, filter, false);

                    return;

                case 'i' :

                    
                    
                    String table = null;

                    if (filter != null) {
                        Matcher matcher = dotPattern.matcher(filter);
                        if (matcher.matches()) {
                            table = (matcher.group(2).length() > 0)
                                    ? matcher.group(2) : null;
                            schema = (matcher.group(1).length() > 0)
                                    ? matcher.group(1) : getCurrentSchema();
                        } else {
                            table = filter;
                        }
                        filter = null;
                    }

                    
                    
                    
                    
                    rs = md.getIndexInfo(null, schema, table, false, true);

                    if (rs == null)
                        throw new BadSpecial(
                            SqltoolRB.metadata_fetch_fail.getString());

                    displaySqlResults(null, rs, listMDIndexCols, null, false);

                    return;

                default :
                    throw new BadSpecial(SqltoolRB.special_d_unknown.getString(
                            Character.toString(c)) + LS + D_OPTIONS_TEXT);
            }

            if (statement == null) {
                if (dbProductName.indexOf("HSQL") > -1) {
                    listSet = listMDTableCols[HSQLDB_ELEMENT];
                } else if (dbProductName.indexOf("Oracle") > -1) {
                    listSet = listMDTableCols[ORACLE_ELEMENT];
                } else {
                    listSet = listMDTableCols[DEFAULT_ELEMENT];
                }


                if (schema == null && filter != null) {
                    Matcher matcher = dotPattern.matcher(filter);
                    if (matcher.matches()) {
                        filter = (matcher.group(2).length() > 0)
                                ? matcher.group(2) : null;
                        schema = (matcher.group(1).length() > 0)
                                ? matcher.group(1)
                                : getCurrentSchema();
                    }
                }
            }

            rs = ((statement == null)
                  ? md.getTables(null, schema, null, types)
                  : statement.getResultSet());

            if (rs == null)
                throw new BadSpecial(SqltoolRB.metadata_fetch_fail.getString());

            displaySqlResults(statement, rs, listSet, filter, false);

            if (additionalSchemas != null) {
                for (String additionalSchema : additionalSchemas) {
                    
                    rs = md.getTables(null, additionalSchema, null,
                                      types);

                    if (rs == null)
                        throw new BadSpecial(
                                SqltoolRB.metadata_fetch_failfor.getString(
                                additionalSchema));

                    if (!rs.next()) continue;

                    displaySqlResults(
                        null,
                        md.getTables(null, additionalSchema, null, types),
                        listSet, filter, false);
                }
            }
        } catch (SQLException se) {
            throw new BadSpecial(SqltoolRB.metadata_fetch_fail.getString(), se);
        } catch (NullPointerException npe) {
            throw new BadSpecial(SqltoolRB.metadata_fetch_fail.getString(),
                    npe);
        } finally {
            excludeSysSchemas = false;

            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException se) {
                    
                    
                } finally {
                    rs = null;
                }
            }

            if (statement != null) try {
                statement.close();
            } catch (SQLException se) {
                
            } finally {
                statement = null;
            }
        }
    }

    private boolean excludeSysSchemas;

    
    private Statement processSQL() throws SQLException, SqlToolError {
        shared.userVars.remove("?");
        requireConnection();
        assert buffer != null:
            "Internal assertion failed.  No buffer in processSQL().";
        assert buffer.type == Token.SQL_TYPE:
            "Internal assertion failed.  "
            + "Token type " + buffer.getTypeString()
            + " in processSQL().";
        
        
        if (sqlExpandMode == null) setSqlExpandMode();
        lastSqlStatement = sqlExpandMode.booleanValue()
                           ? dereference(buffer.val, true) : buffer.val;
        
        
        
        
        
        if ((!permitEmptySqlStatements) && buffer.val == null
                || buffer.val.trim().length() < 1)
            throw new SqlToolError(SqltoolRB.sqlstatement_empty.getString());
            
            
            
            
        Statement statement = null;

        long startTime = 0;
        if (reportTimes) startTime = (new java.util.Date()).getTime();
        try {
            if (doPrepare) {
                if (lastSqlStatement.indexOf('?') < 1) {
                    lastSqlStatement = null;
                    throw new SqlToolError(SqltoolRB.prepare_demandqm.getString());
                }

                doPrepare = false;

                PreparedStatement ps =
                        shared.jdbcConn.prepareStatement(lastSqlStatement);
                statement = ps;

                if (prepareVar == null) {
                    if (binBuffer == null) {
                        lastSqlStatement = null;
                        throw new SqlToolError(
                                SqltoolRB.binbuffer_empty.getString());
                    }

                    ps.setBytes(1, binBuffer);
                } else {
                    String val = shared.userVars.get(prepareVar);

                    if (val == null) {
                        lastSqlStatement = null;
                        throw new SqlToolError(
                                SqltoolRB.plvar_undefined.getString(prepareVar));
                    }

                    prepareVar = null;

                    ps.setString(1, val);
                }

                ps.executeUpdate();
            } else {
                statement = shared.jdbcConn.createStatement();

                statement.execute(lastSqlStatement);
            }
        
        
        
        
        } catch (SQLException se) {
            if (statement != null) try {
                statement.close();
            } catch (SQLException sen) {
                
            } finally {
                statement = null;
            }
            throw se; 
        } catch (SqlToolError ste) {
            if (statement != null) try {
                statement.close();
            } catch (SQLException sen) {
                
            } finally {
                statement = null;
            }
            throw ste; 
        } finally {
            if (reportTimes) {
                long elapsed = (new java.util.Date().getTime()) - startTime;
                
                condlPrintln(SqltoolRB.exectime_report.getString(
                        (int) elapsed), false);
            }
        }

        
        try {
            shared.possiblyUncommitteds = !shared.jdbcConn.getAutoCommit()
                    && !commitOccursPattern.matcher(lastSqlStatement).matches();
        } catch (java.sql.SQLException se) {
            
            
            
            
            
            lastSqlStatement = null; 
            try {
                shared.jdbcConn.close();
            } catch (Exception anye) {
                
            }
            shared.jdbcConn = null;
            shared.possiblyUncommitteds = false;
            stdprintln(SqltoolRB.disconnect_success.getString());
            return null;
        }
        return statement;
    }

    
    private void displaySqlResults(Statement statement, ResultSet r,
                                  int[] incCols,
                                  String filterString,
                                  boolean updateStatus) throws SQLException,
                                  SqlToolError {
        try {
        if (pwDsv != null && csvStyleQuoting && (dsvColDelim.indexOf('"') > -1
                || dsvRowDelim.indexOf('"') > -1))
            throw new SqlToolError(SqltoolRB.dsv_q_nodblquote.getString());
        java.sql.Timestamp ts;
        int dotAt;
        int                updateCount = (statement == null) ? -1
                                                             : statement
                                                                 .getUpdateCount();
        boolean            silent      = silentFetch;
        boolean            binary      = fetchBinary;
        Pattern            filter = null;

        silentFetch = false;
        fetchBinary = false;

        if (filterString != null) try {
            filter = Pattern.compile(filterString);
        } catch (PatternSyntaxException pse) {
            throw new SqlToolError(SqltoolRB.regex_malformat.getString(pse));
        }

        if (excludeSysSchemas)
            stdprintln(SqltoolRB.vendor_nosup_sysschemas.getString());

        switch (updateCount) {
            case -1 :
                if (r == null) {
                    stdprintln(SqltoolRB.noresult.getString(), true);

                    break;
                }

                ResultSetMetaData m        = r.getMetaData();
                int               cols     = m.getColumnCount();
                int               incCount = (incCols == null) ? cols
                                                               : incCols
                                                                   .length;
                String            val;
                List<String[]>    rows        = new ArrayList<String[]>();
                String[]          headerArray = null;
                String[]          fieldArray;
                int[]             maxWidth = new int[incCount];
                int               insi;
                boolean           skip;
                boolean           isValNull;

                
                if (!htmlMode) for (int i = 0; i < maxWidth.length; i++)
                    maxWidth[i] = 0;

                boolean[] rightJust = new boolean[incCount];
                int[]     dataType  = new int[incCount];
                boolean[] autonulls = new boolean[incCount];

                insi        = -1;
                headerArray = new String[incCount];

                for (int i = 1; i <= cols; i++) {
                    if (incCols != null) {
                        skip = true;

                        for (int j = 0; j < incCols.length; j++)
                            if (i == incCols[j]) skip = false;

                        if (skip) continue;
                    }

                    headerArray[++insi] = (pwDsv != null && csvStyleQuoting
                            && allQuoted) ? ('"' + m.getColumnLabel(i) + '"')
                            : m.getColumnLabel(i);
                    dataType[insi]      = m.getColumnType(i);
                    rightJust[insi]     = false;
                    autonulls[insi]     = true;
                    

                    switch (dataType[insi]) {
                        case java.sql.Types.BIGINT :
                        case java.sql.Types.BIT :
                        case java.sql.Types.DECIMAL :
                        case java.sql.Types.DOUBLE :
                        case java.sql.Types.FLOAT :
                        case java.sql.Types.INTEGER :
                        case java.sql.Types.NUMERIC :
                        case java.sql.Types.REAL :
                        case java.sql.Types.SMALLINT :
                        case java.sql.Types.TINYINT :
                            rightJust[insi] = true;
                            break;

                        case java.sql.Types.VARBINARY :
                        case java.sql.Types.VARCHAR :
                        case java.sql.Types.BLOB :
                        case java.sql.Types.CLOB :
                        case java.sql.Types.LONGVARBINARY :
                        case java.sql.Types.LONGVARCHAR :
                            autonulls[insi] = false;
                            break;
                    }

                    if (htmlMode) continue;

                    if (headerArray[insi] != null
                            && headerArray[insi].length() > maxWidth[insi])
                        maxWidth[insi] = headerArray[insi].length();
                }

                boolean filteredOut;

                while (r.next()) {
                    fieldArray  = new String[incCount];
                    insi        = -1;
                    filteredOut = filter != null;

                    for (int i = 1; i <= cols; i++) {
                        
                        
                        if (incCols != null) {
                            skip = true;

                            for (int incCol : incCols)
                                if (i == incCol) skip = false;

                            if (skip) continue;
                        }

                        
                        
                        
                        ++insi;

                        if (!SqlFile.canDisplayType(dataType[insi]))
                            binary = true;

                        val = null;
                        isValNull = true;

                        if (!binary) {
                            
                            switch (dataType[insi]) {
                                case org.hsqldb.types.Types.SQL_TIMESTAMP_WITH_TIME_ZONE:
                                case org.hsqldb.types.Types.SQL_TIME_WITH_TIME_ZONE:
                                case java.sql.Types.TIMESTAMP:
                                case java.sql.Types.DATE:
                                case java.sql.Types.TIME:
                                    ts  = r.getTimestamp(i);
                                    isValNull = r.wasNull();
                                    val = ((ts == null) ? null : ts.toString());
                                    
                                    
                                    
                                    if (dataType[insi]
                                            != java.sql.Types.TIMESTAMP
                                            && dataType[insi]
                                            != org.hsqldb.types.Types.SQL_TIMESTAMP_WITH_TIME_ZONE
                                            && val != null) {
                                        dotAt = val.lastIndexOf('.');
                                        for (int z = dotAt + 1;
                                                z < val.length(); z++)
                                            if (val.charAt(z) != '0') {
                                                dotAt = 0;
                                                break;
                                            }
                                        if (dotAt > 1)
                                            val = val.substring(0, dotAt);
                                    }
                                    break;
                                default:
                                    val = r.getString(i);
                                    isValNull = r.wasNull();

                                    
                                    
                                    
                                    if (val == null) try {
                                        val = streamToString(
                                            r.getAsciiStream(i),
                                            shared.encoding);
                                        isValNull = r.wasNull();
                                    } catch (Exception e) {
                                        
                                        
                                        
                                        
                                    }
                            }
                        }

                        if (binary || (val == null && !isValNull)) {
                            if (pwDsv != null)
                                throw new SqlToolError(
                                        SqltoolRB.dsv_bincol.getString());

                            
                            
                            try {
                                binBuffer =
                                    SqlFile.streamToBytes(r.getBinaryStream(i));
                                isValNull = r.wasNull();
                            } catch (IOException ioe) {
                                throw new SqlToolError(
                                    SqltoolRB.streamread_failure.getString(),
                                    ioe);
                            }

                            stdprintln(SqltoolRB.binbuf_write.getString(
                                       Integer.toString(binBuffer.length),
                                       headerArray[insi],
                                       SqlFile.sqlTypeToString(dataType[insi])
                                    ));

                            if (updateStatus)
                                shared.userVars.put("?", "");
                            if (fetchingVar != null) {
                                shared.userVars.put(fetchingVar, "");
                                updateUserSettings();
                                sqlExpandMode = null;

                                fetchingVar = null;
                            }
                            lastSqlStatement = null;
                            return;
                        }

                        if (excludeSysSchemas && val != null && i == 2)
                            for (String oracleSysSchema : oracleSysSchemas)
                                if (val.equals(oracleSysSchema)) {
                                    filteredOut = true;

                                    break;
                                }

                        
                        if (updateStatus)
                            shared.userVars.put("?",
                                    ((val == null) ? nullRepToken : val));
                        if (fetchingVar != null) {
                            if (val == null)
                                shared.userVars.remove(fetchingVar);
                            else
                                shared.userVars.put(fetchingVar, val);
                            updateUserSettings();
                            sqlExpandMode = null;

                            fetchingVar = null;
                        }

                        if (silent) {
                            lastSqlStatement = null;
                            return;
                        }

                        
                        
                        
                        if (filter != null
                            && (val == null || filter.matcher(val).find())) 
                            filteredOut = false;

                        
                        
                        if (val == null && pwDsv == null) {
                            if (dataType[insi] == java.sql.Types.VARCHAR) {
                                fieldArray[insi] = htmlMode
                                                 ? "\u0000" : nullRepToken;
                            } else {
                                fieldArray[insi] = "";
                            }
                        } else {
                            fieldArray[insi] = val;
                        }

                        
                        if (htmlMode || pwDsv != null) continue;

                        if (fieldArray[insi].length() > maxWidth[insi])
                            maxWidth[insi] = fieldArray[insi].length();
                    }

                    if (!filteredOut) rows.add(fieldArray);
                }
                if (updateStatus && !shared.userVars.containsKey("?"))
                    shared.userVars.put("?", "");
                if (fetchingVar != null) {
                    shared.userVars.remove(fetchingVar);
                    updateUserSettings();
                    sqlExpandMode = null;

                    fetchingVar = null;
                }

                
                
                if (pwDsv == null) {
                    condlPrintln("<TABLE class=\"sqltool\"><THEAD>", true);

                    if (incCount > 1) {
                        condlPrint(SqlFile.htmlRow(COL_HEAD) + LS + PRE_TD, true);

                        for (int i = 0; i < headerArray.length; i++) {
                            condlPrint("<TH>"
                                    + SqlFile.escapeHtml(headerArray[i])
                                    + "</TH>", true);
                            condlPrint(((i > 0) ? "  " : "")
                                    + ((i < headerArray.length - 1
                                        || rightJust[i])
                                       ? StringUtil.toPaddedString(
                                         headerArray[i], maxWidth[i],
                                         ' ', !rightJust[i])
                                       : headerArray[i])
                                    , false);
                        }

                        condlPrintln(LS + PRE_TR + "</TR>", true);
                        condlPrintln("", false);

                        if (!htmlMode) {
                            for (int i = 0; i < headerArray.length; i++)
                                condlPrint(((i > 0) ? "  "
                                                    : "") + SqlFile.divider(
                                                        maxWidth[i]), false);

                            condlPrintln("", false);
                        }
                    }
                    condlPrintln("</THEAD><TBODY>", true);

                    for (int i = 0; i < rows.size(); i++) {
                        condlPrint(SqlFile.htmlRow(((i % 2) == 0) ? COL_EVEN
                                                          : COL_ODD) + LS
                                                          + PRE_TD, true);

                        fieldArray = rows.get(i);

                        for (int j = 0; j < fieldArray.length; j++) {
                            condlPrint("<TD"
                                    + (rightJust[j]
                                       ? " class=\"sqltool-right\"" : "") + ">"
                                    + (fieldArray[j].equals("\u0000")
                                       ? nullRepHtml
                                       : SqlFile.escapeHtml(fieldArray[j]))
                                    + "</TD>", true);
                            condlPrint(((j > 0) ? "  " : "")
                                    + ((j < fieldArray.length - 1
                                        || rightJust[j])
                                       ? StringUtil.toPaddedString(
                                         fieldArray[j], maxWidth[j],
                                         ' ', !rightJust[j])
                                       : fieldArray[j])
                                    , false);
                        }

                        condlPrintln(LS + PRE_TR + "</TR>", true);
                        condlPrintln("", false);
                    }

                    condlPrintln("<TBODY></TABLE>", true);

                    if (interactive && rows.size() != 1)
                        stdprintln(LS + SqltoolRB.rows_fetched.getString(
                                rows.size()), true);

                    break;
                }

                
                if (incCount > 0) {
                    for (int i = 0; i < headerArray.length; i++) {
                        dsvSafe(headerArray[i]);
                        pwDsv.print(headerArray[i]);

                        if (i < headerArray.length - 1) 
                            pwDsv.print(dsvColDelim);
                    }

                    pwDsv.print(dsvRowDelim);
                }

                if (csvStyleQuoting) {
                    Pattern delimPat = Pattern.compile(dsvColDelim);
                    for (String[] fArray : rows)
                        for (int j = 0; j < fArray.length; j++)
                            if (fArray[j] != null && (allQuoted
                                    || fArray[j].indexOf('"') > -1
                                    || fArray[j].indexOf(dsvColDelim) > -1))
                                fArray[j] = '"'
                                        + fArray[j].replace("\"", "\"\"") + '"';
                }
                for (String[] fArray : rows) {
                    for (int j = 0; j < fArray.length; j++) {
                        if (pwDsv == null) dsvSafe(fArray[j]);
                        pwDsv.print((fArray[j] == null)
                                    ? (autonulls[j] ? "" : nullRepToken)
                                    : fArray[j]);

                        if (j < fArray.length - 1) pwDsv.print(dsvColDelim);
                    }

                    pwDsv.print(dsvRowDelim);
                }

                stdprintln(SqltoolRB.rows_fetched_dsv.getString(rows.size()));
                
                
                break;

            default :
                if (updateStatus)
                    shared.userVars.put("?", Integer.toString(updateCount));
                if (fetchingVar != null) {
                    shared.userVars.put(
                            fetchingVar, Integer.toString(updateCount));
                    fetchingVar = null;
                    updateUserSettings();
                    sqlExpandMode = null;
                }

                if (updateCount != 0 && interactive)
                    stdprintln((updateCount == 1)
                        ? SqltoolRB.row_update_singular.getString()
                        : SqltoolRB.row_update_multiple.getString(updateCount));
                break;
        }
        lastSqlStatement = null;  
        } finally {
            if (r != null) try {
                r.close();
            } catch (SQLException se) {
                logger.warning("Failed to close SQL result set: " + se);
            } finally {
                r = null;
            }
            if (statement != null) try {
                statement.close();
            } catch (SQLException se) {
                logger.warning("Failed to close SQL statement: " + se);
            } finally {
                statement = null;
            }
        }
    }

    private static final int    COL_HEAD = 0,
                                COL_ODD  = 1,
                                COL_EVEN = 2
    ;
    private static final String PRE_TR   = "    ";
    private static final String PRE_TD   = "        ";

    
    private static String htmlRow(int colType) {
        switch (colType) {
            case COL_HEAD :
                return PRE_TR + "<TR>";

            case COL_ODD :
                return PRE_TR + "<TR class=\"sqltool-odd\">";

            case COL_EVEN :
                return PRE_TR + "<TR class=\"sqltool-even\">";
        }

        return null;
    }

    
    private static String divider(int len) {
        return (len > DIVIDER.length()) ? DIVIDER
                                        : DIVIDER.substring(0, len);
    }

    
    private void showHistory() throws BadSpecial {
        if (history == null)
            throw new BadSpecial(SqltoolRB.history_unavailable.getString());
        if (history.size() < 1)
            throw new BadSpecial(SqltoolRB.history_none.getString());
        if (shared.psStd == null) return;
          
          
        Token token;
        for (int i = 0; i < history.size(); i++) {
            token = history.get(i);
            shared.psStd.println("#" + (i + oldestHist) + " or "
                    + (i - history.size()) + ':');
            shared.psStd.println(token.reconstitute());
        }
        if (buffer != null)
            shared.psStd.println(SqltoolRB.editbuffer_contents.getString(
                    buffer.reconstitute()));

        shared.psStd.println();
        shared.psStd.println(SqltoolRB.buffer_instructions.getString());
    }

    
    private Token commandFromHistory(int inIndex) throws BadSpecial {
        int index = inIndex;  

        if (history == null)
            throw new BadSpecial(SqltoolRB.history_unavailable.getString());
        if (index == 0)
            throw new BadSpecial(SqltoolRB.history_number_req.getString());
        if (index > 0) {
            
            index -= oldestHist;
            if (index < 0)
                throw new BadSpecial(
                        SqltoolRB.history_backto.getString(oldestHist));
            if (index >= history.size())
                throw new BadSpecial(SqltoolRB.history_upto.getString(
                       history.size() + oldestHist - 1));
        } else {
            
            index += history.size();
            if (index < 0)
                throw new BadSpecial(
                        SqltoolRB.history_back.getString(history.size()));
        }
        return history.get(index);
    }

    
    private Integer historySearch(String findRegex) throws BadSpecial {
        if (history == null)
            throw new BadSpecial(SqltoolRB.history_unavailable.getString());
        Pattern pattern = null;
        try {
            pattern = Pattern.compile("(?ims)" + findRegex);
        } catch (PatternSyntaxException pse) {
            throw new BadSpecial(SqltoolRB.regex_malformat.getString(pse));
        }
        
        
        for (int index = history.size() - 1; index >= 0; index--)
            if (pattern.matcher((history.get(index)).val).find())
                return Integer.valueOf(index + oldestHist);
        return null;
    }

    
    private boolean setBuf(Token newBuffer) {
        if (buffer != null)
        if (buffer != null && buffer.equals(newBuffer)) return false;
        switch (newBuffer.type) {
            case Token.SQL_TYPE:
            case Token.PL_TYPE:
            case Token.SPECIAL_TYPE:
                break;
            default:
                assert false:
                    "Internal assertion failed.  "
                    + "Attempted to add command type "
                    + newBuffer.getTypeString() + " to buffer";
        }
        buffer = new Token(newBuffer.type, new String(newBuffer.val),
                newBuffer.line);
        
        return true;
    }

    int oldestHist = 1;

    
    private boolean historize() {
        if (history == null || buffer == null) return false;
        if (history.size() > 0 &&
                history.get(history.size() - 1).equals(buffer))
            
            return false;
        history.add(buffer);
        if (history.size() <= maxHistoryLength) return true;
        history.remove(0);
        oldestHist++;
        return true;
    }

    
    private void describe(String tableName,
                          String filterString) throws SQLException {
        assert shared.jdbcConn != null:
            "Somehow got to 'describe' even though we have no Conn";
        
        Pattern   filter = null;
        boolean   filterMatchesAll = false;  
        List<String[]> rows = new ArrayList<String[]>();
        String[]  headerArray = {
            SqltoolRB.describe_table_name.getString(),
            SqltoolRB.describe_table_datatype.getString(),
            SqltoolRB.describe_table_width.getString(),
            SqltoolRB.describe_table_nonulls.getString(),
            SqltoolRB.describe_table_precision.getString(),
            SqltoolRB.describe_table_scale.getString(),
        };
        String[]  fieldArray;
        int[]     maxWidth  = { 0, 0, 0, 0, 0, 0 };
        boolean[] rightJust = { false, false, true, false, true, true };
        int       precision, scale;

        if (filterString != null) try {
            filterMatchesAll = (filterString.charAt(0) == '/');
            filter = Pattern.compile(filterMatchesAll
                    ? filterString.substring(1) : filterString);
        } catch (PatternSyntaxException pse) {
            throw new SQLException(SqltoolRB.regex_malformat.getString(pse));
            
            
            
        }

        for (int i = 0; i < headerArray.length; i++) {
            if (htmlMode) continue;

            if (headerArray[i].length() > maxWidth[i]) 
                maxWidth[i] = headerArray[i].length();
        }

        ResultSet r         = null;
        Statement statement = shared.jdbcConn.createStatement();

        
        try {
            statement.execute("SELECT * FROM " + tableName + " WHERE 1 = 2");

            r = statement.getResultSet();

            ResultSetMetaData m    = r.getMetaData();
            int               cols = m.getColumnCount();

            for (int i = 0; i < cols; i++) {
                fieldArray    = new String[6];
                precision = m.getPrecision(i + 1);
                scale = m.getScale(i + 1);
                fieldArray[0] = m.getColumnName(i + 1);

                if (filter != null && (!filterMatchesAll)
                        && !filter.matcher(fieldArray[0]).find()) continue;

                fieldArray[1] = m.getColumnTypeName(i + 1);
                fieldArray[2] = Integer.toString(m.getColumnDisplaySize(i + 1));
                fieldArray[3] = ((m.isNullable(i + 1)
                        == java.sql.ResultSetMetaData.columnNullable)
                        ? "" : "*");
                fieldArray[4] =
                        (precision == 0) ? "" :Integer.toString(precision);
                fieldArray[5] = (scale == 0) ? "" :Integer.toString(scale);

                if (filter != null && filterMatchesAll
                        && !filter.matcher(fieldArray[0]
                            + ' ' + fieldArray[1] + ' ' + fieldArray[2] + ' '
                            + fieldArray[3]).find()) continue;

                rows.add(fieldArray);

                for (int j = 0; j < fieldArray.length; j++)
                    if (fieldArray[j].length() > maxWidth[j])
                        maxWidth[j] = fieldArray[j].length();
            }

            
            condlPrint("<TABLE class=\"sqltool sqltool-describe\"><THEAD>"
                    + LS + SqlFile.htmlRow(COL_HEAD) + LS + PRE_TD, true);

            for (int i = 0; i < headerArray.length; i++) {
                condlPrint("<TH>"
                        + SqlFile.escapeHtml(headerArray[i]) + "</TH>", true);
                condlPrint(((i > 0) ? "  " : "")
                        + ((i < headerArray.length - 1 || rightJust[i])
                           ? StringUtil.toPaddedString(
                             headerArray[i], maxWidth[i], ' ', !rightJust[i])
                           : headerArray[i])
                        , false);
            }

            condlPrintln(LS + PRE_TR + "</TR>", true);
            condlPrintln("", false);
            condlPrintln("</THEAD><TBODY>", true);

            if (!htmlMode) {
                for (int i = 0; i < headerArray.length; i++)
                    condlPrint(((i > 0) ? "  " : "")
                            + SqlFile.divider(maxWidth[i]), false);

                condlPrintln("", false);
            }

            for (int i = 0; i < rows.size(); i++) {
                condlPrint(SqlFile.htmlRow( ((i % 2) == 0)
                        ? COL_EVEN : COL_ODD) + LS + PRE_TD, true);

                fieldArray = rows.get(i);

                for (int j = 0; j < fieldArray.length; j++) {
                    condlPrint("<TD"
                            + (rightJust[j] ? " class=\"sqltool-right\"" : "")
                            + ">" + SqlFile.escapeHtml(fieldArray[j])
                            + "</TD>", true);
                    condlPrint(((j > 0) ? "  " : "")
                            + ((j < fieldArray.length - 1 || rightJust[j])
                               ? StringUtil.toPaddedString(
                                 fieldArray[j], maxWidth[j], ' ', !rightJust[j])
                               : fieldArray[j])
                            , false);
                }

                condlPrintln(LS + PRE_TR + "</TR>", true);
                condlPrintln("", false);
            }

            condlPrintln(LS + "</TBODY></TABLE>", true);
        } finally {
            if (r != null) try {
                r.close();
            } catch (SQLException nse) {
                
            } finally {
                r = null;
            }
            if (statement != null) try {
                statement.close();
            } catch (SQLException nse) {
                
            } finally {
                statement = null;
            }
        }
    }

    
    private boolean eval(String[] inTokens) throws BadSpecial {
        
        
        
        boolean  negate = inTokens.length > 0 && inTokens[0].equals("!");
        String[] tokens = new String[negate ? (inTokens.length - 1)
                                            : inTokens.length];
        String inToken;

        for (int i = 0; i < tokens.length; i++) {
            inToken = inTokens[i + (negate ? 1 : 0)];
            if (inToken.length() > 1 && inToken.charAt(0) == '*') {
                tokens[i] = shared.userVars.get(inToken.substring(1));
            } else {
                tokens[i] = inTokens[i + (negate ? 1 : 0)];
            }
        }

        if (tokens.length == 1)
            return (tokens[0] != null && tokens[0].length() > 0
                    && !tokens[0].equals("0")) ^ negate;

        if (tokens.length == 3) {
            if (tokens[1] == null)
                throw new BadSpecial(SqltoolRB.logical_unrecognized.getString());

            if (tokens[1].equals("!=") || tokens[1].equals("<>")
                    || tokens[1].equals("><")) {
                negate = !negate;
                tokens[1] = "==";
            }
            if (tokens[1].equals(">=") || tokens[1].equals("=>")) {
                negate = !negate;
                tokens[1] = "<";
            }
            if (tokens[1].equals("<=") || tokens[1].equals("=<")) {
                negate = !negate;
                tokens[1] = ">";
            }

            if (tokens[1].equals("==")) {
                if (tokens[0] == null || tokens[2] == null)
                    return (tokens[0] == null && tokens[2] == null) ^ negate;
                return tokens[0].equals(tokens[2]) ^ negate;
            }

            char c1 = (tokens[0] == null || tokens[0].length() < 1)
                        ? '\0' : tokens[0].charAt(0);
            char c2 = (tokens[2] == null || tokens[2].length() < 1)
                        ? '\0' : tokens[2].charAt(0);
            if (tokens[1].equals(">")) {
                if (tokens[0] == null || tokens[2] == null) return !negate;
                if (c1 == '-' && c2 == '-') {
                    negate = !negate;
                } else if (c1 == '-') {
                    return negate;
                } else if (c2 == '-') {
                    return !negate;
                }
                return (tokens[0].length() > tokens[2].length()
                        || ((tokens[0].length() == tokens[2].length())
                        && tokens[0].compareTo(tokens[2]) > 0)) ^ negate;
            }

            if (tokens[1].equals("<")) {
                if (tokens[0] == null || tokens[2] == null) return !negate;
                if (c1 == '-' && c2 == '-') {
                    negate = !negate;
                } else if (c1 == '-') {
                    return !negate;
                } else if (c2 == '-') {
                    return negate;
                }
                return (tokens[2].length() > tokens[0].length()
                        || ((tokens[2].length() == tokens[0].length())
                        && tokens[2].compareTo(tokens[0]) > 0)) ^ negate;
            }
        }

        throw new BadSpecial(SqltoolRB.logical_unrecognized.getString());
    }

    private void closeQueryOutputStream() {
        if (pwQuery == null) return;

        try {
            if (htmlMode) {
                pwQuery.flush();
            }
        } finally {
            try {
                pwQuery.close();
            } finally {
                pwQuery = null; 
            }
        }
    }

    
    private void condlPrintln(String s, boolean printHtml) {
        if ((printHtml && !htmlMode) || (htmlMode && !printHtml)) return;

        if (shared.psStd != null) shared.psStd.println(s);

        if (pwQuery != null) {
            pwQuery.println(s);
            pwQuery.flush();
        }
    }

    
    private void condlPrint(String s, boolean printHtml) {
        if ((printHtml && !htmlMode) || (htmlMode && !printHtml)) return;

        if (shared.psStd != null) shared.psStd.print(s);

        if (pwQuery != null) {
            pwQuery.print(s);
            pwQuery.flush();
        }
    }

    private String formatNicely(Map<?, ?> map, boolean withValues) {
        String       s;
        StringBuffer sb = new StringBuffer();

        if (withValues) {
            SqlFile.appendLine(sb, SqltoolRB.pl_list_parens.getString());
        } else {
            SqlFile.appendLine(sb, SqltoolRB.pl_list_lengths.getString());
        }

        for (Map.Entry<Object, Object> entry
                : new TreeMap<Object, Object>(map).entrySet()) {
            s = (String) entry.getValue();

            SqlFile.appendLine(sb, "    " + (String) entry.getKey()
                    + ": " + (withValues ? ("(" + s + ')')
                    : Integer.toString( s.length())));
        }

        return sb.toString();
    }

    
    private void dump(String varName,
                      File dumpFile) throws IOException, BadSpecial {
        String val = shared.userVars.get(varName);

        if (val == null)
            throw new BadSpecial(SqltoolRB.plvar_undefined.getString(varName));

        OutputStreamWriter osw = new OutputStreamWriter(
                new FileOutputStream(dumpFile), (shared.encoding == null)
                ? DEFAULT_FILE_ENCODING : shared.encoding);

        try {
            osw.write(val);

            if (val.length() > 0) {
                char lastChar = val.charAt(val.length() - 1);

                if (lastChar != '\n' && lastChar != '\r') osw.write(LS);
            }

            osw.flush();
        } finally {
            try {
                osw.close();
            } catch (IOException ioe) {
                
            } finally {
                osw = null;  
            }
        }

        
        
        stdprintln(SqltoolRB.file_wrotechars.getString(
                Long.toString(dumpFile.length()), dumpFile.toString()));
    }

    byte[] binBuffer;

    
    private void dump(File dumpFile) throws IOException, BadSpecial {
        if (binBuffer == null)
            throw new BadSpecial(SqltoolRB.binbuffer_empty.getString());

        int len = 0;
        FileOutputStream fos = new FileOutputStream(dumpFile);

        try {
            fos.write(binBuffer);

            len = binBuffer.length;

            binBuffer = null;

            fos.flush();
        } finally {
            try {
                fos.close();
            } catch (IOException ioe) {
                
            } finally {
                fos = null; 
            }
        }
        stdprintln(SqltoolRB.file_wrotechars.getString(
                len, dumpFile.toString()));
    }

    
    public String streamToString(InputStream isIn, String cs)
            throws IOException {
        InputStream is = isIn;  
        byte[] ba = null;
        int bytesread = 0;
        int retval;
        try {
            try {
                ba = new byte[is.available()];
            } catch (RuntimeException re) {
                throw new IOException(SqltoolRB.read_toobig.getString());
            }
            while (bytesread < ba.length &&
                    (retval = is.read(
                            ba, bytesread, ba.length - bytesread)) > 0) {
                bytesread += retval;
            }
            if (bytesread != ba.length)
                throw new IOException(
                        SqltoolRB.read_partial.getString(bytesread, ba.length));
            try {
                return (cs == null) ? (new String(ba))
                                         : (new String(ba, cs));
            } catch (UnsupportedEncodingException uee) {
                throw new IOException(
                        SqltoolRB.encode_fail.getString(uee));
            } catch (RuntimeException re) {
                throw new IOException(SqltoolRB.read_convertfail.getString());
            }
        } finally {
            try {
                is.close();
            } catch (IOException ioe) {
                
            } finally {
                is = null;  
            }
        }
    }

    
    private void load(String varName, File asciiFile, String cs)
            throws IOException {
        String string = streamToString(new FileInputStream(asciiFile), cs);
        
        shared.userVars.put(varName, string);
        if (!varPattern.matcher(varName).matches())
            errprintln(SqltoolRB.varname_warning.getString(varName));
        updateUserSettings();
        sqlExpandMode = null;
    }

    
    public static byte[] streamToBytes(InputStream is) throws IOException {
        byte[]                xferBuffer = new byte[10240];
        byte[]                outBytes = null;
        int                   i;
        ByteArrayOutputStream baos       = new ByteArrayOutputStream();

        try {
            while ((i = is.read(xferBuffer)) > 0) baos.write(xferBuffer, 0, i);
            outBytes = baos.toByteArray();
        } finally {
            baos = null;  
        }
        return outBytes;
    }

    
    public static byte[] loadBinary(File binFile) throws IOException {
        byte[]                xferBuffer = new byte[10240];
        byte[]                outBytes = null;
        ByteArrayOutputStream baos;
        int                   i;
        FileInputStream       fis        = new FileInputStream(binFile);

        try {
            baos = new ByteArrayOutputStream();
            while ((i = fis.read(xferBuffer)) > 0) baos.write(xferBuffer, 0, i);
            outBytes = baos.toByteArray();
        } finally {
            try {
                fis.close();
            } catch (IOException ioe) {
                
            } finally {
                fis = null; 
                baos = null; 
            }
        }

        return outBytes;
    }

    
    public static boolean canDisplayType(int i) {
        
        switch (i) {
            
            case java.sql.Types.BLOB :
            case java.sql.Types.JAVA_OBJECT :

            
            
            case java.sql.Types.OTHER :
            case java.sql.Types.STRUCT :

                
                return false;
        }

        return true;
    }

    
    private static final int JDBC3_BOOLEAN  = 16;
    private static final int JDBC3_DATALINK = 70;

    
    public static String sqlTypeToString(int i) {
        switch (i) {
            case java.sql.Types.ARRAY :
                return "ARRAY";

            case java.sql.Types.BIGINT :
                return "BIGINT";

            case java.sql.Types.BINARY :
                return "BINARY";

            case java.sql.Types.BIT :
                return "BIT";

            case java.sql.Types.BLOB :
                return "BLOB";

            case JDBC3_BOOLEAN :
                return "BOOLEAN";

            case java.sql.Types.CHAR :
                return "CHAR";

            case java.sql.Types.CLOB :
                return "CLOB";

            case JDBC3_DATALINK :
                return "DATALINK";

            case java.sql.Types.DATE :
                return "DATE";

            case java.sql.Types.DECIMAL :
                return "DECIMAL";

            case java.sql.Types.DISTINCT :
                return "DISTINCT";

            case java.sql.Types.DOUBLE :
                return "DOUBLE";

            case java.sql.Types.FLOAT :
                return "FLOAT";

            case java.sql.Types.INTEGER :
                return "INTEGER";

            case java.sql.Types.JAVA_OBJECT :
                return "JAVA_OBJECT";

            case java.sql.Types.LONGVARBINARY :
                return "LONGVARBINARY";

            case java.sql.Types.LONGVARCHAR :
                return "LONGVARCHAR";

            case java.sql.Types.NULL :
                return "NULL";

            case java.sql.Types.NUMERIC :
                return "NUMERIC";

            case java.sql.Types.OTHER :
                return "OTHER";

            case java.sql.Types.REAL :
                return "REAL";

            case java.sql.Types.REF :
                return "REF";

            case java.sql.Types.SMALLINT :
                return "SMALLINT";

            case java.sql.Types.STRUCT :
                return "STRUCT";

            case java.sql.Types.TIME :
                return "TIME";

            case java.sql.Types.TIMESTAMP :
                return "TIMESTAMP";

            case java.sql.Types.TINYINT :
                return "TINYINT";

            case java.sql.Types.VARBINARY :
                return "VARBINARY";

            case java.sql.Types.VARCHAR :
                return "VARCHAR";

            case org.hsqldb.types.Types.SQL_TIME_WITH_TIME_ZONE :
                return "SQL_TIME_WITH_TIME_ZONE";

            case org.hsqldb.types.Types.SQL_TIMESTAMP_WITH_TIME_ZONE :
                return "SQL_TIMESTAMP_WITH_TIME_ZONE";
        }

        return "Unknown type " + i;
    }

    
    public void dsvSafe(String s) throws SqlToolError {
        assert pwDsv != null && dsvColDelim != null && dsvRowDelim != null
                && nullRepToken != null:
            "Assertion failed.  \n"
            + "dsvSafe called when DSV settings are incomplete";

        if (s == null) return;

        if (s.indexOf(dsvColDelim) > 0)
            throw new SqlToolError(
                    SqltoolRB.dsv_coldelim_present.getString(dsvColDelim));

        if (s.indexOf(dsvRowDelim) > 0)
            throw new SqlToolError(
                    SqltoolRB.dsv_rowdelim_present.getString(dsvRowDelim));

        if (s.trim().equals(nullRepToken))
            
            
            throw new SqlToolError(
                    SqltoolRB.dsv_nullrep_present.getString(nullRepToken));
    }

    
    public static String convertEscapes(String inString) {
        if (inString == null) return null;
        return convertNumericEscapes(
                convertEscapes(convertEscapes(convertEscapes(convertEscapes(
                    convertEscapes(inString, "\\n", "\n"), "\\r", "\r"),
                "\\t", "\t"), "\\\\", "\\"),
            "\\f", "\f")
        );
    }

    
    private static String convertNumericEscapes(String string) {
        String workString = string;
        int i = 0;

        for (char dig = '0'; dig <= '9'; dig++) {
            while ((i = workString.indexOf("\\" + dig, i)) > -1
                    && i < workString.length() - 1)
                workString = convertNumericEscape(string, i);
            while ((i = workString.indexOf("\\x" + dig, i)) > -1
                    && i < workString.length() - 1)
                workString = convertNumericEscape(string, i);
            while ((i = workString.indexOf("\\X" + dig, i)) > -1
                    && i < workString.length() - 1)
                workString = convertNumericEscape(string, i);
        }
        return workString;
    }

    
    private static String convertNumericEscape(String string, int offset) {
        int post = -1;
        int firstDigit = -1;
        int radix = -1;
        if (Character.toUpperCase(string.charAt(offset + 1)) == 'X') {
            firstDigit = offset + 2;
            radix = 16;
            post = firstDigit + 2;
            if (post > string.length()) post = string.length();
        } else {
            firstDigit = offset + 1;
            radix = (Character.toUpperCase(string.charAt(firstDigit)) == '0')
                    ? 8 : 10;
            post = firstDigit + 1;
            while (post < string.length()
                    && Character.isDigit(string.charAt(post))) post++;
        }
        return string.substring(0, offset) + ((char)
                Integer.parseInt(string.substring(firstDigit, post), radix))
                + string.substring(post);
    }

    
    private static String convertEscapes(String string, String from, String to) {
        String workString = string;
        int i = 0;
        int fromLen = from.length();

        while ((i = workString.indexOf(from, i)) > -1
                && i < workString.length() - 1)
            workString = workString.substring(0, i) + to
                         + workString.substring(i + fromLen);
        return workString;
    }

    private void checkFor02(String s) throws SqlToolError {
        try {
            if (csvStyleQuoting && s.indexOf('\u0002') > -1)
                throw new SqlToolError(
                        SqltoolRB.csv_coldelim_present.getString("\\u0002"));
        } catch (RuntimeException re) {
            throw new SqlToolError(SqltoolRB.read_convertfail.getString(), re);
        }
    }

    
    private String preprocessCsvQuoting(String s, int lineNum)
            throws SqlToolError {
        StringBuilder sb = new StringBuilder();
        int offset, segLen, prevOffset;
        if (s.indexOf('"') < 0) return s.replaceAll(dsvColSplitter, "\u0002");
        prevOffset = -1;
        SEEK_QUOTEDFIELD:
        while (prevOffset < s.length() - 1) {
            
            offset = s.indexOf('"', prevOffset + 1);
            segLen = ((offset < 0) ? s.length() : offset)
                    - (prevOffset + 1);
            if (segLen > 1)
                
                
                sb.append(s.substring(
                        prevOffset + 1, prevOffset + 1 + segLen)
                        .replaceAll(dsvColSplitter, "\u0002"));
            if (offset < 0) break; 
            prevOffset = offset;
            while ((offset = s.indexOf(
                    '"', prevOffset + 1)) > -1) {
                if (offset - prevOffset > 1)
                    
                    sb.append(s.substring(
                            prevOffset + 1, offset));
                prevOffset = offset;
                if (s.length() < offset + 2
                        || s.charAt(offset + 1) != '"')
                    
                    continue SEEK_QUOTEDFIELD;
                
                prevOffset++;
                sb.append('"');
            }
            throw new SqlToolError(
                SqltoolRB.csv_quote_unterminated.getString(lineNum));
        }
        return sb.toString();
    }

    
    public void importDsv(String filePath, String skipPrefix)
            throws SqlToolError {
        
        requireConnection();
        
        if (csvStyleQuoting && (dsvColSplitter.indexOf('"') > -1
                || dsvRowSplitter.indexOf('"') > -1))
            throw new SqlToolError(SqltoolRB.dsv_q_nodblquote.getString());
        Matcher matcher;
        SortedMap<String, String> constColMap = null;
        if (dsvConstCols != null) {
            
            
            
            
            constColMap = new TreeMap<String, String>();
            for (String constPair : dsvConstCols.split(dsvColSplitter, -1)) {
                matcher = nameValPairPattern.matcher(constPair);
                if (!matcher.matches())
                    throw new SqlToolError(
                            SqltoolRB.dsv_constcols_nullcol.getString());
                constColMap.put(matcher.group(1).toLowerCase(),
                        ((matcher.groupCount() < 2 || matcher.group(2) == null)
                        ? "" : matcher.group(2)));
            }
        }
        Set<String> skipCols = null;
        if (dsvSkipCols != null) {
            
            
            skipCols = new HashSet<String>();
            for (String skipCol : dsvSkipCols.split(dsvColSplitter, -1))
                skipCols.add(skipCol.trim().toLowerCase());
        }

        FileRecordReader dsvReader = null;
        try {
            dsvReader = new FileRecordReader(filePath, dsvRowSplitter,
                    (shared.encoding == null)
                    ? DEFAULT_FILE_ENCODING : shared.encoding);
        } catch (UnsupportedEncodingException uee) {
            throw new SqlToolError(uee);
        } catch (IOException ioe) {
            throw new SqlToolError(SqltoolRB.file_readfail.getString(filePath));
        } catch (PatternSyntaxException pse) {
            throw new SqlToolError(
                    SqltoolRB.regex_malformat.getString(dsvRowSplitter));
        }





        int retval;

        String dateString;

        List<String> headerList = new ArrayList<String>();
        String    tableName = dsvTargetTable;

        
        int lineCount = 0;
        String trimmedLine = null;
        boolean switching = false;
        int headerOffset = 0;  
        String curLine = null; 
                                  

        try {
        while (true) {
            try {
                curLine = dsvReader.nextRecord();
            } catch (IOException ioe) {
                throw new SqlToolError(ioe);
            }
            if (curLine == null)
                throw new SqlToolError(SqltoolRB.dsv_header_none.getString());
            checkFor02(curLine);
            lineCount++;
            trimmedLine = curLine.trim();
            if (trimmedLine.length() < 1
                    || (skipPrefix != null
                            && trimmedLine.startsWith(skipPrefix))) continue;
            if (trimmedLine.startsWith("targettable=")) {
                if (tableName == null) 
                    tableName = trimmedLine.substring(
                            "targettable=".length()).trim();
                continue;
            }
            if (trimmedLine.equals("headerswitch{")) {
                if (tableName == null)
                    throw new SqlToolError(
                            SqltoolRB.dsv_header_noswitchtarg.getString(
                            lineCount));
                switching = true;
                continue;
            }
            if (trimmedLine.equals("}"))
                throw new SqlToolError(
                        SqltoolRB.dsv_header_noswitchmatch.getString(lineCount));
            if (!switching) break;
            int colonAt = trimmedLine.indexOf(':');
            if (colonAt < 1 || colonAt == trimmedLine.length() - 1)
                throw new SqlToolError(
                        SqltoolRB.dsv_header_nonswitched.getString(lineCount));
            String headerName = trimmedLine.substring(0, colonAt).trim();
            
            
            if (headerName.equals("*")
                    || headerName.equalsIgnoreCase(tableName)){
                headerOffset = 1 + curLine.indexOf(':');
                break;
            }
            
        }

        if (csvStyleQuoting) curLine = preprocessCsvQuoting(curLine, lineCount);
        String headerLine = curLine.substring(headerOffset);
        String colName;
        String[] cols = headerLine.split(
                (csvStyleQuoting ? "\u0002" : dsvColSplitter), -1);
        Set<String> usedCols = new HashSet<String>();  

        for (String col : cols) {
            if (col.length() < 1)
                throw new SqlToolError(SqltoolRB.dsv_nocolheader.getString(
                        headerList.size() + 1, lineCount));

            colName = col.trim().toLowerCase();
            if (colName.equals("-")
                        || (skipCols != null
                                && skipCols.remove(colName))
                        || (constColMap != null
                                && constColMap.containsKey(colName)))
                colName = null;
            headerList.add(colName);
            if (colName == null) continue;
            if (usedCols.contains(colName.toLowerCase()))
                throw new SqlToolError(
                        SqltoolRB.import_col_dup.getString(colName));
            usedCols.add(colName.toLowerCase());
        }
        if (skipCols != null && skipCols.size() > 0)
            throw new SqlToolError(SqltoolRB.dsv_skipcols_missing.getString(
                    skipCols.toString()));

        boolean oneCol = false;  
        for (String header : headerList) if (header != null) {
            oneCol = true;
            break;
        }
        if (oneCol == false)
            
            
            
            throw new SqlToolError(
                    SqltoolRB.dsv_nocolsleft.getString(dsvSkipCols));

        int inputColHeadCount = headerList.size();

        if (constColMap != null) headerList.addAll(constColMap.keySet());

        String[]  headers   = headerList.toArray(new String[0]);
        
        

        if (tableName == null) {
            tableName = dsvReader.getName();

            int i = tableName.lastIndexOf('.');

            if (i > 0) tableName = tableName.substring(0, i);
        }

        StringBuffer tmpSb = new StringBuffer();
        List<String> tmpList = new ArrayList<String>();

        int skippers = 0;
        for (String header : headers) {
            if (header == null) {
                skippers++;
                continue;
            }
            if (tmpSb.length() > 0) tmpSb.append(", ");

            tmpSb.append(header);
            tmpList.add(header);
        }
        boolean[] autonulls = new boolean[headers.length - skippers];
        boolean[] parseDate = new boolean[autonulls.length];
        boolean[] parseBool = new boolean[autonulls.length];
        char[] readFormat = new char[autonulls.length];
        String[] insertFieldName = tmpList.toArray(new String[] {});
        
        
        

        StringBuffer sb = new StringBuffer("INSERT INTO " + tableName + " ("
                                           + tmpSb + ") VALUES (");
        StringBuffer typeQuerySb = new StringBuffer("SELECT " + tmpSb
            + " FROM " + tableName + " WHERE 1 = 2");

        try {
            ResultSetMetaData rsmd =
                    shared.jdbcConn.createStatement().executeQuery(
                    typeQuerySb.toString()).getMetaData();

            if (rsmd.getColumnCount() != autonulls.length)
                throw new SqlToolError(
                        SqltoolRB.dsv_metadata_mismatch.getString());
                
                
                
                
                

            for (int i = 0; i < autonulls.length; i++) {
                autonulls[i] = true;
                parseDate[i] = false;
                parseBool[i] = false;
                readFormat[i] = 's'; 
                switch(rsmd.getColumnType(i + 1)) {
                    case java.sql.Types.BIT :
                        autonulls[i] = true;
                        readFormat[i] = 'b';
                        break;
                    case java.sql.Types.LONGVARBINARY :
                    case java.sql.Types.VARBINARY :
                    case java.sql.Types.BINARY :
                        autonulls[i] = true;
                        readFormat[i] = 'x';
                        break;
                    case java.sql.Types.BOOLEAN:
                        parseBool[i] = true;
                        break;
                    case java.sql.Types.ARRAY :
                        autonulls[i] = true;
                        readFormat[i] = 'a';
                        break;
                    case java.sql.Types.VARCHAR :
                    case java.sql.Types.BLOB :
                    case java.sql.Types.CLOB :
                    case java.sql.Types.LONGVARCHAR :
                        autonulls[i] = false;
                        
                        
                        
                        break;
                    case java.sql.Types.DATE:
                    case java.sql.Types.TIME:
                    case java.sql.Types.TIMESTAMP:
                    case org.hsqldb.types.Types.SQL_TIMESTAMP_WITH_TIME_ZONE:
                    case org.hsqldb.types.Types.SQL_TIME_WITH_TIME_ZONE:
                        parseDate[i] = true;
                }
            }
        } catch (SQLException se) {
            throw new SqlToolError(SqltoolRB.query_metadatafail.getString(
                    typeQuerySb.toString()), se);
        }

        for (int i = 0; i < autonulls.length; i++) {
            if (i > 0) sb.append(", ");

            sb.append('?');
        }

        
        int rejectCount = 0;
        File rejectFile = null;
        File rejectReportFile = null;
        PrintWriter rejectWriter = null;
        PrintWriter rejectReportWriter = null;
        try {
        if (dsvRejectFile != null) try {
            rejectFile = new File(dereferenceAt(dsvRejectFile));
            rejectWriter = new PrintWriter(
                    new OutputStreamWriter(new FileOutputStream(rejectFile),
                    (shared.encoding == null)
                    ? DEFAULT_FILE_ENCODING : shared.encoding));
            rejectWriter.print(headerLine + dsvRowDelim);
        } catch (BadSpecial bs) {
            throw new SqlToolError(SqltoolRB.dsv_rejectfile_setupfail.getString(
                    dsvRejectFile), bs);
        } catch (IOException ioe) {
            throw new SqlToolError(SqltoolRB.dsv_rejectfile_setupfail.getString(
                    dsvRejectFile), ioe);
        }
        if (dsvRejectReport != null) try {
            rejectReportFile = new File(dereferenceAt(dsvRejectReport));
            rejectReportWriter = new PrintWriter(new OutputStreamWriter(
                    new FileOutputStream(rejectReportFile),
                    (shared.encoding == null)
                    ? DEFAULT_FILE_ENCODING : shared.encoding));
            boolean setTitle = !shared.userVars.containsKey("REPORT_TITLE");
            if (setTitle)
                shared.userVars.put("REPORT_TITLE",
                        "SqlTool " + (csvStyleQuoting ? "CSV" : "DSV")
                        + " Reject Report");
            try {
                writeHeader(rejectReportWriter, dsvRejectReport);
            } finally {
                if (setTitle) shared.userVars.remove("REPORT_TITLE");
            }
            rejectReportWriter.println(SqltoolRB.rejectreport_top.getString(
                    dsvReader.getPath(),
                    ((rejectFile == null) ? SqltoolRB.none.getString()
                                    : rejectFile.getPath()),
                    ((rejectFile == null) ? null : rejectFile.getPath())));
        } catch (BadSpecial bs) {
            throw new SqlToolError(
                    SqltoolRB.dsv_rejectreport_setupfail.getString(
                    dsvRejectReport), bs);
        } catch (IOException ioe) {
            throw new SqlToolError(
                    SqltoolRB.dsv_rejectreport_setupfail.getString(
                    dsvRejectReport), ioe);
        }

        int recCount = 0;
        int skipCount = 0;
        PreparedStatement ps = null;
        boolean importAborted = false;
        boolean doResetAutocommit = false;
        try {
            doResetAutocommit = dsvRecordsPerCommit > 0
                && shared.jdbcConn.getAutoCommit();
            if (doResetAutocommit) shared.jdbcConn.setAutoCommit(false);
        } catch (SQLException se) {
            throw new SqlToolError(
                    SqltoolRB.rpc_autocommit_failure.getString(), se);
        }
        
        

        try {
            try {
                ps = shared.jdbcConn.prepareStatement(sb.toString() + ')');
            } catch (SQLException se) {
                throw new SqlToolError(
                        SqltoolRB.insertion_preparefail.getString(
                        sb.toString()), se);
            }
            String[] dataVals = new String[autonulls.length];
            
            int      readColCount;
            int      storeColCount;
            Matcher  arMatcher;
            String   currentFieldName = null;
            String[] arVals;

            
            while (true) try { try {
                try {
                    curLine = dsvReader.nextRecord();
                } catch (IOException ioe) {
                    throw new SqlToolError(ioe);
                }
                if (curLine == null) break;
                checkFor02(curLine);
                if (csvStyleQuoting)
                    curLine = preprocessCsvQuoting(curLine, ++lineCount);
                trimmedLine = curLine.trim();
                if (trimmedLine.length() < 1) continue;  
                if (skipPrefix != null
                        && trimmedLine.startsWith(skipPrefix)) {
                    skipCount++;
                    continue;
                }
                if (switching) {
                    if (trimmedLine.equals("}")) {
                        switching = false;
                        continue;
                    }
                    int colonAt = trimmedLine.indexOf(':');
                    if (colonAt < 1 || colonAt == trimmedLine.length() - 1)
                        throw new SqlToolError(
                                SqltoolRB.dsv_header_matchernonhead.getString(
                                lineCount));
                    continue;
                }
                
                

                
                recCount++;
                

                readColCount = 0;
                storeColCount = 0;
                cols = curLine.split(
                        (csvStyleQuoting ? "\u0002" : dsvColSplitter), -1);

                for (String col : cols) {
                    if (readColCount == inputColHeadCount)
                        throw new RowError(
                                SqltoolRB.dsv_colcount_mismatch.getString(
                                inputColHeadCount, 1 + readColCount));

                    if (headers[readColCount++] != null)
                        dataVals[storeColCount++] = dsvTrimAll ? col.trim() : col;
                }
                if (readColCount < inputColHeadCount)
                    throw new RowError(
                            SqltoolRB.dsv_colcount_mismatch.getString(
                            inputColHeadCount, readColCount));
                

                if (constColMap != null)
                    for (String val : constColMap.values())
                        dataVals[storeColCount++] = val;
                if (storeColCount != dataVals.length)
                    throw new RowError(
                            SqltoolRB.dsv_insertcol_mismatch.getString(
                            dataVals.length, storeColCount));

                for (int i = 0; i < dataVals.length; i++) {
                    currentFieldName = insertFieldName[i];
                    if (autonulls[i]) dataVals[i] = dataVals[i].trim();
                    
                    
                    
                    
                    

                    if (parseDate[i]) {
                        if ((dataVals[i].length() < 1 && autonulls[i])
                              || dataVals[i].equals(nullRepToken)) {
                            ps.setTimestamp(i + 1, null);
                        } else {
                            
                            
                            if (dataVals[i].indexOf(':') > 0
                                    && dataVals[i].indexOf('-') > 0) {
                                dateString = dataVals[i];
                            } else if (dataVals[i].indexOf(':') < 1) {
                                dateString = dataVals[i] + " 0:00:00";
                            } else if (dataVals[i].indexOf('-') < 1) {
                                dateString = "0000-00-00 " + dataVals[i];
                            } else {
                                dateString = null;  
                                assert false:
                                    "Unexpected date/time val: " + dataVals[i];
                            }
                            try {
                                ps.setTimestamp(i + 1,
                                        java.sql.Timestamp.valueOf(dateString));
                            } catch (IllegalArgumentException iae) {
                                throw new RowError(
                                        SqltoolRB.time_bad.getString(
                                        dateString), iae);
                            }
                        }
                    } else if (parseBool[i]) {
                        if ((dataVals[i].length() < 1 && autonulls[i])
                              || dataVals[i].equals(nullRepToken)) {
                            ps.setNull(i + 1, java.sql.Types.BOOLEAN);
                        } else {
                            try {
                                ps.setBoolean(i + 1,
                                        Boolean.parseBoolean(dataVals[i]));
                                
                                
                            } catch (IllegalArgumentException iae) {
                                throw new RowError(
                                        SqltoolRB.boolean_bad.getString(
                                        dataVals[i]), iae);
                            }
                        }
                    } else {
                        switch (readFormat[i]) {
                            case 'b':
                                ps.setBytes(
                                    i + 1,
                                    (dataVals[i].length() < 1) ? null
                                    : SqlFile.bitCharsToBytes(
                                        dataVals[i]));
                                break;
                            case 'x':
                                ps.setBytes(
                                    i + 1,
                                    (dataVals[i].length() < 1) ? null
                                    : SqlFile.hexCharOctetsToBytes(
                                        dataVals[i]));
                                break;
                            case 'a' :
                                if (SqlFile.createArrayOfMethod == null)
                                    throw new SqlToolError(
                                            SqltoolRB.arrayimp_jvmreq
                                            .getString());
                                if (dataVals[i].length() < 1) {
                                    ps.setArray(i + 1, null);
                                    break;
                                }
                                arMatcher = arrayPattern.matcher(dataVals[i]);
                                if (!arMatcher.matches())
                                    throw new RowError(
                                            SqltoolRB.arrayval_malformat
                                            .getString(dataVals[i]));
                                arVals = (arMatcher.group(1) == null)
                                       ? (new String[0])
                                       : arMatcher.group(1).split("\\s*,\\s*");
                                
                                
                                try {
                                    ps.setArray(i + 1, (java.sql.Array)
                                            SqlFile.createArrayOfMethod.invoke(
                                            shared.jdbcConn,
                                            "VARCHAR", arVals));
                                } catch (IllegalAccessException iae) {
                                    throw new RuntimeException(iae);
                                } catch (InvocationTargetException ite) {
                                    if (ite.getCause() != null
                                            &&  ite.getCause()
                                            instanceof AbstractMethodError)
                                        throw new SqlToolError(
                                            SqltoolRB.sqlarray_badjvm
                                            .getString());
                                    throw new RuntimeException(ite);
                                }
                                
                                break;
                            default:
                                ps.setString(
                                    i + 1,
                                    (((dataVals[i].length() < 1 && autonulls[i])
                                      || dataVals[i].equals(nullRepToken))
                                     ? null
                                     : dataVals[i]));
                        }
                    }
                    currentFieldName = null;
                }

                retval = ps.executeUpdate();

                if (retval != 1)
                    throw new RowError(
                            SqltoolRB.inputrec_modified.getString(retval));

                if (dsvRecordsPerCommit > 0
                    && (recCount - rejectCount) % dsvRecordsPerCommit == 0) {
                    shared.jdbcConn.commit();
                    shared.possiblyUncommitteds = false;
                } else {
                    shared.possiblyUncommitteds = true;
                }
            } catch (NumberFormatException nfe) {
                throw new RowError(null, nfe);
            } catch (SQLException se) {
                throw new RowError(null, se);
            } } catch (RowError re) {
                rejectCount++;
                if (rejectWriter != null || rejectReportWriter != null) {
                    if (rejectWriter != null)
                        rejectWriter.print(curLine + dsvRowDelim);
                    if (rejectReportWriter != null)
                        genRejectReportRecord(rejectReportWriter,
                                rejectCount, lineCount,
                                currentFieldName, re.getMessage(),
                                re.getCause());
                } else {
                    importAborted = true;
                    throw new SqlToolError(
                            SqltoolRB.dsv_recin_fail.getString(
                                    lineCount, currentFieldName)
                            + ((re.getMessage() == null)
                                    ? "" : ("  " + re.getMessage())),
                            re.getCause());
                }
            }
        } finally {
            if (ps != null) try {
                ps.close();
            } catch (SQLException se) {
                
                
            } finally {
                ps = null;  
            }
            try {
                if (dsvRecordsPerCommit > 0
                    && (recCount - rejectCount) % dsvRecordsPerCommit != 0) {
                    
                    
                    
                    
                    shared.jdbcConn.commit();
                    shared.possiblyUncommitteds = false;
                }
                if (doResetAutocommit) shared.jdbcConn.setAutoCommit(true);
            } catch (SQLException se) {
                throw new SqlToolError(
                        SqltoolRB.rpc_commit_failure.getString(), se);
            }
            String summaryString = null;
            if (recCount > 0) {
                summaryString = SqltoolRB.dsv_import_summary.getString(
                        ((skipPrefix == null)
                                  ? "" : ("'" + skipPrefix + "'-")),
                        Integer.toString(skipCount),
                        Integer.toString(rejectCount),
                        Integer.toString(recCount - rejectCount),
                        (importAborted ? "importAborted" : null));
                stdprintln(summaryString);
            }
            try {
                if (recCount > rejectCount && dsvRecordsPerCommit < 1
                        && !shared.jdbcConn.getAutoCommit())
                    stdprintln(SqltoolRB.insertions_notcommitted.getString());
            } catch (SQLException se) {
                stdprintln(SqltoolRB.autocommit_fetchfail.getString());
                stdprintln(SqltoolRB.insertions_notcommitted.getString());
                
                
            }
            if (rejectWriter != null) rejectWriter.flush();
            if (rejectReportWriter != null && rejectCount > 0) {
                rejectReportWriter.println(
                        SqltoolRB.rejectreport_bottom.getString(
                        summaryString, revnum));
                writeFooter(rejectReportWriter, dsvRejectReport);
                rejectReportWriter.flush();
            }
        }
        } finally {
            if (rejectWriter != null) try {
                rejectWriter.close();
            } finally {
                rejectWriter = null;  
            }
            if (rejectReportWriter != null) try {
                rejectReportWriter.close();
            } finally {
                rejectReportWriter = null;  
            }
            if (rejectCount == 0) {
                if (rejectFile != null && rejectFile.exists()
                        && !rejectFile.delete())
                    errprintln(SqltoolRB.dsv_rejectfile_purgefail.getString(
                            rejectFile.toString()));
                if (rejectReportFile != null && !rejectReportFile.delete())
                    errprintln(SqltoolRB.dsv_rejectreport_purgefail.getString(
                            rejectReportFile.toString()));
                
            }
        }
        } finally {
            if (dsvReader.isOpen()) try {
                dsvReader.close();
            } catch (Exception ioe) {
                
                logger.error(
                        SqltoolRB.inputfile_closefail.getString() + ": " + ioe);
            }
        }
    }

    protected static void appendLine(StringBuffer sb, String s) {
        sb.append(s + LS);
    }

    
    private static String[] genWinArgs(String monolithic) {
        List<String> list = new ArrayList<String>();
        list.add("cmd.exe");
        list.add("/y");
        list.add("/c");
        Matcher m = wincmdPattern.matcher(monolithic);
        while (m.find()) for (int i = 1; i <= m.groupCount(); i++) {
            if (m.group(i) == null) continue;
            if (m.group(i).length() > 1 && m.group(i).charAt(0) == '"') {
                list.add(m.group(i).substring(1, m.group(i).length() - 1));
                continue;
            }
            list.addAll(Arrays.asList(m.group(i).split("\\s+", -1)));
        }
        return list.toArray(new String[] {});
    }

    private void genRejectReportRecord(PrintWriter pw, int rCount,
            int lCount, String field, String eMsg, Throwable cause) {
        pw.println(SqltoolRB.rejectreport_row.getString(
                "sqltool-" + ((rCount % 2 == 0) ? "even" : "odd"),
                Integer.toString(rCount),
                Integer.toString(lCount),
                ((field == null) ? "" : field),
                (((eMsg == null) ? "" : eMsg)
                        + ((eMsg == null || cause == null) ? "" : "<HR/>")
                        + ((cause == null) ? "" : (
                                (cause instanceof SQLException
                                        && cause.getMessage() != null)
                                    ? cause.getMessage()
                                    : cause.toString()
                                )
                        )
                )));
    }

    
    private TokenList seekTokenSource(String nestingCommand)
            throws BadSpecial, IOException, SqlToolError {
        Token token;
        TokenList newTS = new TokenList();
        Pattern endPattern = null;
        Pattern elsePattern = null;
        if (nestingCommand != null)
            if (nestingCommand.equals("if")) {
                endPattern = Pattern.compile("end\\s+" + nestingCommand);
                elsePattern = Pattern.compile("else");
            } else if (nestingCommand.equals("else")) {
                endPattern = Pattern.compile("end\\s+if");
            } else {
                endPattern = Pattern.compile("end\\s+" + nestingCommand);
            }

        String subNestingCommand;
        Matcher inlineNestMatcher;

        while ((token = scanner.yylex()) != null) {
            if (endPattern != null && token.type == Token.PL_TYPE
                    && endPattern.matcher(token.val).matches()) return newTS;
            if (elsePattern != null && token.type == Token.PL_TYPE
                    && elsePattern.matcher(token.val).matches()) {
                assert token.nestedBlock == null:
                        "else statement's .nested block not null";
                token.nestedBlock = seekTokenSource("else");
                newTS.add(token);
                return newTS;
            }
            inlineNestMatcher = inlineNestMatcher(token);
            if (inlineNestMatcher != null) {
                processInlineBlock(token,
                        inlineNestMatcher.group(1),
                        inlineNestMatcher.group(2));
            } else {
                subNestingCommand = nestingCommand(token);
                if (subNestingCommand != null) 
                    token.nestedBlock = seekTokenSource(subNestingCommand);
            }
            newTS.add(token);
        }
        if (nestingCommand == null) return newTS;
        throw new BadSpecial(
                SqltoolRB.pl_block_unterminated.getString(nestingCommand));
    }

    
    private void processMacro(Token defToken) throws BadSpecial {
        Matcher matcher;
        Token macroToken;

        if (defToken.val.length() < 1)
            throw new BadSpecial(SqltoolRB.macro_tip.getString());
        int newType = -1;
        StringBuffer newVal = new StringBuffer();
        switch (defToken.val.charAt(0)) {
            case '?':
                stdprintln(SqltoolRB.macro_help.getString());
                break;
            case ':':
                matcher = editMacroPattern.matcher(defToken.val);
                if (!matcher.matches())
                    throw new BadSpecial(SqltoolRB.macro_malformat.getString());
                if (buffer == null) {
                    stdprintln(nobufferYetString);
                    return;
                }
                newVal.append(buffer.val);
                if (matcher.groupCount() > 1 && matcher.group(2) != null
                        && matcher.group(2).length() > 0)
                    newVal.append(matcher.group(2));
                newType = buffer.type;
                if (newVal.length() < 1)
                    throw new BadSpecial(SqltoolRB.macrodef_empty.getString());
                if (newVal.charAt(newVal.length() - 1) == ';')
                    throw new BadSpecial(SqltoolRB.macrodef_semi.getString());
                shared.macros.put(matcher.group(1),
                        new Token(buffer.type, newVal, defToken.line));
                break;
            case '=':
                String defString = defToken.val;
                defString = defString.substring(1).trim();
                if (defString.length() < 1) {
                    for (Map.Entry<String, Token> entry
                            : shared.macros.entrySet())
                        stdprintln(entry.getKey() + " = "
                                + entry.getValue().reconstitute());
                    break;
                }

                matcher = legacyEditMacroPattern.matcher(defString);
                if (matcher.matches()) {
                    if (buffer == null) {
                        stdprintln(nobufferYetString);
                        return;
                    }
                    newVal.append(buffer.val);
                    if (matcher.groupCount() > 1 && matcher.group(2) != null
                            && matcher.group(2).length() > 0)
                        newVal.append(matcher.group(2));
                    newType = buffer.type;
                } else {
                    matcher = spMacroPattern.matcher(defString);
                    if (matcher.matches()) {
                        newVal.append(matcher.group(3));
                        newType = (matcher.group(2).equals("*")
                                ?  Token.PL_TYPE : Token.SPECIAL_TYPE);
                    } else {
                        matcher = sqlMacroPattern.matcher(defString);
                        if (!matcher.matches())
                            throw new BadSpecial(
                                    SqltoolRB.macro_malformat.getString());
                        newVal.append(matcher.group(2));
                        newType = Token.SQL_TYPE;
                    }
                }
                if (newVal.length() < 1)
                    throw new BadSpecial(SqltoolRB.macrodef_empty.getString());
                if (newVal.charAt(newVal.length() - 1) == ';')
                    throw new BadSpecial(SqltoolRB.macrodef_semi.getString());
                shared.macros.put(matcher.group(1),
                        new Token(newType, newVal, defToken.line));
                break;
            default:
                matcher = useFnPattern.matcher(defToken.val);
                if (matcher.matches()) {
                    macroToken = shared.macros.get(matcher.group(1) + ')');
                    if (macroToken == null)
                        throw new BadSpecial(
                                SqltoolRB.macro_undefined.getString(
                                matcher.group(1) + "...)"));
                    String[] splitVars = null;
                    if (matcher.groupCount() > 1 && matcher.group(2) != null
                            && matcher.group(2).length() > 0) {
                        
                        splitVars = matcher.group(2).split("\\s*,\\s*", -1);
                    } else {
                        splitVars = new String[0];
                    }
                    String thirdGroup = (matcher.groupCount() > 2
                            && matcher.group(3) != null)
                        ? matcher.group(3) : null;
                    preempt = thirdGroup != null && thirdGroup.endsWith(";");
                    if (preempt) {
                        if (thirdGroup.length() == 1) {
                            thirdGroup = null;
                        } else {
                            thirdGroup = thirdGroup.substring(0,
                                    thirdGroup.length() - 1);
                        }
                    }
                    Matcher templateM = fnParamPat.matcher(macroToken.val);
                    int prevEnd = 0;
                    String varVal;
                    int varNum;
                    setBuf(macroToken);
                    buffer.val = "";
                    buffer.line = defToken.line;
                    while (templateM.find()) {
                        buffer.val += macroToken.val
                                .substring(prevEnd, templateM.start());
                        varNum = Integer.valueOf(
                                templateM.group(templateM.groupCount()));
                        varVal = (varNum > 0 && varNum <= splitVars.length)
                                ? splitVars[varNum-1] : null;
                        if (varVal == null && (templateM.groupCount() < 2 ||
                                templateM.group(1) == null ||
                                templateM.group(1).length() < 1))
                            throw new BadSpecial(
                                    SqltoolRB.plvar_undefined.getString(
                                    templateM.group(templateM.groupCount())));
                        if (varVal != null) buffer.val += varVal;
                        prevEnd = templateM.end();
                    }
                    buffer.val += macroToken.val.substring(prevEnd);
                    if (thirdGroup != null) buffer.val += thirdGroup;
                    return;
                }

                matcher = useMacroPattern.matcher(defToken.val);
                if (!matcher.matches())
                    throw new BadSpecial(SqltoolRB.macro_malformat.getString());
                macroToken = shared.macros.get(matcher.group(1));
                if (macroToken == null)
                    throw new BadSpecial(SqltoolRB.macro_undefined.getString(
                            matcher.group(1)));
                setBuf(macroToken);
                buffer.line = defToken.line;
                if (matcher.groupCount() > 1 && matcher.group(2) != null
                        && matcher.group(2).length() > 0)
                    buffer.val += matcher.group(2);
                preempt = matcher.group(matcher.groupCount()).equals(";");
        }
    }

    
    public static byte[] hexCharOctetsToBytes(String hexChars) {
        int chars = hexChars.length();
        if (chars != (chars / 2) * 2)
            throw new NumberFormatException("Hex character lists contains "
                + "an odd number of characters: " + chars);
        byte[] ba = new byte[chars/2];
        int offset = 0;
        char c;
        int octet;
        for (int i = 0; i < chars; i++) {
            octet = 0;
            c = hexChars.charAt(i);
            if (c >= 'a' && c <= 'f') {
                octet += 10 + c - 'a';
            } else if (c >= 'A' && c <= 'F') {
                octet += 10 + c - 'A';
            } else if (c >= '0' && c <= '9') {
                octet += c - '0';
            } else {
                throw new NumberFormatException(
                    "Non-hex character in input at offset " + i + ": " + c);
            }
            octet = octet << 4;
            c = hexChars.charAt(++i);
            if (c >= 'a' && c <= 'f') {
                octet += 10 + c - 'a';
            } else if (c >= 'A' && c <= 'F') {
                octet += 10 + c - 'A';
            } else if (c >= '0' && c <= '9') {
                octet += c - '0';
            } else {
                throw new NumberFormatException(
                    "Non-hex character in input at offset " + i + ": " + c);
            }

            ba[offset++] = (byte) octet;
        }
        assert ba.length == offset:
            "Internal accounting problem.  Expected to fill buffer of "
            + "size "+ ba.length + ", but wrote only " + offset + " bytes";
        return ba;
    }

    
    public static byte[] bitCharsToBytes(String hexChars) {
        if (hexChars == null) throw new NullPointerException();
        
        throw new NumberFormatException(
                "Sorry.  Bit exporting not supported yet");
    }

    private void requireConnection() throws SqlToolError {
        if (shared.jdbcConn == null)
            throw new SqlToolError(SqltoolRB.no_required_conn.getString());
    }

    
    public static String getBanner(Connection c) {
        try {
            DatabaseMetaData md = c.getMetaData();
            return (md == null)
                    ? null
                    : SqltoolRB.jdbc_established.getString(
                            md.getDatabaseProductName(),
                            md.getDatabaseProductVersion(),
                            md.getUserName(),
                                    (c.isReadOnly() ? "R/O " : "R/W ")
                                    + RCData.tiToString(
                                    c.getTransactionIsolation()));
        } catch (SQLException se) {
            return null;
        }
    }

    private void displayConnBanner() {
        String msg = (shared.jdbcConn == null)
                   ? SqltoolRB.disconnected_msg.getString()
                   : SqlFile.getBanner(shared.jdbcConn);
        stdprintln((msg == null)
                  ? SqltoolRB.connected_fallbackmsg.getString()
                  : msg);
    }

    private String dereferenceAt(String s) throws BadSpecial {
        if (s.indexOf('@') != 0) return s;
        if (baseDir == null)
            throw new BadSpecial(SqltoolRB.illegal_at.getString());
        return baseDir.getPath() + s.substring(1);
    }

    
    public static String escapeHtml(String s) {
        StringBuilder sb = new StringBuilder();
        char[] charArray = s.toCharArray();
        for (char c : charArray) switch (c) {
          case '"':
            sb.append("&quot;");
            break;
          case '\'':
            
            sb.append("&apos;");
            break;
          case '&':
            sb.append("&amp;");
            break;
          case '<':
            sb.append("&lt;");
            break;
          case '>':
            sb.append("&gt;");
            break;
          default:
            sb.append(c);
        }
        return sb.toString();
    }

    
    private void writeHeader(PrintWriter pWriter, String filePath)
            throws BadSpecial, SqlToolError {
        char[] readBfr = new char[1024];
        int i;
        StringWriter sWriter = new StringWriter();
        InputStreamReader isr = null;
        String str;
        try {
            InputStream is = (topHtmlFile == null)
                    ? getClass().getResourceAsStream(
                    "sqltool/top-boilerplate.html")
                    : new FileInputStream(topHtmlFile);
            if (is == null)
                throw new IOException("Missing resource: "
                    + ((topHtmlFile == null)
                    ? topHtmlFile
                    : "sqltool/top-boilerplate"));
            isr = new InputStreamReader(is);
            while ((i = isr.read(readBfr)) > -1)
                sWriter.write(readBfr, 0, i);
            readBfr = null;
            str = sWriter.toString();
            sWriter.close();
        } catch (Exception e) {
            throw new BadSpecial(
                    SqltoolRB.file_writefail.getString(filePath), e);
        } finally {
            try {
                if (isr != null) isr.close();
            } catch (IOException ioe) {
                
            }
        }
        pWriter.write(dereference(str.replaceAll("\\r?\\n", LS), true));
    }

    
    private void writeFooter(PrintWriter pwQuery, String filePath)
            throws SqlToolError {
        char[] readBfr = new char[1024];
        int i;
        StringWriter sWriter = new StringWriter();
        InputStreamReader isr = null;
        String str;
        try {
            InputStream is = (bottomHtmlFile == null)
                    ? getClass().getResourceAsStream(
                    "sqltool/bottom-boilerplate.html")
                    : new FileInputStream(bottomHtmlFile);
            if (is == null)
                throw new IOException("Missing resource: "
                    + ((bottomHtmlFile == null)
                    ? bottomHtmlFile
                    : "sqltool/bottom-boilerplate"));
            isr = new InputStreamReader(is);
            while ((i = isr.read(readBfr)) > -1) sWriter.write(readBfr, 0, i);
            readBfr = null;
            str = sWriter.toString();
            sWriter.close();
        } catch (Exception e) {
            throw new SqlToolError(
                    SqltoolRB.file_writefail.getString(filePath), e);
        } finally {
            try {
                if (isr != null) isr.close();
            } catch (IOException ioe) {
                
            }
        }
        pwQuery.write(dereference(str.replaceAll("\\r?\\n", LS), true));
    }

    private void processInlineBlock(
            Token t, String ifCmdText, String nestingText)
            throws BadSpecial, IOException, SqlToolError  {
        assert t.nestedBlock == null:
            "Inline-nest command has .nestBlock pre-populated";
        SqlFileScanner storedScanner = scanner;
        try {
            scanner = new SqlFileScanner(new StringReader(nestingText + '\n'));
            scanner.setStdPrintStream(shared.psStd);
            scanner.setRawLeadinPrompt("");
            scanner.setInteractive(interactive);
            t.nestedBlock = seekTokenSource(null);
        } finally {
            scanner = storedScanner;
        }
        t.val = ifCmdText;
    }
}
