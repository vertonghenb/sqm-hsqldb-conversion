


package org.hsqldb.cmdline;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PipedReader;
import java.io.PipedWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import org.hsqldb.lib.FrameworkLogger;
import org.hsqldb.lib.RCData;
import org.hsqldb.cmdline.sqltool.Token;




public class SqlTool {
    private static FrameworkLogger logger =
            FrameworkLogger.getLog(SqlTool.class);
    public static final String DEFAULT_RCFILE =
        System.getProperty("user.home") + "/sqltool.rc";
    
    private static final String revString = "$Revision: 4720 $";
    private static final int revStringLength = revString.length();
    private static final String  revnum =
            (revStringLength - " $".length() > "$Revision: ".length())
            ?  revString.substring("$Revision: ".length(),
                    revStringLength - " $".length())
            : "<UNTRACKED>";

    public static final int SQLTOOLERR_EXITVAL = 1;
    public static final int SYNTAXERR_EXITVAL = 11;
    public static final int RCERR_EXITVAL = 2;
    public static final int SQLERR_EXITVAL = 3;
    public static final int IOERR_EXITVAL = 4;
    public static final int FILEERR_EXITVAL = 5;
    public static final int INPUTERR_EXITVAL = 6;
    public static final int CONNECTERR_EXITVAL = 7;

    
    private static String CMDLINE_ID = "cmdline";

    
    public static String LS = System.getProperty("line.separator");

    
    private static class BadCmdline extends Exception {
        static final long serialVersionUID = -2134764796788108325L;
        BadCmdline() {
            
        }
    }

    
    private static BadCmdline bcl = new BadCmdline();

    
    private static class PrivateException extends Exception {
        static final long serialVersionUID = -7765061479594523462L;

        

        PrivateException(String s) {
            super(s);
        }
    }

    public static class SqlToolException extends Exception {
        static final long serialVersionUID = 1424909871915188519L;

        int exitValue = 1;
        SqlToolException(String message, int exitValue) {
            super(message);
            this.exitValue = exitValue;
        }
        SqlToolException(int exitValue, String message) {
            this(message, exitValue);
        }
        SqlToolException(int exitValue) {
            super();
            this.exitValue = exitValue;
        }
    }

    
    private static String promptForPassword(String username)
    throws PrivateException {

        BufferedReader console;
        String         password;

        password = null;

        try {
            console = new BufferedReader(new InputStreamReader(System.in));

            
            System.out.print(SqltoolRB.passwordFor_prompt.getString(
                    RCData.expandSysPropVars(username)));

            
            password = console.readLine();

            if (password == null) {
                password = "";
            } else {
                password = password.trim();
            }
        } catch (IOException e) {
            throw new PrivateException(e.getMessage());
        } finally {
            console = null;  
        }

        return password;
    }

    
    private static void varParser(String inVarString,
                                  Map<String, String> varMap,
                                  boolean lowerCaseKeys)
                                  throws PrivateException {

        int       equals;
        String    var;
        String    val;

        if (varMap == null) {
            throw new IllegalArgumentException(
                    "varMap is null in SqlTool.varParser call");
        }
        if (inVarString == null) {
            throw new IllegalArgumentException(
                    "inVarString is null in SqlTool.varParser call");
        }
        boolean escapesPresent = inVarString.indexOf("\\,") > -1;
        String  varString = escapesPresent ?
                inVarString.replace("\\,", "\u0002")
                : inVarString;

        for (String token : varString.split("\\s*,\\s*")) {
            equals = token.indexOf('=');

            if (equals < 1) {
                throw new PrivateException(
                    SqltoolRB.SqlTool_varset_badformat.getString());
            }

            var = token.substring(0, equals).trim();
            val = token.substring(equals + 1).trim();
            if (escapesPresent) {
                val = val.replace("\u0002", ",");
            }

            if (var.length() < 1) {
                throw new PrivateException(
                    SqltoolRB.SqlTool_varset_badformat.getString());
            }

            if (lowerCaseKeys) {
                var = var.toLowerCase();
            }

            varMap.put(var, val);
        }
    }

    
    public static void main(String[] args) {
        try {
            SqlTool.objectMain(args);
        } catch (SqlToolException fr) {
            System.err.println(
                    (fr.getMessage() == null) ? fr : fr.getMessage());
            System.exit(fr.exitValue);
        }
        System.exit(0);
    }

    
    public static void objectMain(String[] arg) throws SqlToolException {
        logger.finer("Invoking SqlTool");

        
        String  rcFile           = null;
        PipedReader  tmpReader   = null;
        String  sqlText          = null;
        String  driver           = null;
        String  targetDb         = null;
        boolean debug            = false;
        File[]  scriptFiles      = null;
        int     i                = -1;
        boolean listMode         = false;
        boolean interactive      = false;
        boolean noinput          = false;
        boolean noautoFile       = false;
        boolean autoCommit       = false;
        Boolean coeOverride      = null;
        Boolean stdinputOverride = null;
        String  rcParams         = null;
        String  rcUrl            = null;
        String  rcUsername       = null;
        String  rcPassword       = null;
        String  rcCharset        = null;
        String  rcTruststore     = null;
        String  rcTransIso       = null;
        Map<String, String> rcFields = null;
        String  parameter;
        SqlFile[] sqlFiles       = null;
        Connection conn          = null;
        Map<String, String> userVars = new HashMap<String, String>();

        try { 
        try { 
            while ((i + 1 < arg.length))
            if (arg[i + 1].startsWith("--")) {
                i++;

                if (arg[i].length() == 2) {
                    break;             
                }

                parameter = arg[i].substring(2).toLowerCase();

                if (parameter.equals("help")) {
                    System.out.println(SqltoolRB.SqlTool_syntax.getString(
                            revnum, RCData.DEFAULT_JDBC_DRIVER));
                    return;
                }
                if (parameter.equals("abortonerr")) {
                    if (coeOverride != null) {
                        throw new SqlToolException(SYNTAXERR_EXITVAL,
                                SqltoolRB.SqlTool_abort_continue_mutuallyexclusive.getString());
                    }

                    coeOverride = Boolean.FALSE;
                } else if (parameter.equals("continueonerr")) {
                    if (coeOverride != null) {
                        throw new SqlToolException(SYNTAXERR_EXITVAL,
                                SqltoolRB.SqlTool_abort_continue_mutuallyexclusive.getString());
                    }

                    coeOverride = Boolean.TRUE;
                } else if (parameter.startsWith("continueonerr=")) {
                    if (coeOverride != null) {
                        throw new SqlToolException(SYNTAXERR_EXITVAL,
                                SqltoolRB.SqlTool_abort_continue_mutuallyexclusive.getString());
                    }

                    coeOverride = Boolean.valueOf(
                            arg[i].substring("--continueonerr=".length()));
                } else if (parameter.equals("list")) {
                    if (listMode) {
                        throw new SqlToolException(SYNTAXERR_EXITVAL,
                                SqltoolRB.SqlTool_params_redundant.getString());
                    }
                    listMode = true;
                } else if (parameter.equals("rcfile")) {
                    if (++i == arg.length) {
                        throw bcl;
                    }
                    if (rcFile != null) {
                        throw new SqlToolException(SYNTAXERR_EXITVAL,
                                SqltoolRB.SqlTool_params_redundant.getString());
                    }

                    rcFile = arg[i];
                } else if (parameter.startsWith("rcfile=")) {
                    if (rcFile != null) {
                        throw new SqlToolException(SYNTAXERR_EXITVAL,
                                SqltoolRB.SqlTool_params_redundant.getString());
                    }
                    rcFile = arg[i].substring("--rcfile=".length());
                } else if (parameter.equals("setvar")) {
                    if (++i == arg.length) {
                        throw bcl;
                    }

                    try {
                        varParser(arg[i], userVars, false);
                    } catch (PrivateException pe) {
                        throw new SqlToolException(
                                RCERR_EXITVAL, pe.getMessage());
                    }
                } else if (parameter.startsWith("setvar=")) {
                    try {
                        varParser(arg[i].substring("--setvar=".length()),
                                userVars, false);
                    } catch (PrivateException pe) {
                        throw new SqlToolException(
                                RCERR_EXITVAL, pe.getMessage());
                    }
                } else if (parameter.equals("sql")) {
                    noinput = true;    

                    if (++i == arg.length) {
                        throw bcl;
                    }
                    if (sqlText != null) {
                        throw new SqlToolException(SYNTAXERR_EXITVAL,
                                SqltoolRB.SqlTool_params_redundant.getString());
                    }

                    sqlText = arg[i];
                } else if (parameter.startsWith("sql=")) {
                    noinput = true;    
                    if (sqlText != null) {
                        throw new SqlToolException(SYNTAXERR_EXITVAL,
                                SqltoolRB.SqlTool_params_redundant.getString());
                    }
                    sqlText = arg[i].substring("--sql=".length());
                } else if (parameter.equals("debug")) {
                    if (debug) {
                        throw new SqlToolException(SYNTAXERR_EXITVAL,
                                SqltoolRB.SqlTool_params_redundant.getString());
                    }
                    debug = true;
                } else if (parameter.equals("noautofile")) {
                    if (noautoFile) {
                        throw new SqlToolException(SYNTAXERR_EXITVAL,
                                SqltoolRB.SqlTool_params_redundant.getString());
                    }
                    noautoFile = true;
                } else if (parameter.equals("autocommit")) {
                    if (autoCommit) {
                        throw new SqlToolException(SYNTAXERR_EXITVAL,
                                SqltoolRB.SqlTool_params_redundant.getString());
                    }
                    autoCommit = true;
                } else if (parameter.equals("stdinput")) {
                    noinput          = false;
                    if (stdinputOverride != null) {
                        throw new SqlToolException(SYNTAXERR_EXITVAL,
                                SqltoolRB.SqlTool_params_redundant.getString());
                    }
                    stdinputOverride = Boolean.TRUE;
                } else if (parameter.equals("noinput")) {
                    noinput          = true;
                    if (stdinputOverride != null) {
                        throw new SqlToolException(SYNTAXERR_EXITVAL,
                                SqltoolRB.SqlTool_params_redundant.getString());
                    }
                    stdinputOverride = Boolean.FALSE;
                } else if (parameter.equals("driver")) {
                    if (++i == arg.length) {
                        throw bcl;
                    }
                    if (driver != null) {
                        throw new SqlToolException(SYNTAXERR_EXITVAL,
                                SqltoolRB.SqlTool_params_redundant.getString());
                    }

                    driver = arg[i];
                } else if (parameter.startsWith("driver=")) {
                    if (driver != null) {
                        throw new SqlToolException(SYNTAXERR_EXITVAL,
                                SqltoolRB.SqlTool_params_redundant.getString());
                    }
                    driver = arg[i].substring("--driver=".length());
                } else if (parameter.equals("inlinerc")) {
                    if (++i == arg.length) {
                        throw bcl;
                    }
                    if (rcParams != null) {
                        throw new SqlToolException(SYNTAXERR_EXITVAL,
                                SqltoolRB.SqlTool_params_redundant.getString());
                    }

                    rcParams = arg[i];
                } else if (parameter.startsWith("inlinerc=")) {
                    if (rcParams != null) {
                        throw new SqlToolException(SYNTAXERR_EXITVAL,
                                SqltoolRB.SqlTool_params_redundant.getString());
                    }
                    rcParams = arg[i].substring("--inlinerc=".length());
                } else {
                    throw bcl;
                }
            } else if (arg[i + 1].startsWith("-P")
                    || arg[i + 1].startsWith("-p")) {
                i++;
                boolean sepSwitch = arg[i].length() < 3;
                if (sepSwitch) {
                    if (++i == arg.length) {
                        throw bcl;
                    }
                }

                int equalAt = arg[i].indexOf('=');
                if (equalAt < (sepSwitch ? 1 : 3)) {
                    throw new SqlToolException(RCERR_EXITVAL,
                            "Specified var assignment contains no '='");
                }
                userVars.put(arg[i].substring(sepSwitch ? 0 : 2, equalAt),
                        arg[i].substring(equalAt + 1));
            } else {
                break;
            }

            if (!listMode && rcParams == null && ++i != arg.length) {
                
                targetDb = arg[i];
                if (targetDb.equals("-")) targetDb = null;
            }

            int scriptIndex = 0;

            if (sqlText != null) {
                try {
                    tmpReader = new PipedReader();
                    PipedWriter tmpWriter = new PipedWriter(tmpReader);
                    
                    
                    
                    
                    try {
                        tmpWriter.write(sqlText + LS);
                        tmpWriter.flush();
                    } finally {
                        try {
                            tmpWriter.close();
                        } finally {
                            tmpWriter = null;  
                        }
                    }
                } catch (IOException ioe) {
                    throw new SqlToolException(IOERR_EXITVAL,
                            SqltoolRB.sqltempfile_fail.getString(
                            ioe.toString()));
                }
            }

            if (stdinputOverride != null) {
                noinput = !stdinputOverride.booleanValue();
            }

            interactive = (!noinput) && (arg.length <= i + 1);

            if (arg.length == i + 2 && arg[i + 1].equals("-")) {
                if (stdinputOverride == null) {
                    noinput = false;
                }
            } else if (arg.length > i + 1) {

                
                scriptFiles = new File[arg.length - i - 1
                        + ((stdinputOverride == null
                                || !stdinputOverride.booleanValue()) ? 0 : 1)];

                if (debug) {
                    System.err.println("scriptFiles has "
                                       + scriptFiles.length + " elements");
                }

                while (i + 1 < arg.length) {
                    scriptFiles[scriptIndex++] = new File(arg[++i]);
                }

                if (stdinputOverride != null
                        && stdinputOverride.booleanValue()) {
                    scriptFiles[scriptIndex++] = null;
                    noinput                    = true;
                }
            }
        } catch (BadCmdline bcle) {
            throw new SqlToolException(SYNTAXERR_EXITVAL,
                    SqltoolRB.SqlTool_syntax.getString(
                    revnum, RCData.DEFAULT_JDBC_DRIVER));
        }

        RCData conData = null;

        
        if (rcParams != null) {
            rcFields = new HashMap<String, String>();

            try {
                varParser(rcParams, rcFields, true);
            } catch (PrivateException e) {
                throw new SqlToolException(SYNTAXERR_EXITVAL, e.getMessage());
            }

            rcUrl        = rcFields.remove("url");
            rcUsername   = rcFields.remove("user");
            rcCharset    = rcFields.remove("charset");
            rcTruststore = rcFields.remove("truststore");
            rcPassword   = rcFields.remove("password");
            rcTransIso   = rcFields.remove("transiso");

            
            if (rcUrl == null || rcUrl.length() < 1)
                throw new SqlToolException(RCERR_EXITVAL,
                        SqltoolRB.rcdata_inlineurl_missing.getString());
            
            
            if (rcPassword != null && rcPassword.length() > 0)
                throw new SqlToolException(RCERR_EXITVAL,
                        SqltoolRB.rcdata_password_visible.getString());
            if (rcFields.size() > 0) {
                throw new SqlToolException(INPUTERR_EXITVAL,
                        SqltoolRB.rcdata_inline_extravars.getString(
                        rcFields.keySet().toString()));
            }

            if (rcUsername != null && rcPassword == null) try {
                rcPassword   = promptForPassword(rcUsername);
            } catch (PrivateException e) {
                throw new SqlToolException(INPUTERR_EXITVAL,
                        SqltoolRB.password_readfail.getString(e.getMessage()));
            }
            try {
                conData = new RCData(CMDLINE_ID, rcUrl, rcUsername,
                                     rcPassword, driver, rcCharset,
                                     rcTruststore, null, rcTransIso);
            } catch (RuntimeException re) {
                throw re;  
            } catch (Exception e) {
                throw new SqlToolException(RCERR_EXITVAL,
                        SqltoolRB.rcdata_genfromvalues_fail.getString());
            }
        } else if (listMode || targetDb != null) {
            try {
                conData = new RCData(new File((rcFile == null)
                                              ? DEFAULT_RCFILE
                                              : rcFile), targetDb);
            } catch (RuntimeException re) {
                throw re;  
            } catch (Exception e) {
                throw new SqlToolException(RCERR_EXITVAL,
                        SqltoolRB.conndata_retrieval_fail.getString(
                        targetDb, e.getMessage()));
            }
        }

        
            
        

        if (listMode) {
            
            
            
            
            return;
        }

        if (interactive) System.out.print("SqlTool v. " + revnum + '.' + LS);

        if (conData != null) try {
            conn = conData.getConnection(
                driver, System.getProperty("javax.net.ssl.trustStore"));

            conn.setAutoCommit(autoCommit);

            String conBanner;
            if (interactive && (conBanner = SqlFile.getBanner(conn)) != null) {
                System.out.println(conBanner);
            }
        } catch (RuntimeException re) {
            throw re;  
        } catch (Exception e) {
            if (debug) logger.error(e.getClass().getName(), e);

            
            String reportUser = (conData.username == null)
                    ? "<DFLTUSER>" : conData.username;
            throw new SqlToolException(CONNECTERR_EXITVAL,
                    SqltoolRB.connection_fail.getString(conData.url,
                    reportUser, e.getMessage()));
        }

        File[] emptyFileArray      = {};
        File[] singleNullFileArray = { null };
        File   autoFile            = null;

        if (interactive && !noautoFile) {
            autoFile = new File(System.getProperty("user.home")
                                + "/auto.sql");

            if ((!autoFile.isFile()) || !autoFile.canRead()) {
                autoFile = null;
            }
        }

        if (scriptFiles == null) {

            
            
            scriptFiles = (noinput ? emptyFileArray
                                   : singleNullFileArray);
        }

        int numFiles = scriptFiles.length;

        if (tmpReader != null) {
            numFiles += 1;
        }

        if (autoFile != null) {
            numFiles += 1;
        }

        sqlFiles = new SqlFile[numFiles];

        
        int interactiveFileIndex = -1;
        String encoding = (conData == null) ? null : conData.charset;

        try {
            int fileIndex = 0;

            if (autoFile != null) {
                sqlFiles[fileIndex++] = new SqlFile(autoFile, encoding);
            }

            if (tmpReader != null) {
                sqlFiles[fileIndex++] = new SqlFile(
                        tmpReader, "--sql", System.out, null, false, null);
            }

            for (File scriptFile : scriptFiles) {
                if (interactiveFileIndex < 0 && interactive) {
                    interactiveFileIndex = fileIndex;
                }

                sqlFiles[fileIndex++] = (scriptFile == null)
                        ?  (new SqlFile(encoding, interactive))
                        :  (new SqlFile(scriptFile,encoding, interactive));
            }
        } catch (IOException ioe) {
            try {
                if (conn != null) conn.close();
            } catch (Exception e) {
                
            }

            throw new SqlToolException(FILEERR_EXITVAL, ioe.getMessage());
        }
        } finally {
            
            
            tmpReader = null;  
        }

        Map<String, Token> macros = null;
        try {
            for (SqlFile sqlFile : sqlFiles) {
                if (conn != null) sqlFile.setConnection(conn);
                if (userVars.size() > 0) sqlFile.addUserVars(userVars);
                if (macros != null) sqlFile.addMacros(macros);
                if (coeOverride != null)
                    sqlFile.setContinueOnError(coeOverride.booleanValue());

                sqlFile.execute();
                userVars = sqlFile.getUserVars();
                macros = sqlFile.getMacros();
                conn = sqlFile.getConnection();
            }
            
            
        } catch (SqlToolError ste) {
            throw new SqlToolException(SQLTOOLERR_EXITVAL);
        } catch (SQLException se) {
            
            
            throw new SqlToolException(SQLERR_EXITVAL);
        } finally {
            try {
                if (conn != null) conn.close();
            } catch (Exception e) {
                
            }
        }
    }
}
