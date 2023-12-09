






package org.hsqldb.cmdline.sqltool;

import java.io.PrintStream;
import org.hsqldb.lib.FrameworkLogger;



public class SqlFileScanner implements TokenSource {

  
  public static final int YYEOF = -1;

  
  private static final int ZZ_BUFFERSIZE = 16384;

  
  public static final int SPECIAL = 12;
  public static final int SQL_DOUBLE_QUOTED = 8;
  public static final int SQL_SINGLE_QUOTED = 6;
  public static final int GOBBLE = 10;
  public static final int RAW = 4;
  public static final int SQL = 2;
  public static final int YYINITIAL = 0;
  public static final int EDIT = 16;
  public static final int PL = 14;
  public static final int PROMPT_CHANGE_STATE = 20;
  public static final int MACRO = 18;

  
  private static final int ZZ_LEXSTATE[] = { 
     0,  0,  1,  2,  3,  3,  4,  4,  5,  5,  6,  6,  7,  7,  8,  8, 
     9,  9, 10, 10, 11, 11
  };

  
  private static final String ZZ_CMAP_PACKED = 
    "\11\0\1\6\1\2\1\0\1\6\1\1\22\0\1\6\1\0\1\3"+
    "\4\0\1\33\2\0\1\5\2\0\1\7\1\32\1\4\12\0\1\31"+
    "\1\10\5\0\1\20\1\11\1\16\1\26\1\12\1\22\1\13\1\0"+
    "\1\14\2\0\1\27\1\0\1\15\1\24\1\25\1\0\1\17\1\0"+
    "\1\21\1\23\6\0\1\30\4\0\1\20\1\11\1\16\1\26\1\12"+
    "\1\22\1\13\1\0\1\14\2\0\1\27\1\0\1\15\1\24\1\25"+
    "\1\0\1\17\1\0\1\21\1\23\uff8a\0";

  
  private static final char [] ZZ_CMAP = zzUnpackCMap(ZZ_CMAP_PACKED);

  
  private static final int [] ZZ_ACTION = zzUnpackAction();

  private static final String ZZ_ACTION_PACKED_0 =
    "\14\0\1\1\2\2\1\3\1\4\1\5\1\6\1\7"+
    "\1\10\3\1\1\11\1\12\1\13\2\14\1\15\2\13"+
    "\1\16\1\17\2\20\1\13\1\0\2\21\1\0\1\22"+
    "\1\23\1\22\1\24\2\25\1\22\2\26\2\22\2\27"+
    "\2\30\2\31\2\32\7\0\2\33\10\0\2\34\2\35"+
    "\1\0\2\36\1\37\3\0\1\40\1\41\3\0\2\42"+
    "\23\0";

  private static int [] zzUnpackAction() {
    int [] result = new int[114];
    int offset = 0;
    offset = zzUnpackAction(ZZ_ACTION_PACKED_0, offset, result);
    return result;
  }

  private static int zzUnpackAction(String packed, int offset, int [] result) {
    int i = 0;       
    int j = offset;  
    int l = packed.length();
    while (i < l) {
      int count = packed.charAt(i++);
      int value = packed.charAt(i++);
      do result[j++] = value; while (--count > 0);
    }
    return j;
  }


  
  private static final int [] ZZ_ROWMAP = zzUnpackRowMap();

  private static final String ZZ_ROWMAP_PACKED_0 =
    "\0\0\0\34\0\70\0\124\0\160\0\214\0\250\0\304"+
    "\0\340\0\374\0\u0118\0\u0134\0\u0150\0\u016c\0\u0150\0\u0150"+
    "\0\u0188\0\u0150\0\u01a4\0\u01c0\0\u0150\0\u01dc\0\u01f8\0\u0214"+
    "\0\u0150\0\u0150\0\u0150\0\u0230\0\u0150\0\u0150\0\u024c\0\u0268"+
    "\0\u0150\0\u0150\0\u0284\0\u0150\0\u02a0\0\u02bc\0\u02d8\0\u0150"+
    "\0\u02f4\0\u0310\0\u032c\0\u0348\0\u0150\0\u0364\0\u0150\0\u0150"+
    "\0\u0380\0\u0150\0\u039c\0\u03b8\0\u03d4\0\u0150\0\u03f0\0\u0150"+
    "\0\u040c\0\u0150\0\u0428\0\u0150\0\u0444\0\u0460\0\u047c\0\u0498"+
    "\0\u04b4\0\u04d0\0\u02a0\0\u04ec\0\u0150\0\u0508\0\u0524\0\u0540"+
    "\0\u055c\0\u0578\0\u0594\0\u05b0\0\u05cc\0\u05e8\0\u0150\0\u0604"+
    "\0\u0150\0\u0620\0\u063c\0\u0150\0\u0150\0\u0658\0\u0674\0\u0690"+
    "\0\u0150\0\u0150\0\u06ac\0\u06c8\0\u06e4\0\u0700\0\u0150\0\u071c"+
    "\0\u0738\0\u0754\0\u0770\0\u078c\0\u07a8\0\u07c4\0\u07e0\0\u07fc"+
    "\0\u0818\0\u0834\0\u0850\0\u086c\0\u0888\0\u08a4\0\u08c0\0\u08dc"+
    "\0\u08f8\0\u0914";

  private static int [] zzUnpackRowMap() {
    int [] result = new int[114];
    int offset = 0;
    offset = zzUnpackRowMap(ZZ_ROWMAP_PACKED_0, offset, result);
    return result;
  }

  private static int zzUnpackRowMap(String packed, int offset, int [] result) {
    int i = 0;  
    int j = offset;  
    int l = packed.length();
    while (i < l) {
      int high = packed.charAt(i++) << 16;
      result[j++] = high | packed.charAt(i++);
    }
    return j;
  }

  
  private static final int [] ZZ_TRANS = zzUnpackTrans();

  private static final String ZZ_TRANS_PACKED_0 =
    "\1\15\1\16\1\17\1\20\1\21\1\22\1\23\1\24"+
    "\1\25\1\26\4\15\1\27\7\15\1\30\1\15\1\31"+
    "\1\32\1\15\1\20\1\33\1\34\1\35\1\36\1\37"+
    "\2\33\1\40\1\41\22\33\1\42\1\33\1\43\1\44"+
    "\1\36\1\37\1\33\1\45\1\40\1\41\22\33\1\42"+
    "\1\46\1\47\1\50\3\46\1\4\23\46\1\51\1\46"+
    "\33\52\1\53\3\54\1\55\30\54\1\7\1\56\1\57"+
    "\31\7\1\60\1\61\1\62\1\60\1\63\2\60\1\64"+
    "\25\60\1\65\1\66\1\60\1\63\2\60\1\64\25\60"+
    "\1\67\1\70\32\60\1\71\1\72\1\60\1\63\2\60"+
    "\1\64\24\60\1\0\1\73\1\74\67\0\1\17\36\0"+
    "\1\75\34\0\1\23\25\0\1\24\2\0\31\24\12\0"+
    "\1\76\40\0\1\77\26\0\1\100\23\0\1\35\36\0"+
    "\1\101\35\0\1\102\26\0\1\44\32\0\1\43\1\44"+
    "\3\0\1\103\25\0\1\46\1\47\1\50\31\46\2\0"+
    "\1\50\31\0\1\46\1\104\1\105\3\46\1\51\1\46"+
    "\1\106\23\46\33\52\34\0\1\60\3\54\1\0\30\54"+
    "\2\0\1\57\33\0\1\62\36\0\1\107\35\0\1\110"+
    "\26\0\1\66\33\0\1\70\33\0\1\72\33\0\1\74"+
    "\31\0\5\75\1\111\26\75\13\0\1\112\32\0\1\113"+
    "\37\0\1\114\15\0\5\101\1\115\26\101\1\102\1\116"+
    "\1\117\31\102\2\0\1\105\31\0\1\46\1\120\1\121"+
    "\3\46\1\106\25\46\5\107\1\122\26\107\1\110\1\123"+
    "\1\124\31\110\4\75\1\125\1\111\26\75\14\0\1\126"+
    "\37\0\1\127\42\0\1\130\4\0\4\101\1\131\1\115"+
    "\26\101\2\0\1\117\33\0\1\121\31\0\4\107\1\132"+
    "\1\122\26\107\2\0\1\124\46\0\1\133\37\0\1\134"+
    "\32\0\1\135\14\0\1\136\1\137\3\0\1\133\37\0"+
    "\1\140\40\0\1\141\16\0\1\137\37\0\1\142\37\0"+
    "\1\133\27\0\1\142\13\0\1\143\2\0\1\144\31\0"+
    "\1\145\27\0\1\146\31\0\1\147\42\0\1\150\25\0"+
    "\1\151\33\0\1\152\36\0\1\153\24\0\1\154\35\0"+
    "\1\155\45\0\1\156\31\0\1\157\32\0\1\160\25\0"+
    "\1\161\35\0\1\162\14\0\1\161\1\136\1\137\31\161"+
    "\12\0\1\161\21\0";

  private static int [] zzUnpackTrans() {
    int [] result = new int[2352];
    int offset = 0;
    offset = zzUnpackTrans(ZZ_TRANS_PACKED_0, offset, result);
    return result;
  }

  private static int zzUnpackTrans(String packed, int offset, int [] result) {
    int i = 0;       
    int j = offset;  
    int l = packed.length();
    while (i < l) {
      int count = packed.charAt(i++);
      int value = packed.charAt(i++);
      value--;
      do result[j++] = value; while (--count > 0);
    }
    return j;
  }


  
  private static final int ZZ_UNKNOWN_ERROR = 0;
  private static final int ZZ_NO_MATCH = 1;
  private static final int ZZ_PUSHBACK_2BIG = 2;

  
  private static final String ZZ_ERROR_MSG[] = {
    "Unkown internal scanner error",
    "Error: could not match input",
    "Error: pushback value was too large"
  };

  
  private static final int [] ZZ_ATTRIBUTE = zzUnpackAttribute();

  private static final String ZZ_ATTRIBUTE_PACKED_0 =
    "\14\0\1\11\1\1\2\11\1\1\1\11\2\1\1\11"+
    "\3\1\3\11\1\1\2\11\2\1\2\11\1\1\1\11"+
    "\1\1\1\0\1\1\1\11\1\0\3\1\1\11\1\1"+
    "\2\11\1\1\1\11\3\1\1\11\1\1\1\11\1\1"+
    "\1\11\1\1\1\11\7\0\1\1\1\11\10\0\1\1"+
    "\1\11\1\1\1\11\1\0\1\1\2\11\3\0\2\11"+
    "\3\0\1\1\1\11\23\0";

  private static int [] zzUnpackAttribute() {
    int [] result = new int[114];
    int offset = 0;
    offset = zzUnpackAttribute(ZZ_ATTRIBUTE_PACKED_0, offset, result);
    return result;
  }

  private static int zzUnpackAttribute(String packed, int offset, int [] result) {
    int i = 0;       
    int j = offset;  
    int l = packed.length();
    while (i < l) {
      int count = packed.charAt(i++);
      int value = packed.charAt(i++);
      do result[j++] = value; while (--count > 0);
    }
    return j;
  }

  
  private java.io.Reader zzReader;

  
  private int zzState;

  
  private int zzLexicalState = YYINITIAL;

  
  private char zzBuffer[] = new char[ZZ_BUFFERSIZE];

  
  private int zzMarkedPos;

  
  private int zzPushbackPos;

  
  private int zzCurrentPos;

  
  private int zzStartRead;

  
  private int zzEndRead;

  
  private int yyline;

  
  private int yychar;

  
  private int yycolumn;

  
  private boolean zzAtBOL = true;

  
  private boolean zzAtEOF;

  
  private boolean zzEOFDone;

  
    static private FrameworkLogger logger =
            FrameworkLogger.getLog(SqlFileScanner.class);
    private StringBuffer commandBuffer = new StringBuffer();
    private boolean interactive;
    private PrintStream psStd = System.out;
    private String magicPrefix;
    private int requestedState = YYINITIAL;
    private String rawLeadinPrompt;
    private boolean specialAppendState;
    
    

    public void setRequestedState(int requestedState) {
        this.requestedState = requestedState;
    }

    
    public void setRawLeadinPrompt(String rawLeadinPrompt) {
        this.rawLeadinPrompt = rawLeadinPrompt;
    }

    private void rawLeadinPrompt() {
        if (!interactive) {
            return;
        }
        assert rawLeadinPrompt != null:
            "Internal assertion failed.  "
            + "Scanner's message Resource Bundle not initialized properly";
        psStd.println(rawLeadinPrompt);
    }

    
    private void trimBuffer() {
        int len = commandBuffer.length();
        commandBuffer.setLength(len -
            ((len > 1 && commandBuffer.charAt(len - 2) == '\r') ? 2 : 1));
    }

    public void setCommandBuffer(String s) {
        commandBuffer.setLength(0);
        commandBuffer.append(s);
    }

    public void setInteractive(boolean interactive) {
        this.interactive = interactive;
    }

    public void setMagicPrefix(String magicPrefix) {
        this.magicPrefix = magicPrefix;
    }

    public void setStdPrintStream(PrintStream psStd) {
        this.psStd = psStd;
    }

    
    private String sqlPrompt = null;
    public void setSqlPrompt(String sqlPrompt)
    {
        this.sqlPrompt = sqlPrompt;
    }
    public String getSqlPrompt() {
        return sqlPrompt;
    }

    
    private String sqltoolPrompt = null;
    public void setSqltoolPrompt(String sqltoolPrompt)
    {
        this.sqltoolPrompt = sqltoolPrompt;
    }
    public String getSqltoolPrompt() {
        return sqltoolPrompt;
    }
    
    private String rawPrompt = null;
    public void setRawPrompt(String rawPrompt)
    {
        this.rawPrompt = rawPrompt;
    }
    public String getRawPrompt() {
        return rawPrompt;
    }

    private void debug(String id, String msg) {
        logger.finest(id + ":  [" + msg + ']');
    }

    public String strippedYytext() {
        String lineString = yytext();
        int len = lineString.length();
        len = len - ((len > 1 && lineString.charAt(len - 2) == '\r') ? 2 : 1);
        return (lineString.substring(0, len));
    }

    
    public void pushbackTrim() {
        String lineString = yytext();
        int len = lineString.length();
        yypushback((len > 1 && lineString.charAt(len - 2) == '\r') ? 2 : 1);
    }

    private void prompt(String s) {
        if (!interactive) return;
        psStd.print(s);
    }

    public void prompt() {
        if (sqltoolPrompt != null) prompt(sqltoolPrompt);
        specialAppendState = (interactive && magicPrefix != null);
        
        if (interactive && magicPrefix != null) {
            psStd.print(magicPrefix);
            magicPrefix = null;
        }
    }


  
  public SqlFileScanner(java.io.Reader in) {
    this.zzReader = in;
  }

  
  public SqlFileScanner(java.io.InputStream in) {
    this(new java.io.InputStreamReader(in));
  }

  
  private static char [] zzUnpackCMap(String packed) {
    char [] map = new char[0x10000];
    int i = 0;  
    int j = 0;  
    while (i < 132) {
      int  count = packed.charAt(i++);
      char value = packed.charAt(i++);
      do map[j++] = value; while (--count > 0);
    }
    return map;
  }


  
  private boolean zzRefill() throws java.io.IOException {

    
    if (zzStartRead > 0) {
      System.arraycopy(zzBuffer, zzStartRead,
                       zzBuffer, 0,
                       zzEndRead-zzStartRead);

      
      zzEndRead-= zzStartRead;
      zzCurrentPos-= zzStartRead;
      zzMarkedPos-= zzStartRead;
      zzPushbackPos-= zzStartRead;
      zzStartRead = 0;
    }

    
    if (zzCurrentPos >= zzBuffer.length) {
      
      char newBuffer[] = new char[zzCurrentPos*2];
      System.arraycopy(zzBuffer, 0, newBuffer, 0, zzBuffer.length);
      zzBuffer = newBuffer;
    }

    
    int numRead = zzReader.read(zzBuffer, zzEndRead,
                                            zzBuffer.length-zzEndRead);

    if (numRead < 0) {
      return true;
    }
    else {
      zzEndRead+= numRead;
      return false;
    }
  }

    
  
  public final void yyclose() throws java.io.IOException {
    zzAtEOF = true;            
    zzEndRead = zzStartRead;  

    if (zzReader != null)
      zzReader.close();
  }


  
  public final void yyreset(java.io.Reader reader) {
    zzReader = reader;
    zzAtBOL  = true;
    zzAtEOF  = false;
    zzEndRead = zzStartRead = 0;
    zzCurrentPos = zzMarkedPos = zzPushbackPos = 0;
    yyline = yychar = yycolumn = 0;
    zzLexicalState = YYINITIAL;
  }


  
  public final int yystate() {
    return zzLexicalState;
  }


  
  public final void yybegin(int newState) {
    zzLexicalState = newState;
  }


  
  public final String yytext() {
    return new String( zzBuffer, zzStartRead, zzMarkedPos-zzStartRead );
  }


  
  public final char yycharat(int pos) {
    return zzBuffer[zzStartRead+pos];
  }


  
  public final int yylength() {
    return zzMarkedPos-zzStartRead;
  }


  
  private void zzScanError(int errorCode) {
    String message;
    try {
      message = ZZ_ERROR_MSG[errorCode];
    }
    catch (ArrayIndexOutOfBoundsException e) {
      message = ZZ_ERROR_MSG[ZZ_UNKNOWN_ERROR];
    }

    throw new Error(message);
  } 


  
  public void yypushback(int number)  {
    if ( number > yylength() )
      zzScanError(ZZ_PUSHBACK_2BIG);

    zzMarkedPos -= number;
  }


  
  private void zzDoEOF() throws java.io.IOException {
    if (!zzEOFDone) {
      zzEOFDone = true;
      yyclose();
    }
  }


  
  public Token yylex() throws java.io.IOException {
    int zzInput;
    int zzAction;

    
    int zzCurrentPosL;
    int zzMarkedPosL;
    int zzEndReadL = zzEndRead;
    char [] zzBufferL = zzBuffer;
    char [] zzCMapL = ZZ_CMAP;

    int [] zzTransL = ZZ_TRANS;
    int [] zzRowMapL = ZZ_ROWMAP;
    int [] zzAttrL = ZZ_ATTRIBUTE;

    while (true) {
      zzMarkedPosL = zzMarkedPos;

      boolean zzR = false;
      for (zzCurrentPosL = zzStartRead; zzCurrentPosL < zzMarkedPosL;
                                                             zzCurrentPosL++) {
        switch (zzBufferL[zzCurrentPosL]) {
        case '\u000B':
        case '\u000C':
        case '\u0085':
        case '\u2028':
        case '\u2029':
          yyline++;
          yycolumn = 0;
          zzR = false;
          break;
        case '\r':
          yyline++;
          yycolumn = 0;
          zzR = true;
          break;
        case '\n':
          if (zzR)
            zzR = false;
          else {
            yyline++;
            yycolumn = 0;
          }
          break;
        default:
          zzR = false;
          yycolumn++;
        }
      }

      if (zzR) {
        
        boolean zzPeek;
        if (zzMarkedPosL < zzEndReadL)
          zzPeek = zzBufferL[zzMarkedPosL] == '\n';
        else if (zzAtEOF)
          zzPeek = false;
        else {
          boolean eof = zzRefill();
          zzEndReadL = zzEndRead;
          zzMarkedPosL = zzMarkedPos;
          zzBufferL = zzBuffer;
          if (eof) 
            zzPeek = false;
          else 
            zzPeek = zzBufferL[zzMarkedPosL] == '\n';
        }
        if (zzPeek) yyline--;
      }
      if (zzMarkedPosL > zzStartRead) {
        switch (zzBufferL[zzMarkedPosL-1]) {
        case '\n':
        case '\u000B':
        case '\u000C':
        case '\u0085':
        case '\u2028':
        case '\u2029':
          zzAtBOL = true;
          break;
        case '\r': 
          if (zzMarkedPosL < zzEndReadL)
            zzAtBOL = zzBufferL[zzMarkedPosL] != '\n';
          else if (zzAtEOF)
            zzAtBOL = false;
          else {
            boolean eof = zzRefill();
            zzMarkedPosL = zzMarkedPos;
            zzEndReadL = zzEndRead;
            zzBufferL = zzBuffer;
            if (eof) 
              zzAtBOL = false;
            else 
              zzAtBOL = zzBufferL[zzMarkedPosL] != '\n';
          }
          break;
        default:
          zzAtBOL = false;
        }
      }
      zzAction = -1;

      zzCurrentPosL = zzCurrentPos = zzStartRead = zzMarkedPosL;
  
      if (zzAtBOL)
        zzState = ZZ_LEXSTATE[zzLexicalState+1];
      else
        zzState = ZZ_LEXSTATE[zzLexicalState];


      zzForAction: {
        while (true) {
    
          if (zzCurrentPosL < zzEndReadL)
            zzInput = zzBufferL[zzCurrentPosL++];
          else if (zzAtEOF) {
            zzInput = YYEOF;
            break zzForAction;
          }
          else {
            
            zzCurrentPos  = zzCurrentPosL;
            zzMarkedPos   = zzMarkedPosL;
            boolean eof = zzRefill();
            
            zzCurrentPosL  = zzCurrentPos;
            zzMarkedPosL   = zzMarkedPos;
            zzBufferL      = zzBuffer;
            zzEndReadL     = zzEndRead;
            if (eof) {
              zzInput = YYEOF;
              break zzForAction;
            }
            else {
              zzInput = zzBufferL[zzCurrentPosL++];
            }
          }
          int zzNext = zzTransL[ zzRowMapL[zzState] + zzCMapL[zzInput] ];
          if (zzNext == -1) break zzForAction;
          zzState = zzNext;

          int zzAttributes = zzAttrL[zzState];
          if ( (zzAttributes & 1) == 1 ) {
            zzAction = zzState;
            zzMarkedPosL = zzCurrentPosL;
            if ( (zzAttributes & 8) == 8 ) break zzForAction;
          }

        }
      }

      
      zzMarkedPos = zzMarkedPosL;

      switch (zzAction < 0 ? zzAction : ZZ_ACTION[zzAction]) {
        case 19: 
          { commandBuffer.append(yytext());
        debug("SQL '", yytext());
        yybegin(SQL);
          }
        case 35: break;
        case 9: 
          { commandBuffer.setLength(0);
    yybegin(SPECIAL);
          }
        case 36: break;
        case 30: 
          { pushbackTrim();
        
        debug("Spl. -- Comment", yytext());
          }
        case 37: break;
        case 10: 
          { commandBuffer.setLength(0);
    yybegin(EDIT);
          }
        case 38: break;
        case 21: 
          { yybegin(YYINITIAL);
    debug("Gobbled", yytext());
    prompt();
          }
        case 39: break;
        case 31: 
          { 
    debug ("/**/ Comment", yytext());
          }
        case 40: break;
        case 8: 
          { return new Token(Token.SQL_TYPE, yyline);
          }
        case 41: break;
        case 2: 
          { prompt();
          }
        case 42: break;
        case 22: 
          { if (commandBuffer.toString().trim().equals(".")) {
        commandBuffer.setLength(0);
        yybegin(RAW);
        rawLeadinPrompt();
        if (rawPrompt != null) prompt(rawPrompt);
    } else {
        requestedState = YYINITIAL;
        yybegin(PROMPT_CHANGE_STATE);
        pushbackTrim();
        return new Token(Token.SPECIAL_TYPE, commandBuffer, yyline);
    }
          }
        case 43: break;
        case 28: 
          { specialAppendState = false;
        commandBuffer.append(yytext());
        
        debug("SQL -- Comment", yytext());
          }
        case 44: break;
        case 17: 
          { if (commandBuffer.length() > 0) commandBuffer.append('\n');
        commandBuffer.append(strippedYytext());
        if (rawPrompt != null) prompt(rawPrompt);
          }
        case 45: break;
        case 26: 
          { yybegin(requestedState);
    prompt();
          }
        case 46: break;
        case 4: 
          { commandBuffer.setLength(0);
    yybegin(MACRO);
          }
        case 47: break;
        case 18: 
          { commandBuffer.append(yytext());
          }
        case 48: break;
        case 11: 
          { specialAppendState = false;
        commandBuffer.append(yytext());
          }
        case 49: break;
        case 25: 
          { requestedState = YYINITIAL;
    yybegin(PROMPT_CHANGE_STATE);
    pushbackTrim();
    return new Token(Token.MACRO_TYPE, commandBuffer, yyline);
          }
        case 50: break;
        case 16: 
          { if (interactive && !specialAppendState) {
            requestedState = YYINITIAL;
            yybegin(PROMPT_CHANGE_STATE);
            pushbackTrim();
            trimBuffer();
            return new Token(Token.BUFFER_TYPE, commandBuffer, yyline);
        }
        specialAppendState = false;
        commandBuffer.append(yytext());
          }
        case 51: break;
        case 29: 
          { yybegin(YYINITIAL);
        prompt();
        return new Token(Token.RAWEXEC_TYPE, commandBuffer, yyline);
          }
        case 52: break;
        case 27: 
          { yybegin(YYINITIAL);
        prompt();
        return new Token(Token.RAW_TYPE, commandBuffer, yyline);
          }
        case 53: break;
        case 14: 
          { specialAppendState = false;
        yybegin(YYINITIAL);
        return new Token(Token.SQL_TYPE, commandBuffer, yyline);
          }
        case 54: break;
        case 33: 
          { 
        debug("Spl. /**/ Comment", yytext());
          }
        case 55: break;
        case 3: 
          { yybegin(GOBBLE);
    return new Token(Token.SYNTAX_ERR_TYPE, yytext(), yyline);
          }
        case 56: break;
        case 20: 
          { commandBuffer.append(yytext());
        yybegin(SQL);
        debug("SQL \"", yytext());
          }
        case 57: break;
        case 1: 
          { setCommandBuffer(yytext());
    yybegin(SQL);
          }
        case 58: break;
        case 23: 
          { requestedState = YYINITIAL;
    yybegin(PROMPT_CHANGE_STATE);
    pushbackTrim();
    return new Token(Token.PL_TYPE, commandBuffer, yyline);
          }
        case 59: break;
        case 6: 
          { 
    debug("Whitespace", yytext());
          }
        case 60: break;
        case 12: 
          { specialAppendState = false;
        commandBuffer.append(yytext());
        if (sqlPrompt != null) prompt(sqlPrompt);
          }
        case 61: break;
        case 24: 
          { requestedState = YYINITIAL;
    yybegin(PROMPT_CHANGE_STATE);
    pushbackTrim();
    return new Token(Token.EDIT_TYPE, commandBuffer, yyline);
          }
        case 62: break;
        case 7: 
          { debug ("-- Comment", yytext());
          }
        case 63: break;
        case 15: 
          { specialAppendState = false;
        commandBuffer.append(yytext());
        yybegin(SQL_SINGLE_QUOTED);
          }
        case 64: break;
        case 5: 
          { commandBuffer.setLength(0);
    yybegin(PL);
          }
        case 65: break;
        case 34: 
          { 
    setCommandBuffer(strippedYytext());
    yybegin(RAW);
    rawLeadinPrompt();
    if (rawPrompt != null) prompt(rawPrompt);
          }
        case 66: break;
        case 32: 
          { specialAppendState = false;
        commandBuffer.append(yytext());
        
        debug("SQL /**/ Comment", yytext());
          }
        case 67: break;
        case 13: 
          { specialAppendState = false;
        commandBuffer.append(yytext());
        yybegin(SQL_DOUBLE_QUOTED);
          }
        case 68: break;
        default: 
          if (zzInput == YYEOF && zzStartRead == zzCurrentPos) {
            zzAtEOF = true;
            zzDoEOF();
            switch (zzLexicalState) {
            case SPECIAL: {
              yybegin(YYINITIAL);
    return new Token(Token.UNTERM_TYPE, commandBuffer, yyline);
            }
            case 115: break;
            case SQL_DOUBLE_QUOTED: {
              yybegin(YYINITIAL);
    return new Token(Token.UNTERM_TYPE, commandBuffer, yyline);
            }
            case 116: break;
            case SQL_SINGLE_QUOTED: {
              yybegin(YYINITIAL);
    return new Token(Token.UNTERM_TYPE, commandBuffer, yyline);
            }
            case 117: break;
            case RAW: {
              yybegin(YYINITIAL);
    return new Token(Token.UNTERM_TYPE, commandBuffer, yyline);
            }
            case 118: break;
            case SQL: {
              yybegin(YYINITIAL);
    return new Token(Token.UNTERM_TYPE, commandBuffer, yyline);
            }
            case 119: break;
            case EDIT: {
              yybegin(YYINITIAL);
    return new Token(Token.UNTERM_TYPE, commandBuffer, yyline);
            }
            case 120: break;
            case PL: {
              yybegin(YYINITIAL);
    return new Token(Token.UNTERM_TYPE, commandBuffer, yyline);
            }
            case 121: break;
            case MACRO: {
              yybegin(YYINITIAL);
    return new Token(Token.UNTERM_TYPE, commandBuffer, yyline);
            }
            case 122: break;
            default:
            return null;
            }
          } 
          else {
            zzScanError(ZZ_NO_MATCH);
          }
      }
    }
  }


}
