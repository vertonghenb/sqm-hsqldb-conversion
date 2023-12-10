package org.hsqldb.util.preprocessor;
public class Option {
    public static final int DEFAULT   = 0;    
    public static final int BACKUP    = 1<<0; 
    public static final int FILTER    = 1<<1; 
    public static final int INDENT    = 1<<2; 
    public static final int TEST_ONLY = 1<<3; 
    public static final int VERBOSE   = 1<<4; 
    private Option(){}
    public static boolean isDefault(int options) {
        return options == DEFAULT;
    }
    public static int setDefault(int options, boolean _default) {
        return (_default) ? DEFAULT : options;
    }
    public static boolean isBackup(int options) {
        return ((options & BACKUP) != 0);
    }
    public static int setBackup(int options, boolean backup) {
        return (backup) ? (options | BACKUP) : (options & ~BACKUP);
    }
    public static boolean isFilter(int options) {
        return ((options & FILTER) != 0);
    }
    public static int setFilter(int options, boolean filter) {
        return (filter) ? (options | FILTER) : (options & ~FILTER);
    }
    public static boolean isIndent(int options) {
        return ((options & INDENT) != 0);
    }
    public static int setIndent(int options, boolean indent) {
        return (indent) ? (options | INDENT) : (options & ~INDENT);
    }
    public static boolean isTestOnly(int options) {
        return ((options & TEST_ONLY) != 0);
    }
    public static int setTestOnly(int options, boolean testOnly) {
        return (testOnly) ? (options | TEST_ONLY) : (options & ~TEST_ONLY);
    }
    public static boolean isVerbose(int options) {
        return ((options & VERBOSE) != 0);
    }
    public static int setVerbose(int options, boolean verbose) {
        return (verbose) ? (options | VERBOSE) : (options & ~VERBOSE);
    }
}