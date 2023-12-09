


package org.hsqldb.lib;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.PropertyResourceBundle;
import java.util.Map;
import java.util.HashMap;
import java.util.Locale;
import java.util.ResourceBundle;
import java.util.MissingResourceException;
import java.util.Enumeration;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.io.InputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;





public class RefCapablePropertyResourceBundle {
    private PropertyResourceBundle wrappedBundle;
    private String baseName;
    private String language, country, variant;
    static private Map<ResourceBundle, RefCapablePropertyResourceBundle>
            allBundles =
            new HashMap<ResourceBundle, RefCapablePropertyResourceBundle>();
    public static String LS = System.getProperty("line.separator");
    private Pattern sysPropVarPattern = Pattern.compile(
            "(?s)\\Q${\\E([^}]+?)(?:\\Q:+\\E([^}]+))?\\Q}");
    private Pattern posPattern = Pattern.compile(
            "(?s)\\Q%{\\E(\\d)(?:\\Q:+\\E([^}]+))?\\Q}");
    private ClassLoader loader;  

    public static final int THROW_BEHAVIOR = 0;
    public static final int EMPTYSTRING_BEHAVIOR = 1;
    public static final int NOOP_BEHAVIOR = 2;

    public Enumeration<String> getKeys() {
        return wrappedBundle.getKeys();
    }

    private RefCapablePropertyResourceBundle(String baseName,
            PropertyResourceBundle wrappedBundle, ClassLoader loader) {
        this.baseName = baseName;
        this.wrappedBundle = wrappedBundle;
        Locale locale = wrappedBundle.getLocale();
        this.loader = loader;
        language = locale.getLanguage();
        country = locale.getCountry();
        variant = locale.getVariant();
        if (language.length() < 1) language = null;
        if (country.length() < 1) country = null;
        if (variant.length() < 1) variant = null;
    }

    
    public String getExpandedString(String key, int behavior) {
        String s = getString(key);
        Matcher matcher = sysPropVarPattern.matcher(s);
        int previousEnd = 0;
        StringBuffer sb = new StringBuffer();
        String varName, varValue;
        String condlVal;  
        while (matcher.find()) {
            varName = matcher.group(1);
            condlVal = ((matcher.groupCount() > 1) ? matcher.group(2) : null);
            varValue = System.getProperty(varName);
            if (condlVal != null) {
                
                
                varValue = ((varValue == null)
                        ? ""
                        : condlVal.replaceAll("\\Q$" + varName + "\\E\\b",
                                Matcher.quoteReplacement(varValue)));
            }
            if (varValue == null) switch (behavior) {
                case THROW_BEHAVIOR:
                    throw new RuntimeException(
                            "No Sys Property set for variable '"
                            + varName + "' in property value ("
                            + s + ").");
                case EMPTYSTRING_BEHAVIOR:
                    varValue = "";
                    break;
                case NOOP_BEHAVIOR:
                    break;
                default:
                    throw new RuntimeException(
                            "Undefined value for behavior: " + behavior);
            }
            sb.append(s.substring(previousEnd, matcher.start())
                        + ((varValue == null) ? matcher.group() : varValue));
            previousEnd = matcher.end();
        }
        return (previousEnd < 1) ? s
                                 : (sb.toString() + s.substring(previousEnd));
    }

    
    public String posSubst(String s, String[] subs, int behavior) {
        Matcher matcher = posPattern.matcher(s);
        int previousEnd = 0;
        StringBuffer sb = new StringBuffer();
        String varValue;
        int varIndex;
        String condlVal;  
        while (matcher.find()) {
            varIndex = Integer.parseInt(matcher.group(1)) - 1;
            condlVal = ((matcher.groupCount() > 1) ? matcher.group(2) : null);
            varValue = ((varIndex < subs.length) ? subs[varIndex] : null);
            if (condlVal != null) {
                
                
                varValue = ((varValue == null)
                        ? ""
                        : condlVal.replaceAll("\\Q%" + (varIndex+1) + "\\E\\b",
                                Matcher.quoteReplacement(varValue)));
            }
            
            if (varValue == null) switch (behavior) {
                case THROW_BEHAVIOR:
                    throw new RuntimeException(
                            Integer.toString(subs.length)
                            + " positional values given, but property string "
                            + "contains (" + matcher.group() + ").");
                case EMPTYSTRING_BEHAVIOR:
                    varValue = "";
                    break;
                case NOOP_BEHAVIOR:
                    break;
                default:
                    throw new RuntimeException(
                            "Undefined value for behavior: " + behavior);
            }
            sb.append(s.substring(previousEnd, matcher.start())
                        + ((varValue == null) ? matcher.group() : varValue));
            previousEnd = matcher.end();
        }
        return (previousEnd < 1) ? s
                                 : (sb.toString() + s.substring(previousEnd));
    }

    public String getExpandedString(String key, String[] subs,
            int missingPropertyBehavior, int missingPosValueBehavior) {
        return posSubst(getExpandedString(key, missingPropertyBehavior), subs,
                missingPosValueBehavior);
    }
    public String getString(String key, String[] subs, int behavior) {
        return posSubst(getString(key), subs, behavior);
    }

    
    public String toString() {
        return baseName + " for " + language + " / " + country + " / "
            + variant;
    }

    
    public String getString(String key) {
        String value = wrappedBundle.getString(key);
        if (value.length() < 1) {
            value = getStringFromFile(key);
            
            
            if (value.indexOf('\r') > -1)
                value = value.replaceAll("\\Q\r\n", "\n")
                        .replaceAll("\\Q\r", "\n");
            if (value.length() > 0 && value.charAt(value.length() - 1) == '\n')
                value = value.substring(0, value.length() - 1);
        }
        return RefCapablePropertyResourceBundle.toNativeLs(value);
    }

    
    public static String toNativeLs(String inString) {
        return LS.equals("\n") ? inString : inString.replaceAll("\\Q\n", LS);
    }

    
    public static RefCapablePropertyResourceBundle getBundle(String baseName,
            ClassLoader loader) {
        return getRef(baseName, ResourceBundle.getBundle(baseName,
                Locale.getDefault(), loader), loader);
    }
    
    public static RefCapablePropertyResourceBundle
            getBundle(String baseName, Locale locale, ClassLoader loader) {
        return getRef(baseName,
                ResourceBundle.getBundle(baseName, locale, loader), loader);
    }

    
    static private RefCapablePropertyResourceBundle getRef(String baseName,
            ResourceBundle rb, ClassLoader loader) {
        if (!(rb instanceof PropertyResourceBundle))
            throw new MissingResourceException(
                    "Found a Resource Bundle, but it is a "
                            + rb.getClass().getName(),
                    PropertyResourceBundle.class.getName(), null);
        if (allBundles.containsKey(rb)) return allBundles.get(rb);
        RefCapablePropertyResourceBundle newPRAFP =
                new RefCapablePropertyResourceBundle(baseName,
                        (PropertyResourceBundle) rb, loader);
        allBundles.put(rb, newPRAFP);
        return newPRAFP;
    }

    
    private InputStream getMostSpecificStream(
            String key, String l, String c, String v) {
        final String filePath = baseName.replace('.', '/') + '/' + key
                + ((l == null) ? "" : ("_" + l))
                + ((c == null) ? "" : ("_" + c))
                + ((v == null) ? "" : ("_" + v))
                + ".text";
        
        InputStream is = (InputStream) AccessController.doPrivileged(
            new PrivilegedAction() {

            public InputStream run() {
                return loader.getResourceAsStream(filePath);
            }
        });
        
        
        return (is == null && l != null)
            ? getMostSpecificStream(key, ((c == null) ? null : l),
                    ((v == null) ? null : c), null)
            : is;
    }

    private String getStringFromFile(String key) {
        byte[] ba = null;
        int bytesread = 0;
        int retval;
        InputStream  inputStream =
                getMostSpecificStream(key, language, country, variant);
        if (inputStream == null)
            throw new MissingResourceException(
                    "Key '" + key
                    + "' is present in .properties file with no value, yet "
                    + "text file resource is missing",
                    RefCapablePropertyResourceBundle.class.getName(), key);
        try {
            try {
                ba = new byte[inputStream.available()];
            } catch (RuntimeException re) {
                throw new MissingResourceException(
                    "Resource is too big to read in '" + key + "' value in one "
                    + "gulp.\nPlease run the program with more RAM "
                    + "(try Java -Xm* switches).: " + re,
                    RefCapablePropertyResourceBundle.class.getName(), key);
            } catch (IOException ioe) {
                throw new MissingResourceException(
                    "Failed to read in value for key '" + key + "': " + ioe,
                    RefCapablePropertyResourceBundle.class.getName(), key);
            }
            try {
                while (bytesread < ba.length &&
                        (retval = inputStream.read(
                                ba, bytesread, ba.length - bytesread)) > 0) {
                    bytesread += retval;
                }
            } catch (IOException ioe) {
                throw new MissingResourceException(
                    "Failed to read in value for '" + key + "': " + ioe,
                    RefCapablePropertyResourceBundle.class.getName(), key);
            }
        } finally {
            try {
                inputStream.close();
            } catch (IOException ioe) {
                System.err.println("Failed to close input stream: " + ioe);
            }
        }
        if (bytesread != ba.length) {
            throw new MissingResourceException(
                    "Didn't read all bytes.  Read in "
                      + bytesread + " bytes out of " + ba.length
                      + " bytes for key '" + key + "'",
                    RefCapablePropertyResourceBundle.class.getName(), key);
        }
        try {
            return new String(ba, "ISO-8859-1");
        } catch (UnsupportedEncodingException uee) {
            throw new RuntimeException(uee);
        } catch (RuntimeException re) {
            throw new MissingResourceException(
                "Value for key '" + key + "' too big to convert to String.  "
                + "Please run the program with more RAM "
                + "(try Java -Xm* switches).: " + re,
                RefCapablePropertyResourceBundle.class.getName(), key);
        }
    }
}
