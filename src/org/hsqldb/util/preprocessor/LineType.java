package org.hsqldb.util.preprocessor;
import java.lang.reflect.Field;
import java.util.Hashtable;
import java.util.Locale;
class LineType {
    static final int UNKNOWN    = 0;
    static final int DEF        = 1;
    static final int DEFINE     = 1;
    static final int ELIF       = 2;
    static final int ELIFDEF    = 3;
    static final int ELIFNDEF   = 4;
    static final int ELSE       = 5;
    static final int ENDIF      = 6;
    static final int ENDINCLUDE = 7;
    static final int HIDDEN     = 8;
    static final int IF         = 9;
    static final int IFDEF      = 10;
    static final int IFNDEF     = 11;
    static final int INCLUDE    = 12;
    static final int UNDEF      = 13;
    static final int UNDEFINE   = 13;
    static final int VISIBLE    = 14;
    private static Hashtable directives;
    private static String[]  labels;
    static synchronized String[] labels() {
        if (labels == null) {
            init();
        }
        return labels;
    }
    static synchronized Hashtable directives() {
        if (directives == null) {
            init();
        }
        return directives;
    }
    private static void init() {
        directives     = new Hashtable();
        labels         = new String[17];
        Field[] fields = LineType.class.getDeclaredFields();
        for (int i = 0, j = 0; i < fields.length; i++) {
            Field field = fields[i];
            if (field.getType().equals(Integer.TYPE)) {
                String label = field.getName();
                try {
                    int value = field.getInt(null);
                    labels[value] = label;
                    switch(value) {
                        case VISIBLE :
                        case HIDDEN : {
                            break;
                        }
                        default : {
                            String key = Line.DIRECTIVE_PREFIX
                                    + label.toLowerCase(Locale.ENGLISH);
                            directives.put(key, new Integer(value));
                            break;
                        }
                    }
                } catch (IllegalArgumentException ex) {
                } catch (IllegalAccessException ex) {
                }
            }
        }
    }
}