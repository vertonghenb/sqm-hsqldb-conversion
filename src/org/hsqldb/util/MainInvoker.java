package org.hsqldb.util;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
public class MainInvoker {
    private static String[] emptyStringArray = new String[0];
    private static void syntaxFailure() {
        System.err.println(SYNTAX_MSG);
        System.exit(2);
    }
    public static void main(String[] sa) {
        if (sa.length > 0 && sa[0].equals("--help")) {
            System.err.println(SYNTAX_MSG);
            System.exit(0);
        }
        ArrayList outList  = new ArrayList();
        int       curInArg = -1;
        try {
            while (++curInArg < sa.length) {
                if (sa[curInArg].length() < 1) {
                    if (outList.size() < 1) {
                        syntaxFailure();
                    }
                    invoke((String) outList.remove(0),
                           (String[]) outList.toArray(emptyStringArray));
                    outList.clear();
                } else {
                    outList.add(sa[curInArg]);
                }
            }
            if (outList.size() < 1) {
                syntaxFailure();
            }
            invoke((String) outList.remove(0),
                   (String[]) outList.toArray(emptyStringArray));
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
    public static String LS = System.getProperty("line.separator");
    private static String SYNTAX_MSG =
        "    java org.hsqldb.util.MainInvoker "
        + "[package1.Class1 [arg1a arg1b...] \"\"]... \\\n"
        + "    packageX.ClassX [argXa argXb...]\n" + "OR\n"
        + "    java org.hsqldb.util.MainInvoker --help\n\n"
        + "Note that you can only invoke classes in 'named' (non-default) "
        + "packages.  Delimit multiple classes with empty strings.";
    static {
        if (!LS.equals("\n")) {
            SYNTAX_MSG = SYNTAX_MSG.replaceAll("\n", LS);
        }
    }
    public static void invoke(String className,
                              String[] args)
                              throws ClassNotFoundException,
                                     NoSuchMethodException,
                                     IllegalAccessException,
                                     InvocationTargetException {
        Class    c;
        Method   method;
        Class[]  stringArrayCA = { emptyStringArray.getClass() };
        Object[] objectArray   = { (args == null) ? emptyStringArray
                                                  : args };
        c      = Class.forName(className);
        method = c.getMethod("main", stringArrayCA);
        method.invoke(null, objectArray);
    }
}