


package org.hsqldb.cmdline;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;




public class SqlToolSprayer {

    public static String LS = System.getProperty("line.separator");
    private static String SYNTAX_MSG =
        "SYNTAX:  java [-D...] SqlToolSprayer 'SQL;' [urlid1 urlid2...]\n"
        + "System properties you may use [default values]:\n"
        + "    sqltoolsprayer.period (in ms.) [500]\n"
        + "    sqltoolsprayer.maxtime (in ms.) [0]\n"
        + "    sqltoolsprayer.monfile (filepath) [none]\n"
        + "    sqltoolsprayer.rcfile (filepath) [none.  SqlTool default used.]";
    static {
        if (!LS.equals("\n")) {
            SYNTAX_MSG = SYNTAX_MSG.replaceAll("\n", LS);
        }
    }

    public static void main(String[] sa) {

        if (sa.length < 1) {
            System.err.println(SYNTAX_MSG);
            System.exit(4);
        }

        long period = ((System.getProperty("sqltoolsprayer.period") == null)
                       ? 500
                       : Integer.parseInt(
                           System.getProperty("sqltoolsprayer.period")));
        long maxtime = ((System.getProperty("sqltoolsprayer.maxtime") == null)
                        ? 0
                        : Integer.parseInt(
                            System.getProperty("sqltoolsprayer.maxtime")));
        String rcFile   = System.getProperty("sqltoolsprayer.rcfile");
        File monitorFile =
            (System.getProperty("sqltoolsprayer.monfile") == null) ? null
                                                                   : new File(
                                                                       System.getProperty(
                                                                           "sqltoolsprayer.monfile"));
        ArrayList<String> urlids = new ArrayList<String>();

        for (int i = 1; i < sa.length; i++) {
            urlids.add(sa[i]);
        }

        if (urlids.size() < 1) {
            System.err.println("No urlids specified.  Nothing to spray.");
            System.exit(5);
        }

        boolean[] status = new boolean[urlids.size()];

        String[] withRcArgs    = {
            "--sql=" +  sa[0], "--rcfile=" + rcFile, null
        };
        String[] withoutRcArgs = {
            "--sql=" + sa[0], null
        };
        String[] sqlToolArgs   = (rcFile == null) ? withoutRcArgs
                                                  : withRcArgs;
        boolean  onefailed     = false;
        long     startTime     = (new Date()).getTime();

        while (true) {
            if (monitorFile != null && !monitorFile.exists()) {
                System.err.println("Required file is gone:  " + monitorFile);
                System.exit(2);
            }

            onefailed = false;

            for (int i = 0; i < status.length; i++) {
                if (status[i]) {
                    continue;
                }

                sqlToolArgs[sqlToolArgs.length - 1] = urlids.get(i);
                
                

                try {
                    SqlTool.objectMain(sqlToolArgs);

                    status[i] = true;

                    System.err.println("Success for instance '"
                                       + urlids.get(i) + "'");
                } catch (SqlTool.SqlToolException se) {
                    onefailed = true;
                }
            }

            if (!onefailed) {
                break;
            }

            if (maxtime == 0 || (new Date()).getTime() > startTime + maxtime) {
                break;
            }

            try {
                Thread.sleep(period);
            } catch (InterruptedException ie) {
                
            }
        }

        ArrayList<String> failedUrlids = new ArrayList<String>();

        
        for (int i = 0; i < status.length; i++) {
            if (status[i] != true) {
                failedUrlids.add(urlids.get(i));
            }
        }

        if (failedUrlids.size() > 0) {
            System.err.println("Failed instances:   " + failedUrlids);
            System.exit(1);
        }

        System.exit(0);
    }
}
