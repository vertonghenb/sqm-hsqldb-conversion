package org.hsqldb.util;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.Vector;
public class CodeSwitcher {
    private static final String ls = System.getProperty("line.separator",
        "\n");
    private Vector           vList;
    private Vector           vSwitchOn;
    private Vector           vSwitchOff;
    private Vector           vSwitches;
    private static final int MAX_LINELENGTH = 82;
    public static void main(String[] a) {
        CodeSwitcher s = new CodeSwitcher();
        if (a.length == 0) {
            showUsage();
            return;
        }
        File listFile = null;
        File baseDir  = null;
        for (int i = 0; i < a.length; i++) {
            String p = a[i];
            if (p.startsWith("+")) {
                s.vSwitchOn.addElement(p.substring(1));
            } else if (p.startsWith("--basedir=")) {
                baseDir = new File(p.substring("--basedir=".length()));
            } else if (p.startsWith("--pathlist=")) {
                listFile = new File(p.substring("--pathlist=".length()));
            } else if (p.startsWith("-")) {
                s.vSwitchOff.addElement(p.substring(1));
            } else {
                s.addDir(p);
            }
        }
        if (baseDir != null) {
            if (listFile == null) {
                System.err.println(
                    "--basedir= setting ignored, since only used for list files");
            } else {
                if (!baseDir.isDirectory()) {
                    System.err.println("Skipping listfile since basedir '"
                                       + baseDir.getAbsolutePath()
                                       + "' is not a directory");
                    listFile = null;
                }
            }
        }
        if (listFile != null) {
            try {
                BufferedReader br =
                    new BufferedReader(new FileReader(listFile));
                String st, p;
                int    hashIndex;
                File   f;
                while ((st = br.readLine()) != null) {
                    hashIndex = st.indexOf('#');
                    p         = ((hashIndex > -1) ? st.substring(0, hashIndex)
                                                  : st).trim();
                    if (p.length() < 1) {
                        continue;
                    }
                    f = (baseDir == null) ? (new File(p))
                                          : (new File(baseDir, p));
                    if (f.isFile()) {
                        s.addDir(f);
                    } else {
                        System.err.println("Skipping non-file '" + p.trim()
                                           + "'");
                    }
                }
            } catch (Exception e) {
                System.err.println("Failed to read pathlist file '"
                                   + listFile.getAbsolutePath() + "'");
            }
        }
        if (s.size() < 1) {
            printError("No path specified, or no specified paths qualify");
            showUsage();
        }
        s.process();
        if (s.vSwitchOff.size() == 0 && s.vSwitchOn.size() == 0) {
            s.printSwitches();
        }
    }
    public int size() {
        return (vList == null) ? 0
                               : vList.size();
    }
    static void showUsage() {
        System.out.print("Usage: java CodeSwitcher paths|{--pathlist=listfile} "
                         + "[{+|-}label...] [+][-]\n"
                         + "If no labels are specified then all used\n"
                         + "labels in the source code are shown.\n"
                         + "Use +MODE to switch on the things labeld MODE\n"
                         + "Use -MODE to switch off the things labeld MODE\n"
                         + "Path: Any number of path or files may be\n"
                         + "specified. Use . for the current directory\n"
                         + "(including sub-directories).\n"
                         + "Example: java CodeSwitcher +JAVA2 .\n"
                         + "This example switches on code labeled JAVA2\n"
                         + "in all *.java files in the current directory\n"
                         + "and all subdirectories.\n");
    }
    CodeSwitcher() {
        vList      = new Vector();
        vSwitchOn  = new Vector();
        vSwitchOff = new Vector();
        vSwitches  = new Vector();
    }
    void process() {
        int len = vList.size();
        for (int i = 0; i < len; i++) {
            System.out.print(".");
            String file = (String) vList.elementAt(i);
            if (!processFile(file)) {
                System.out.println("in file " + file + " !");
            }
        }
        System.out.println("");
    }
    void printSwitches() {
        System.out.println("Used labels:");
        for (int i = 0; i < vSwitches.size(); i++) {
            System.out.println((String) (vSwitches.elementAt(i)));
        }
    }
    void addDir(String path) {
        addDir(new File(path));
    }
    void addDir(File f) {
        if (f.isFile() && f.getName().endsWith(".java")) {
            vList.addElement(f.getPath());
        } else if (f.isDirectory()) {
            File[] list = f.listFiles();
            for (int i = 0; i < list.length; i++) {
                addDir(list[i]);
            }
        }
    }
    boolean processFile(String name) {
        File    f         = new File(name);
        File    fnew      = new File(name + ".new");
        int     state     = 0;    
        boolean switchoff = false;
        boolean working   = false;
        try {
            Vector v  = getFileLines(f);
            Vector v1 = new Vector(v.size());
            for (int i = 0; i < v.size(); i++) {
                v1.addElement(v.elementAt(i));
            }
            for (int i = 0; i < v.size(); i++) {
                String line = (String) v.elementAt(i);
                if (line == null) {
                    break;
                }
                if (working) {
                    if (line.equals("/*") || line.equals("*/")) {
                        v.removeElementAt(i--);
                        continue;
                    }
                }
                if (line.startsWith("//#")) {
                    if (line.startsWith("//#ifdef ")) {
                        if (state != 0) {
                            printError("'#ifdef' not allowed inside '#ifdef'");
                            return false;
                        }
                        state = 1;
                        String s = line.substring(9);
                        if (vSwitchOn.indexOf(s) != -1) {
                            working   = true;
                            switchoff = false;
                        } else if (vSwitchOff.indexOf(s) != -1) {
                            working = true;
                            v.insertElementAt("/*", ++i);
                            switchoff = true;
                        }
                        if (vSwitches.indexOf(s) == -1) {
                            vSwitches.addElement(s);
                        }
                    } else if (line.startsWith("//#ifndef ")) {
                        if (state != 0) {
                            printError(
                                "'#ifndef' not allowed inside '#ifdef'");
                            return false;
                        }
                        state = 1;
                        String s = line.substring(10);
                        if (vSwitchOff.indexOf(s) != -1) {
                            working   = true;
                            switchoff = false;
                        } else if (vSwitchOn.indexOf(s) != -1) {
                            working = true;
                            v.insertElementAt("/*", ++i);
                            switchoff = true;
                        }
                        if (vSwitches.indexOf(s) == -1) {
                            vSwitches.addElement(s);
                        }
                    } else if (line.startsWith("//#else")) {
                        if (state != 1) {
                            printError("'#else' without '#ifdef'");
                            return false;
                        }
                        state = 2;
                        if (!working) {}
                        else if (switchoff) {
                            if (v.elementAt(i - 1).equals("")) {
                                v.insertElementAt("*/", i - 1);
                                i++;
                            } else {
                                v.insertElementAt("*/", i++);
                            }
                            switchoff = false;
                        } else {
                            v.insertElementAt("/*", ++i);
                            switchoff = true;
                        }
                    } else if (line.startsWith("//#endif")) {
                        if (state == 0) {
                            printError("'#endif' without '#ifdef'");
                            return false;
                        }
                        state = 0;
                        if (working && switchoff) {
                            if (v.elementAt(i - 1).equals("")) {
                                v.insertElementAt("*/", i - 1);
                                i++;
                            } else {
                                v.insertElementAt("*/", i++);
                            }
                        }
                        working = false;
                    } else {}
                }
            }
            if (state != 0) {
                printError("'#endif' missing");
                return false;
            }
            boolean filechanged = false;
            for (int i = 0; i < v.size(); i++) {
                if (!v1.elementAt(i).equals(v.elementAt(i))) {
                    filechanged = true;
                    break;
                }
            }
            if (!filechanged) {
                return true;
            }
            writeFileLines(v, fnew);
            File fbak = new File(name + ".bak");
            fbak.delete();
            f.renameTo(fbak);
            File fcopy = new File(name);
            fnew.renameTo(fcopy);
            fbak.delete();
            return true;
        } catch (Exception e) {
            printError(e.toString());
            return false;
        }
    }
    static Vector getFileLines(File f) throws IOException {
        LineNumberReader read = new LineNumberReader(new FileReader(f));
        Vector           v    = new Vector();
        for (;;) {
            String line = read.readLine();
            if (line == null) {
                break;
            }
            v.addElement(line);
        }
        read.close();
        return v;
    }
    static void writeFileLines(Vector v, File f) throws IOException {
        FileWriter write = new FileWriter(f);
        for (int i = 0; i < v.size(); i++) {
            write.write((String) v.elementAt(i));
            write.write(ls);
        }
        write.flush();
        write.close();
    }
    static void printError(String error) {
        System.out.println("");
        System.out.println("ERROR: " + error);
    }
}