package org.hsqldb.lib;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import org.hsqldb.HsqlDateTime;
public class SimpleLog {
    public static final int LOG_NONE   = 0;
    public static final int LOG_ERROR  = 1;
    public static final int LOG_NORMAL = 2;
    public static final int LOG_DETAIL = 3;
    public static final String   logTypeNameEngine = "ENGINE";
    public static final String[] logTypeNames      = {
        "", "ERROR ", "NORMAL", "DETAIL"
    };
    private PrintWriter  writer;
    private int          level;
    private boolean      isSystem;
    private String       filePath;
    private StringBuffer sb;
    public SimpleLog(String path, int level) {
        this.isSystem = path == null;
        this.filePath = path;
        sb            = new StringBuffer(256);
        setLevel(level);
    }
    private void setupWriter() {
        if (level == LOG_NONE) {
            close();
            return;
        }
        if (writer == null) {
            if (isSystem) {
                writer = new PrintWriter(System.out);
            } else {
                File file = new File(filePath);
                setupLog(file);
            }
        }
    }
    private void setupLog(File file) {
        try {
            FileUtil.getFileUtil().makeParentDirectories(file);
            writer = new PrintWriter(new FileWriter(file, true),
                                     true);
        } catch (Exception e) {
            isSystem = true;
            writer   = new PrintWriter(System.out);
        }
    }
    public int getLevel() {
        return level;
    }
    public void setLevel(int level) {
        this.level = level;
        setupWriter();
    }
    public PrintWriter getPrintWriter() {
        return writer;
    }
    public synchronized void logContext(int atLevel, String message) {
        if (level < atLevel) {
            return;
        }
        sb.append(HsqlDateTime.getSystemTimeString()).append(' ');
        sb.append(logTypeNames[atLevel]).append(' ').append(message);
        writer.println(sb.toString());
        sb.setLength(0);
    }
    public synchronized void logContext(int atLevel, String prefix,
                                        String message, String suffix) {
        if (level < atLevel) {
            return;
        }
        sb.append(HsqlDateTime.getSystemTimeString()).append(' ');
        sb.append(logTypeNames[atLevel]).append(' ').append(prefix);
        sb.append(' ').append(message).append(' ').append(suffix);
        writer.println(sb.toString());
        sb.setLength(0);
    }
    public synchronized void logContext(Throwable t, String message,
                                        int atLevel) {
        if (level == LOG_NONE) {
            return;
        }
        if (writer == null) {
            return;
        }
        sb.append(HsqlDateTime.getSystemTimeString()).append(' ');
        sb.append(logTypeNames[atLevel]).append(' ').append(message);
        Throwable           temp     = new Throwable();
        StackTraceElement[] elements = temp.getStackTrace();
        if (elements.length > 1) {
            sb.append(elements[1].getClassName()).append('.');
            sb.append(elements[1].getMethodName());
        }
        elements = t.getStackTrace();
        if (elements.length > 0) {
            sb.append(elements[0].getClassName()).append('.');
            sb.append(' ').append(elements[0].getMethodName());
        }
        sb.append(' ').append(t.toString());
        writer.println(sb.toString());
        sb.setLength(0);
    }
    public void flush() {
        if (writer != null) {
            writer.flush();
        }
    }
    public void close() {
        if (writer != null && !isSystem) {
            writer.flush();
            writer.close();
        }
        writer = null;
    }
}