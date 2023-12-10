package org.hsqldb.lib.java;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.DriverManager;
import java.util.Properties;
import java.text.Collator;
import java.io.RandomAccessFile;
public class JavaSystem {
    public static int gcFrequency;
    public static int memoryRecords;
    public static void gc() {
        if ((gcFrequency > 0) && (memoryRecords > gcFrequency)) {
            memoryRecords = 0;
            System.gc();
        }
    }
    public static IOException toIOException(Throwable t) {
        if (t instanceof IOException) {
            return (IOException) t;
        }
        return new IOException(t);
    }
    static final BigDecimal BD_1  = BigDecimal.valueOf(1L);
    static final BigDecimal MBD_1 = BigDecimal.valueOf(-1L);
    public static int precision(BigDecimal o) {
        if (o == null) {
            return 0;
        }
        int precision;
        if (o.compareTo(BD_1) < 0 && o.compareTo(MBD_1) > 0) {
            precision = o.scale();
        } else {
            precision = o.precision();
        }
        return precision;
    }
    public static String toString(BigDecimal o) {
        if (o == null) {
            return null;
        }
        return o.toPlainString();
    }
    public static int compareIngnoreCase(String a, String b) {
        return a.compareToIgnoreCase(b);
    }
    public static double parseDouble(String s) {
        return Double.parseDouble(s);
    }
    public static BigInteger unscaledValue(BigDecimal o) {
        return o.unscaledValue();
    }
    public static void setLogToSystem(boolean value) {
        try {
            PrintWriter newPrintWriter = (value) ? new PrintWriter(System.out)
                                                 : null;
            DriverManager.setLogWriter(newPrintWriter);
        } catch (Exception e) {}
    }
    public static void deleteOnExit(File f) {
        f.deleteOnExit();
    }
    public static void saveProperties(Properties props, String name,
                                      OutputStream os) throws IOException {
        props.store(os, name);
    }
    public static void runFinalizers() {
        System.runFinalizersOnExit(true);
    }
    public static boolean createNewFile(File file) {
        try {
            return file.createNewFile();
        } catch (IOException e) {}
        return false;
    }
    public static void setRAFileLength(RandomAccessFile raFile,
                                       long length) throws IOException {
        raFile.setLength(length);
    }
}