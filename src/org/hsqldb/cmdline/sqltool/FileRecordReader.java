package org.hsqldb.cmdline.sqltool;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.io.FileInputStream;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
public class FileRecordReader {
    public static final int INITIAL_CHARBUFFER_SIZE = 10240;
    private File file;
    private InputStreamReader reader;
    private Pattern recordPattern;
    private long postRead;
    private StringBuilder stringBuffer = new StringBuilder();
    private char[] charBuffer = new char[INITIAL_CHARBUFFER_SIZE];
    public FileRecordReader(
            String filePath, String recordDelimiterRegex, String encoding)
            throws FileNotFoundException, UnsupportedEncodingException {
        file = new File(filePath);
        reader = new InputStreamReader(new FileInputStream(file), encoding);
        recordPattern = Pattern.compile(
                "(.*?)(" + recordDelimiterRegex + ").*", Pattern.DOTALL);
    }
    public void close() throws IOException {
        if (reader == null)
            throw new IllegalStateException("File already closed: " + file);
        reader.close();
        reader = null;
    }
    public String getName() {
        return file.getName();
    }
    public String getPath() {
        return file.getPath();
    }
    public String getAbsolutePath() {
        return file.getAbsolutePath();
    }
    public boolean isOpen() {
        return reader != null;
    }
    public static void main(String[] sa) throws IOException {
        if (sa.length != 2)
            throw new IllegalArgumentException(
                    "SYNTAX: java " + FileRecordReader.class.getName()
                    + " file.txt RECORD_DELIM");
        FileRecordReader frr = new FileRecordReader(sa[0], sa[1], "UTF-8");
        int i = 0;
        String r;
        while ((r = frr.nextRecord()) != null)
            System.out.println("Rec #" + (++i) + ":  [" + r + ']');
    }
    public String nextRecord() throws IOException {
        Matcher matcher;
        boolean reloaded = false;
        while (true) {
            matcher = recordPattern.matcher(stringBuffer);
            if (matcher.matches()) {
                String rec = matcher.group(1);
                stringBuffer.delete(0,  matcher.end(2));
                return rec;
            }
            if (reader == null) {
                if (stringBuffer.length() < 1) return null;
                String rec = stringBuffer.toString();
                stringBuffer.setLength(0);
                return rec;
            }
            reload(reloaded);
            reloaded = true;
        }
    }
    private void reload(boolean increaseBuffer) throws IOException {
        if (reader == null)
            throw new IllegalStateException(
                    "Attempt to reload after source file has been closed");
        if (increaseBuffer) charBuffer = new char[charBuffer.length * 2];
        int retVal = reader.read(charBuffer);
        if (retVal > 0)
            stringBuffer.append(charBuffer, 0, retVal);
        else
            close();
    }
}