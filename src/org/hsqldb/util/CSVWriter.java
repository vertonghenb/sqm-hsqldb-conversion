package org.hsqldb.util;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
public class CSVWriter {
    private String             newline = System.getProperty("line.separator");
    private OutputStreamWriter writer  = null;
    private int                nbrCols = 0;
    private int                nbrRows = 0;
    public CSVWriter(File file, String encoding) throws IOException {
        if (encoding == null) {
            encoding = System.getProperty("file.encoding");
        }
        FileOutputStream fout = new FileOutputStream(file);
        writer = new OutputStreamWriter(fout, encoding);
    }
    public void writeHeader(String[] header) throws IOException {
        this.nbrCols = header.length;
        doWriteData(header);
    }
    public void writeData(String[] data) throws IOException {
        doWriteData(data);
    }
    public void close() throws IOException {
        this.writer.close();
    }
    private void doWriteData(String[] values) throws IOException {
        for (int i = 0; i < values.length; i++) {
            if (i > 0) {
                this.writer.write(";");
            }
            if (values[i] != null) {
                this.writer.write("\"");
                this.writer.write(this.toCsvValue(values[i]));
                this.writer.write("\"");
            }
        }
        this.writer.write(newline);
        this.nbrRows++;
    }
    private String toCsvValue(String str) {
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < str.length(); i++) {
            char c = str.charAt(i);
            sb.append(c);
            switch (c) {
                case '"' :
                    sb.append('"');
                    break;
            }
        }
        return sb.toString();
    }
}