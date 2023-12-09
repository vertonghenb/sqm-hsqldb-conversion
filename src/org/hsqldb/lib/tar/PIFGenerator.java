


package org.hsqldb.lib.tar;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.util.Date;


public class PIFGenerator extends ByteArrayOutputStream {

    OutputStreamWriter writer;
    String             name;
    int                fakePid;    
    char               typeFlag;

    public String getName() {
        return name;
    }

    protected PIFGenerator() {

        try {
            writer = new OutputStreamWriter(this, "UTF-8");
        } catch (UnsupportedEncodingException uee) {
            throw new RuntimeException(
                "Serious problem.  JVM can't encode UTF-8", uee);
        }

        fakePid = (int) (new Date().getTime() % 100000L);

        
        
    }

    
    public PIFGenerator(int sequenceNum) {

        this();

        if (sequenceNum < 1) {

            
            throw new IllegalArgumentException("Sequence numbers start at 1");
        }

        typeFlag = 'g';
        name = System.getProperty("java.io.tmpdir") + "/GlobalHead." + fakePid
               + '.' + sequenceNum;
    }

    
    public PIFGenerator(File file) {

        this();

        typeFlag = 'x';

        String parentPath = (file.getParentFile() == null) ? "."
                                                           : file.getParentFile()
                                                               .getPath();

        name = parentPath + "/PaxHeaders." + fakePid + '/' + file.getName();
    }

    
    public void addRecord(String key,
                          boolean b)
                          throws TarMalformatException, IOException {
        addRecord(key, Boolean.toString(b));
    }

    
    public void addRecord(String key,
                          int i) throws TarMalformatException, IOException {
        addRecord(key, Integer.toString(i));
    }

    
    public void addRecord(String key,
                          long l) throws TarMalformatException, IOException {
        addRecord(key, Long.toString(l));
    }

    
    public void addRecord(String key,
                          String value)
                          throws TarMalformatException, IOException {

        if (key == null || value == null || key.length() < 1
                || value.length() < 1) {
            throw new TarMalformatException(RB.zero_write.getString());
        }

        int lenWithoutIlen = key.length() + value.length() + 3;

        
        int lenW = 0;    

        if (lenWithoutIlen < 8) {
            lenW = lenWithoutIlen + 1;    
        } else if (lenWithoutIlen < 97) {
            lenW = lenWithoutIlen + 2;    
        } else if (lenWithoutIlen < 996) {
            lenW = lenWithoutIlen + 3;    
        } else if (lenWithoutIlen < 9995) {
            lenW = lenWithoutIlen + 4;    
        } else if (lenWithoutIlen < 99994) {
            lenW = lenWithoutIlen + 5;
        } else {
            throw new TarMalformatException(RB.pif_toobig.getString(99991));
        }

        writer.write(Integer.toString(lenW));
        writer.write(' ');
        writer.write(key);
        writer.write('=');
        writer.write(value);
        writer.write('\n');
        writer.flush();    
    }
}
