package org.hsqldb.lib.tar;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
public class PIFData extends HashMap<String, String> {
    static final long serialVersionUID = 3086795680582315773L;
    private static Pattern pifRecordPattern =
        Pattern.compile("\\d+ +([^=]+)=(.*)");
    public Long getSize() {
        return sizeObject;
    }
    private Long sizeObject = null;
    public PIFData(InputStream stream)
    throws TarMalformatException, IOException {
        BufferedReader br = null;
        try {
            br = new BufferedReader(new InputStreamReader(stream, "UTF-8"));
            String  s, k, v;
            Matcher m;
            int     lineNum = 0;
            while ((s = br.readLine()) != null) {
                lineNum++;
                m = pifRecordPattern.matcher(s);
                if (!m.matches()) {
                    throw new TarMalformatException(
                        RB.pif_malformat.getString(lineNum, s));
                }
                k = m.group(1);
                v = m.group(2);
                if (v == null || v.length() < 1) {
                    remove(k);
                } else {
                    put(k, v);
                }
            }
        } finally {
            try {
                stream.close();
            } finally {
                br = null;  
            }
        }
        String sizeString = get("size");
        if (sizeString != null) {
            try {
                sizeObject = Long.valueOf(sizeString);
            } catch (NumberFormatException nfe) {
                throw new TarMalformatException(
                    RB.pif_malformat_size.getString(sizeString));
            }
        }
    }
}