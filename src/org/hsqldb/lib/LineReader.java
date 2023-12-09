


package org.hsqldb.lib;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;


public class LineReader {

    boolean                   finished = false;
    boolean                   wasCR    = false;
    boolean                   wasEOL   = false;
    HsqlByteArrayOutputStream baOS     = new HsqlByteArrayOutputStream(1024);

    
    final InputStream stream;
    final Charset     charset;
    final String      charsetName;

    public LineReader(InputStream stream, String charsetName) {

        this.stream      = stream;
        this.charsetName = charsetName;
        this.charset     = Charset.forName(charsetName);
    }

    public String readLine() throws IOException {

        if (finished) {
            return null;
        }

        while (true) {
            int c = stream.read();

            if (c == -1) {
                finished = true;

                if (baOS.size() == 0) {
                    return null;
                }

                break;
            }

            switch (c) {

                case '\r' : {
                    wasCR = true;;

                    break;
                }
                case '\n' : {
                    if (wasCR) {
                        wasCR = false;

                        continue;
                    } else {
                        break;
                    }
                }
                default : {
                    baOS.write(c);

                    wasCR = false;

                    continue;
                }
            }

            break;
        }

        
        String string = new String(baOS.getBuffer(), 0, baOS.size(),
                                   charsetName);

        baOS.reset();

        return string;
    }

    public void close() throws IOException {
        stream.close();
    }
}
