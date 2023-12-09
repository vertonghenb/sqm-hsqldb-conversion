


package org.hsqldb.lib;

import java.util.ArrayList;
import java.util.List;


public class AppendableException extends Exception {

    static final long    serialVersionUID = -1002629580611098803L;
    public static String LS = System.getProperty("line.separator");
    public List<String>  appendages       = null;

    public String getMessage() {

        String message = super.getMessage();

        if (appendages == null) {
            return message;
        }

        StringBuffer sb = new StringBuffer();

        if (message != null) {
            sb.append(message);
        }

        for (String appendage : appendages) {
            if (sb.length() > 0) {
                sb.append(LS);
            }

            sb.append(appendage);
        }

        return sb.toString();
    }

    public void appendMessage(String s) {

        if (appendages == null) {
            appendages = new ArrayList<String>();
        }

        appendages.add(s);
    }

    public AppendableException() {
        
    }

    public AppendableException(String s) {
        super(s);
    }

    public AppendableException(Throwable cause) {
        super(cause);
    }

    public AppendableException(String string, Throwable cause) {
         super(string, cause);
    }
}
