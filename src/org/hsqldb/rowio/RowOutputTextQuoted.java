


package org.hsqldb.rowio;

import org.hsqldb.lib.StringConverter;


public class RowOutputTextQuoted extends RowOutputText {

    public RowOutputTextQuoted(String fieldSep, String varSep,
                               String longvarSep, boolean allQuoted,
                               String encoding) {
        super(fieldSep, varSep, longvarSep, allQuoted, encoding);
    }

    protected String checkConvertString(String s, String sep) {

        if (allQuoted || s.length() == 0 || s.indexOf('\"') != -1
                || (sep.length() > 0 && s.indexOf(sep) != -1)
                || hasUnprintable(s)) {
            s = StringConverter.toQuotedString(s, '\"', true);
        }

        return s;
    }

    private static boolean hasUnprintable(String s) {

        for (int i = 0, len = s.length(); i < len; i++) {
            if (Character.isISOControl(s.charAt(i))) {
                return true;
            }
        }

        return false;
    }
}
