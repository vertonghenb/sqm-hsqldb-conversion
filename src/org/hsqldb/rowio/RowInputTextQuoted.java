


package org.hsqldb.rowio;

import java.io.IOException;

import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;


public class RowInputTextQuoted extends RowInputText {

    private static final int NORMAL_FIELD   = 0;
    private static final int NEED_END_QUOTE = 1;
    private static final int FOUND_QUOTE    = 2;
    private char[]           qtext;

    public RowInputTextQuoted(String fieldSep, String varSep,
                              String longvarSep, boolean allQuoted) {
        super(fieldSep, varSep, longvarSep, allQuoted);
    }

    public void setSource(String text, int pos, int byteSize) {

        super.setSource(text, pos, byteSize);

        qtext = text.toCharArray();
    }

    protected String getField(String sep, int sepLen,
                              boolean isEnd) throws IOException {

        
        String s = null;

        if (next >= qtext.length || qtext[next] != '\"') {
            return super.getField(sep, sepLen, isEnd);
        }

        try {
            field++;

            StringBuffer sb    = new StringBuffer();
            boolean      done  = false;
            int          state = NORMAL_FIELD;
            int          end   = -1;

            if (!isEnd) {
                end = text.indexOf(sep, next);
            }

            for (; next < qtext.length; next++) {
                switch (state) {

                    case NORMAL_FIELD :
                    default :
                        if (next == end) {
                            next += sepLen;
                            done = true;
                        } else if (qtext[next] == '\"') {

                            
                            state = NEED_END_QUOTE;
                        } else {
                            sb.append(qtext[next]);
                        }
                        break;

                    case NEED_END_QUOTE :
                        if (qtext[next] == '\"') {
                            state = FOUND_QUOTE;
                        } else {
                            sb.append(qtext[next]);
                        }
                        break;

                    case FOUND_QUOTE :
                        if (qtext[next] == '\"') {

                            
                            sb.append(qtext[next]);

                            state = NEED_END_QUOTE;
                        } else {
                            next  += sepLen - 1;
                            state = NORMAL_FIELD;

                            if (!isEnd) {
                                next++;

                                done = true;
                            }
                        }
                        break;
                }

                if (done) {
                    break;
                }
            }

            s = sb.toString();
        } catch (Exception e) {
            Object[] messages = new Object[] {
                new Integer(field), e.toString()
            };

            throw new IOException(
                Error.getMessage(
                    ErrorCode.M_TEXT_SOURCE_FIELD_ERROR, 0, messages));
        }

        return s;
    }
}
