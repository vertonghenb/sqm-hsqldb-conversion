


package org.hsqldb.persist;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.HsqlByteArrayOutputStream;
import org.hsqldb.rowio.RowInputInterface;
import org.hsqldb.rowio.RowInputText;


public class TextFileReader {

    private RandomAccessInterface     dataFile;
    private RowInputInterface         rowIn;
    private TextFileSettings          textFileSettings;
    private String                    header;
    private boolean                   isReadOnly;
    private HsqlByteArrayOutputStream buffer;

    TextFileReader(RandomAccessInterface dataFile,
                   TextFileSettings textFileSettings, RowInputInterface rowIn,
                   boolean isReadOnly) {

        this.dataFile         = dataFile;
        this.textFileSettings = textFileSettings;
        this.rowIn            = rowIn;
        this.isReadOnly       = isReadOnly;
        this.buffer           = new HsqlByteArrayOutputStream(128);
    }

    public RowInputInterface readObject(int pos) {

        boolean hasQuote  = false;
        boolean complete  = false;
        boolean wasCR     = false;
        boolean wasNormal = false;

        buffer.reset();

        pos = findNextUsedLinePos(pos);

        if (pos == -1) {
            return null;
        }

        try {
            dataFile.seek(pos);

            while (!complete) {
                int c = dataFile.read();

                wasNormal = false;

                if (c == -1) {
                    if (buffer.size() == 0) {
                        return null;
                    }

                    complete = true;

                    if (wasCR) {
                        break;
                    }

                    if (!isReadOnly) {
                        dataFile.write(TextFileSettings.BYTES_LINE_SEP, 0,
                                       TextFileSettings.BYTES_LINE_SEP.length);
                        buffer.write(TextFileSettings.BYTES_LINE_SEP);
                    }

                    break;
                }

                switch (c) {

                    case TextFileSettings.DOUBLE_QUOTE_CHAR :
                        wasNormal = true;
                        complete  = wasCR;
                        wasCR     = false;

                        if (textFileSettings.isQuoted) {
                            hasQuote = !hasQuote;
                        }
                        break;

                    case TextFileSettings.CR_CHAR :
                        wasCR = !hasQuote;
                        break;

                    case TextFileSettings.LF_CHAR :
                        complete = !hasQuote;
                        break;

                    default :
                        wasNormal = true;
                        complete  = wasCR;
                        wasCR     = false;
                }

                buffer.write(c);
            }

            if (complete) {
                if (wasNormal) {
                    buffer.setPosition(buffer.size() - 1);
                }

                String rowString;

                try {
                    rowString =
                        buffer.toString(textFileSettings.stringEncoding);
                } catch (UnsupportedEncodingException e) {
                    rowString = buffer.toString();
                }

                ((RowInputText) rowIn).setSource(rowString, pos,
                                                 buffer.size());

                return rowIn;
            }

            return null;
        } catch (IOException e) {
            throw Error.error(ErrorCode.TEXT_FILE_IO, e);
        }
    }

    public int readHeaderLine() {

        boolean complete  = false;
        boolean wasCR     = false;
        boolean wasNormal = false;

        buffer.reset();

        try {
            dataFile.seek(0);
        } catch (IOException e) {
            throw Error.error(ErrorCode.TEXT_FILE_IO, e);
        }

        while (!complete) {
            wasNormal = false;

            int c;

            try {
                c = dataFile.read();

                if (c == -1) {
                    if (buffer.size() == 0) {
                        return 0;
                    }

                    complete = true;

                    if (!isReadOnly) {
                        dataFile.write(TextFileSettings.BYTES_LINE_SEP, 0,
                                       TextFileSettings.BYTES_LINE_SEP.length);
                        buffer.write(TextFileSettings.BYTES_LINE_SEP);
                    }

                    break;
                }
            } catch (IOException e) {
                throw Error.error(ErrorCode.TEXT_FILE);
            }

            switch (c) {

                case TextFileSettings.CR_CHAR :
                    wasCR = true;
                    break;

                case TextFileSettings.LF_CHAR :
                    complete = true;
                    break;

                default :
                    wasNormal = true;
                    complete  = wasCR;
                    wasCR     = false;
            }

            if (wasCR || complete) {
                continue;
            }

            buffer.write(c);
        }

        if (wasNormal) {
            buffer.setPosition(buffer.size() - 1);
        }

        try {
            header = buffer.toString(textFileSettings.stringEncoding);
        } catch (UnsupportedEncodingException e) {
            header = buffer.toString();
        }

        return buffer.size();
    }

    

    
    private int findNextUsedLinePos(int pos) {

        try {
            int     firstPos   = pos;
            int     currentPos = pos;
            boolean wasCR      = false;

            dataFile.seek(pos);

            while (true) {
                int c = dataFile.read();

                currentPos++;

                switch (c) {

                    case TextFileSettings.CR_CHAR :
                        wasCR = true;
                        break;

                    case TextFileSettings.LF_CHAR :
                        wasCR = false;

                        ((RowInputText) rowIn).skippedLine();

                        firstPos = currentPos;
                        break;

                    case ' ' :
                        if (wasCR) {
                            wasCR = false;

                            ((RowInputText) rowIn).skippedLine();
                        }
                        break;

                    case -1 :
                        return -1;

                    default :
                        if (wasCR) {
                            wasCR = false;

                            ((RowInputText) rowIn).skippedLine();
                        }

                        return firstPos;
                }
            }
        } catch (IOException e) {
            throw Error.error(ErrorCode.TEXT_FILE_IO, e);
        }
    }

    public String getHeaderLine() {
        return header;
    }

    public int getLineNumber() {
        return ((RowInputText) rowIn).getLineNumber();
    }
}
