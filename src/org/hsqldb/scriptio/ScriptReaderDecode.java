


package org.hsqldb.scriptio;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.zip.GZIPInputStream;

import org.hsqldb.Database;
import org.hsqldb.Session;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.LineReader;
import org.hsqldb.lib.StringConverter;
import org.hsqldb.persist.Crypto;
import org.hsqldb.rowio.RowInputTextLog;


public class ScriptReaderDecode extends ScriptReaderText {

    DataInputStream dataInput;
    Crypto          crypto;
    byte[]          buffer = new byte[256];

    public ScriptReaderDecode(Database db, String fileName, Crypto crypto,
                              boolean forLog) throws IOException {
        this(db, db.logger.getFileAccess().openInputStreamElement(fileName),
             crypto, forLog);
    }

    public ScriptReaderDecode(Database db, InputStream inputStream,
                              Crypto crypto,
                              boolean forLog) throws IOException {

        super(db);

        this.crypto = crypto;
        rowIn       = new RowInputTextLog();

        if (forLog) {
            dataInput =
                new DataInputStream(new BufferedInputStream(inputStream));
        } else {
            InputStream stream =
                crypto.getInputStream(new BufferedInputStream(inputStream));

            stream       = new GZIPInputStream(stream);
            dataStreamIn = new LineReader(stream, ScriptWriterText.ISO_8859_1);
        }
    }

    public boolean readLoggedStatement(Session session) {

        if (dataInput == null) {
            return super.readLoggedStatement(session);
        }

        int count;

        try {
            count = dataInput.readInt();

            if (count * 2 > buffer.length) {
                buffer = new byte[count * 2];
            }

            dataInput.readFully(buffer, 0, count);
        } catch (Throwable t) {
            return false;
        }

        count = crypto.decode(buffer, 0, count, buffer, 0);

        String s;

        try {
            s = new String(buffer, 0, count, "ISO-8859-1");
        } catch (UnsupportedEncodingException e) {
            throw Error.error(e, ErrorCode.FILE_IO_ERROR, null);
        }

        lineCount++;


        statement = StringConverter.unicodeStringToString(s);

        if (statement == null) {
            return false;
        }

        processStatement(session);

        return true;
    }

    public void close() {

        try {
            if (dataStreamIn != null) {
                dataStreamIn.close();
            }

            if (dataInput != null) {
                dataInput.close();
            }
        } catch (Exception e) {}
    }
}
