


package org.hsqldb.scriptio;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

import org.hsqldb.Database;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.FileAccess;
import org.hsqldb.lib.FileUtil;
import org.hsqldb.lib.HsqlByteArrayOutputStream;
import org.hsqldb.persist.Crypto;


public class ScriptWriterEncode extends ScriptWriterText {

    Crypto                    crypto;
    HsqlByteArrayOutputStream byteOut;
    OutputStream              cryptOut;
    public ScriptWriterEncode(Database db, OutputStream outputStream,
                              FileAccess.FileSync descriptor,
                              boolean includeCached, Crypto crypto) {

        super(db, outputStream, descriptor, includeCached);

        try {
            cryptOut = crypto.getOutputStream(fileStreamOut);
            fileStreamOut = new GZIPOutputStream(cryptOut);
            isCrypt       = true;
        } catch (IOException e) {
            throw Error.error(e, ErrorCode.FILE_IO_ERROR,
                              ErrorCode.M_Message_Pair, new Object[] {
                e.toString(), outFile
            });
        }
    }

    public ScriptWriterEncode(Database db, String file, boolean includeCached,
                              Crypto crypto) {

        super(db, file, includeCached, true, false);

        try {
            cryptOut = crypto.getOutputStream(fileStreamOut);
            fileStreamOut = new GZIPOutputStream(cryptOut);
            isCrypt       = true;
        } catch (IOException e) {
            throw Error.error(e, ErrorCode.FILE_IO_ERROR,
                              ErrorCode.M_Message_Pair, new Object[] {
                e.toString(), outFile
            });
        }
    }

    public ScriptWriterEncode(Database db, String file, Crypto crypto) {

        super(db, file, false, false, false);

        this.crypto = crypto;
        byteOut     = new HsqlByteArrayOutputStream();
        isCrypt     = true;
    }

    protected void openFile() {

        try {
            FileAccess   fa  = isDump ? FileUtil.getFileUtil()
                                      : database.logger.getFileAccess();
            OutputStream fos = fa.openOutputStreamElement(outFile);

            outDescriptor = fa.getFileSync(fos);
            fileStreamOut = fos;
            fileStreamOut = new BufferedOutputStream(fos, 1 << 14);
        } catch (IOException e) {
            throw Error.error(e, ErrorCode.FILE_IO_ERROR,
                              ErrorCode.M_Message_Pair, new Object[] {
                e.toString(), outFile
            });
        }
    }

    
    protected void finishStream() throws IOException {

        if (fileStreamOut instanceof GZIPOutputStream) {
            ((GZIPOutputStream) fileStreamOut).finish();
        }
    }

    void writeRowOutToFile() throws IOException {

        synchronized (fileStreamOut) {
            if (byteOut == null) {
                fileStreamOut.write(rowOut.getBuffer(), 0, rowOut.size());

                byteCount += rowOut.size();

                lineCount++;

                return;
            }

            int count = crypto.getEncodedSize(rowOut.size());

            byteOut.ensureRoom(count + 4);

            count = crypto.encode(rowOut.getBuffer(), 0, rowOut.size(),
                                  byteOut.getBuffer(), 4);

            byteOut.setPosition(0);
            byteOut.writeInt(count);
            fileStreamOut.write(byteOut.getBuffer(), 0, count + 4);

            byteCount += rowOut.size();

            lineCount++;
        }
    }
}
