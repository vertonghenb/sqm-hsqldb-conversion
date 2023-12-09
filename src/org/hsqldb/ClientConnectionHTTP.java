


package org.hsqldb;

import java.io.IOException;
import java.io.OutputStream;

import org.hsqldb.lib.InOutUtil;
import org.hsqldb.result.Result;


public class ClientConnectionHTTP extends ClientConnection {

    static final String ENCODING = "8859_1";
    static final int    IDLENGTH = 12;    

    public ClientConnectionHTTP(String host, int port, String path,
                                String database, boolean isTLS, String user,
                                String password, int timeZoneSeconds) {
        super(host, port, path, database, isTLS, user, password,
              timeZoneSeconds);
    }

    protected void initConnection(String host, int port, boolean isTLS) {}

    public synchronized Result execute(Result r) {

        super.openConnection(host, port, isTLS);

        Result result = super.execute(r);

        super.closeConnection();

        return result;
    }

    protected void write(Result r) throws IOException, HsqlException {

        dataOutput.write("POST ".getBytes(ENCODING));
        dataOutput.write(path.getBytes(ENCODING));
        dataOutput.write(" HTTP/1.0\r\n".getBytes(ENCODING));
        dataOutput.write(
            "Content-Type: application/octet-stream\r\n".getBytes(ENCODING));
        dataOutput.write(("Content-Length: " + rowOut.size() + IDLENGTH
                          + "\r\n").getBytes(ENCODING));
        dataOutput.write("\r\n".getBytes(ENCODING));
        dataOutput.writeInt(r.getDatabaseId());
        dataOutput.writeLong(r.getSessionId());
        r.write(this, dataOutput, rowOut);
    }

    protected Result read() throws IOException, HsqlException {

        
        
        
        rowOut.reset();

        for (;;) {
            int count = InOutUtil.readLine(dataInput, (OutputStream) rowOut);

            if (count <= 2) {
                break;
            }
        }

        
        Result result = Result.newResult(dataInput, rowIn);

        result.readAdditionalResults(this, dataInput, rowIn);

        return result;
    }

    protected void handshake() throws IOException {

        
    }
}
