package org.hsqldb.server;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.Socket;
import org.hsqldb.DatabaseManager;
import org.hsqldb.HsqlException;
import org.hsqldb.Session;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.lib.DataOutputStream;
import org.hsqldb.lib.InOutUtil;
import org.hsqldb.persist.HsqlDatabaseProperties;
import org.hsqldb.resources.BundleHandler;
import org.hsqldb.result.Result;
import org.hsqldb.result.ResultConstants;
import org.hsqldb.rowio.RowInputBinary;
import org.hsqldb.rowio.RowOutputBinary;
class WebServerConnection implements Runnable {
    static final String         ENCODING = "ISO-8859-1";
    private Socket              socket;
    private WebServer           server;
    private static final int    REQUEST_TYPE_BAD  = 0;
    private static final int    REQUEST_TYPE_GET  = 1;
    private static final int    REQUEST_TYPE_HEAD = 2;
    private static final int    REQUEST_TYPE_POST = 3;
    private static final String HEADER_OK         = "HTTP/1.0 200 OK";
    private static final String HEADER_BAD_REQUEST =
        "HTTP/1.0 400 Bad Request";
    private static final String HEADER_NOT_FOUND = "HTTP/1.0 404 Not Found";
    private static final String HEADER_FORBIDDEN = "HTTP/1.0 403 Forbidden";
    static final int            BUFFER_SIZE      = 256;
    final byte[]                mainBuffer       = new byte[BUFFER_SIZE];
    private RowOutputBinary     rowOut = new RowOutputBinary(mainBuffer);
    private RowInputBinary      rowIn            = new RowInputBinary(rowOut);
    static byte[] BYTES_GET;
    static byte[] BYTES_HEAD;
    static byte[] BYTES_POST;
    static byte[] BYTES_CONTENT;
    static {
        try {
            BYTES_GET     = "GET".getBytes("ISO-8859-1");
            BYTES_HEAD    = "HEAD".getBytes("ISO-8859-1");
            BYTES_POST    = "POST".getBytes("ISO-8859-1");
            BYTES_CONTENT = "Content-Length: ".getBytes("ISO-8859-1");
        } catch (UnsupportedEncodingException e) {
            Error.runtimeError(ErrorCode.U_S0500, "RowOutputTextLog");
        }
    }
    static final byte[] BYTES_WHITESPACE = new byte[] {
        (byte) ' ', (byte) '\t'
    };
    private static final int hnd_content_types =
        BundleHandler.getBundleHandle("content-types", null);
    WebServerConnection(Socket socket, WebServer server) {
        this.server = server;
        this.socket = socket;
    }
    private String getMimeTypeString(String name) {
        int    pos;
        String key;
        String mimeType;
        if (name == null) {
            return ServerConstants.SC_DEFAULT_WEB_MIME;
        }
        pos      = name.lastIndexOf('.');
        key      = null;
        mimeType = null;
        if (pos >= 0) {
            key      = name.substring(pos).toLowerCase();
            mimeType = server.serverProperties.getProperty(key);
        }
        if (mimeType == null && key.length() > 1) {
            mimeType = BundleHandler.getString(hnd_content_types,
                                               key.substring(1));
        }
        return mimeType == null ? ServerConstants.SC_DEFAULT_WEB_MIME
                                : mimeType;
    }
    public void run() {
        DataInputStream inStream = null;
        try {
            inStream = new DataInputStream(socket.getInputStream());
            int    count;
            String request;
            String name   = null;
            int    method = REQUEST_TYPE_BAD;
            int    len    = -1;
            do {
                count = InOutUtil.readLine(inStream, rowOut);
                if (count == 0) {
                    throw new Exception();
                }
            } while (count < 2);
            byte[] byteArray = rowOut.getBuffer();
            int    offset    = rowOut.size() - count;
            if (ArrayUtil.containsAt(byteArray, offset, BYTES_POST)) {
                method = REQUEST_TYPE_POST;
                offset += BYTES_POST.length;
            } else if (ArrayUtil.containsAt(byteArray, offset, BYTES_GET)) {
                method = REQUEST_TYPE_GET;
                offset += BYTES_GET.length;
            } else if (ArrayUtil.containsAt(byteArray, offset, BYTES_HEAD)) {
                method = REQUEST_TYPE_HEAD;
                offset += BYTES_HEAD.length;
            } else {
                method = REQUEST_TYPE_BAD;
            }
            count = ArrayUtil.countStartElementsAt(byteArray, offset,
                                                   BYTES_WHITESPACE);
            if (count == 0) {
                method = REQUEST_TYPE_BAD;
            }
            offset += count;
            count = ArrayUtil.countNonStartElementsAt(byteArray, offset,
                    BYTES_WHITESPACE);
            name = new String(byteArray, offset, count, ENCODING);
            switch (method) {
                case REQUEST_TYPE_BAD :
                    processError(REQUEST_TYPE_BAD);
                    break;
                case REQUEST_TYPE_GET :
                    processGet(name, true);
                    break;
                case REQUEST_TYPE_HEAD :
                    processGet(name, false);
                    break;
                case REQUEST_TYPE_POST :
                    processPost(inStream, name);
                    break;
            }
        } catch (Exception e) {
            server.printStackTrace(e);
        } finally {
            try {
                if (inStream != null) {
                    inStream.close();
                }
                socket.close();
            } catch (IOException ioe) {
                server.printStackTrace(ioe);
            }
        }
    }
    private void processPost(InputStream inStream,
                             String name) throws IOException {
        try {
            InOutUtil.readLine(inStream, rowOut);
            int count  = InOutUtil.readLine(inStream, rowOut);
            int offset = rowOut.size() - count;
            byte[] byteArray = rowOut.getBuffer();
            if (!ArrayUtil.containsAt(byteArray, offset, BYTES_CONTENT)) {
                throw new Exception();
            }
            count  -= BYTES_CONTENT.length;
            offset += BYTES_CONTENT.length;
            String lenStr = new String(byteArray, offset, count - 2);
            int    length = Integer.parseInt(lenStr);
            InOutUtil.readLine(inStream, rowOut);
        } catch (Exception e) {
            processError(HttpURLConnection.HTTP_BAD_REQUEST);
            return;
        }
        processQuery(inStream);
    }
    void processQuery(InputStream inStream) {
        try {
            DataInputStream dataIn     = new DataInputStream(inStream);
            int             databaseID = dataIn.readInt();
            long            sessionID  = dataIn.readLong();
            int             mode       = dataIn.readByte();
            Session session = DatabaseManager.getSession(databaseID,
                sessionID);
            Result resultIn = Result.newResult(session, mode, dataIn, rowIn);
            resultIn.setDatabaseId(databaseID);
            resultIn.setSessionId(sessionID);
            Result resultOut;
            if (resultIn.getType() == ResultConstants.CONNECT) {
                try {
                    String databaseName = resultIn.getDatabaseName();
                    int    dbIndex      = server.getDBIndex(databaseName);
                    int    dbID         = server.dbID[dbIndex];
                    session =
                        DatabaseManager.newSession(dbID,
                                                   resultIn.getMainString(),
                                                   resultIn.getSubString(),
                                                   resultIn.getZoneString(),
                                                   resultIn.getUpdateCount());
                    resultIn.readAdditionalResults(session, dataIn, rowIn);
                    resultOut = Result.newConnectionAcknowledgeResponse(
                        session.getDatabase(), session.getId(), dbID);
                } catch (HsqlException e) {
                    resultOut = Result.newErrorResult(e);
                } catch (RuntimeException e) {
                    resultOut = Result.newErrorResult(e);
                }
            } else {
                int dbID = resultIn.getDatabaseId();
                if (session == null) {
                    resultOut = Result.newErrorResult(
                        Error.error(ErrorCode.SERVER_DATABASE_DISCONNECTED));
                } else {
                    resultIn.setSession(session);
                    resultIn.readLobResults(session, dataIn, rowIn);
                    resultOut = session.execute(resultIn);
                }
            }
            int type = resultIn.getType();
            if (type == ResultConstants.DISCONNECT
                    || type == ResultConstants.RESETSESSION) {
                return;
            }
            DataOutputStream dataOut =
                new DataOutputStream(socket.getOutputStream());
            String header = getHead(HEADER_OK, false,
                                    "application/octet-stream", rowOut.size());
            dataOut.write(header.getBytes(ENCODING));
            dataOut.flush();
            resultOut.write(session, dataOut, rowOut);
            dataOut.close();
        } catch (Exception e) {
            server.printStackTrace(e);
        }
    }
    private void processGet(String name, boolean send) {
        try {
            String       hdr;
            OutputStream os;
            InputStream  is;
            int          b;
            if (name.endsWith("/")) {
                name += server.getDefaultWebPage();
            }
            if (name.indexOf("..") != -1) {
                processError(HttpURLConnection.HTTP_FORBIDDEN);
                return;
            }
            name = server.getWebRoot() + name;
            if (File.separatorChar != '/') {
                name = name.replace('/', File.separatorChar);
            }
            is = null;
            server.printWithThread("GET " + name);
            try {
                File file = new File(name);
                is = new DataInputStream(new FileInputStream(file));
                hdr = getHead(HEADER_OK, true, getMimeTypeString(name),
                              (int) file.length());
            } catch (IOException e) {
                processError(HttpURLConnection.HTTP_NOT_FOUND);
                if (is != null) {
                    is.close();
                }
                return;
            }
            os = new BufferedOutputStream(socket.getOutputStream());
            os.write(hdr.getBytes(ENCODING));
            if (send) {
                while ((b = is.read()) != -1) {
                    os.write(b);
                }
            }
            os.flush();
            os.close();
            is.close();
        } catch (Exception e) {
            server.printError("processGet: " + e.toString());
            server.printStackTrace(e);
        }
    }
    String getHead(String responseCodeString, boolean addInfo,
                   String mimeType, int length) {
        StringBuffer sb = new StringBuffer(128);
        sb.append(responseCodeString).append("\r\n");
        if (addInfo) {
            sb.append("Allow: GET, HEAD, POST\nMIME-Version: 1.0\r\n");
            sb.append("Server: ").append(
                HsqlDatabaseProperties.PRODUCT_NAME).append("\r\n");
        }
        if (mimeType != null) {
            sb.append("Content-Type: ").append(mimeType).append("\r\n");
            sb.append("Content-Length: ").append(length).append("\r\n");
        }
        sb.append("\r\n");
        return sb.toString();
    }
    private void processError(int code) {
        String msg;
        server.printWithThread("processError " + code);
        switch (code) {
            case HttpURLConnection.HTTP_BAD_REQUEST :
                msg = getHead(HEADER_BAD_REQUEST, false, null, 0);
                msg += BundleHandler.getString(WebServer.webBundleHandle,
                                               "BAD_REQUEST");
                break;
            case HttpURLConnection.HTTP_FORBIDDEN :
                msg = getHead(HEADER_FORBIDDEN, false, null, 0);
                msg += BundleHandler.getString(WebServer.webBundleHandle,
                                               "FORBIDDEN");
                break;
            case HttpURLConnection.HTTP_NOT_FOUND :
            default :
                msg = getHead(HEADER_NOT_FOUND, false, null, 0);
                msg += BundleHandler.getString(WebServer.webBundleHandle,
                                               "NOT_FOUND");
                break;
        }
        try {
            OutputStream os =
                new BufferedOutputStream(socket.getOutputStream());
            os.write(msg.getBytes(ENCODING));
            os.flush();
            os.close();
        } catch (Exception e) {
            server.printError("processError: " + e.toString());
            server.printStackTrace(e);
        }
    }
    String getConnectionThreadName() {
        return "HSQLDB HTTP Connection @" + Integer.toString(hashCode(), 16);
    }
}