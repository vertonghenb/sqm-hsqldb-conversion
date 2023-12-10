package org.hsqldb.server;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.hsqldb.DatabaseManager;
import org.hsqldb.DatabaseURL;
import org.hsqldb.HsqlException;
import org.hsqldb.Session;
import org.hsqldb.lib.DataOutputStream;
import org.hsqldb.persist.HsqlProperties;
import org.hsqldb.result.Result;
import org.hsqldb.result.ResultConstants;
import org.hsqldb.rowio.RowInputBinary;
import org.hsqldb.rowio.RowOutputBinary;
/**
 * Servlet can act as an interface between the client and the database for the
 * the client / server mode of HSQL Database Engine. It uses the HTTP protocol
 * for communication. This class is not required if the included HSQLDB
 * Weberver is used on the server host. But if the host is running a J2EE
 * application server or a servlet container such as Tomcat, the Servlet class
 * can be hosted on this server / container to serve external requests from
 * external hosts.<p>
 * The remote applet / application should
 * use the normal JDBC interfaces to connect to the URL of this servlet. An
 * example URL is:
 * <pre>
 * jdbc:hsqldb:http://myhost.com:8080/servlet/org.hsqldb.server.Servlet
 * </pre>
 * The database path/name is taken from the servlet engine property:
 * <pre>
 * hsqldb.server.database
 * </pre>
 * <p>
 * If the database is deployed in the WEB-INF directory of the servlet container,
 * the property:
 * <pre>
 *  hsqldb.server.use_web-inf_path
 * </pre>
 * should be set "true" in the web.xml file of the servlet container.
 * In this case, the database path should begin with a "/".
 *
 * JDBC connections via the HTTP protocol are persistent
 * in the JDBC sense. The JDBC Connection that is established can support
 * transactions spanning several Statement calls and real PreparedStatement
 * calls are supported. This class has been rewritten to support the new
 * features.<p>
 * (fredt@users)<p>
 *
 * Extensively rewritten for HSQLDB.
 *
 * @author Thomas Mueller (Hypersonic SQL Group)
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 2.0.1
 * @since Hypersonic SQL
 */
public class Servlet extends javax.servlet.http.HttpServlet {
    private static final int BUFFER_SIZE = 256;
    private String           dbType;
    private String           dbPath;
    private String           errorStr;
    private RowOutputBinary  rowOut;
    private RowInputBinary   rowIn;
    private int              iQueries;
    public void init(ServletConfig config) {
        try {
            super.init(config);
            rowOut = new RowOutputBinary(BUFFER_SIZE, 1);
            rowIn  = new RowInputBinary(rowOut);
        } catch (ServletException e) {
            log(e.toString());
        }
        String dbStr = getInitParameter("hsqldb.server.database");
        if (dbStr == null) {
            dbStr = ".";
        }
        String useWebInfStr =
            getInitParameter("hsqldb.server.use_web-inf_path");
        if (!dbStr.equals(".") && "true".equalsIgnoreCase(useWebInfStr)) {
            dbStr = getServletContext().getRealPath("/") + "WEB-INF/" + dbStr;
        }
        HsqlProperties dbURL = DatabaseURL.parseURL(dbStr, false, false);
        log("Database filename = " + dbStr);
        if (dbURL == null) {
            errorStr = "Bad Database name";
        } else {
            dbPath = dbURL.getProperty("database");
            dbType = dbURL.getProperty("connection_type");
            try {
                DatabaseManager.getDatabase(dbType, dbPath, dbURL);
            } catch (HsqlException e) {
                errorStr = e.getMessage();
            }
        }
        if (errorStr == null) {
            log("Initialization completed.");
        } else {
            log("Database could not be initialised.");
            log(errorStr);
        }
    }
    private static long lModified = 0;
    protected long getLastModified(HttpServletRequest req) {
        return lModified++;
    }
    public void doGet(HttpServletRequest request,
                      HttpServletResponse response)
                      throws IOException, ServletException {
        String query = request.getQueryString();
        if ((query == null) || (query.length() == 0)) {
            response.setContentType("text/html");
            response.setHeader("Pragma", "no-cache");
            PrintWriter out = response.getWriter();
            out.println(
                "<html><head><title>HSQL Database Engine Servlet</title>");
            out.println("</head><body><h1>HSQL Database Engine Servlet</h1>");
            out.println("The servlet is running.<p>");
            if (errorStr == null) {
                out.println("The database is also running.<p>");
                out.println("Database name: " + dbType + dbPath + "<p>");
                out.println("Queries processed: " + iQueries + "<p>");
            } else {
                out.println("<h2>The database is not running!</h2>");
                out.println("The error message is:<p>");
                out.println(errorStr);
            }
            out.println("</body></html>");
        }
    }
    public void doPost(HttpServletRequest request,
                       HttpServletResponse response)
                       throws IOException, ServletException {
        synchronized (this) {
            DataInputStream  inStream = null;
            DataOutputStream dataOut  = null;
            try {
                inStream = new DataInputStream(request.getInputStream());
                int  databaseID = inStream.readInt();
                long sessionID  = inStream.readLong();
                int  mode       = inStream.readByte();
                Session session = DatabaseManager.getSession(databaseID,
                    sessionID);
                Result resultIn = Result.newResult(session, mode, inStream,
                                                   rowIn);
                resultIn.setDatabaseId(databaseID);
                resultIn.setSessionId(sessionID);
                Result resultOut;
                if (resultIn.getType() == ResultConstants.CONNECT) {
                    try {
                        session = DatabaseManager.newSession(
                            dbType, dbPath, resultIn.getMainString(),
                            resultIn.getSubString(), new HsqlProperties(),
                            resultIn.getZoneString(),
                            resultIn.getUpdateCount());
                        resultIn.readAdditionalResults(null, inStream, rowIn);
                        resultOut = Result.newConnectionAcknowledgeResponse(
                            session.getDatabase(), session.getId(),
                            session.getDatabase().getDatabaseID());
                    } catch (HsqlException e) {
                        resultOut = Result.newErrorResult(e);
                    }
                } else {
                    int  dbId      = resultIn.getDatabaseId();
                    long sessionId = resultIn.getSessionId();
                    session = DatabaseManager.getSession(dbId, sessionId);
                    resultIn.readLobResults(session, inStream, rowIn);
                    resultOut = session.execute(resultIn);
                }
                response.setContentType("application/octet-stream");
                response.setContentLength(rowOut.size());
                dataOut = new DataOutputStream(response.getOutputStream());
                resultOut.write(session, dataOut, rowOut);
                iQueries++;
            } catch (HsqlException e) {}
            finally {
                if (dataOut != null) {
                    dataOut.close();
                }
                if (inStream != null) {
                    inStream.close();
                }
            }
        }
    }
}