package org.hsqldb.server;
import org.hsqldb.lib.FileUtil;
import org.hsqldb.persist.HsqlProperties;
import org.hsqldb.resources.BundleHandler;
/**
 *  The HSQLDB HTTP protocol network database server. <p>
 *
 *  WebServer has two distinct functions:<p>
 *
 *  The primary function is to allow client/server access to HSQLDB databases
 *  via the HTTP protocol. This protocol is less efficient than the HSQL
 *  protocol used by the Server class and should be used only in situations
 *  where sandboxes or firewalls between the client and the server do not
 *  allow the use of the HSQL protocol. One example is client/server access by
 *  an applet running in browsers on remote hosts and accessing the database
 *  engine on the HTTP server from which the applet originated. From version
 *  1.7.2, HTTP database connections are persistent and support transactions.
 *  Similar to HSQL connections, they should be explicitly closed to free the
 *  server resources. <p>
 *
 *  The secondary function of WebServer is to act as a simple general purpose
 *  HTTP server. It is aimed to support the minimum requirements set out by
 *  the HTTP/1.0 standard. The HEAD and GET methods can be used to query and
 *  retreive static files from the HTTP server.<p>
 *
 *  Both the database server and HTTP server functions of WebServer can be
 *  configured with the webserver.properties file. It contains entries for the
 *  database server similar to those for the HSQL protocol Server class. In
 *  addition, a list mapping different file endings to their mime types may be
 *  included in this file. (fredt@users) <p>
 *
 * From the command line, the options are as follows: <p>
 * <pre>
 * +-----------------+-------------+----------+------------------------------+
 * |    OPTION       |    TYPE     | DEFAULT  |         DESCRIPTION          |
 * +-----------------+-------------+----------+------------------------------|
 * | --help          |             |          | prints this message          |
 * | --address       | name|number | any      | server inet address          |
 * | --port          | number      | 80       | port at which server listens |
 * | --database.i    | [type]spec  | 0=test   | path of database i           |
 * | --dbname.i      | alias       |          | url alias for database i     |
 * | --silent        | true|false  | true     | false => display all queries |
 * | --trace         | true|false  | false    | display JDBC trace messages  |
 * | --no_system_exit| true|false  | false    | do not issue System.exit()   |
 * +-----------------+-------------+----------+------------------------------+
 * </pre>
 *
 *  Example of the webserver.properties file:
 *
 * <pre>
 * server.port=80
 * server.database.0=test
 * server.dbname.0=...
 * ...
 * server.database.n=...
 * server.dbname.n=...
 * server.silent=true
 *
 * .htm=text/html
 * .html=text/html
 * .txt=text/plain
 * .gif=image/gif
 * .class=application/octet-stream
 * .jpg=image/jpeg
 * .jgep=image/jpeg
 * .zip=application/x-zip-compressed
 * </pre>
 *
 * <ul>
 *   <li>For server.root, use '/'  as the separator, even for DOS/Windows.
 *   <li>File extensions for mime types must be lowercase and start with '.'
 * </ul>
 *
 * @author Campbell Boucher-Burnet (boucherb@users dot sourceforge.net)
 * @author Fred Toussi (fredt@users dot sourceforge.net)
 * @version 1.7.2
 * @since 1.7.2
 */
public class WebServer extends Server {
    /**
     * Handle to resource bundle providing i18n for things like
     * HTTP error pages.
     */
    static int webBundleHandle = BundleHandler.getBundleHandle("webserver",
        null);
    public WebServer() {
        super(ServerConstants.SC_PROTOCOL_HTTP);
    }
    /**
     *  Starts a new WebServer.
     *
     * @param  args the "command line" parameters with which to start
     *      the WebServer.  "-?" will cause the command line arguments
     *      help to be printed to the standard output
     */
    public static void main(String[] args) {
        HsqlProperties argProps = null;
        argProps = HsqlProperties.argArrayToProps(args,
                ServerProperties.sc_key_prefix);
        String[] errors = argProps.getErrorKeys();
        if (errors.length != 0) {
            System.out.println("no value for argument:" + errors[0]);
            printHelp("webserver.help");
            return;
        }
        String propsPath = argProps.getProperty(ServerProperties.sc_key_props);
        String propsExtension = "";
        if (propsPath == null) {
            propsPath      = "webserver";
            propsExtension = ".properties";
        }
        propsPath = FileUtil.getFileUtil().canonicalOrAbsolutePath(propsPath);
        ServerProperties fileProps = ServerConfiguration.getPropertiesFromFile(
            ServerConstants.SC_PROTOCOL_HTTP, propsPath, propsExtension);
        ServerProperties props =
            fileProps == null
            ? new ServerProperties(ServerConstants.SC_PROTOCOL_HTTP)
            : fileProps;
        props.addProperties(argProps);
        ServerConfiguration.translateDefaultDatabaseProperty(props);
        ServerConfiguration.translateDefaultNoSystemExitProperty(props);
        ServerConfiguration.translateAddressProperty(props);
        Server server = new WebServer();
        try {
            server.setProperties(props);
        } catch (Exception e) {
            server.printError("Failed to set properties");
            server.printStackTrace(e);
            return;
        }
        server.print("Startup sequence initiated from main() method");
        if (fileProps != null) {
            server.print("Loaded properties from [" + propsPath
                         + ".properties]");
        } else {
            server.print("Could not load properties from file");
            server.print("Using cli/default properties only");
        }
        server.start();
    }
    /**
     * Retrieves the name of the web page served when no page is specified.
     * This attribute is relevant only when server protocol is HTTP(S).
     *
     * @return the name of the web page served when no page is specified
     *
     * @jmx.managed-attribute
     *  access="read-write"
     *  description="Used when server protocol is HTTP(S)"
     */
    public String getDefaultWebPage() {
        return serverProperties.getProperty(
            ServerProperties.sc_key_web_default_page);
    }
    /**
     * Retrieves a String object describing the command line and
     * properties options for this Server.
     *
     * @return the command line and properties options help for this Server
     */
    public String getHelpString() {
        return BundleHandler.getString(serverBundleHandle, "webserver.help");
    }
    /**
     * Retrieves this server's product name.  <p>
     *
     * Typically, this will be something like: "HSQLDB xxx server".
     *
     * @return the product name of this server
     *
     * @jmx.managed-attribute
     *  access="read-only"
     *  description="Of Server"
     */
    public String getProductName() {
        return "HSQLDB web server";
    }
    /**
     * Retrieves a string respresentaion of the network protocol
     * this server offers, typically one of 'HTTP', HTTPS', 'HSQL' or 'HSQLS'.
     *
     * @return string respresentation of this server's protocol
     *
     * @jmx.managed-attribute
     *  access="read-only"
     *  description="Used to handle connections"
     */
    public String getProtocol() {
        return isTls() ? "HTTPS"
                       : "HTTP";
    }
    /**
     * Retrieves the root context (directory) from which web content
     * is served.  This property is relevant only when the server
     * protocol is HTTP(S).  Although unlikely, it may be that in the future
     * other contexts, such as jar urls may be supported, so that pages can
     * be served from the contents of a jar or from the JVM class path.
     *
     * @return the root context (directory) from which web content is served
     *
     * @jmx.managed-attribute
     *  access="read-write"
     *  description="Context (directory)"
     */
    public String getWebRoot() {
        return serverProperties.getProperty(ServerProperties.sc_key_web_root);
    }
}