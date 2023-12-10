package org.hsqldb.server;
import org.hsqldb.lib.FileUtil;
import org.hsqldb.persist.HsqlProperties;
import org.hsqldb.resources.BundleHandler;
public class WebServer extends Server {
    static int webBundleHandle = BundleHandler.getBundleHandle("webserver",
        null);
    public WebServer() {
        super(ServerConstants.SC_PROTOCOL_HTTP);
    }
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
    public String getDefaultWebPage() {
        return serverProperties.getProperty(
            ServerProperties.sc_key_web_default_page);
    }
    public String getHelpString() {
        return BundleHandler.getString(serverBundleHandle, "webserver.help");
    }
    public String getProductName() {
        return "HSQLDB web server";
    }
    public String getProtocol() {
        return isTls() ? "HTTPS"
                       : "HTTP";
    }
    public String getWebRoot() {
        return serverProperties.getProperty(ServerProperties.sc_key_web_root);
    }
}