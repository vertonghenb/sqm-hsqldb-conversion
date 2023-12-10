package org.hsqldb.server;
import java.sql.SQLException;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.HsqlException;
import org.hsqldb.jdbc.Util;
import org.hsqldb.persist.HsqlProperties;
public class HsqlServerFactory {
    private HsqlServerFactory() {}
    public static HsqlSocketRequestHandler createHsqlServer(String dbFilePath,
            boolean debugMessages, boolean silentMode) throws SQLException {
        ServerProperties props =
            new ServerProperties(ServerConstants.SC_PROTOCOL_HSQL);
        props.setProperty("server.dbname.0", "");
        props.setProperty("server.database.0", dbFilePath);
        props.setProperty("server.trace", debugMessages);
        props.setProperty("server.silent", silentMode);
        Server server = new Server();
        try {
            server.setProperties(props);
        } catch (Exception e) {
            throw new SQLException("Failed to set server properties: " + e);
        }
        if (!server.openDatabases()) {
            Throwable t = server.getServerError();
            if (t instanceof HsqlException) {
                throw Util.sqlException((HsqlException) t);
            } else {
                throw Util.sqlException(Error.error(ErrorCode.GENERAL_ERROR));
            }
        }
        server.setState(ServerConstants.SERVER_STATE_ONLINE);
        return server;
    }
}