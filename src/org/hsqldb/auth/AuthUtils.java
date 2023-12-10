package org.hsqldb.auth;
import java.sql.Array;
import java.sql.Connection;
import java.util.Set;
import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashSet;
import org.hsqldb.jdbc.JDBCArrayBasic;
import org.hsqldb.lib.FrameworkLogger;
public class AuthUtils {
    private static FrameworkLogger logger =
            FrameworkLogger.getLog(AuthUtils.class);
    private AuthUtils() {
    }
    static String getInitialSchema(Connection c) throws SQLException {
        ResultSet rs = c.createStatement().executeQuery(
                "SELECT initial_schema FROM information_schema.system_users\n"
                + "WHERE user_name = current_user");
        try {
            if (!rs.next()) {
                throw new IllegalStateException(
                        "Failed to retrieve initial_schema for current user");
            }
            return rs.getString(1);
        } finally {
            if (rs != null) try {
                rs.close();
            } catch (SQLException se) {
                logger.error("Failed "
                        + "to close ResultSet for retrieving initial schema");
            }
            rs = null;  
        }
    }
    static Set getEnabledRoles(Connection c) throws SQLException {
        Set roles = new HashSet<String>();
        ResultSet rs = c.createStatement().executeQuery(
                "SELECT * FROM information_schema.enabled_roles");
        try {
            while (rs.next()) roles.add(rs.getString(1));
        } finally {
            if (rs != null) try {
                rs.close();
            } catch (SQLException se) {
                logger.error(
                        "Failed to close ResultSet for retrieving db name");
            }
            rs = null;  
        }
        return roles;
    }
}