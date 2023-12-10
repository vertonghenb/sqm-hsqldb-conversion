package org.hsqldb.sample;
import java.sql.Connection;
import java.sql.SQLException;
import org.hsqldb.lib.RCData;
import org.hsqldb.cmdline.SqlFile;
import org.hsqldb.cmdline.SqlToolError;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
public class SqlFileEmbedder {
    private Connection conn;
    public Connection getConn() {
        return conn;
    }
    public static void main(String[] sa) throws Exception {
        if (sa.length < 3) {
            System.err.println("SYNTAX:  " + SqlFileEmbedder.class.getName()
                    + " path/ro/file.rc URLID file1.sql...");
            System.exit(2);
        }
        SqlFileEmbedder embedder =
                new SqlFileEmbedder(new File(sa[0]), sa[1]);
        String[] files = new String[sa.length - 2];
        for (int i = 0; i < sa.length - 2; i++) {
            files[i] = sa[i + 2];
        }
        try {
            embedder.executeFiles(files);
        } finally {
            try {
                embedder.getConn().close();
            } catch (SQLException se) {
            }
        }
    }
    public SqlFileEmbedder(File rcFile, String urlid) throws Exception {
        conn = (new RCData(rcFile, urlid)).getConnection();
        conn.setAutoCommit(false);
    }
    public void executeFiles(String[] fileStrings)
            throws IOException, SqlToolError, SQLException {
        Map<String, String> sqlVarMap = new HashMap<String, String>();
        sqlVarMap.put("invoker", getClass().getName());
        File file;
        SqlFile sqlFile;
        for (String fileString : fileStrings) {
            file = new File(fileString);
            if (!file.isFile())
                throw new IOException("SQL file not present: "
                        + file.getAbsolutePath());
            sqlFile = new SqlFile(file);
            sqlFile.setConnection(conn);
            sqlFile.addUserVars(sqlVarMap);
            sqlFile.execute();
            conn = sqlFile.getConnection();
            sqlVarMap = sqlFile.getUserVars();
        }
    }
}