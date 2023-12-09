


package org.hsqldb.jdbc;

import java.sql.NClob;


public class JDBCNClob extends JDBCClob implements NClob {

    protected JDBCNClob() {
        super();
    }

    public JDBCNClob(String data) throws java.sql.SQLException {
        super(data);
    }
}
