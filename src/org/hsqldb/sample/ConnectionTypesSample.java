package org.hsqldb.sample;
public class ConnectionTypesSample extends java.util.Vector {
    public ConnectionTypesSample() {
        addElement("Example");
        addElement("org.example:ExampleDriver");
        addElement("jdbc:example");
        addElement("Sybase");
        addElement("com.sybase.jdbc.SybDriver");
        addElement("jdbc:sybase:Tds:"
                   + "\u00ABhost?\u00BB:2638/\u00ABdatabase?\u00BB");
    }
}