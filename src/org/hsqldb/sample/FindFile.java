


package org.hsqldb.sample;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


class FindFile {

    
    public static void main(String[] arg) {

        
        try {

            
            Class.forName("org.hsqldb.jdbc.JDBCDriver");

            
            
            
            
            Connection conn =
                DriverManager.getConnection("jdbc:hsqldb:testfiles", "SA",
                                            "");

            
            if (arg.length == 1) {

                
                
                listFiles(conn, arg[0]);
            } else if ((arg.length == 2) && arg[0].equals("-init")) {

                
                
                fillFileNames(conn, arg[1]);
            } else {

                
                System.out.println("Usage:");
                System.out.println("java FindFile -init .");
                System.out.println("  Re-create database from directory '.'");
                System.out.println("java FindFile name");
                System.out.println("  Find files like 'name'");
            }

            
            conn.close();
        } catch (Exception e) {

            
            System.out.println(e);
            e.printStackTrace();
        }
    }

    

    
    static void listFiles(Connection conn, String name) throws SQLException {

        System.out.println("Files like '" + name + "'");

        
        name = name.toUpperCase();

        
        Statement stat = conn.createStatement();

        
        
        
        ResultSet result = stat.executeQuery("SELECT Path FROM Files WHERE "
                                             + "UCASE(Path) LIKE '%" + name
                                             + "%' ESCAPE ':'");

        
        while (result.next()) {

            
            
            System.out.println(result.getString(1));
        }

        
        result.close();
    }

    

    
    static void fillFileNames(Connection conn,
                              String root) throws SQLException {

        System.out.println("Re-creating the database...");

        
        Statement stat = conn.createStatement();

        
        try {
            stat.executeUpdate("DROP TABLE Files");
        } catch (SQLException e) {    
        }

        
        
        stat.execute("CREATE TABLE Files"
                     + "(Path varchar(255),Name varchar(255))");

        
        stat.close();

        
        PreparedStatement prep =
            conn.prepareCall("INSERT INTO Files (Path,Name) VALUES (?,?)");

        
        fillPath(root, "", prep);

        
        prep.close();
        System.out.println("Finished");
    }

    

    
    static void fillPath(String path, String name,
                         PreparedStatement prep) throws SQLException {

        File f = new File(path);

        if (f.isFile()) {

            
            prep.clearParameters();

            
            prep.setString(1, path);

            
            prep.setString(2, name);

            
            prep.execute();
        } else if (f.isDirectory()) {
            if (!path.endsWith(File.separator)) {
                path += File.separator;
            }

            String[] list = f.list();

            
            for (int i = 0; (list != null) && (i < list.length); i++) {
                fillPath(path + list[i], list[i], prep);
            }
        }
    }
}
