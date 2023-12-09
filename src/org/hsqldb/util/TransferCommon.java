


package org.hsqldb.util;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Vector;




class TransferCommon {

    static void savePrefs(String f, DataAccessPoint sourceDb,
                          DataAccessPoint targetDb, Traceable tracer,
                          Vector tTable) {

        TransferTable t;

        try {
            FileOutputStream   fos = new FileOutputStream(f);
            ObjectOutputStream oos = new ObjectOutputStream(fos);

            for (int i = 0; i < tTable.size(); i++) {
                t          = (TransferTable) tTable.elementAt(i);
                t.sourceDb = null;
                t.destDb   = null;
                t.tracer   = null;
            }

            oos.writeObject(tTable);

            for (int i = 0; i < tTable.size(); i++) {
                t          = (TransferTable) tTable.elementAt(i);
                t.tracer   = tracer;
                t.sourceDb = (TransferDb) sourceDb;
                t.destDb   = targetDb;
            }
        } catch (IOException e) {
            System.out.println("pb in SavePrefs : " + e.toString());
            e.printStackTrace();
        }
    }

    static Vector loadPrefs(String f, DataAccessPoint sourceDb,
                            DataAccessPoint targetDb, Traceable tracer) {

        TransferTable     t;
        Vector            tTable = null;
        ObjectInputStream ois    = null;

        try {
            FileInputStream fis = new FileInputStream(f);

            ois    = new ObjectInputStream(fis);
            tTable = (Vector) ois.readObject();

            for (int i = 0; i < tTable.size(); i++) {
                t          = (TransferTable) tTable.elementAt(i);
                t.tracer   = tracer;
                t.sourceDb = (TransferDb) sourceDb;
                t.destDb   = targetDb;
            }
        } catch (ClassNotFoundException e) {
            System.out.println("class not found pb in LoadPrefs : "
                               + e.toString());

            tTable = new Vector();
        } catch (IOException e) {
            System.out.println("IO pb in LoadPrefs : actionPerformed"
                               + e.toString());

            tTable = new Vector();
        } finally {
            if (ois != null) {
                try {
                    ois.close();
                } catch (IOException ioe) {}
            }
        }

        return (tTable);
    }

    private TransferCommon() {}
}
