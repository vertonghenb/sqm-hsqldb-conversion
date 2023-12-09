


package org.hsqldb.util;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Vector;


class TransferResultSet {

    Vector   vRows = null;
    int      iRowIdx;
    int      iMaxRowIdx;
    int      iColumnCount;
    String[] sColumnNames = null;
    int[]    iColumnTypes = null;

    TransferResultSet(ResultSet r) {

        iRowIdx      = 0;
        iMaxRowIdx   = 0;
        iColumnCount = 0;
        vRows        = new Vector();

        try {
            while (r.next()) {
                if (sColumnNames == null) {
                    iColumnCount = r.getMetaData().getColumnCount();
                    sColumnNames = new String[iColumnCount + 1];
                    iColumnTypes = new int[iColumnCount + 1];

                    for (int Idx = 0; Idx < iColumnCount; Idx++) {
                        sColumnNames[Idx + 1] =
                            r.getMetaData().getColumnName(Idx + 1);
                        iColumnTypes[Idx + 1] =
                            r.getMetaData().getColumnType(Idx + 1);
                    }

                    vRows.addElement(null);
                }

                iMaxRowIdx++;

                Object[] Values = new Object[iColumnCount + 1];

                for (int Idx = 0; Idx < iColumnCount; Idx++) {
                    Values[Idx + 1] = r.getObject(Idx + 1);
                }

                vRows.addElement(Values);
            }
        } catch (SQLException SQLE) {
            iRowIdx      = 0;
            iMaxRowIdx   = 0;
            iColumnCount = 0;
            vRows        = new Vector();
        }
    }

    TransferResultSet() {

        iRowIdx      = 0;
        iMaxRowIdx   = 0;
        iColumnCount = 0;
        vRows        = new Vector();
    }

    void addRow(String[] Name, int[] type, Object[] Values,
                int nbColumns) throws Exception {

        if ((Name.length != type.length) || (Name.length != Values.length)
                || (Name.length != (nbColumns + 1))) {
            throw new Exception("Size of parameter incoherent");
        }

        if (sColumnNames == null) {
            iColumnCount = nbColumns;
            sColumnNames = Name;
            iColumnTypes = type;

            vRows.addElement(null);
        }

        if ((iMaxRowIdx > 0) && (this.getColumnCount() != nbColumns)) {
            throw new Exception("Wrong number of columns: "
                                + this.getColumnCount()
                                + " column is expected");
        }

        iMaxRowIdx++;

        vRows.addElement(Values);
    }

    boolean next() {

        iRowIdx++;

        return ((iRowIdx <= iMaxRowIdx) && (iMaxRowIdx > 0));
    }

    String getColumnName(int columnIdx) {

        if ((iMaxRowIdx <= 0) || (iMaxRowIdx < iRowIdx)) {
            return null;
        }

        return sColumnNames[columnIdx];
    }

    int getColumnCount() {

        if ((iMaxRowIdx <= 0) || (iMaxRowIdx < iRowIdx)) {
            return 0;
        }

        return iColumnCount;
    }

    int getColumnType(int columnIdx) {

        if ((iMaxRowIdx <= 0) || (iMaxRowIdx < iRowIdx)) {
            return 0;
        }

        return iColumnTypes[columnIdx];
    }

    Object getObject(int columnIdx) {

        if ((iMaxRowIdx <= 0) || (iMaxRowIdx < iRowIdx)) {
            return null;
        }

        return ((Object[]) vRows.elementAt(iRowIdx))[columnIdx];
    }
}
