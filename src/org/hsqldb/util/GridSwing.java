package org.hsqldb.util;
import java.util.Vector;
import java.awt.Component;
import javax.swing.JTable;
import javax.swing.event.TableModelEvent;
import javax.swing.table.AbstractTableModel;
import javax.swing.table.TableCellRenderer;
import javax.swing.table.TableColumn;
import javax.swing.table.TableModel;
class GridSwing extends AbstractTableModel {
    JTable   jtable = null;
    Object[] headers;
    Vector   rows;
    public GridSwing() {
        super();
        headers = new Object[0];    
        rows    = new Vector();     
    }
    public String getColumnName(int i) {
        return headers[i].toString();
    }
    public Class getColumnClass(int i) {
        if (rows.size() > 0) {
            Object o = getValueAt(0, i);
            if (o != null) {
                if ((o instanceof java.sql.Timestamp)
                    || (o instanceof java.sql.Time)) {
                    return Object.class;  
                }
                return o.getClass();
            }
        }
        return super.getColumnClass(i);
    }
    public int getColumnCount() {
        return headers.length;
    }
    public int getRowCount() {
        return rows.size();
    }
    public Object[] getHead() {
        return headers;
    }
    public Vector getData() {
        return rows;
    }
    public Object getValueAt(int row, int col) {
        if (row >= rows.size()) {
            return null;
        }
        Object[] colArray = (Object[]) rows.elementAt(row);
        if (col >= colArray.length) {
            return null;
        }
        return colArray[col];
    }
    public void setHead(Object[] h) {
        headers = new Object[h.length];
        for (int i = 0; i < h.length; i++) {
            headers[i] = h[i];
        }
    }
    public void addRow(Object[] r) {
        Object[] row = new Object[r.length];
        for (int i = 0; i < r.length; i++) {
            row[i] = r[i];
            if (row[i] == null) {
            }
        }
        rows.addElement(row);
    }
    public void clear() {
        rows.removeAllElements();
    }
    public void setJTable(JTable table) {
        jtable = table;
    }
    public void fireTableChanged(TableModelEvent e) {
        super.fireTableChanged(e);
        autoSizeTableColumns(jtable);
    }
    public static void autoSizeTableColumns(JTable table) {
        TableModel  model        = table.getModel();
        TableColumn column       = null;
        Component   comp         = null;
        int         headerWidth  = 0;
        int         maxCellWidth = Integer.MIN_VALUE;
        int         cellWidth    = 0;
        TableCellRenderer headerRenderer =
            table.getTableHeader().getDefaultRenderer();
        for (int i = 0; i < table.getColumnCount(); i++) {
            column = table.getColumnModel().getColumn(i);
            comp = headerRenderer.getTableCellRendererComponent(table,
                    column.getHeaderValue(), false, false, 0, 0);
            headerWidth  = comp.getPreferredSize().width + 10;
            maxCellWidth = Integer.MIN_VALUE;
            for (int j = 0; j < Math.min(model.getRowCount(), 30); j++) {
                TableCellRenderer r = table.getCellRenderer(j, i);
                comp = r.getTableCellRendererComponent(table,
                                                       model.getValueAt(j, i),
                                                       false, false, j, i);
                cellWidth = comp.getPreferredSize().width;
                if (cellWidth >= maxCellWidth) {
                    maxCellWidth = cellWidth;
                }
            }
            column.setPreferredWidth(Math.max(headerWidth, maxCellWidth)
                                     + 10);
        }
    }
}