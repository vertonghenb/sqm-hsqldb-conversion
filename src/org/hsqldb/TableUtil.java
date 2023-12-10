package org.hsqldb;
import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.lib.ArrayUtil;
import org.hsqldb.types.Type;
public class TableUtil {
    static Table newSingleColumnTable(Database database, HsqlName tableName,
                                      int tableType, HsqlName colName,
                                      Type colType) {
        TableDerived table;
        table = new TableDerived(database, tableName, tableType);
        ColumnSchema column = new ColumnSchema(colName, colType, false, true,
                                               null);
        table.addColumn(column);
        table.createPrimaryKeyConstraint(table.getName(), new int[]{ 0 },
                                         true);
        return table;
    }
    static void setTableIndexesForSubquery(Table table, boolean fullIndex,
                                           boolean uniqueRows) {
        int[] cols = null;
        if (fullIndex) {
            cols = new int[table.getColumnCount()];
            ArrayUtil.fillSequence(cols);
        }
        table.createPrimaryKey(null, uniqueRows ? cols
                                                : null, false);
        if (uniqueRows) {
            table.fullIndex = table.getPrimaryIndex();
        } else if (fullIndex) {
            table.fullIndex = table.createIndexForColumns(null, cols);
        }
    }
    public static void addAutoColumns(Table table, Type[] colTypes) {
        for (int i = 0; i < colTypes.length; i++) {
            ColumnSchema column =
                new ColumnSchema(HsqlNameManager.getAutoColumnName(i),
                                 colTypes[i], true, false, null);
            table.addColumnNoCheck(column);
        }
    }
    public static void setColumnsInSchemaTable(Table table,
            HsqlName[] columnNames, Type[] columnTypes) {
        for (int i = 0; i < columnNames.length; i++) {
            HsqlName columnName = columnNames[i];
            columnName = table.database.nameManager.newColumnSchemaHsqlName(
                table.getName(), columnName);
            ColumnSchema column = new ColumnSchema(columnName, columnTypes[i],
                                                   true, false, null);
            table.addColumn(column);
        }
        table.setColumnStructures();
    }
}