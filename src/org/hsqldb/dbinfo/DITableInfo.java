


package org.hsqldb.dbinfo;

import java.util.Locale;

import org.hsqldb.Table;
import org.hsqldb.TableBase;
import org.hsqldb.resources.BundleHandler;
import org.hsqldb.store.ValuePool;


final class DITableInfo {

    
    int                bestRowTemporary   = 0;
    int                bestRowTransaction = 1;
    int                bestRowSession     = 2;
    int                bestRowUnknown     = 0;
    int                bestRowNotPseudo   = 1;
    static final short tableIndexOther    = 3;

    
    private static final int HALF_MAX_INT = Integer.MAX_VALUE >>> 1;

    
    private int hnd_column_remarks = -1;

    
    private int hnd_table_remarks = -1;

    
    private Table table;

    
    DITableInfo() {
        setupBundles();
    }

    
    void setupBundles() {

        Locale oldLocale;

        synchronized (BundleHandler.class) {
            oldLocale = BundleHandler.getLocale();

            BundleHandler.setLocale(Locale.getDefault());

            hnd_column_remarks =
                BundleHandler.getBundleHandle("column-remarks", null);
            hnd_table_remarks = BundleHandler.getBundleHandle("table-remarks",
                    null);

            BundleHandler.setLocale(oldLocale);
        }
    }

    
    Integer getBRIPseudo() {
        return ValuePool.getInt(bestRowNotPseudo);
    }

    
    Integer getBRIScope() {
        return (table.isWritable()) ? ValuePool.getInt(bestRowTemporary)
                                    : ValuePool.getInt(bestRowSession);
    }

    
    String getColName(int i) {
        return table.getColumn(i).getName().name;
    }

    
    String getColRemarks(int i) {

        String key;

        if (table.getTableType() != TableBase.INFO_SCHEMA_TABLE) {
            return table.getColumn(i).getName().comment;
        }

        key = getName() + "_" + getColName(i);

        return BundleHandler.getString(hnd_column_remarks, key);
    }

    
    String getHsqlType() {

        switch (table.getTableType()) {

            case TableBase.MEMORY_TABLE :
            case TableBase.TEMP_TABLE :
            case TableBase.INFO_SCHEMA_TABLE :
                return "MEMORY";

            case TableBase.CACHED_TABLE :
                return "CACHED";

            case TableBase.TEMP_TEXT_TABLE :
            case TableBase.TEXT_TABLE :
                return "TEXT";

            case TableBase.VIEW_TABLE :
            default :
                return null;
        }
    }

    
    String getName() {
        return table.getName().name;
    }

    
    String getRemark() {

        return (table.getTableType() == TableBase.INFO_SCHEMA_TABLE)
               ? BundleHandler.getString(hnd_table_remarks, getName())
               : table.getName().comment;
    }

    
    String getJDBCStandardType() {

        switch (table.getTableType()) {

            case TableBase.VIEW_TABLE :
                return "VIEW";

            case TableBase.TEMP_TABLE :
            case TableBase.TEMP_TEXT_TABLE :
                return "GLOBAL TEMPORARY";

            case TableBase.INFO_SCHEMA_TABLE :
                return "SYSTEM TABLE";

            default :
                if (table.getOwner().isSystem() ) {
                    return "SYSTEM TABLE";
                }
                return "TABLE";
        }
    }

    
    Table getTable() {
        return this.table;
    }

    
    void setTable(Table table) {
        this.table = table;
    }
}
