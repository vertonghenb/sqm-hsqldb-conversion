package org.hsqldb.persist;
import java.util.Enumeration;
import org.hsqldb.Database;
import org.hsqldb.DatabaseURL;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.HashMap;
import org.hsqldb.lib.HashSet;
import org.hsqldb.lib.Iterator;
import org.hsqldb.lib.Set;
import org.hsqldb.lib.StringUtil;
public class HsqlDatabaseProperties extends HsqlProperties {
    private static final String hsqldb_method_class_names =
        "hsqldb.method_class_names";
    private static HashSet accessibleJavaMethodNames;
    static {
        try {
            String prop = System.getProperty(hsqldb_method_class_names);
            if (prop != null) {
                accessibleJavaMethodNames = new HashSet();
                String[] names = StringUtil.split(prop, ";");
                for (int i = 0; i < names.length; i++) {
                    accessibleJavaMethodNames.add(names[i]);
                }
            }
        } catch (Exception e) {}
    }
    public static boolean supportsJavaMethod(String name) {
        if (accessibleJavaMethodNames == null) {
            return true;
        }
        if (name.startsWith("java.lang.Math.")) {
            return true;
        }
        if (accessibleJavaMethodNames.contains(name)) {
            return true;
        }
        Iterator it = accessibleJavaMethodNames.iterator();
        while (it.hasNext()) {
            String className = (String) it.next();
            int    limit     = className.lastIndexOf(".*");
            if (limit < 1) {
                continue;
            }
            if (name.startsWith(className.substring(0, limit + 1))) {
                return true;
            }
        }
        return false;
    }
    public static final int SYSTEM_PROPERTY = 0;
    public static final int FILE_PROPERTY   = 1;
    public static final int SQL_PROPERTY    = 2;
    public static final int     FILES_NOT_MODIFIED = 0;
    public static final int     FILES_MODIFIED     = 1;
    public static final int     FILES_MODIFIED_NEW = 2;
    public static final int     FILES_NEW          = 3;
    private static final String MODIFIED_NO        = "no";
    private static final String MODIFIED_YES       = "yes";
    private static final String MODIFIED_YES_NEW   = "yes-new-files";
    private static final String MODIFIED_NO_NEW    = "no-new-files";
    private static final HashMap dbMeta   = new HashMap(67);
    private static final HashMap textMeta = new HashMap(17);
    public static final String VERSION_STRING_1_8_0 = "1.8.0";
    public static final String THIS_VERSION         = "2.2.8";
    public static final String THIS_FULL_VERSION    = "2.2.8";
    public static final String THIS_CACHE_VERSION   = "2.0.0";
    public static final String PRODUCT_NAME         = "HSQL Database Engine";
    public static final int    MAJOR                = 2,
                               MINOR                = 2,
                               REVISION             = 8;
    public static final String system_lockfile_poll_retries_property =
        "hsqldb.lockfile_poll_retries";
    public static final String system_max_char_or_varchar_display_size =
        "hsqldb.max_char_or_varchar_display_size";
    public static final String hsqldb_inc_backup = "hsqldb.inc_backup";
    public static final String  hsqldb_version  = "version";
    public static final String  hsqldb_readonly = "readonly";
    private static final String hsqldb_modified = "modified";
    public static final String runtime_gc_interval = "runtime.gc_interval";
    public static final String url_ifexists        = "ifexists";
    public static final String url_create          = "create";
    public static final String url_default_schema  = "default_schema";
    public static final String url_check_props     = "check_props";
    public static final String url_get_column_name = "get_column_name";
    public static final String url_storage_class_name = "storage_class_name";
    public static final String url_fileaccess_class_name =
        "fileaccess_class_name";
    public static final String url_storage_key = "storage_key";
    public static final String url_shutdown    = "shutdown";
    public static final String url_crypt_key      = "crypt_key";
    public static final String url_crypt_type     = "crypt_type";
    public static final String url_crypt_provider = "crypt_provider";
    public static final String url_crypt_lobs     = "crypt_lobs";
    public static final String hsqldb_tx       = "hsqldb.tx";
    public static final String hsqldb_tx_level = "hsqldb.tx_level";
    public static final String hsqldb_tx_conflict_rollback =
        "hsqldb.tx_conflict_rollback";
    public static final String hsqldb_applog         = "hsqldb.applog";
    public static final String hsqldb_sqllog         = "hsqldb.sqllog";
    public static final String hsqldb_lob_file_scale = "hsqldb.lob_file_scale";
    public static final String hsqldb_cache_file_scale =
        "hsqldb.cache_file_scale";
    public static final String hsqldb_cache_free_count =
        "hsqldb.cache_free_count";
    public static final String hsqldb_cache_rows = "hsqldb.cache_rows";
    public static final String hsqldb_cache_size = "hsqldb.cache_size";
    public static final String hsqldb_default_table_type =
        "hsqldb.default_table_type";
    public static final String hsqldb_defrag_limit   = "hsqldb.defrag_limit";
    public static final String hsqldb_files_readonly = "files_readonly";
    public static final String hsqldb_lock_file      = "hsqldb.lock_file";
    public static final String hsqldb_log_data       = "hsqldb.log_data";
    public static final String hsqldb_log_size       = "hsqldb.log_size";
    public static final String hsqldb_nio_data_file  = "hsqldb.nio_data_file";
    public static final String hsqldb_nio_max_size   = "hsqldb.nio_max_size";
    public static final String hsqldb_script_format  = "hsqldb.script_format";
    public static final String hsqldb_temp_directory = "hsqldb.temp_directory";
    public static final String hsqldb_result_max_memory_rows =
        "hsqldb.result_max_memory_rows";
    public static final String hsqldb_write_delay = "hsqldb.write_delay";
    public static final String hsqldb_write_delay_millis =
        "hsqldb.write_delay_millis";
    public static final String hsqldb_full_log_replay =
        "hsqldb.full_log_replay";
    public static final String sql_ref_integrity       = "sql.ref_integrity";
    public static final String sql_compare_in_locale = "sql.compare_in_locale";
    public static final String sql_enforce_size        = "sql.enforce_size";
    public static final String sql_enforce_strict_size =
        "sql.enforce_strict_size";    
    public static final String sql_enforce_refs  = "sql.enforce_refs";
    public static final String sql_enforce_names = "sql.enforce_names";
    public static final String sql_enforce_types = "sql.enforce_types";
    public static final String sql_enforce_tdcd  = "sql.enforce_tdc_delete";
    public static final String sql_enforce_tdcu  = "sql.enforce_tdc_update";
    public static final String sql_concat_nulls  = "sql.concat_nulls";
    public static final String sql_nulls_first   = "sql.nulls_first";
    public static final String sql_unique_nulls  = "sql.unique_nulls";
    public static final String sql_convert_trunc = "sql.convert_trunc";
    public static final String sql_avg_scale     = "sql.avg_scale";
    public static final String sql_double_nan    = "sql.double_nan";
    public static final String sql_syntax_db2    = "sql.syntax_db2";
    public static final String sql_syntax_mss    = "sql.syntax_mss";
    public static final String sql_syntax_mys    = "sql.syntax_mys";
    public static final String sql_syntax_ora    = "sql.syntax_ora";
    public static final String sql_syntax_pgs    = "sql.syntax_pgs";
    public static final String jdbc_translate_tti_types =
        "jdbc.translate_tti_types";
    public static final String sql_identity_is_pk = "sql.identity_is_pk";
    public static final String sql_longvar_is_lob = "sql.longvar_is_lob";
    public static final String textdb_cache_scale = "textdb.cache_scale";
    public static final String textdb_cache_size_scale =
        "textdb.cache_size_scale";
    public static final String textdb_cache_rows = "textdb.cache_rows";
    public static final String textdb_cache_size = "textdb.cache_size";
    public static final String textdb_all_quoted = "textdb.all_quoted";
    public static final String textdb_allow_full_path =
        "textdb.allow_full_path";
    public static final String textdb_encoding     = "textdb.encoding";
    public static final String textdb_ignore_first = "textdb.ignore_first";
    public static final String textdb_quoted       = "textdb.quoted";
    public static final String textdb_fs           = "textdb.fs";
    public static final String textdb_vs           = "textdb.vs";
    public static final String textdb_lvs          = "textdb.lvs";
    static {
        textMeta.put(textdb_allow_full_path,
                     HsqlProperties.getMeta(textdb_allow_full_path,
                                            SYSTEM_PROPERTY, false));
        textMeta.put(textdb_quoted,
                     HsqlProperties.getMeta(textdb_quoted, SQL_PROPERTY,
                                            true));
        textMeta.put(textdb_all_quoted,
                     HsqlProperties.getMeta(textdb_all_quoted, SQL_PROPERTY,
                                            false));
        textMeta.put(textdb_ignore_first,
                     HsqlProperties.getMeta(textdb_ignore_first, SQL_PROPERTY,
                                            false));
        textMeta.put(textdb_fs,
                     HsqlProperties.getMeta(textdb_fs, SQL_PROPERTY, ","));
        textMeta.put(textdb_vs,
                     HsqlProperties.getMeta(textdb_vs, SQL_PROPERTY, null));
        textMeta.put(textdb_lvs,
                     HsqlProperties.getMeta(textdb_lvs, SQL_PROPERTY, null));
        textMeta.put(textdb_encoding,
                     HsqlProperties.getMeta(textdb_encoding, SQL_PROPERTY,
                                            "ISO-8859-1"));
        textMeta.put(textdb_cache_scale,
                     HsqlProperties.getMeta(textdb_cache_scale, SQL_PROPERTY,
                                            10, 8, 16));
        textMeta.put(textdb_cache_size_scale,
                     HsqlProperties.getMeta(textdb_cache_size_scale,
                                            SQL_PROPERTY, 10, 6, 20));
        textMeta.put(textdb_cache_rows,
                     HsqlProperties.getMeta(textdb_cache_rows, SQL_PROPERTY,
                                            1000, 100, 1000000));
        textMeta.put(textdb_cache_size,
                     HsqlProperties.getMeta(textdb_cache_size, SQL_PROPERTY,
                                            100, 10, 1000000));
        dbMeta.putAll(textMeta);
        dbMeta.put(hsqldb_version,
                   HsqlProperties.getMeta(hsqldb_version, FILE_PROPERTY,
                                          null));
        dbMeta.put(hsqldb_modified,
                   HsqlProperties.getMeta(hsqldb_modified, FILE_PROPERTY,
                                          null));
        dbMeta.put(hsqldb_readonly,
                   HsqlProperties.getMeta(hsqldb_readonly, FILE_PROPERTY,
                                          false));
        dbMeta.put(hsqldb_files_readonly,
                   HsqlProperties.getMeta(hsqldb_files_readonly,
                                          FILE_PROPERTY, false));
        dbMeta.put(hsqldb_tx,
                   HsqlProperties.getMeta(hsqldb_tx, SQL_PROPERTY, "LOCKS"));
        dbMeta.put(hsqldb_tx_level,
                   HsqlProperties.getMeta(hsqldb_tx_level, SQL_PROPERTY,
                                          "READ_COMMITTED"));
        dbMeta.put(hsqldb_temp_directory,
                   HsqlProperties.getMeta(hsqldb_temp_directory, SQL_PROPERTY,
                                          null));
        dbMeta.put(hsqldb_default_table_type,
                   HsqlProperties.getMeta(hsqldb_default_table_type,
                                          SQL_PROPERTY, "MEMORY"));
        dbMeta.put(hsqldb_tx_conflict_rollback,
                   HsqlProperties.getMeta(hsqldb_tx_conflict_rollback,
                                          SQL_PROPERTY, true));
        dbMeta.put(jdbc_translate_tti_types,
                   HsqlProperties.getMeta(jdbc_translate_tti_types,
                                          SQL_PROPERTY, true));
        dbMeta.put(hsqldb_inc_backup,
                   HsqlProperties.getMeta(hsqldb_inc_backup, SQL_PROPERTY,
                                          true));
        dbMeta.put(hsqldb_lock_file,
                   HsqlProperties.getMeta(hsqldb_lock_file, SQL_PROPERTY,
                                          true));
        dbMeta.put(hsqldb_log_data,
                   HsqlProperties.getMeta(hsqldb_log_data, SQL_PROPERTY,
                                          true));
        dbMeta.put(hsqldb_nio_data_file,
                   HsqlProperties.getMeta(hsqldb_nio_data_file, SQL_PROPERTY,
                                          true));
        dbMeta.put(hsqldb_full_log_replay,
                   HsqlProperties.getMeta(hsqldb_full_log_replay,
                                          SQL_PROPERTY, false));
        dbMeta.put(sql_ref_integrity,
                   HsqlProperties.getMeta(sql_ref_integrity, SQL_PROPERTY,
                                          true));
        dbMeta.put(sql_enforce_names,
                   HsqlProperties.getMeta(sql_enforce_names, SQL_PROPERTY,
                                          false));
        dbMeta.put(sql_enforce_refs,
                   HsqlProperties.getMeta(sql_enforce_refs, SQL_PROPERTY,
                                          false));
        dbMeta.put(sql_enforce_size,
                   HsqlProperties.getMeta(sql_enforce_size, SQL_PROPERTY,
                                          true));
        dbMeta.put(sql_enforce_types,
                   HsqlProperties.getMeta(sql_enforce_types, SQL_PROPERTY,
                                          false));
        dbMeta.put(sql_enforce_tdcd,
                   HsqlProperties.getMeta(sql_enforce_tdcd, SQL_PROPERTY,
                                          true));
        dbMeta.put(sql_enforce_tdcu,
                   HsqlProperties.getMeta(sql_enforce_tdcu, SQL_PROPERTY,
                                          true));
        dbMeta.put(sql_concat_nulls,
                   HsqlProperties.getMeta(sql_concat_nulls, SQL_PROPERTY,
                                          true));
        dbMeta.put(sql_nulls_first,
                   HsqlProperties.getMeta(sql_nulls_first, SQL_PROPERTY,
                                          true));
        dbMeta.put(sql_unique_nulls,
                   HsqlProperties.getMeta(sql_unique_nulls, SQL_PROPERTY,
                                          true));
        dbMeta.put(sql_convert_trunc,
                   HsqlProperties.getMeta(sql_convert_trunc, SQL_PROPERTY,
                                          true));
        dbMeta.put(sql_avg_scale,
                   HsqlProperties.getMeta(sql_avg_scale, SQL_PROPERTY, 0, 0,
                                          10));
        dbMeta.put(sql_double_nan,
                   HsqlProperties.getMeta(sql_double_nan, SQL_PROPERTY, true));
        dbMeta.put(sql_syntax_db2,
                   HsqlProperties.getMeta(sql_syntax_db2, SQL_PROPERTY,
                                          false));
        dbMeta.put(sql_syntax_mss,
                   HsqlProperties.getMeta(sql_syntax_mss, SQL_PROPERTY,
                                          false));
        dbMeta.put(sql_syntax_mys,
                   HsqlProperties.getMeta(sql_syntax_mys, SQL_PROPERTY,
                                          false));
        dbMeta.put(sql_syntax_ora,
                   HsqlProperties.getMeta(sql_syntax_ora, SQL_PROPERTY,
                                          false));
        dbMeta.put(sql_syntax_pgs,
                   HsqlProperties.getMeta(sql_syntax_pgs, SQL_PROPERTY,
                                          false));
        dbMeta.put(sql_compare_in_locale,
                   HsqlProperties.getMeta(sql_compare_in_locale, SQL_PROPERTY,
                                          false));
        dbMeta.put(sql_identity_is_pk,
                   HsqlProperties.getMeta(sql_identity_is_pk, SQL_PROPERTY,
                                          false));
        dbMeta.put(sql_longvar_is_lob,
                   HsqlProperties.getMeta(sql_longvar_is_lob, SQL_PROPERTY,
                                          false));
        dbMeta.put(hsqldb_write_delay,
                   HsqlProperties.getMeta(hsqldb_write_delay, SQL_PROPERTY,
                                          true));
        dbMeta.put(hsqldb_write_delay_millis,
                   HsqlProperties.getMeta(hsqldb_write_delay_millis,
                                          SQL_PROPERTY, 500, 0, 10000));
        dbMeta.put(hsqldb_applog,
                   HsqlProperties.getMeta(hsqldb_applog, SQL_PROPERTY, 0, 0,
                                          3));
        dbMeta.put(hsqldb_sqllog,
                   HsqlProperties.getMeta(hsqldb_sqllog, SQL_PROPERTY, 0, 0,
                                          3));
        dbMeta.put(hsqldb_script_format,
                   HsqlProperties.getMeta(hsqldb_script_format, SQL_PROPERTY,
                                          0, new int[] {
            0, 1, 3
        }));
        dbMeta.put(hsqldb_lob_file_scale,
                   HsqlProperties.getMeta(hsqldb_lob_file_scale, SQL_PROPERTY,
                                          32, new int[] {
            1, 2, 4, 8, 16, 32
        }));
        dbMeta.put(hsqldb_cache_file_scale,
                   HsqlProperties.getMeta(hsqldb_cache_file_scale,
                                          SQL_PROPERTY, 8, new int[] {
            1, 8, 16, 32, 64, 128, 256, 512, 1024
        }));
        dbMeta.put(hsqldb_log_size,
                   HsqlProperties.getMeta(hsqldb_log_size, SQL_PROPERTY, 50,
                                          0, 1000));
        dbMeta.put(hsqldb_defrag_limit,
                   HsqlProperties.getMeta(hsqldb_defrag_limit, SQL_PROPERTY,
                                          0, 0, 100));
        dbMeta.put(runtime_gc_interval,
                   HsqlProperties.getMeta(runtime_gc_interval, SQL_PROPERTY,
                                          0, 0, 1000000));
        dbMeta.put(hsqldb_cache_size,
                   HsqlProperties.getMeta(hsqldb_cache_size, SQL_PROPERTY,
                                          10000, 100, 1000000));
        dbMeta.put(hsqldb_cache_rows,
                   HsqlProperties.getMeta(hsqldb_cache_rows, SQL_PROPERTY,
                                          50000, 100, 1000000));
        dbMeta.put(hsqldb_cache_free_count,
                   HsqlProperties.getMeta(hsqldb_cache_free_count,
                                          SQL_PROPERTY, 512, 0, 4096));
        dbMeta.put(hsqldb_result_max_memory_rows,
                   HsqlProperties.getMeta(hsqldb_result_max_memory_rows,
                                          SQL_PROPERTY, 0, 0, 1024 * 1024));
        dbMeta.put(hsqldb_nio_max_size,
                   HsqlProperties.getMeta(hsqldb_nio_max_size, SQL_PROPERTY,
                                          256, 64, 2048));
    }
    private Database database;
    public HsqlDatabaseProperties(Database db) {
        super(dbMeta, db.getPath(), db.logger.getFileAccess(),
              db.isFilesInJar());
        database = db;
        setNewDatabaseProperties();
    }
    void setNewDatabaseProperties() {
        setProperty(hsqldb_version, THIS_VERSION);
        setProperty(hsqldb_modified, MODIFIED_NO_NEW);
        if (database.logger.isStoredFileAccess()) {
            setProperty(hsqldb_cache_rows, 25000);
            setProperty(hsqldb_cache_size, 6000);
            setProperty(hsqldb_log_size, 10);
            setProperty(sql_enforce_size, true);
            setProperty(hsqldb_nio_data_file, false);
            setProperty(hsqldb_lock_file, true);
            setProperty(hsqldb_default_table_type, "cached");
            setProperty(jdbc_translate_tti_types, true);
        }
    }
    public boolean load() {
        boolean exists;
        if (!DatabaseURL.isFileBasedDatabaseType(database.getType())) {
            return true;
        }
        try {
            exists = super.load();
        } catch (Throwable t) {
            throw Error.error(t, ErrorCode.FILE_IO_ERROR,
                              ErrorCode.M_LOAD_SAVE_PROPERTIES, new Object[] {
                t.toString(), fileName
            });
        }
        if (!exists) {
            return false;
        }
        filterLoadedProperties();
        String version = getStringProperty(hsqldb_version);
        int    check = version.substring(0, 5).compareTo(VERSION_STRING_1_8_0);
        if (check < 0) {
            throw Error.error(ErrorCode.WRONG_DATABASE_FILE_VERSION);
        }
        if (check == 0) {
            if (getIntegerProperty(hsqldb_script_format) != 0) {
                throw Error.error(ErrorCode.WRONG_DATABASE_FILE_VERSION);
            }
        }
        check = version.substring(0, 2).compareTo(THIS_VERSION);
        if (check > 0) {
            throw Error.error(ErrorCode.WRONG_DATABASE_FILE_VERSION);
        }
        return true;
    }
    public void save() {
        if (!DatabaseURL.isFileBasedDatabaseType(database.getType())
                || database.isFilesReadOnly() || database.isFilesInJar()) {
            return;
        }
        try {
            HsqlProperties props = new HsqlProperties(dbMeta,
                database.getPath(), database.logger.getFileAccess(), false);
            if (getIntegerProperty(hsqldb_script_format) == 3) {
                props.setProperty(hsqldb_script_format, 3);
            }
            props.setProperty(hsqldb_version, THIS_VERSION);
            props.setProperty(hsqldb_modified, getProperty(hsqldb_modified));
            props.save(fileName + ".properties" + ".new");
            fa.renameElement(fileName + ".properties" + ".new",
                             fileName + ".properties");
        } catch (Throwable t) {
            database.logger.logSevereEvent("save failed", t);
            throw Error.error(t, ErrorCode.FILE_IO_ERROR,
                              ErrorCode.M_LOAD_SAVE_PROPERTIES, new Object[] {
                t.toString(), fileName
            });
        }
    }
    void filterLoadedProperties() {
        String val = stringProps.getProperty(sql_enforce_strict_size);
        if (val != null) {
            stringProps.setProperty(sql_enforce_size, val);
        }
        Enumeration en = stringProps.propertyNames();
        while (en.hasMoreElements()) {
            String  key    = (String) en.nextElement();
            boolean accept = dbMeta.containsKey(key);
            if (!accept) {
                stringProps.remove(key);
            }
        }
    }
    public void setURLProperties(HsqlProperties p) {
        boolean strict = false;
        if (p == null) {
            return;
        }
        String val = p.getProperty(sql_enforce_strict_size);
        if (val != null) {
            p.setProperty(sql_enforce_size, val);
            p.removeProperty(sql_enforce_strict_size);
        }
        strict = p.isPropertyTrue(url_check_props, false);
        for (Enumeration e = p.propertyNames(); e.hasMoreElements(); ) {
            String   propertyName  = (String) e.nextElement();
            String   propertyValue = p.getProperty(propertyName);
            boolean  valid         = false;
            boolean  validVal      = false;
            String   error         = null;
            Object[] meta          = (Object[]) dbMeta.get(propertyName);
            if (meta != null
                    && ((Integer) meta[HsqlProperties.indexType]).intValue()
                       == SQL_PROPERTY) {
                valid = true;
                error = HsqlProperties.validateProperty(propertyName,
                        propertyValue, meta);
                validVal = error == null;
            }
            if (propertyName.startsWith("sql.")
                    || propertyName.startsWith("hsqldb.")
                    || propertyName.startsWith("textdb.")) {
                if (strict && !valid) {
                    throw Error.error(ErrorCode.X_42555, propertyName);
                }
                if (strict && !validVal) {
                    throw Error.error(ErrorCode.X_42556, propertyName);
                }
            }
        }
        for (Enumeration e = p.propertyNames(); e.hasMoreElements(); ) {
            String   propertyName = (String) e.nextElement();
            Object[] meta         = (Object[]) dbMeta.get(propertyName);
            if (meta != null
                    && ((Integer) meta[HsqlProperties.indexType]).intValue()
                       == SQL_PROPERTY) {
                setDatabaseProperty(propertyName, p.getProperty(propertyName));
            }
        }
    }
    public Set getUserDefinedPropertyData() {
        Set      set = new HashSet();
        Iterator it  = dbMeta.values().iterator();
        while (it.hasNext()) {
            Object[] row = (Object[]) it.next();
            if (((Integer) row[HsqlProperties.indexType]).intValue()
                    == SQL_PROPERTY) {
                set.add(row);
            }
        }
        return set;
    }
    public boolean isUserDefinedProperty(String key) {
        Object[] row = (Object[]) dbMeta.get(key);
        return row != null
               && ((Integer) row[HsqlProperties.indexType]).intValue()
                  == SQL_PROPERTY;
    }
    public boolean isBoolean(String key) {
        Object[] row = (Object[]) dbMeta.get(key);
        return row != null && row[HsqlProperties.indexClass].equals("Boolean")
               && ((Integer) row[HsqlProperties.indexType]).intValue()
                  == SQL_PROPERTY;
    }
    public boolean isIntegral(String key) {
        Object[] row = (Object[]) dbMeta.get(key);
        return row != null && row[HsqlProperties.indexClass].equals("Integer")
               && ((Integer) row[HsqlProperties.indexType]).intValue()
                  == SQL_PROPERTY;
    }
    public boolean isString(String key) {
        Object[] row = (Object[]) dbMeta.get(key);
        return row != null && row[HsqlProperties.indexClass].equals("String")
               && ((Integer) row[HsqlProperties.indexType]).intValue()
                  == SQL_PROPERTY;
    }
    public boolean setDatabaseProperty(String key, String value) {
        Object[] meta  = (Object[]) dbMeta.get(key);
        String   error = HsqlProperties.validateProperty(key, value, meta);
        if (error != null) {
            return false;
        }
        stringProps.put(key, value);
        return true;
    }
    public int getDefaultWriteDelay() {
        if (database.logger.isStoredFileAccess()) {
            return 2000;
        }
        return 500;
    }
    public static final int NO_MESSAGE = 1;
    public int getErrorLevel() {
        return NO_MESSAGE;
    }
    public boolean divisionByZero() {
        return false;
    }
    public void setDBModified(int mode) {
        String value;
        switch (mode) {
            case FILES_NOT_MODIFIED :
                value = MODIFIED_NO;
                break;
            case FILES_MODIFIED :
                value = MODIFIED_YES;
                break;
            case FILES_MODIFIED_NEW :
                value = MODIFIED_YES_NEW;
                break;
            default :
                throw Error.runtimeError(ErrorCode.U_S0500,
                                         "HsqlDatabaseProperties");
        }
        stringProps.put(hsqldb_modified, value);
        save();
    }
    public int getDBModified() {
        String value = getStringProperty(hsqldb_modified);
        if (MODIFIED_YES.equals(value)) {
            return FILES_MODIFIED;
        } else if (MODIFIED_YES_NEW.equals(value)) {
            return FILES_MODIFIED_NEW;
        } else if (MODIFIED_NO_NEW.equals(value)) {
            return FILES_NEW;
        }
        return FILES_NOT_MODIFIED;
    }
    public String getProperty(String key) {
        Object[] metaData = (Object[]) dbMeta.get(key);
        if (metaData == null) {
            throw Error.error(ErrorCode.X_42555, key);
        }
        return stringProps.getProperty(key);
    }
    public String getPropertyString(String key) {
        Object[] metaData = (Object[]) dbMeta.get(key);
        if (metaData == null) {
            throw Error.error(ErrorCode.X_42555, key);
        }
        String prop = stringProps.getProperty(key);
        boolean isSystem =
            ((Integer) metaData[HsqlProperties.indexType]).intValue()
            == SYSTEM_PROPERTY;
        if (prop == null && isSystem) {
            try {
                prop = System.getProperty(key);
            } catch (SecurityException e) {}
        }
        if (prop == null) {
            Object value = metaData[HsqlProperties.indexDefaultValue];
            if (value == null) {
                return null;
            }
            return String.valueOf(value);
        }
        return prop;
    }
    public boolean isPropertyTrue(String key) {
        Boolean  value;
        Object[] metaData = (Object[]) dbMeta.get(key);
        if (metaData == null) {
            throw Error.error(ErrorCode.X_42555, key);
        }
        value = (Boolean) metaData[HsqlProperties.indexDefaultValue];
        String prop = null;
        boolean isSystem =
            ((Integer) metaData[HsqlProperties.indexType]).intValue()
            == SYSTEM_PROPERTY;
        if (isSystem) {
            try {
                prop = System.getProperty(key);
            } catch (SecurityException e) {}
        } else {
            prop = stringProps.getProperty(key);
        }
        if (prop != null) {
            value = Boolean.valueOf(prop);
        }
        return value.booleanValue();
    }
    public String getStringProperty(String key) {
        String   value;
        Object[] metaData = (Object[]) dbMeta.get(key);
        if (metaData == null) {
            throw Error.error(ErrorCode.X_42555, key);
        }
        value = (String) metaData[HsqlProperties.indexDefaultValue];
        String prop = stringProps.getProperty(key);
        if (prop != null) {
            value = prop;
        }
        return value;
    }
    public int getIntegerProperty(String key) {
        int      value;
        Object[] metaData = (Object[]) dbMeta.get(key);
        if (metaData == null) {
            throw Error.error(ErrorCode.X_42555, key);
        }
        value =
            ((Integer) metaData[HsqlProperties.indexDefaultValue]).intValue();
        String prop = stringProps.getProperty(key);
        if (prop != null) {
            try {
                value = Integer.parseInt(prop);
            } catch (NumberFormatException e) {}
        }
        return value;
    }
    public static Iterator getPropertiesMetaIterator() {
        return dbMeta.values().iterator();
    }
    public String getClientPropertiesAsString() {
        if (isPropertyTrue(jdbc_translate_tti_types)) {
            StringBuffer sb = new StringBuffer(jdbc_translate_tti_types);
            sb.append('=').append(true);
            return sb.toString();
        }
        return "";
    }
    public boolean isVersion18() {
        String version =
            getStringProperty(HsqlDatabaseProperties.hsqldb_version);
        return version.substring(0, 4).equals("1.8.");
    }
}