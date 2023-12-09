


package org.hsqldb.lib.tar;

import org.hsqldb.lib.ValidatingResourceBundle;
import org.hsqldb.lib.RefCapableRBInterface;




public enum RB implements RefCapableRBInterface {
    DbBackup_syntax,
    DbBackup_syntaxerr,
    TarGenerator_syntax,
    pad_block_write,
    cleanup_rmfail,
    TarReader_syntax,
    unsupported_entry_present,
    bpr_write,
    stream_buffer_report,
    write_queue_report,
    file_missing,
    modified_property,
    file_disappeared,
    file_changed,
    file_appeared,
    pif_malformat,
    pif_malformat_size,
    zero_write,
    pif_toobig,
    read_denied,
    compression_unknown,
    insufficient_read,
    decompression_ranout,
    move_work_file,
    cant_overwrite,
    cant_write_dir,
    no_parent_dir,
    bad_block_write_len,
    illegal_block_boundary,
    workfile_delete_fail,
    unsupported_ext,
    dest_exists,
    parent_not_dir,
    cant_write_parent,
    parent_create_fail,
    tar_field_toobig,
    missing_supp_path,
    nonfile_entry,
    read_lt_1,
    data_changed,
    unexpected_header_key,
    tarreader_syntaxerr,
    unsupported_mode,
    dir_x_conflict,
    pif_unknown_datasize,
    pif_data_toobig,
    data_size_unknown,
    extraction_exists,
    extraction_exists_notfile,
    extraction_parent_not_dir,
    extraction_parent_not_writable,
    extraction_parent_mkfail,
    write_count_mismatch,
    header_field_missing,
    checksum_mismatch,
    create_only_normal,
    bad_header_value,
    bad_numeric_header_value,
    listing_format,
    ;

    private static ValidatingResourceBundle vrb =
            new ValidatingResourceBundle(
                    RB.class.getPackage().getName() + ".rb", RB.class);
    static {
        vrb.setMissingPosValueBehavior(
                ValidatingResourceBundle.NOOP_BEHAVIOR);
        vrb.setMissingPropertyBehavior(
                ValidatingResourceBundle.NOOP_BEHAVIOR);
    }

    public String getString() {
        return vrb.getString(this);
    }
    public String toString() {
        return ValidatingResourceBundle.resourceKeyFor(this);
    }
    public String getExpandedString() {
        return vrb.getExpandedString(this);
    }
    public String getExpandedString(String... strings) {
        return vrb.getExpandedString(this, strings);
    }
    public String getString(String... strings) {
        return vrb.getString(this, strings);
    }
    public String getString(int i1) {
        return vrb.getString(this, i1);
    }
    public String getString(int i1, int i2) {
        return vrb.getString(this, i1, i2);
    }
    public String getString(int i1, int i2, int i3) {
        return vrb.getString(this, i1, i2, i3);
    }
    public String getString(int i1, String s2) {
        return vrb.getString(this, i1, s2);
    }
    public String getString(String s1, int i2) {
        return vrb.getString(this, s1, i2);
    }
    public String getString(int i1, int i2, String s3) {
        return vrb.getString(this, i1, i2, s3);
    }
    public String getString(int i1, String s2, int i3) {
        return vrb.getString(this, i1, s2, i3);
    }
    public String getString(String s1, int i2, int i3) {
        return vrb.getString(this, s1, i2, i3);
    }
    public String getString(int i1, String s2, String s3) {
        return vrb.getString(this, i1, s3, s3);
    }
    public String getString(String s1, String s2, int i3) {
        return vrb.getString(this, s1, s2, i3);
    }
    public String getString(String s1, int i2, String s3) {
        return vrb.getString(this, s1, i2, s3);
    }
}
