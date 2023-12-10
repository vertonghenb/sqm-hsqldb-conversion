package org.hsqldb.lib.tar;
@SuppressWarnings("boxing")
public enum TarHeaderField {
    name(0, 100),
    mode(100, 107),
    uid(108, 115),
    gid(116, 123),
    size(124, 135),
    mtime(136, 147),  
    checksum(148, 156),
    typeflag(156, 157), 
    magic(257, 263),
    uname(265, 296),
    gname(297, 328),
    prefix(345, 399),
    ;
    private TarHeaderField(int start, int stop) {
        this.start = start;
        this.stop = stop;
    }
    private int start, stop;
    public int getStart() { return start; }
    public int getStop() { return stop; }
}