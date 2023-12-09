


package org.hsqldb.lib;

public interface RefCapableRBInterface {
    public String getString();
    public String getString(String... strings);
    public String getExpandedString();
    public String getExpandedString(String... strings);
    public String getString(int i1);
    public String getString(int i1, int i2);
    public String getString(int i1, int i2, int i3);
    public String getString(int i1, String s2);
    public String getString(String s1, int i2);
    public String getString(int i1, int i2, String s3);
    public String getString(int i1, String s2, int i3);
    public String getString(String s1, int i2, int i3);
    public String getString(int i1, String s2, String s3);
    public String getString(String s1, String s2, int i3);
    public String getString(String s1, int i2, String s3);
}
