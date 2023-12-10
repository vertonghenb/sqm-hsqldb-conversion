package org.hsqldb.lib;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.util.Random;
import org.hsqldb.lib.java.JavaSystem;
public class FileUtil implements FileAccess {
    private static FileUtil      fileUtil      = new FileUtil();
    private static FileAccessRes fileAccessRes = new FileAccessRes();
    FileUtil() {}
    public static FileUtil getFileUtil() {
        return fileUtil;
    }
    public static FileAccess getFileAccess(boolean isResource) {
        return isResource ? (FileAccess) fileAccessRes
                          : (FileAccess) fileUtil;
    }
    public boolean isStreamElement(java.lang.String elementName) {
        return (new File(elementName)).exists();
    }
    public InputStream openInputStreamElement(java.lang.String streamName)
    throws java.io.IOException {
        try {
            return new FileInputStream(new File(streamName));
        } catch (Throwable e) {
            throw JavaSystem.toIOException(e);
        }
    }
    public void createParentDirs(String filename) {
        makeParentDirectories(new File(filename));
    }
    public void removeElement(String filename) {
        if (isStreamElement(filename)) {
            delete(filename);
        }
    }
    public void renameElement(java.lang.String oldName,
                              java.lang.String newName) {
        renameWithOverwrite(oldName, newName);
    }
    public java.io.OutputStream openOutputStreamElement(
            java.lang.String streamName) throws java.io.IOException {
        return new FileOutputStream(new File(streamName));
    }
    public final boolean fsIsIgnoreCase =
        (new File("A")).equals(new File("a"));
    public final boolean fsNormalizesPosixSeparator =
        (new File("/")).getPath().endsWith(File.separator);
    final Random random = new Random(System.currentTimeMillis());
    public boolean delete(String filename) {
        return (new File(filename)).delete();
    }
    public void deleteOnExit(File f) {
        JavaSystem.deleteOnExit(f);
    }
    public boolean exists(String filename) {
        return (new File(filename)).exists();
    }
    public boolean exists(String fileName, boolean resource, Class cla) {
        if (fileName == null || fileName.length() == 0) {
            return false;
        }
        return resource ? null != cla.getResource(fileName)
                        : FileUtil.getFileUtil().exists(fileName);
    }
    private boolean renameWithOverwrite(String oldname, String newname) {
        File file = new File(oldname);
        delete(newname);
        boolean renamed = file.renameTo(new File(newname));
        if (renamed) {
            return true;
        }
        System.gc();
        delete(newname);
        if (exists(newname)) {
            new File(newname).renameTo(new File(newDiscardFileName(newname)));
        }
        return file.renameTo(new File(newname));
    }
    public String absolutePath(String path) {
        return (new File(path)).getAbsolutePath();
    }
    public File canonicalFile(File f) throws IOException {
        return new File(f.getCanonicalPath());
    }
    public File canonicalFile(String path) throws IOException {
        return new File(new File(path).getCanonicalPath());
    }
    public String canonicalPath(File f) throws IOException {
        return f.getCanonicalPath();
    }
    public String canonicalPath(String path) throws IOException {
        return new File(path).getCanonicalPath();
    }
    public String canonicalOrAbsolutePath(String path) {
        try {
            return canonicalPath(path);
        } catch (Exception e) {
            return absolutePath(path);
        }
    }
    public void makeParentDirectories(File f) {
        String parent = f.getParent();
        if (parent != null) {
            new File(parent).mkdirs();
        } else {
            parent = f.getPath();
            int index = parent.lastIndexOf('/');
            if (index > 0) {
                parent = parent.substring(0, index);
                new File(parent).mkdirs();
            }
        }
    }
    public static String makeDirectories(String path) {
        try {
            File file = new File(path);
            file.mkdirs();
            return file.getCanonicalPath();
        } catch (IOException e) {
            return null;
        }
    }
    public FileAccess.FileSync getFileSync(java.io.OutputStream os)
    throws java.io.IOException {
        return new FileSync((FileOutputStream) os);
    }
    public static class FileSync implements FileAccess.FileSync {
        FileDescriptor outDescriptor;
        FileSync(FileOutputStream os) throws IOException {
            outDescriptor = os.getFD();
        }
        public void sync() throws IOException {
            outDescriptor.sync();
        }
    }
    public static class FileAccessRes implements FileAccess {
        public boolean isStreamElement(String fileName) {
            URL url = null;
            try {
                url = getClass().getResource(fileName);
                if (url == null) {
                    ClassLoader cl =
                        Thread.currentThread().getContextClassLoader();
                    if (cl != null) {
                        url = cl.getResource(fileName);
                    }
                }
            } catch (Throwable t) {
            }
            return url != null;
        }
        public InputStream openInputStreamElement(final String fileName)
        throws IOException {
            InputStream fis = null;
            try {
                fis = getClass().getResourceAsStream(fileName);
                if (fis == null) {
                    ClassLoader cl =
                        Thread.currentThread().getContextClassLoader();
                    if (cl != null) {
                        fis = cl.getResourceAsStream(fileName);
                    }
                }
            } catch (Throwable t) {
            } finally {
                if (fis == null) {
                    throw new FileNotFoundException(fileName);
                }
            }
            return fis;
        }
        public void createParentDirs(java.lang.String filename) {}
        public void removeElement(java.lang.String filename) {}
        public void renameElement(java.lang.String oldName,
                                  java.lang.String newName) {}
        public java.io.OutputStream openOutputStreamElement(String streamName)
        throws IOException {
            throw new IOException();
        }
        public FileAccess.FileSync getFileSync(OutputStream os)
        throws IOException {
            throw new IOException();
        }
    }
    public static boolean deleteOrRenameDatabaseFiles(String path) {
        DatabaseFilenameFilter filter = new DatabaseFilenameFilter(path);
        File[] fileList = filter.getExistingFileListInDirectory();
        for (int i = 0; i < fileList.length; i++) {
            fileList[i].delete();
        }
        File tempDir = new File(filter.canonicalFile.getPath() + ".tmp");
        if (tempDir.isDirectory()) {
            File[] tempList = tempDir.listFiles();
            for (int i = 0; i < tempList.length; i++) {
                tempList[i].delete();
            }
            tempDir.delete();
        }
        fileList = filter.getExistingMainFileSetList();
        if (fileList.length == 0) {
            return true;
        }
        System.gc();
        for (int i = 0; i < fileList.length; i++) {
            fileList[i].delete();
        }
        fileList = filter.getExistingMainFileSetList();
        for (int i = 0; i < fileList.length; i++) {
            fileList[i].renameTo(
                new File(newDiscardFileName(fileList[i].getPath())));
        }
        return true;
    }
    public static File[] getDatabaseFileList(String path) {
        DatabaseFilenameFilter filter = new DatabaseFilenameFilter(path);
        return filter.getExistingFileListInDirectory();
    }
    public static String newDiscardFileName(String filename) {
        String timestamp = StringUtil.toPaddedString(
            Integer.toHexString((int) System.currentTimeMillis()), 8, '0',
            true);
        String discardName = filename + "." + timestamp + ".old";
        return discardName;
    }
    static class DatabaseFilenameFilter implements FilenameFilter {
        String[] suffixes = new String[] {
            ".backup", ".properties", ".script", ".data", ".log", ".lck",
            ".lobs", ".sql.log", ".app.log"
        };
        private String dbName;
        private File   parent;
        private File   canonicalFile;
        DatabaseFilenameFilter(String dbName) {
            this.dbName   = dbName;
            canonicalFile = new File(dbName);
            try {
                canonicalFile = canonicalFile.getCanonicalFile();
            } catch (Exception e) {}
            parent = canonicalFile.getParentFile();
        }
        public File[] getCompleteMainFileSetList() {
            File[] fileList = new File[suffixes.length];
            for (int i = 0; i < suffixes.length; i++) {
                fileList[i] = new File(canonicalFile.getPath() + suffixes[i]);
            }
            return fileList;
        }
        public File[] getExistingMainFileSetList() {
            File[]        fileList = getCompleteMainFileSetList();
            HsqlArrayList list     = new HsqlArrayList();
            for (int i = 0; i < fileList.length; i++) {
                if (fileList[i].exists()) {
                    list.add(fileList[i]);
                }
            }
            fileList = new File[list.size()];
            list.toArray(fileList);
            return fileList;
        }
        public File[] getExistingFileListInDirectory() {
            File[] list = parent.listFiles(this);
            return list == null ? new File[]{}
                                : list;
        }
        public boolean accept(File dir, String name) {
            if (parent.equals(dir) && name.indexOf(dbName) == 0) {
                String suffix = name.substring(dbName.length());
                for (int i = 0; i < suffixes.length; i++) {
                    if (suffix.equals(suffixes[i])) {
                        return true;
                    }
                    if (suffix.startsWith(suffixes[i])) {
                        if (suffix.length() == suffixes[i].length()) {
                            return true;
                        }
                        if (name.endsWith(".new")) {
                            if (suffix.length() == suffixes[i].length() + 4) {
                                return true;
                            }
                        } else if (name.endsWith(".old")) {
                            if (suffix.length()
                                    == suffixes[i].length() + 9 + 4) {
                                return true;
                            }
                        }
                    }
                }
            }
            return false;
        }
    }
}