package org.hsqldb.lib.tar;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;
import org.hsqldb.lib.InputStreamInterface;
public class DbBackup {
    public static void main(String[] sa)
    throws IOException, TarMalformatException {
        try {
            if (sa.length < 1) {
                System.out.println(
                    RB.DbBackup_syntax.getString(DbBackup.class.getName()));
                System.out.println();
                System.out.println(RB.listing_format.getString());
                System.exit(0);
            }
            if (sa[0].equals("--save")) {
                boolean overWrite = sa.length > 1
                                    && sa[1].equals("--overwrite");
                if (sa.length != (overWrite ? 4
                                            : 3)) {
                    throw new IllegalArgumentException();
                }
                DbBackup backup = new DbBackup(new File(sa[sa.length - 2]),
                                               sa[sa.length - 1]);
                backup.setOverWrite(overWrite);
                backup.write();
            } else if (sa[0].equals("--list")) {
                if (sa.length < 2) {
                    throw new IllegalArgumentException();
                }
                String[] patternStrings = null;
                if (sa.length > 2) {
                    patternStrings = new String[sa.length - 2];
                    for (int i = 2; i < sa.length; i++) {
                        patternStrings[i - 2] = sa[i];
                    }
                }
                new TarReader(new File(sa[1]), TarReader
                    .LIST_MODE, patternStrings, new Integer(DbBackup
                        .generateBufferBlockValue(new File(sa[1]))), null)
                            .read();
            } else if (sa[0].equals("--extract")) {
                boolean overWrite = sa.length > 1
                                    && sa[1].equals("--overwrite");
                int firstPatInd = overWrite ? 4
                                            : 3;
                if (sa.length < firstPatInd) {
                    throw new IllegalArgumentException();
                }
                String[] patternStrings = null;
                if (sa.length > firstPatInd) {
                    patternStrings = new String[sa.length - firstPatInd];
                    for (int i = firstPatInd; i < sa.length; i++) {
                        patternStrings[i - firstPatInd] = sa[i];
                    }
                }
                File tarFile       = new File(sa[overWrite ? 2
                                                           : 1]);
                int  tarReaderMode = overWrite ? TarReader.OVERWRITE_MODE
                                               : TarReader.EXTRACT_MODE;
                new TarReader(
                    tarFile, tarReaderMode, patternStrings,
                    new Integer(DbBackup.generateBufferBlockValue(tarFile)),
                    new File(sa[firstPatInd - 1])).read();
            } else {
                throw new IllegalArgumentException();
            }
        } catch (IllegalArgumentException iae) {
            System.out.println(
                RB.DbBackup_syntaxerr.getString(DbBackup.class.getName()));
            System.exit(2);
        }
    }
    protected File         dbDir;
    protected File         archiveFile;
    protected String       instanceName;
    protected boolean      overWrite       = false;    
    protected boolean      abortUponModify = true;     
    File[]                 componentFiles;
    InputStreamInterface[] componentStreams;
    boolean[]              existList;
    public DbBackup(File archiveFile, String dbPath) {
        this.archiveFile = archiveFile;
        File dbPathFile = new File(dbPath);
        dbDir          = dbPathFile.getAbsoluteFile().getParentFile();
        instanceName   = dbPathFile.getName();
        componentFiles = new File[] {
            new File(dbDir, instanceName + ".properties"),
            new File(dbDir, instanceName + ".script"),
            new File(dbDir, instanceName + ".data"),
            new File(dbDir, instanceName + ".backup"),
            new File(dbDir, instanceName + ".log"),
            new File(dbDir, instanceName + ".lobs")
        };
        componentStreams = new InputStreamInterface[componentFiles.length];
        existList        = new boolean[componentFiles.length];
    }
    public DbBackup(File archiveFile, String dbPath, boolean script) {
        this.archiveFile = archiveFile;
        File dbPathFile = new File(dbPath);
        dbDir        = dbPathFile.getAbsoluteFile().getParentFile();
        instanceName = dbPathFile.getName();
        componentFiles = new File[]{
            new File(dbDir, instanceName + ".script"), };
        componentStreams = new InputStreamInterface[componentFiles.length];
        existList        = new boolean[componentFiles.length];
        abortUponModify  = false;
    }
    public void setStream(String fileExtension, InputStreamInterface is) {
        for (int i = 0; i < componentFiles.length; i++) {
            if (componentFiles[i].getName().endsWith(fileExtension)) {
                componentStreams[i] = is;
                break;
            }
        }
    }
    public void setOverWrite(boolean overWrite) {
        this.overWrite = overWrite;
    }
    public void setAbortUponModify(boolean abortUponModify) {
        this.abortUponModify = abortUponModify;
    }
    public boolean getOverWrite() {
        return overWrite;
    }
    public boolean getAbortUponModify() {
        return abortUponModify;
    }
    public void write() throws IOException, TarMalformatException {
        long startTime = new java.util.Date().getTime();
        checkEssentialFiles();
        TarGenerator generator = new TarGenerator(archiveFile, overWrite,
            new Integer(DbBackup.generateBufferBlockValue(componentFiles)));
        for (int i = 0; i < componentFiles.length; i++) {
            boolean exists = componentStreams[i] != null
                             || componentFiles[i].exists();
            if (!exists) {
                continue;
            }
            if (componentStreams[i] == null) {
                generator.queueEntry(componentFiles[i].getName(),
                                     componentFiles[i]);
                existList[i] = true;
            } else {
                generator.queueEntry(componentFiles[i].getName(),
                                     componentStreams[i]);
            }
        }
        generator.write();
        checkFilesNotChanged(startTime);
    }
    void checkEssentialFiles()
    throws FileNotFoundException, IllegalStateException {
        if (!componentFiles[0].getName().endsWith(".properties")) {
            return;
        }
        for (int i = 0; i < 2; i++) {
            boolean exists = componentStreams[i] != null
                             || componentFiles[i].exists();
            if (!exists) {
                throw new FileNotFoundException(
                    RB.file_missing.getString(
                        componentFiles[i].getAbsolutePath()));
            }
        }
        if (!abortUponModify) {
            return;
        }
        Properties      p   = new Properties();
        FileInputStream fis = null;
        try {
            File propertiesFile = componentFiles[0];
            fis = new FileInputStream(propertiesFile);
            p.load(fis);
        } catch (IOException io) {}
        finally {
            try {
                if (fis != null) {
                    fis.close();
                }
            } catch (IOException io) {}
            finally {
                fis = null;    
            }
        }
        String modifiedString = p.getProperty("modified");
        if (modifiedString != null
                && (modifiedString.equalsIgnoreCase("yes")
                    || modifiedString.equalsIgnoreCase("true"))) {
            throw new IllegalStateException(
                RB.modified_property.getString(modifiedString));
        }
    }
    void checkFilesNotChanged(long startTime) throws FileNotFoundException {
        if (!abortUponModify) {
            return;
        }
        try {
            for (int i = 0; i < componentFiles.length; i++) {
                if (componentFiles[i].exists()) {
                    if (!existList[i]) {
                        throw new FileNotFoundException(
                            RB.file_disappeared.getString(
                                componentFiles[i].getAbsolutePath()));
                    }
                    if (componentFiles[i].lastModified() > startTime) {
                        throw new FileNotFoundException(
                            RB.file_changed.getString(
                                componentFiles[i].getAbsolutePath()));
                    }
                } else if (existList[i]) {
                    throw new FileNotFoundException(
                        RB.file_appeared.getString(
                            componentFiles[i].getAbsolutePath()));
                }
            }
        } catch (IllegalStateException ise) {
            if (!archiveFile.delete()) {
                System.out.println(
                    RB.cleanup_rmfail.getString(
                        archiveFile.getAbsolutePath()));
            }
            throw ise;
        }
    }
    static protected int generateBufferBlockValue(File[] files) {
        long maxFileSize = 0;
        for (int i = 0; i < files.length; i++) {
            if (files[i] == null) {
                continue;
            }
            if (files[i].length() > maxFileSize) {
                maxFileSize = files[i].length();
            }
        }
        int idealBlocks = (int) (maxFileSize / (10L * 512L));
        if (idealBlocks < 1) {
            return 1;
        }
        if (idealBlocks > 40 * 1024) {
            return 40 * 1024;
        }
        return idealBlocks;
    }
    static protected int generateBufferBlockValue(File file) {
        return generateBufferBlockValue(new File[]{ file });
    }
}