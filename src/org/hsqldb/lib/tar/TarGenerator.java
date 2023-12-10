package org.hsqldb.lib.tar;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import org.hsqldb.lib.InputStreamInterface;
import org.hsqldb.lib.InputStreamWrapper;
import org.hsqldb.lib.StringUtil;
public class TarGenerator {
    public static void main(String[] sa)
    throws IOException, TarMalformatException {
        if (sa.length < 1) {
            System.out.println(
                    RB.TarGenerator_syntax.getString(DbBackup.class.getName()));
            System.exit(0);
        }
        TarGenerator generator = new TarGenerator(new File(sa[0]), true, null);
        if (sa.length == 1) {
            generator.queueEntry("stdin", System.in, 10240);
        } else {
            for (int i = 1; i < sa.length; i++) {
                generator.queueEntry(new File(sa[i]));
            }
        }
        generator.write();
    }
    protected TarFileOutputStream archive;
    protected List<TarEntrySupplicant> entryQueue =
            new ArrayList<TarEntrySupplicant>();
    protected long                paxThreshold = 0100000000000L;
    public void setPaxThreshold(long paxThreshold) {
        this.paxThreshold = paxThreshold;
    }
    public long getPaxThreshold() {
        return paxThreshold;
    }
    public TarGenerator(File inFile, boolean overWrite,
                        Integer blocksPerRecord) throws IOException {
        File archiveFile = inFile.getAbsoluteFile();
        int compression = TarFileOutputStream.Compression.NO_COMPRESSION;
        if (archiveFile.getName().endsWith(".tgz")
                || archiveFile.getName().endsWith(".tar.gz")) {
            compression = TarFileOutputStream.Compression.GZIP_COMPRESSION;
        } else if (archiveFile.getName().endsWith(".tar")) {
        } else {
            throw new IllegalArgumentException(RB.unsupported_ext.getString(
                    getClass().getName(), archiveFile.getPath()));
        }
        if (archiveFile.exists()) {
            if (!overWrite) {
                throw new IOException(
                        RB.dest_exists.getString(archiveFile.getPath()));
            }
        } else {
            File parentDir = archiveFile.getParentFile();
            if (parentDir.exists()) {
                if (!parentDir.isDirectory()) {
                    throw new IOException(
                        RB.parent_not_dir.getString(parentDir.getPath()));
                }
                if (!parentDir.canWrite()) {
                    throw new IOException(
                        RB.cant_write_parent.getString(parentDir.getPath()));
                }
            } else {
                if (!parentDir.mkdirs()) {
                    throw new IOException(
                        RB.parent_create_fail.getString(parentDir.getPath()));
                }
            }
        }
        archive = (blocksPerRecord == null)
                  ? new TarFileOutputStream(archiveFile, compression)
                  : new TarFileOutputStream(archiveFile, compression,
                                            blocksPerRecord.intValue());
        if (blocksPerRecord != null && TarFileOutputStream.debug) {
            System.out.println(
                    RB.bpr_write.getString(blocksPerRecord.intValue()));
        }
    }
    public void queueEntry(File file)
    throws FileNotFoundException, TarMalformatException {
        queueEntry(null, file);
    }
    public void queueEntry(String entryPath,
                           File file)
                           throws FileNotFoundException,
                                  TarMalformatException {
        entryQueue.add(new TarEntrySupplicant(entryPath, file, archive));
    }
    public void queueEntry(String entryPath,
                           InputStreamInterface is)
                           throws FileNotFoundException,
                                  TarMalformatException {
        entryQueue.add(new TarEntrySupplicant(entryPath, is, archive));
    }
    public void queueEntry(String entryPath, InputStream inStream,
                           int maxBytes)
                           throws IOException, TarMalformatException {
        entryQueue.add(new TarEntrySupplicant(entryPath, inStream, maxBytes,
                                              '0', archive));
    }
    public void write() throws IOException, TarMalformatException {
        if (TarFileOutputStream.debug) {
            System.out.println(
                    RB.write_queue_report.getString(entryQueue.size()));
        }
        TarEntrySupplicant entry;
        try {
            for (int i = 0; i < entryQueue.size(); i++) {
                System.err.print(Integer.toString(i + 1) + " / "
                                 + entryQueue.size() + ' ');
                entry = entryQueue.get(i);
                System.err.print(entry.getPath() + "... ");
                if (entry.getDataSize() >= paxThreshold) {
                    entry.makeXentry().write();
                    System.err.print("x... ");
                }
                entry.write();
                archive.assertAtBlockBoundary();
                System.err.println();
            }
            archive.finish();
        } catch (IOException ioe) {
            System.err.println();    
            try {
                for (TarEntrySupplicant sup : entryQueue) {
                    sup.close();
                }
                archive.close();
            } catch (IOException ne) {
            }
            throw ioe;
        }
    }
    static protected class TarEntrySupplicant {
        static protected byte[] HEADER_TEMPLATE =
            TarFileOutputStream.ZERO_BLOCK.clone();
        static Character              swapOutDelim = null;
        final protected static byte[] ustarBytes   = {
            'u', 's', 't', 'a', 'r'
        };
        static {
            char c = System.getProperty("file.separator").charAt(0);
            if (c != '/') {
                swapOutDelim = new Character(c);
            }
            try {
                writeField(TarHeaderField.uid, 0L, HEADER_TEMPLATE);
                writeField(TarHeaderField.gid, 0L, HEADER_TEMPLATE);
            } catch (TarMalformatException tme) {
                throw new RuntimeException(tme);
            }
            int magicStart = TarHeaderField.magic.getStart();
            for (int i = 0; i < ustarBytes.length; i++) {
                HEADER_TEMPLATE[magicStart + i] = ustarBytes[i];
            }
            HEADER_TEMPLATE[263] = '0';
            HEADER_TEMPLATE[264] = '0';
        }
        static protected void writeField(TarHeaderField field, String newValue,
                                         byte[] target)
                                         throws TarMalformatException {
            int    start = field.getStart();
            int    stop  = field.getStop();
            byte[] ba;
            try {
                ba = newValue.getBytes("ISO-8859-1");
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }
            if (ba.length > stop - start) {
                throw new TarMalformatException(
                    RB.tar_field_toobig.getString(field.toString(), newValue));
            }
            for (int i = 0; i < ba.length; i++) {
                target[start + i] = ba[i];
            }
        }
        static protected void clearField(TarHeaderField field, byte[] target) {
            int start = field.getStart();
            int stop  = field.getStop();
            for (int i = start; i < stop; i++) {
                target[i] = 0;
            }
        }
        static protected void writeField(TarHeaderField field, long newValue,
                                         byte[] target)
                                         throws TarMalformatException {
            TarEntrySupplicant.writeField(
                field,
                TarEntrySupplicant.prePaddedOctalString(
                    newValue, field.getStop() - field.getStart()), target);
        }
        public static String prePaddedOctalString(long val, int width) {
            return StringUtil.toPaddedString(Long.toOctalString(val), width,
                                             '0', false);
        }
        protected byte[] rawHeader = HEADER_TEMPLATE.clone();
        protected String fileMode  = DEFAULT_FILE_MODES;
        protected InputStreamInterface         inputStream;
        protected String              path;
        protected long                modTime;
        protected TarFileOutputStream tarStream;
        protected long                dataSize;    
        protected boolean             paxSized = false;
        public String getPath() {
            return path;
        }
        public long getDataSize() {
            return dataSize;
        }
        protected TarEntrySupplicant(String path, char typeFlag,
                                     TarFileOutputStream tarStream)
                                     throws TarMalformatException {
            if (path == null) {
                throw new IllegalArgumentException(
                    RB.missing_supp_path.getString());
            }
            this.path = (swapOutDelim == null) ? path
                                               : path.replace(
                                                   swapOutDelim.charValue(),
                                                   '/');
            this.tarStream = tarStream;
            writeField(TarHeaderField.typeflag, typeFlag);
            if (typeFlag == '\0' || typeFlag == ' ') {
                writeField(TarHeaderField.uname,
                           System.getProperty("user.name"), HEADER_TEMPLATE);
                writeField(TarHeaderField.gname, "root", HEADER_TEMPLATE);
            }
        }
        public TarEntrySupplicant makeXentry()
        throws IOException, TarMalformatException {
            PIFGenerator pif = new PIFGenerator(new File(path));
            pif.addRecord("size", dataSize);
            paxSized = true;
            return new TarEntrySupplicant(
                pif.getName(), new ByteArrayInputStream(pif.toByteArray()),
                pif.size(), 'x', tarStream);
        }
        public TarEntrySupplicant(String path, File file,
                                  TarFileOutputStream tarStream)
                                  throws FileNotFoundException,
                                         TarMalformatException {
            this(((path == null) ? file.getPath()
                                 : path), '0', tarStream);
            if (!file.isFile()) {
                throw new IllegalArgumentException(
                        RB.nonfile_entry.getString());
            }
            if (!file.canRead()) {
                throw new IllegalArgumentException(
                    RB.read_denied.getString(file.getAbsolutePath()));
            }
            modTime     = file.lastModified() / 1000L;
            fileMode    = TarEntrySupplicant.getLameMode(file);
            dataSize    = file.length();
            inputStream = new InputStreamWrapper(new FileInputStream(file));
        }
        public TarEntrySupplicant(String path, InputStreamInterface is,
                                  TarFileOutputStream tarStream)
                                  throws FileNotFoundException,
                                         TarMalformatException {
            this( path, '0', tarStream);
            modTime     = System.currentTimeMillis() / 1000L;
            fileMode    = DEFAULT_FILE_MODES;
            inputStream = is;
        }
        public TarEntrySupplicant(String path, InputStream origStream,
                                  int maxBytes, char typeFlag,
                                  TarFileOutputStream tarStream)
                                  throws IOException, TarMalformatException {
            this(path, typeFlag, tarStream);
            if (maxBytes < 1) {
                throw new IllegalArgumentException(RB.read_lt_1.getString());
            }
            int               i;
            PipedOutputStream outPipe = new PipedOutputStream();
            try {
                inputStream = new InputStreamWrapper(new PipedInputStream(outPipe));
                while ((i =
                        origStream
                            .read(tarStream.writeBuffer, 0, tarStream
                                .writeBuffer.length)) > 0) {
                    outPipe.write(tarStream.writeBuffer, 0, i);
                }
                outPipe.flush();    
                dataSize = inputStream.available();
                if (TarFileOutputStream.debug) {
                    System.out.println(
                        RB.stream_buffer_report.getString(
                                Long.toString(dataSize)));
                }
            } catch (IOException ioe) {
                close();
                throw ioe;
            } finally {
                try {
                    outPipe.close();
                } finally {
                    outPipe = null;  
                }
            }
            modTime = new java.util.Date().getTime() / 1000L;
        }
        public void close() throws IOException {
            if (inputStream == null) {
                return;
            }
            try {
                inputStream.close();
            } finally {
                inputStream = null;  
            }
        }
        protected long headerChecksum() {
            long sum = 0;
            for (int i = 0; i < rawHeader.length; i++) {
                boolean isInRange =
                    (i >= TarHeaderField.checksum.getStart()
                     && i < TarHeaderField.checksum.getStop());
                sum += isInRange ? 32
                                 : (255 & rawHeader[i]);
            }
            return sum;
        }
        protected void clearField(TarHeaderField field) {
            TarEntrySupplicant.clearField(field, rawHeader);
        }
        protected void writeField(TarHeaderField field, String newValue)
                throws TarMalformatException {
            TarEntrySupplicant.writeField(field, newValue, rawHeader);
        }
        protected void writeField(TarHeaderField field, long newValue)
                throws TarMalformatException {
            TarEntrySupplicant.writeField(field, newValue, rawHeader);
        }
        protected void writeField(TarHeaderField field, char c)
                throws TarMalformatException {
            TarEntrySupplicant.writeField(field, Character.toString(c),
                                          rawHeader);
        }
        public void write() throws IOException, TarMalformatException {
            int i;
            try {
                long sizeLimit = inputStream.getSizeLimit();
                if (sizeLimit == 0) {
                    return;
                }
                if (sizeLimit > 0) {
                    dataSize = sizeLimit;
                }
                writeField(TarHeaderField.name, path);
                writeField(TarHeaderField.mode, fileMode);
                if (!paxSized) {
                    writeField(TarHeaderField.size, dataSize);
                }
                writeField(TarHeaderField.mtime, modTime);
                writeField(
                    TarHeaderField.checksum,
                    TarEntrySupplicant.prePaddedOctalString(
                        headerChecksum(), 6) + "\0 ");
                tarStream.writeBlock(rawHeader);
                long dataStart = tarStream.getBytesWritten();
                while ((i = inputStream.read(tarStream.writeBuffer)) > 0) {
                    tarStream.write(i);
                }
                if (dataStart + dataSize != tarStream.getBytesWritten()) {
                    throw new IOException(
                            RB.data_changed.getString(Long.toString(dataSize),
                            Long.toString(
                            tarStream.getBytesWritten() - dataStart)));
                }
                tarStream.padCurrentBlock();
            } finally {
                close();
            }
        }
        static protected String getLameMode(File file) {
            int umod = 0;
            if (file.canExecute()) {
                umod = 1;
            }
            if (file.canWrite()) {
                umod += 2;
            }
            if (file.canRead()) {
                umod += 4;
            }
            return "0" + umod + "00";
        }
        public static final String DEFAULT_FILE_MODES = "600";
    }
}