


package org.hsqldb.lib.tar;

import org.hsqldb.lib.StringUtil;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.text.SimpleDateFormat;
import java.util.regex.Pattern;


public class TarReader {

    public static final int LIST_MODE = 0;

    
    public static final int EXTRACT_MODE = 1;

    
    public static final int OVERWRITE_MODE = 2;

    
    public static void main(String[] sa)
    throws IOException, TarMalformatException {

        if (sa.length < 1) {
            System.out.println(
                    RB.TarReader_syntax.getString(TarReader.class.getName()));
            System.out.println(RB.listing_format.getString());
            System.exit(0);
        }

        File exDir = (sa.length > 1 && sa[1].startsWith("--directory="))
                     ? (new File(sa[1].substring("--directory=".length())))
                     : null;
        int firstPatInd = (exDir == null) ? 2
                                          : 3;

        if (sa.length < firstPatInd
                || ((!sa[0].equals("t")) && !sa[0].equals("x"))) {
            throw new IllegalArgumentException(
                RB.tarreader_syntaxerr.getString(TarReader.class.getName()));
        }

        String[] patternStrings = null;

        if (sa.length > firstPatInd) {
            patternStrings = new String[sa.length - firstPatInd];

            for (int i = firstPatInd; i < sa.length; i++) {
                patternStrings[i - firstPatInd] = sa[i];
            }
        }

        if (sa[0].equals("t") && exDir != null) {
            throw new IllegalArgumentException(RB.dir_x_conflict.getString());
        }

        int dirIndex      = (exDir == null) ? 1
                                            : 2;
        int tarReaderMode = sa[0].equals("t") ? LIST_MODE
                                              : EXTRACT_MODE;

        new TarReader(new File(sa[dirIndex]), tarReaderMode, patternStrings,
                      null, exDir).read();
    }

    protected TarFileInputStream archive;
    protected Pattern[]          patterns = null;
    protected int                mode;
    protected File               extractBaseDir;    

    
    

    
    public TarReader(File inFile, int mode, String[] patternStrings,
                     Integer readBufferBlocks, File inDir) throws IOException {

        this.mode = mode;

        File archiveFile = inFile.getAbsoluteFile();

        extractBaseDir = (inDir == null) ? null
                                         : inDir.getAbsoluteFile();

        int compression =
            TarFileOutputStream.Compression.NO_COMPRESSION;

        if (archiveFile.getName().endsWith(".tgz")
                || archiveFile.getName().endsWith(".gz")) {
            compression = TarFileOutputStream.Compression.GZIP_COMPRESSION;
        }

        if (patternStrings != null) {
            patterns = new Pattern[patternStrings.length];

            for (int i = 0; i < patternStrings.length; i++) {
                patterns[i] = Pattern.compile(patternStrings[i]);
            }
        }

        
        
        archive = (readBufferBlocks == null)
                  ? new TarFileInputStream(archiveFile, compression)
                  : new TarFileInputStream(archiveFile, compression,
                                           readBufferBlocks.intValue());
    }

    public void read() throws IOException, TarMalformatException {

        TarEntryHeader header;
        boolean        anyUnsupporteds = false;
        boolean        matched;
        Long           paxSize   = null;
        String         paxString = null;

        try {
            EACH_HEADER:
            while (archive.readNextHeaderBlock()) {
                header = new TarEntryHeader(archive.readBuffer);

                char entryType = header.getEntryType();

                if (entryType == 'x') {
                    
                    paxSize   = getPifData(header).getSize();
                    paxString = header.toString();

                    continue;
                }

                if (paxSize != null) {

                    
                    
                    header.setDataSize(paxSize.longValue());

                    paxSize = null;
                }

                if (patterns != null) {
                    matched = false;

                    for (int i = 0; i < patterns.length; i++) {
                        if (patterns[i].matcher(header.getPath()).matches()) {
                            matched = true;

                            break;
                        }
                    }

                    if (!matched) {
                        paxString = null;

                        skipFileData(header);

                        continue EACH_HEADER;
                    }
                }

                if (entryType != '\0' && entryType != '0'
                        && entryType != 'x') {
                    anyUnsupporteds = true;
                }

                switch (mode) {

                    case LIST_MODE :
                        if (paxString != null) {
                            System.out.println(paxString);
                        }

                        System.out.println(header.toString());
                        skipFileData(header);
                        break;

                    case EXTRACT_MODE :
                    case OVERWRITE_MODE :
                        if (paxString != null) {
                            System.out.println(paxString);
                        }

                        
                        System.out.println(header.toString());

                        
                        
                        if (entryType == '\0' || entryType == '0'
                                || entryType == 'x') {
                            extractFile(header);
                        } else {
                            skipFileData(header);
                        }
                        break;

                    default :
                        throw new IllegalArgumentException(
                                RB.unsupported_mode.getString(mode));
                }

                paxString = null;
            }

            if (anyUnsupporteds) {
                System.out.println(RB.unsupported_entry_present.getString());
            }
        } catch (IOException ioe) {
            archive.close();

            throw ioe;
        }
    }

    protected PIFData getPifData(TarEntryHeader header)
    throws IOException, TarMalformatException {

        
        long dataSize = header.getDataSize();

        if (dataSize < 1) {
            throw new TarMalformatException(
                    RB.pif_unknown_datasize.getString());
        }

        if (dataSize > Integer.MAX_VALUE) {
            throw new TarMalformatException(RB.pif_data_toobig.getString(
                    Long.toString(dataSize), Integer.MAX_VALUE));
        }

        int readNow;
        int readBlocks = (int) (dataSize / 512L);
        int modulus    = (int) (dataSize % 512L);

        
        PipedInputStream  inPipe  = null;
        PipedOutputStream outPipe = new PipedOutputStream();

        
        try {
            inPipe = new PipedInputStream(outPipe);
            while (readBlocks > 0) {
                readNow = (readBlocks > archive.getReadBufferBlocks())
                          ? archive.getReadBufferBlocks()
                          : readBlocks;

                archive.readBlocks(readNow);

                readBlocks -= readNow;

                outPipe.write(archive.readBuffer, 0, readNow * 512);
            }

            if (modulus != 0) { archive.readBlock();
                outPipe.write(archive.readBuffer, 0, modulus);
            }

            outPipe.flush();    
        } catch (IOException ioe) {
            if (inPipe != null) {
                inPipe.close();
            }

            throw ioe;
        } finally {
            try {
                outPipe.close();
            } finally {
                outPipe = null;  
                inPipe = null;  
            }
        }

        return new PIFData(inPipe);
    }

    protected void extractFile(TarEntryHeader header)
    throws IOException, TarMalformatException {

        if (header.getDataSize() < 1) {
            throw new TarMalformatException(RB.data_size_unknown.getString());
        }

        int  readNow;
        int  readBlocks = (int) (header.getDataSize() / 512L);
        int  modulus    = (int) (header.getDataSize() % 512L);
        File newFile    = header.generateFile();

        if (!newFile.isAbsolute()) {
            newFile = (extractBaseDir == null) ? newFile.getAbsoluteFile()
                                               : new File(extractBaseDir,
                                               newFile.getPath());
        }

        
        File parentDir = newFile.getParentFile();

        if (newFile.exists()) {
            if (mode != TarReader.OVERWRITE_MODE) {
                throw new IOException(
                    RB.extraction_exists.getString(newFile.getAbsolutePath()));
            }

            if (!newFile.isFile()) {
                throw new IOException(
                        RB.extraction_exists_notfile.getString(
                        newFile.getAbsolutePath()));
            }

            
            
        }

        if (parentDir.exists()) {
            if (!parentDir.isDirectory()) {
                throw new IOException(
                        RB.extraction_parent_not_dir.getString(
                        parentDir.getAbsolutePath()));
            }

            if (!parentDir.canWrite()) {
                throw new IOException(
                        RB.extraction_parent_not_writable.getString(
                        parentDir.getAbsolutePath()));
            }
        } else {
            if (!parentDir.mkdirs()) {
                throw new IOException(
                        RB.extraction_parent_mkfail.getString(
                        parentDir.getAbsolutePath()));
            }
        }

        int              fileMode  = header.getFileMode();
        FileOutputStream outStream = new FileOutputStream(newFile);

        try {


            
            
            
            newFile.setExecutable(false, false);
            newFile.setReadable(false, false);
            newFile.setWritable(false, false);
            newFile.setExecutable(((fileMode & 0100) != 0), true);
            newFile.setReadable((fileMode & 0400) != 0, true);
            newFile.setWritable((fileMode & 0200) != 0, true);


            while (readBlocks > 0) {
                readNow = (readBlocks > archive.getReadBufferBlocks())
                          ? archive.getReadBufferBlocks()
                          : readBlocks;

                archive.readBlocks(readNow);

                readBlocks -= readNow;

                outStream.write(archive.readBuffer, 0, readNow * 512);
            }

            if (modulus != 0) {
                archive.readBlock();
                outStream.write(archive.readBuffer, 0, modulus);
            }

            outStream.flush();
        } finally {
            try {
                outStream.close();
            } finally {
                outStream = null;  
            }
        }

        newFile.setLastModified(header.getModTime() * 1000);

        if (newFile.length() != header.getDataSize()) {
            throw new IOException(RB.write_count_mismatch.getString(
                    Long.toString(header.getDataSize()),
                    newFile.getAbsolutePath(),
                    Long.toString(newFile.length())));
        }
    }

    protected void skipFileData(TarEntryHeader header)
    throws IOException, TarMalformatException {

        
        if (header.getDataSize() == 0) {
            return;
        }

        if (header.getDataSize() < 0) {
            throw new TarMalformatException(RB.data_size_unknown.getString());
        }

        int skipNow;
        int oddBlocks  = (header.getDataSize() % 512L == 0L) ? 0
                                                             : 1;
        int skipBlocks = (int) (header.getDataSize() / 512L) + oddBlocks;

        while (skipBlocks > 0) {
            skipNow = (skipBlocks > archive.getReadBufferBlocks())
                      ? archive.getReadBufferBlocks()
                      : skipBlocks;

            archive.readBlocks(skipNow);

            skipBlocks -= skipNow;
        }
    }

    
    @SuppressWarnings("serial")
    static protected class TarEntryHeader {

        static protected class MissingField extends Exception {

            private TarHeaderField field;

            public MissingField(TarHeaderField field) {
                this.field = field;
            }

            public String getMessage() {
                return RB.header_field_missing.getString(field.toString());
            }
        }

        protected SimpleDateFormat sdf =
            new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

        
        public TarEntryHeader(byte[] rawHeader) throws TarMalformatException {

            this.rawHeader = rawHeader;

            Long expectedCheckSum = readInteger(TarHeaderField.checksum);

            try {
                if (expectedCheckSum == null) {
                    throw new MissingField(TarHeaderField.checksum);
                }

                long calculatedCheckSum = headerChecksum();

                if (expectedCheckSum.longValue() != calculatedCheckSum) {
                    throw new TarMalformatException(
                            RB.checksum_mismatch.getString(
                            expectedCheckSum.toString(),
                            Long.toString(calculatedCheckSum)));
                }

                path = readString(TarHeaderField.name);

                if (path == null) {
                    throw new MissingField(TarHeaderField.name);
                }

                Long longObject = readInteger(TarHeaderField.mode);

                if (longObject == null) {
                    throw new MissingField(TarHeaderField.mode);
                }

                fileMode   = (int) longObject.longValue();
                longObject = readInteger(TarHeaderField.size);

                if (longObject != null) {
                    dataSize = longObject.longValue();
                }

                longObject = readInteger(TarHeaderField.mtime);

                if (longObject == null) {
                    throw new MissingField(TarHeaderField.mtime);
                }

                modTime = longObject.longValue();
            } catch (MissingField mf) {
                throw new TarMalformatException(mf.getMessage());
            }

            entryType = readChar(TarHeaderField.typeflag);
            ownerName = readString(TarHeaderField.uname);

            String pathPrefix = readString(TarHeaderField.prefix);

            if (pathPrefix != null) {
                path = pathPrefix + '/' + path;
            }

            
            
            ustar = isUstar();
        }

        protected byte[] rawHeader;
        
        protected String  path;
        protected int     fileMode;
        protected long    dataSize = -1;    
        protected long    modTime;
        protected char    entryType;
        protected String  ownerName;
        protected boolean ustar;

        
        public File generateFile() {

            if (entryType != '\0' && entryType != '0') {
                throw new IllegalStateException(
                        RB.create_only_normal.getString());
            }

            
            
            
            
            return new File(path);
        }

        public char getEntryType() {
            return entryType;
        }

        public String getPath() {
            return path;
        }

        
        public void setDataSize(long dataSize) {
            this.dataSize = dataSize;
        }

        public long getDataSize() {
            return dataSize;
        }

        public long getModTime() {
            return modTime;
        }

        public int getFileMode() {
            return fileMode;
        }

        
        public String toString() {

            StringBuffer sb =
                new StringBuffer(sdf.format(new Long(modTime * 1000L)) + ' ');

            sb.append((entryType == '\0') ? ' '
                                          : entryType);
            sb.append(ustar ? '*'
                            : ' ');
            sb.append(
                " "
                + StringUtil.toPaddedString(
                    Integer.toOctalString(fileMode), 4, ' ', false) + ' '
                        + StringUtil.toPaddedString(
                            Long.toString(dataSize), 11, ' ', false) + "  ");
            sb.append(StringUtil.toPaddedString(((ownerName == null) ? "-"
                                                                     : ownerName), 8,
                                                                     ' ',
                                                                     true));
            sb.append("  " + path);

            return sb.toString();
        }

        
        public boolean isUstar() throws TarMalformatException {

            String magicString = readString(TarHeaderField.magic);

            return magicString != null && magicString.startsWith("ustar");
        }

        
        public static int indexOf(byte[] ba, byte val, int from, int to) {

            for (int i = from; i < to; i++) {
                if (ba[i] == val) {
                    return i - from;
                }
            }

            return -1;
        }

        protected char readChar(TarHeaderField field) throws TarMalformatException {

            
            String s = readString(field);

            return (s == null) ? '\0'
                               : s.charAt(0);
        }

        
        protected String readString(TarHeaderField field) throws TarMalformatException {

            int start = field.getStart();
            int stop  = field.getStop();
            int termIndex = TarEntryHeader.indexOf(rawHeader, (byte) 0, start,
                                                   stop);

            switch (termIndex) {

                case 0 :
                    return null;

                case -1 :
                    termIndex = stop - start;
                    break;
            }

            try {
                return new String(rawHeader, start, termIndex);
            } catch (Throwable t) {

                
                throw new TarMalformatException(
                        RB.bad_header_value.getString(field.toString()));
            }
        }

        
        protected Long readInteger(TarHeaderField field)
                throws TarMalformatException {

            String s = readString(field);

            if (s == null) {
                return null;
            }

            try {
                return Long.valueOf(s, 8);
            } catch (NumberFormatException nfe) {
                throw new TarMalformatException(
                        RB.bad_numeric_header_value.getString(
                        field.toString(), nfe.toString()));
            }
        }

        protected long headerChecksum() {

            long sum = 0;

            for (int i = 0; i < 512; i++) {
                boolean isInRange =
                    (i >= TarHeaderField.checksum.getStart()
                     && i < TarHeaderField.checksum.getStop());

                
                
                
                sum += isInRange ? 32
                                 : (255 & rawHeader[i]);
            }

            return sum;
        }
    }
}
