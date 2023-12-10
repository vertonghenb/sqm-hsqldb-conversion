package org.hsqldb.lib.tar;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;
public class TarFileOutputStream {
    public interface Compression {
        public static final int NO_COMPRESSION            = 0;
        public static final int GZIP_COMPRESSION          = 1;
        public static final int DEFAULT_COMPRESSION       = NO_COMPRESSION;
        public static final int DEFAULT_BLOCKS_PER_RECORD = 20;
    }
    public static boolean debug = Boolean.getBoolean("DEBUG");
    protected int         blocksPerRecord;
    protected long        bytesWritten = 0;
    private OutputStream  writeStream;
    private File          targetFile;
    private File          writeFile;
    public byte[] writeBuffer;
    public static final byte[] ZERO_BLOCK = new byte[512];
    public TarFileOutputStream(File targetFile) throws IOException {
        this(targetFile, Compression.DEFAULT_COMPRESSION);
    }
    public TarFileOutputStream(File targetFile,
                               int compressionType) throws IOException {
        this(targetFile, compressionType,
             TarFileOutputStream.Compression.DEFAULT_BLOCKS_PER_RECORD);
    }
    public TarFileOutputStream(File targetFile, int compressionType,
                               int blocksPerRecord) throws IOException {
        this.blocksPerRecord = blocksPerRecord;
        this.targetFile      = targetFile;
        writeFile = new File(targetFile.getParentFile(),
                             targetFile.getName() + "-partial");
        if (this.writeFile.exists()) {
            throw new IOException(
                    RB.move_work_file.getString(writeFile.getAbsolutePath()));
        }
        if (targetFile.exists() && !targetFile.canWrite()) {
            throw new IOException(
                    RB.cant_overwrite.getString(targetFile.getAbsolutePath()));
        }
        File parentDir = targetFile.getAbsoluteFile().getParentFile();
        if (parentDir.exists() && parentDir.isDirectory()) {
            if (!parentDir.canWrite()) {
                throw new IOException(RB.cant_write_dir.getString(
                        parentDir.getAbsolutePath()));
            }
        } else {
            throw new IOException(
                    RB.no_parent_dir.getString(parentDir.getAbsolutePath()));
        }
        writeBuffer = new byte[blocksPerRecord * 512];
        switch (compressionType) {
            case TarFileOutputStream.Compression.NO_COMPRESSION :
                writeStream = new FileOutputStream(writeFile);
                break;
            case TarFileOutputStream.Compression.GZIP_COMPRESSION :
                writeStream =
                    new GZIPOutputStream(new FileOutputStream(writeFile),
                                         writeBuffer.length);
                break;
            default :
                throw new IllegalArgumentException(
                    RB.compression_unknown.getString(compressionType));
        }
        writeFile.setExecutable(false, true);
        writeFile.setExecutable(false, false);
        writeFile.setReadable(false, false);
        writeFile.setReadable(true, true);
        writeFile.setWritable(false, false);
        writeFile.setWritable(true, true);
    }
    public void write(byte[] byteArray, int byteCount) throws IOException {
        writeStream.write(byteArray, 0, byteCount);
        bytesWritten += byteCount;
    }
    public void write(int byteCount) throws IOException {
        write(writeBuffer, byteCount);
    }
    public void writeBlock(byte[] block) throws IOException {
        if (block.length != 512) {
            throw new IllegalArgumentException(
                    RB.bad_block_write_len.getString(block.length));
        }
        write(block, block.length);
    }
    public void writePadBlocks(int blockCount) throws IOException {
        for (int i = 0; i < blockCount; i++) {
            write(ZERO_BLOCK, ZERO_BLOCK.length);
        }
    }
    public void writePadBlock() throws IOException {
        writePadBlocks(1);
    }
    public int bytesLeftInBlock() {
        int modulus = (int) (bytesWritten % 512L);
        if (modulus == 0) {
            return 0;
        }
        return 512 - modulus;
    }
    public void assertAtBlockBoundary() {
        if (bytesLeftInBlock() != 0) {
            throw new IllegalArgumentException(
                RB.illegal_block_boundary.getString(
                        Long.toString(bytesWritten)));
        }
    }
    public void padCurrentBlock() throws IOException {
        int padBytes = bytesLeftInBlock();
        if (padBytes == 0) {
            return;
        }
        write(ZERO_BLOCK, padBytes);
        assertAtBlockBoundary();
    }
    public void flush() throws IOException {
        writeStream.flush();
    }
    public void close() throws IOException {
        if (writeStream == null) {
            return;
        }
        try {
            writeStream.close();
            if (!writeFile.delete()) {
                throw new IOException(
                        RB.workfile_delete_fail.getString(
                        writeFile.getAbsolutePath()));
            }
        } finally {
            writeStream = null;  
        }
    }
    public long getBytesWritten() {
        return bytesWritten;
    }
    public void finish() throws IOException {
        try {
            long finalBlock = bytesWritten / 512 + 2;
            if (finalBlock % blocksPerRecord != 0) {
                finalBlock = (finalBlock / blocksPerRecord + 1)
                             * blocksPerRecord;
            }
            int finalPadBlocks = (int) (finalBlock - bytesWritten / 512L);
            if (TarFileOutputStream.debug) {
                System.out.println(
                        RB.pad_block_write.getString(finalPadBlocks));
            }
            writePadBlocks(finalPadBlocks);
        } catch (IOException ioe) {
            try {
                close();
            } catch (IOException ne) {
            }
            throw ioe;
        }
        writeStream.close();
        writeFile.renameTo(targetFile);
    }
}