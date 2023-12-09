


package org.hsqldb.lib.tar;

import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;


public class TarFileInputStream {

    
    protected long bytesRead = 0;

    
    
    private InputStream readStream;

    
    protected byte[] readBuffer;
    protected int    readBufferBlocks;
    protected int    compressionType;

    
    public TarFileInputStream(File sourceFile) throws IOException {
        this(sourceFile, TarFileOutputStream.Compression.DEFAULT_COMPRESSION);
    }

    
    public TarFileInputStream(File sourceFile,
                              int compressionType) throws IOException {
        this(sourceFile, compressionType,
             TarFileOutputStream.Compression.DEFAULT_BLOCKS_PER_RECORD);
    }

    public int getReadBufferBlocks() {
        return readBufferBlocks;
    }

    
    public TarFileInputStream(File sourceFile, int compressionType,
                              int readBufferBlocks) throws IOException {

        if (!sourceFile.isFile()) {
            throw new FileNotFoundException(sourceFile.getAbsolutePath());
        }

        if (!sourceFile.canRead()) {
            throw new IOException(
                    RB.read_denied.getString(sourceFile.getAbsolutePath()));
        }

        this.readBufferBlocks = readBufferBlocks;
        this.compressionType  = compressionType;
        readBuffer            = new byte[readBufferBlocks * 512];

        switch (compressionType) {

            case TarFileOutputStream.Compression.NO_COMPRESSION :
                readStream = new FileInputStream(sourceFile);
                break;

            case TarFileOutputStream.Compression.GZIP_COMPRESSION :
                readStream =
                    new GZIPInputStream(new FileInputStream(sourceFile),
                                        readBuffer.length);
                break;

            default :
                throw new IllegalArgumentException(
                    RB.compression_unknown.getString(compressionType));
        }
    }

    
    public void readBlocks(int blocks)
    throws IOException, TarMalformatException {

        
        if (compressionType
                != TarFileOutputStream.Compression.NO_COMPRESSION) {
            readCompressedBlocks(blocks);

            return;
        }

        int i = readStream.read(readBuffer, 0, blocks * 512);

        bytesRead += i;

        if (i != blocks * 512) {
            throw new TarMalformatException(
                RB.insufficient_read.getString(blocks * 512, i));
        }
    }

    
    protected void readCompressedBlocks(int blocks) throws IOException {

        int bytesSoFar    = 0;
        int requiredBytes = 512 * blocks;

        
        int i;

        while (bytesSoFar < requiredBytes) {
            i = readStream.read(readBuffer, bytesSoFar,
                                requiredBytes - bytesSoFar);

            if (i < 0) {
                throw new EOFException(RB.decompression_ranout.getString(
                        bytesSoFar, requiredBytes));
            }

            bytesRead  += i;
            bytesSoFar += i;
        }
    }

    
    public void readBlock() throws IOException, TarMalformatException {
        readBlocks(1);
    }

    
    public boolean readNextHeaderBlock()
    throws IOException, TarMalformatException {

        
        
        try {
            while (readStream.available() > 0) {
                readBlock();

                if (readBuffer[0] != 0) {
                    return true;
                }
            }
        } catch (EOFException ee) {
            
        }

        close();

        return false;
    }

    
    public void close() throws IOException {
        if (readStream == null) {
            return;
        }
        try {
            readStream.close();
        } finally {
            readStream = null;  
        }
    }
}
