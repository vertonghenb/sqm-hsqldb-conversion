


package org.hsqldb.server;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;


class OdbcPacketOutputStream extends DataOutputStream {
    private ByteArrayOutputStream byteArrayOutputStream;
    private ByteArrayOutputStream stringWriterOS = new ByteArrayOutputStream();
    private DataOutputStream stringWriterDos =
        new DataOutputStream(stringWriterOS);
    private int packetStart = 0; 

    public int getSize() {
        return written - packetStart;
    }

    
    synchronized void write(String s) throws IOException {
        write(s, true);
    }

    synchronized void write(String s, boolean nullTerm)
    throws IOException {
        stringWriterDos.writeUTF(s);
        write(stringWriterOS.toByteArray(), 2, stringWriterOS.size() - 2);
        stringWriterOS.reset();
        if (nullTerm) {
            writeByte(0);
        }
    }

    synchronized void writeSized(String s) throws IOException {
        stringWriterDos.writeUTF(s);
        byte[] ba = stringWriterOS.toByteArray();
        stringWriterOS.reset();

        writeInt(ba.length - 2);
        write(ba, 2, ba.length - 2);
    }

    synchronized void reset() throws IOException {
        byteArrayOutputStream.reset();
        packetStart = written;
        writeInt(-1); 
    }

    static OdbcPacketOutputStream newOdbcPacketOutputStream()
    throws IOException {
        return new OdbcPacketOutputStream(new ByteArrayOutputStream());
    }

    protected OdbcPacketOutputStream(
    ByteArrayOutputStream byteArrayOutputStream) throws IOException {
        super(byteArrayOutputStream);
        this.byteArrayOutputStream = byteArrayOutputStream;
        reset();
    }

    
    synchronized int xmit(
    char packetType, org.hsqldb.lib.DataOutputStream destinationStream)
    throws IOException {
        byte[] ba = byteArrayOutputStream.toByteArray();
        ba[0] = (byte) (ba.length >> 24);
        ba[1] = (byte) (ba.length >> 16);
        ba[2] = (byte) (ba.length >> 8);
        ba[3] = (byte) ba.length;
        reset();
        destinationStream.writeByte(packetType);
        destinationStream.write(ba);
        destinationStream.flush();
        return ba.length;
    }

    synchronized public void close() throws IOException {
        super.close();
        stringWriterDos.close();
    }

    
    synchronized public void writeByteChar(char c) throws IOException {
        writeByte(c);
    }
}
