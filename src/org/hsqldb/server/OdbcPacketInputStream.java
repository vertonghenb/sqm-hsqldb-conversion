


package org.hsqldb.server;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.hsqldb.HsqlException;
import org.hsqldb.types.BinaryData;


class OdbcPacketInputStream extends DataInputStream {
    char packetType;
    private InputStream bufferStream;

    
    static OdbcPacketInputStream newOdbcPacketInputStream(
    char cType, InputStream streamSource, int sizeInt) throws IOException {
        return newOdbcPacketInputStream(
            cType, streamSource, new Integer(sizeInt));
    }

    
    static OdbcPacketInputStream newOdbcPacketInputStream(
    char cType, InputStream streamSource) throws IOException {
        return newOdbcPacketInputStream(cType, streamSource, null);
    }

    static private OdbcPacketInputStream newOdbcPacketInputStream(
    char cType, InputStream streamSource, Integer packetSizeObj)
    throws IOException {
        int bytesRead, i;
        int packetSize = 0;

        if (packetSizeObj == null) {
            byte[] fourBytes = new byte[4];
            bytesRead = 0;
            while ((i = streamSource.read(fourBytes, bytesRead,
                fourBytes.length - bytesRead)) > 0) {
                bytesRead += i;
            }
            if (bytesRead != fourBytes.length) {
                throw new EOFException("Failed to read size header int");
            }
            packetSize =
                ((fourBytes[0] & 0xff) << 24) + ((fourBytes[1] & 0xff) <<16)
                + ((fourBytes[2] & 0xff) << 8) + (fourBytes[3] & 0xff) - 4;
            
        } else {
            packetSize = packetSizeObj.intValue();
        }
        byte[] xferBuffer = new byte[packetSize];
        bytesRead = 0;
        while ((i = streamSource.read(xferBuffer, bytesRead,
            xferBuffer.length - bytesRead)) > 0) {
            bytesRead += i;
        }
        if (bytesRead != xferBuffer.length) {
            throw new EOFException (
                    "Failed to read packet contents from given stream");
        }
        return new OdbcPacketInputStream(
            cType, new ByteArrayInputStream(xferBuffer));
    }

    private OdbcPacketInputStream(char packetType, InputStream bufferStream) {
        super(bufferStream);
        this.packetType = packetType;
    }

    
    Map readStringPairs() throws IOException {
        String key;
        Map map = new HashMap();
        while (true) {
            key = readString();
            if (key.length() < 1) {
                break;
            }
            map.put(key, readString());
        }
        return map;
    }

    
    String readString() throws IOException {
        
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        baos.write((byte) 'X');
        baos.write((byte) 'X');
        

        int i;
        while ((i = readByte()) > 0) {
            baos.write((byte) i);
        }
        byte[] ba = baos.toByteArray();
        baos.close();

        int len = ba.length - 2;
        ba[0] = (byte) (len >>> 8);
        ba[1] = (byte) len;

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(ba));
        String s = dis.readUTF();
        
        
        
        dis.close();
        return s;
    }

    BinaryData readSizedBinaryData() throws IOException {
        int len = readInt();
        try {
            return (len < 0) ? null : new BinaryData((long) len, this);
        } catch (HsqlException he) {
            throw new IOException(he.getMessage());
        }
    }

    String readSizedString() throws IOException {
        int len = readInt();
        return (len < 0) ? null : readString(len);
    }

    
    String readString(int len) throws IOException {
        
        int bytesRead = 0;
        int i;
        byte[] ba = new byte[len + 2];
        ba[0] = (byte) (len >>> 8);
        ba[1] = (byte) len;
        while ((i = read(ba, 2 + bytesRead, len - bytesRead)) > -1
            && bytesRead < len) {
            bytesRead += i;
        }
        if (bytesRead != len) {
            throw new EOFException("Packet ran dry");
        }
        for (i = 2; i < ba.length - 1; i++) {
            if (ba[i] == 0) {
                throw new RuntimeException(
                        "Null internal to String at offset " + (i - 2));
            }
        }

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(ba));
        String s = dis.readUTF();
        
        
        
        dis.close();
        return s;
    }

    public char readByteChar() throws IOException {
        return (char) readByte();
    }
}
