


package org.hsqldb.lib;

import java.io.IOException;
import java.io.InputStream;
import java.io.UTFDataFormatException;

import org.hsqldb.store.BitMap;


public class StringConverter {

    private static final byte[] HEXBYTES = {
        (byte) '0', (byte) '1', (byte) '2', (byte) '3', (byte) '4', (byte) '5',
        (byte) '6', (byte) '7', (byte) '8', (byte) '9', (byte) 'a', (byte) 'b',
        (byte) 'c', (byte) 'd', (byte) 'e', (byte) 'f'
    };

    private static int getNibble(int value) {

        if (value >= '0' && value <= '9') {
            return value - '0';
        }

        if (value >= 'a' && value <= 'f') {
            return 10 + value - 'a';
        }

        if (value >= 'A' && value <= 'F') {
            return 10 + value - 'A';
        }

        return -1;
    }

    
    public static byte[] hexStringToByteArray(String s) throws IOException {

        int     l    = s.length();
        byte[]  data = new byte[l / 2 + (l % 2)];
        int     n,
                b    = 0;
        boolean high = true;
        int     i    = 0;

        for (int j = 0; j < l; j++) {
            char c = s.charAt(j);

            if (c == ' ') {
                continue;
            }

            n = getNibble(c);

            if (n == -1) {
                throw new IOException(
                    "hexadecimal string contains non hex character");    
            }

            if (high) {
                b    = (n & 0xf) << 4;
                high = false;
            } else {
                b         += (n & 0xf);
                high      = true;
                data[i++] = (byte) b;
            }
        }

        if (!high) {
            throw new IOException(
                "hexadecimal string with odd number of characters");    
        }

        if (i < data.length) {
            data = (byte[]) ArrayUtil.resizeArray(data, i);
        }

        return data;
    }

    
    public static BitMap sqlBitStringToBitMap(String s) throws IOException {

        int    l = s.length();
        int    n;
        int    bitIndex = 0;
        BitMap map      = new BitMap(l);

        for (int j = 0; j < l; j++) {
            char c = s.charAt(j);

            if (c == ' ') {
                continue;
            }

            n = getNibble(c);

            if (n != 0 && n != 1) {
                throw new IOException(
                    "hexadecimal string contains non hex character");    
            }

            if (n == 1) {
                map.set(bitIndex);
            }

            bitIndex++;
        }

        map.setSize(bitIndex);

        return map;
    }

    
    public static String byteArrayToHexString(byte[] b) {

        int    len = b.length;
        char[] s   = new char[len * 2];

        for (int i = 0, j = 0; i < len; i++) {
            int c = ((int) b[i]) & 0xff;

            s[j++] = (char) HEXBYTES[c >> 4 & 0xf];
            s[j++] = (char) HEXBYTES[c & 0xf];
        }

        return new String(s);
    }

    
    public static String byteArrayToSQLHexString(byte[] b) {

        int    len = b.length;
        char[] s   = new char[len * 2 + 3];

        s[0] = 'X';
        s[1] = '\'';

        int j = 2;

        for (int i = 0; i < len; i++) {
            int c = ((int) b[i]) & 0xff;

            s[j++] = (char) HEXBYTES[c >> 4 & 0xf];
            s[j++] = (char) HEXBYTES[c & 0xf];
        }

        s[j] = '\'';

        return new String(s);
    }

    
    public static String byteArrayToBitString(byte[] bytes, int bitCount) {

        char[] s = new char[bitCount];

        for (int j = 0; j < bitCount; j++) {
            byte b = bytes[j / 8];

            s[j] = BitMap.isSet(b, j % 8) ? '1'
                                          : '0';
        }

        return new String(s);
    }

    
    public static String byteArrayToSQLBitString(byte[] bytes, int bitCount) {

        char[] s = new char[bitCount + 3];

        s[0] = 'B';
        s[1] = '\'';

        int pos = 2;

        for (int j = 0; j < bitCount; j++) {
            byte b = bytes[j / 8];

            s[pos++] = BitMap.isSet(b, j % 8) ? '1'
                                              : '0';
        }

        s[pos] = '\'';

        return new String(s);
    }

    
    public static void writeHexBytes(byte[] o, int from, byte[] b) {

        int len = b.length;

        for (int i = 0; i < len; i++) {
            int c = ((int) b[i]) & 0xff;

            o[from++] = HEXBYTES[c >> 4 & 0xf];
            o[from++] = HEXBYTES[c & 0xf];
        }
    }

    public static String byteArrayToString(byte[] b, String charset) {

        try {
            return (charset == null) ? new String(b)
                                     : new String(b, charset);
        } catch (Exception e) {}

        return null;
    }

    
    public static void stringToUnicodeBytes(HsqlByteArrayOutputStream b,
            String s, boolean doubleSingleQuotes) {

        if (s == null) {
            return;
        }

        final int len = s.length();
        char[]    chars;
        int       extras = 0;

        if (len == 0) {
            return;
        }

        chars = s.toCharArray();

        b.ensureRoom(len * 2 + 5);

        for (int i = 0; i < len; i++) {
            char c = chars[i];

            if (c == '\\') {
                if ((i < len - 1) && (chars[i + 1] == 'u')) {
                    b.writeNoCheck(c);    
                    b.writeNoCheck('u');
                    b.writeNoCheck('0');
                    b.writeNoCheck('0');
                    b.writeNoCheck('5');
                    b.writeNoCheck('c');

                    extras += 5;
                } else {
                    b.write(c);
                }
            } else if ((c >= 0x0020) && (c <= 0x007f)) {
                b.writeNoCheck(c);        

                if (c == '\'' && doubleSingleQuotes) {
                    b.writeNoCheck(c);

                    extras++;
                }
            } else {
                b.writeNoCheck('\\');
                b.writeNoCheck('u');
                b.writeNoCheck(HEXBYTES[(c >> 12) & 0xf]);
                b.writeNoCheck(HEXBYTES[(c >> 8) & 0xf]);
                b.writeNoCheck(HEXBYTES[(c >> 4) & 0xf]);
                b.writeNoCheck(HEXBYTES[c & 0xf]);

                extras += 5;
            }

            if (extras > len) {
                b.ensureRoom(len + extras + 5);

                extras = 0;
            }
        }
    }






    
    public static String unicodeStringToString(String s) {

        if ((s == null) || (s.indexOf("\\u") == -1)) {
            return s;
        }

        int    len = s.length();
        char[] b   = new char[len];
        int    j   = 0;

        for (int i = 0; i < len; i++) {
            char c = s.charAt(i);

            if (c == '\\' && i < len - 5) {
                char c1 = s.charAt(i + 1);

                if (c1 == 'u') {
                    i++;

                    
                    int k = getNibble(s.charAt(++i)) << 12;

                    k      += getNibble(s.charAt(++i)) << 8;
                    k      += getNibble(s.charAt(++i)) << 4;
                    k      += getNibble(s.charAt(++i));
                    b[j++] = (char) k;
                } else {
                    b[j++] = c;
                }
            } else {
                b[j++] = c;
            }
        }

        return new String(b, 0, j);
    }

    public static String readUTF(byte[] bytearr, int offset,
                                 int length) throws IOException {

        char[] buf = new char[length];

        return readUTF(bytearr, offset, length, buf);
    }

    public static String readUTF(byte[] bytearr, int offset, int length,
                                 char[] buf) throws IOException {

        int bcount = 0;
        int c, char2, char3;
        int count = 0;

        while (count < length) {
            c = (int) bytearr[offset + count];

            if (bcount == buf.length) {
                buf = (char[]) ArrayUtil.resizeArray(buf, length);
            }

            if (c > 0) {

                
                count++;

                buf[bcount++] = (char) c;

                continue;
            }

            c &= 0xff;

            switch (c >> 4) {

                case 12 :
                case 13 :

                    
                    count += 2;

                    if (count > length) {
                        throw new UTFDataFormatException();
                    }

                    char2 = (int) bytearr[offset + count - 1];

                    if ((char2 & 0xC0) != 0x80) {
                        throw new UTFDataFormatException();
                    }

                    buf[bcount++] = (char) (((c & 0x1F) << 6)
                                            | (char2 & 0x3F));
                    break;

                case 14 :

                    
                    count += 3;

                    if (count > length) {
                        throw new UTFDataFormatException();
                    }

                    char2 = (int) bytearr[offset + count - 2];
                    char3 = (int) bytearr[offset + count - 1];

                    if (((char2 & 0xC0) != 0x80) || ((char3 & 0xC0) != 0x80)) {
                        throw new UTFDataFormatException();
                    }

                    buf[bcount++] = (char) (((c & 0x0F) << 12)
                                            | ((char2 & 0x3F) << 6)
                                            | ((char3 & 0x3F) << 0));
                    break;

                default :

                    
                    throw new UTFDataFormatException();
            }
        }

        
        return new String(buf, 0, bcount);
    }

    
    public static int stringToUTFBytes(String str,
                                       HsqlByteArrayOutputStream out) {

        int strlen = str.length();
        int c,
            count  = 0;

        if (out.count + strlen + 8 > out.buffer.length) {
            out.ensureRoom(strlen + 8);
        }

        char[] arr = str.toCharArray();

        for (int i = 0; i < strlen; i++) {
            c = arr[i];

            if (c >= 0x0001 && c <= 0x007F) {
                out.buffer[out.count++] = (byte) c;

                count++;
            } else if (c > 0x07FF) {
                out.buffer[out.count++] = (byte) (0xE0 | ((c >> 12) & 0x0F));
                out.buffer[out.count++] = (byte) (0x80 | ((c >> 6) & 0x3F));
                out.buffer[out.count++] = (byte) (0x80 | ((c >> 0) & 0x3F));
                count                   += 3;
            } else {
                out.buffer[out.count++] = (byte) (0xC0 | ((c >> 6) & 0x1F));
                out.buffer[out.count++] = (byte) (0x80 | ((c >> 0) & 0x3F));
                count                   += 2;
            }

            if (out.count + 8 > out.buffer.length) {
                out.ensureRoom(strlen - i + 8);
            }
        }

        return count;
    }

    public static int getUTFSize(String s) {

        int len = (s == null) ? 0
                              : s.length();
        int l   = 0;

        for (int i = 0; i < len; i++) {
            int c = s.charAt(i);

            if ((c >= 0x0001) && (c <= 0x007F)) {
                l++;
            } else if (c > 0x07FF) {
                l += 3;
            } else {
                l += 2;
            }
        }

        return l;
    }

    
    public static String inputStreamToString(InputStream is,
            String encoding) throws IOException {

        HsqlByteArrayOutputStream baOS = new HsqlByteArrayOutputStream(1024);

        while (true) {
            int c = is.read();

            if (c == -1) {
                break;
            }

            baOS.write(c);
        }

        return new String(baOS.getBuffer(), 0, baOS.size(), encoding);
    }



    
    public static String toQuotedString(String s, char quoteChar,
                                        boolean extraQuote) {

        if (s == null) {
            return null;
        }

        int    count = extraQuote ? count(s, quoteChar)
                                  : 0;
        int    len   = s.length();
        char[] b     = new char[2 + count + len];
        int    i     = 0;
        int    j     = 0;

        b[j++] = quoteChar;

        for (; i < len; i++) {
            char c = s.charAt(i);

            b[j++] = c;

            if (extraQuote && c == quoteChar) {
                b[j++] = c;
            }
        }

        b[j] = quoteChar;

        return new String(b);
    }

    
    static int count(final String s, final char c) {

        int pos   = 0;
        int count = 0;

        if (s != null) {
            while ((pos = s.indexOf(c, pos)) > -1) {
                count++;
                pos++;
            }
        }

        return count;
    }

    
    public static void stringToHtmlBytes(HsqlByteArrayOutputStream b,
                                         String s) {

        if (s == null) {
            return;
        }

        final int len = s.length();
        char[]    chars;

        if (len == 0) {
            return;
        }

        chars = s.toCharArray();

        b.ensureRoom(len);

        for (int i = 0; i < len; i++) {
            char c = chars[i];

            if (c > 0x007f || c == '"' || c == '&' || c == '<' || c == '>') {
                int codePoint = Character.codePointAt(chars, i);

                if (Character.charCount(codePoint) == 2) {
                    i++;
                }

                b.ensureRoom(16);
                b.writeNoCheck('&');
                b.writeNoCheck('#');
                b.writeBytes(String.valueOf(codePoint));
                b.writeNoCheck(';');
            } else if (c < 0x0020 ) {
                b.writeNoCheck(' ');
            } else {
                b.writeNoCheck(c);
            }
        }
    }

    
    public static String toStringUUID(byte[] b) {

        char[] chars = new char[36];
        int    hexIndex;

        if (b == null) {
            return null;
        }

        if (b.length != 16) {
            throw new NumberFormatException();
        }

        for (int i = 0, j = 0; i < b.length; ) {
            hexIndex   = (b[i] & 0xf0) >> 4;
            chars[j++] = (char) HEXBYTES[hexIndex];
            hexIndex   = b[i] & 0xf;
            chars[j++] = (char) HEXBYTES[hexIndex];

            i++;

            if (i >= 4 && i <= 10 && (i % 2) == 0) {
                chars[j++] = '-';
            }
        }

        return new String(chars);
    }

    
    public static byte[] toBinaryUUID(String s) {

        byte[] bytes = new byte[16];

        if (s == null) {
            return null;
        }

        if (s.length() != 36) {
            throw new NumberFormatException();
        }

        for (int i = 0, j = 0; i < bytes.length; ) {
            char c    = s.charAt(j++);
            int  high = getNibble(c);

            c        = s.charAt(j++);
            bytes[i] = (byte) ((high << 4) + getNibble(c));

            i++;

            if (i >= 4 && i <= 10 && (i % 2) == 0) {
                c = s.charAt(j++);

                if (c != '-') {}
            }
        }

        return bytes;
    }
}
