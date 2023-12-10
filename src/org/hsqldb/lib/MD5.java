package org.hsqldb.lib;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
public final class MD5 {
    private static MessageDigest md5;
    public static final String encode(String string,
            String encoding) throws RuntimeException {
        return StringConverter.byteArrayToHexString(digest(string,
                encoding));
    }
    public String digest(String string) throws RuntimeException {
        return encode(string, "ISO-8859-1");
    }
    public static byte[] digest(String string,
                                      String encoding)
                                      throws RuntimeException {
        byte[] data;
        if (encoding == null) {
            encoding = "ISO-8859-1";
        }
        try {
            data = string.getBytes(encoding);
        } catch (UnsupportedEncodingException x) {
            throw new RuntimeException(x.toString());
        }
        return digest(data);
    }
    public static final byte[] digest(byte[] data)
    throws RuntimeException {
        synchronized (MD5.class) {
            if (md5 == null) {
                try {
                    md5 = MessageDigest.getInstance("MD5");
                } catch (NoSuchAlgorithmException e) {
                    throw new RuntimeException(e.toString());
                }
            }
            return md5.digest(data);
        }
    }
}