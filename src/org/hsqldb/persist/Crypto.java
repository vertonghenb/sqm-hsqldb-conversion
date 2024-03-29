package org.hsqldb.persist;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.CipherOutputStream;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.KeyGenerator;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.ShortBufferException;
import javax.crypto.spec.SecretKeySpec;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.StringConverter;
public class Crypto {
    SecretKeySpec key;
    Cipher        outCipher;
    Cipher        inCipher;
    Cipher        inStreamCipher;
    Cipher        outStreamCipher;
    public Crypto(String keyString, String cipherName, String provider) {
        try {
            byte[] encodedKey =
                StringConverter.hexStringToByteArray(keyString);
            key       = new SecretKeySpec(encodedKey, cipherName);
            outCipher = provider == null ? Cipher.getInstance(cipherName)
                                         : Cipher.getInstance(cipherName,
                                         provider);
            outCipher.init(Cipher.ENCRYPT_MODE, key);
            outStreamCipher = provider == null ? Cipher.getInstance(cipherName)
                                         : Cipher.getInstance(cipherName,
                                         provider);
            outStreamCipher.init(Cipher.ENCRYPT_MODE, key);
            inCipher = provider == null ? Cipher.getInstance(cipherName)
                                        : Cipher.getInstance(cipherName,
                                        provider);
            inCipher.init(Cipher.DECRYPT_MODE, key);
            inStreamCipher = provider == null ? Cipher.getInstance(cipherName)
                                        : Cipher.getInstance(cipherName,
                                        provider);
            inStreamCipher.init(Cipher.DECRYPT_MODE, key);
            return;
        } catch (NoSuchPaddingException e) {
            throw Error.error(ErrorCode.X_S0531, e);
        } catch (NoSuchAlgorithmException e) {
            throw Error.error(ErrorCode.X_S0531, e);
        } catch (InvalidKeyException e) {
            throw Error.error(ErrorCode.X_S0531, e);
        } catch (NoSuchProviderException e) {
            throw Error.error(ErrorCode.X_S0531, e);
        } catch (IOException e) {
            throw Error.error(ErrorCode.X_S0531, e);
        }
    }
    public synchronized InputStream getInputStream(InputStream in) {
        if (inCipher == null) {
            return in;
        }
        try {
            inStreamCipher.init(Cipher.DECRYPT_MODE, key);
            return new CipherInputStream(in, inStreamCipher);
        } catch (java.security.InvalidKeyException e) {
            throw Error.error(ErrorCode.X_S0531, e);
        }
    }
    public synchronized OutputStream getOutputStream(OutputStream out) {
        if (outCipher == null) {
            return out;
        }
        try {
            outStreamCipher.init(Cipher.ENCRYPT_MODE, key);
            return new CipherOutputStream(out, outStreamCipher);
        } catch (java.security.InvalidKeyException e) {
            throw Error.error(ErrorCode.X_S0531, e);
        }
    }
    public synchronized int decode(byte[] source, int sourceOffset,
                                   int length, byte[] dest, int destOffset) {
        if (inCipher == null) {
            return length;
        }
        try {
            inCipher.init(Cipher.DECRYPT_MODE, key);
            return inCipher.doFinal(source, sourceOffset, length, dest,
                                    destOffset);
        } catch (java.security.InvalidKeyException e) {
            throw Error.error(ErrorCode.X_S0531, e);
        } catch (BadPaddingException e) {
            throw Error.error(ErrorCode.X_S0531, e);
        } catch (IllegalBlockSizeException e) {
            throw Error.error(ErrorCode.X_S0531, e);
        } catch (ShortBufferException e) {
            throw Error.error(ErrorCode.X_S0531, e);
        }
    }
    public synchronized int encode(byte[] source, int sourceOffset,
                                   int length, byte[] dest, int destOffset) {
        if (outCipher == null) {
            return length;
        }
        try {
            outCipher.init(Cipher.ENCRYPT_MODE, key);
            return outCipher.doFinal(source, sourceOffset, length, dest,
                                     destOffset);
        } catch (java.security.InvalidKeyException e) {
            throw Error.error(ErrorCode.X_S0531, e);
        } catch (BadPaddingException e) {
            throw Error.error(ErrorCode.X_S0531, e);
        } catch (IllegalBlockSizeException e) {
            throw Error.error(ErrorCode.X_S0531, e);
        } catch (ShortBufferException e) {
            throw Error.error(ErrorCode.X_S0531, e);
        }
    }
    public static byte[] getNewKey(String cipherName, String provider) {
        try {
            KeyGenerator generator = provider == null
                                     ? KeyGenerator.getInstance(cipherName)
                                     : KeyGenerator.getInstance(cipherName,
                                         provider);
            SecretKey key = generator.generateKey();
            byte[]    raw = key.getEncoded();
            return raw;
        } catch (java.security.NoSuchAlgorithmException e) {
            throw Error.error(ErrorCode.X_S0531, e);
        } catch (NoSuchProviderException e) {
            throw Error.error(ErrorCode.X_S0531, e);
        }
    }
    public synchronized int getEncodedSize(int size) {
        try {
            return outCipher.getOutputSize(size);
        } catch (IllegalStateException ex) {
            try {
                outCipher.init(Cipher.ENCRYPT_MODE, key);
                return outCipher.getOutputSize(size);
            } catch (java.security.InvalidKeyException e) {
                throw Error.error(ErrorCode.X_S0531, e);
            }
        }
    }
}