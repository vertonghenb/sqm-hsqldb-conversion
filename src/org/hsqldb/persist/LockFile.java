package org.hsqldb.persist;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import org.hsqldb.DatabaseManager;
import org.hsqldb.HsqlDateTime;
import org.hsqldb.HsqlException;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.FileUtil;
import org.hsqldb.lib.HsqlTimer;
import org.hsqldb.lib.StringConverter;
public class LockFile {
    public static final long HEARTBEAT_INTERVAL = 10000;
    public static final long HEARTBEAT_INTERVAL_PADDED = 10100;
    protected static final byte[] MAGIC = {
        0x48, 0x53, 0x51, 0x4c, 0x4c, 0x4f, 0x43, 0x4b
    };
    public static final int USED_REGION = 16;
    public static final int POLL_RETRIES_DEFAULT = 10;
    public static final String POLL_RETRIES_PROPERTY =
        "hsqldb.lockfile.poll.retries";
    public static final String POLL_INTERVAL_PROPERTY =
        "hsqldb.lockfile.poll.interval";
    public static final boolean USE_NIO_FILELOCK_DEFAULT = false;
    public static final String USE_NIO_FILELOCK_PROPERTY =
        "hsqldb.lockfile.nio.filelock";
    public static final boolean NIO_FILELOCK_AVAILABLE;
    public static final Class NIO_LOCKFILE_CLASS;
    protected static final HsqlTimer timer = DatabaseManager.getTimer();
    static {
        synchronized (LockFile.class) {
            boolean use = USE_NIO_FILELOCK_DEFAULT;
            try {
                use = "true".equalsIgnoreCase(
                    System.getProperty(USE_NIO_FILELOCK_PROPERTY, use ? "true"
                                                                      : "false"));
            } catch (Exception e) {}
            boolean avail = false;
            Class   clazz = null;
            if (use) {
                try {
                    Class.forName("java.nio.channels.FileLock");
                    clazz = Class.forName("org.hsqldb.persist.NIOLockFile");
                    avail = true;
                } catch (Exception e) {}
            }
            NIO_FILELOCK_AVAILABLE = avail;
            NIO_LOCKFILE_CLASS     = clazz;
        }
    }
    protected File file;
    private String cpath;
    protected volatile RandomAccessFile raf;
    protected volatile boolean locked;
    private volatile Object timerTask;
    private static final LockFile newNIOLockFile() {
        if (NIO_FILELOCK_AVAILABLE && NIO_LOCKFILE_CLASS != null) {
            try {
                return (LockFile) NIO_LOCKFILE_CLASS.newInstance();
            } catch (Exception e) {
            }
        }
        return null;
    }
    protected LockFile() {}
    public static final LockFile newLockFile(final String path)
    throws FileCanonicalizationException, FileSecurityException {
        LockFile lockFile = newNIOLockFile();
        if (lockFile == null) {
            lockFile = new LockFile();
        }
        lockFile.setPath(path);
        return lockFile;
    }
    public static final LockFile newLockFileLock(final String path)
    throws HsqlException {
        LockFile lockFile = null;
        try {
            lockFile = LockFile.newLockFile(path + ".lck");
        } catch (LockFile.BaseException e) {
            throw Error.error(ErrorCode.LOCK_FILE_ACQUISITION_FAILURE,
                              e.getMessage());
        }
        boolean locked = false;
        try {
            locked = lockFile.tryLock();
        } catch (LockFile.BaseException e) {
            throw Error.error(ErrorCode.LOCK_FILE_ACQUISITION_FAILURE,
                              e.getMessage());
        }
        if (!locked) {
            throw Error.error(ErrorCode.LOCK_FILE_ACQUISITION_FAILURE,
                              lockFile.toString());
        }
        return lockFile;
    }
    private final void checkHeartbeat(boolean withCreateNewFile)
    throws LockFile.FileSecurityException,
           LockFile.LockHeldExternallyException,
           LockFile.UnexpectedEndOfFileException,
           LockFile.UnexpectedFileIOException,
           LockFile.UnexpectedFileNotFoundException,
           LockFile.WrongLengthException, LockFile.WrongMagicException {
        long now;
        long lastHeartbeat;
        long length = 0;
        try {
            if (withCreateNewFile) {
                try {
                    if (file.createNewFile()) {
                        return;
                    }
                } catch (IOException ioe) {}
            }
            if (!file.exists()) {
                return;
            }
            length = file.length();
        } catch (SecurityException se) {
            throw new FileSecurityException(this, "checkHeartbeat", se);
        }
        if (length != USED_REGION) {
            if (length == 0) {
                file.delete();
                return;
            }
            throw new WrongLengthException(this, "checkHeartbeat", length);
        }
        now           = System.currentTimeMillis();
        lastHeartbeat = readHeartbeat();
        if (Math.abs(now - lastHeartbeat) <= (HEARTBEAT_INTERVAL_PADDED)) {
            throw new LockHeldExternallyException(this, "checkHeartbeat", now,
                                                  lastHeartbeat);
        }
    }
    private final void closeRAF() throws LockFile.UnexpectedFileIOException {
        if (raf != null) {
            try {
                raf.close();
            } catch (IOException ex) {
                throw new UnexpectedFileIOException(this, "closeRAF", ex);
            } finally {
                raf = null;
            }
        }
    }
    protected boolean doOptionalLockActions() {
        return false;
    }
    protected boolean doOptionalReleaseActions() {
        return false;
    }
    private final void setPath(String path)
    throws LockFile.FileCanonicalizationException,
           LockFile.FileSecurityException {
        path      = FileUtil.getFileUtil().canonicalOrAbsolutePath(path);
        this.file = new File(path);
        try {
            FileUtil.getFileUtil().makeParentDirectories(this.file);
        } catch (SecurityException ex) {
            throw new FileSecurityException(this, "setPath", ex);
        }
        try {
            this.file = FileUtil.getFileUtil().canonicalFile(path);
        } catch (SecurityException ex) {
            throw new FileSecurityException(this, "setPath", ex);
        } catch (IOException ex) {
            throw new FileCanonicalizationException(this, "setPath", ex);
        }
        this.cpath = this.file.getPath();
    }
    private final void openRAF()
    throws LockFile.UnexpectedFileNotFoundException,
           LockFile.FileSecurityException, LockFile.UnexpectedFileIOException {
        try {
            raf = new RandomAccessFile(file, "rw");
        } catch (SecurityException ex) {
            throw new FileSecurityException(this, "openRAF", ex);
        } catch (FileNotFoundException ex) {
            throw new UnexpectedFileNotFoundException(this, "openRAF", ex);
        } catch (IOException ex) {
            throw new UnexpectedFileIOException(this, "openRAF", ex);
        }
    }
    private final void checkMagic(final DataInputStream dis)
    throws LockFile.FileSecurityException,
           LockFile.UnexpectedEndOfFileException,
           LockFile.UnexpectedFileIOException, LockFile.WrongMagicException {
        boolean      success = true;
        final byte[] magic   = new byte[MAGIC.length];
        try {
            for (int i = 0; i < MAGIC.length; i++) {
                magic[i] = dis.readByte();
                if (MAGIC[i] != magic[i]) {
                    success = false;
                }
            }
        } catch (SecurityException ex) {
            throw new FileSecurityException(this, "checkMagic", ex);
        } catch (EOFException ex) {
            throw new UnexpectedEndOfFileException(this, "checkMagic", ex);
        } catch (IOException ex) {
            throw new UnexpectedFileIOException(this, "checkMagic", ex);
        }
        if (!success) {
            throw new WrongMagicException(this, "checkMagic", magic);
        }
    }
    private final long readHeartbeat()
    throws LockFile.FileSecurityException,
           LockFile.UnexpectedFileNotFoundException,
           LockFile.UnexpectedEndOfFileException,
           LockFile.UnexpectedFileIOException, LockFile.WrongMagicException {
        FileInputStream fis = null;
        DataInputStream dis = null;
        try {
            if (!file.exists()) {
                return Long.MIN_VALUE;
            }
            fis = new FileInputStream(file);
            dis = new DataInputStream(fis);
            checkMagic(dis);
            return dis.readLong();
        } catch (SecurityException ex) {
            throw new FileSecurityException(this, "readHeartbeat", ex);
        } catch (FileNotFoundException ex) {
            throw new UnexpectedFileNotFoundException(this, "readHeartbeat",
                    ex);
        } catch (EOFException ex) {
            throw new UnexpectedEndOfFileException(this, "readHeartbeat", ex);
        } catch (IOException ex) {
            throw new UnexpectedFileIOException(this, "readHeartbeat", ex);
        } finally {
            if (fis != null) {
                try {
                    fis.close();
                } catch (IOException ioe) {
                }
            }
        }
    }
    private final void startHeartbeat() {
        if (timerTask == null || HsqlTimer.isCancelled(timerTask)) {
            Runnable runner = new HeartbeatRunner();
            timerTask = timer.schedulePeriodicallyAfter(0, HEARTBEAT_INTERVAL,
                    runner, true);
        }
    }
    private final void stopHeartbeat() {
        if (timerTask != null && !HsqlTimer.isCancelled(timerTask)) {
            HsqlTimer.cancel(timerTask);
            timerTask = null;
        }
    }
    private final void writeMagic()
    throws LockFile.FileSecurityException,
           LockFile.UnexpectedEndOfFileException,
           LockFile.UnexpectedFileIOException {
        try {
            raf.seek(0);
            raf.write(MAGIC);
        } catch (SecurityException ex) {
            throw new FileSecurityException(this, "writeMagic", ex);
        } catch (EOFException ex) {
            throw new UnexpectedEndOfFileException(this, "writeMagic", ex);
        } catch (IOException ex) {
            throw new UnexpectedFileIOException(this, "writeMagic", ex);
        }
    }
    private final void writeHeartbeat()
    throws LockFile.FileSecurityException,
           LockFile.UnexpectedEndOfFileException,
           LockFile.UnexpectedFileIOException {
        try {
            raf.seek(MAGIC.length);
            raf.writeLong(System.currentTimeMillis());
        } catch (SecurityException ex) {
            throw new FileSecurityException(this, "writeHeartbeat", ex);
        } catch (EOFException ex) {
            throw new UnexpectedEndOfFileException(this, "writeHeartbeat", ex);
        } catch (IOException ex) {
            throw new UnexpectedFileIOException(this, "writeHeartbeat", ex);
        }
    }
    public final boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        } else if (obj instanceof LockFile) {
            LockFile other = (LockFile) obj;
            return (this.file == null) ? other.file == null
                                       : this.file.equals(other.file);
        }
        return false;
    }
    public final String getCanonicalPath() {
        return cpath;
    }
    public final int hashCode() {
        return file == null ? 0
                            : file.hashCode();
    }
    public final boolean isLocked() {
        return locked;
    }
    public static final boolean isLocked(final String path) {
        boolean locked = true;
        try {
            LockFile lockFile = LockFile.newLockFile(path);
            lockFile.checkHeartbeat(false);
            locked = false;
        } catch (Exception e) {}
        return locked;
    }
    public boolean isValid() {
        return isLocked() && file != null && file.exists() && raf != null;
    }
    public String toString() {
        return new StringBuffer(super.toString()).append("[file =").append(
            cpath).append(", exists=").append(file.exists()).append(
            ", locked=").append(isLocked()).append(", valid=").append(
            isValid()).append(", ").append(toStringImpl()).append(
            "]").toString();
    }
    protected String toStringImpl() {
        return "";
    }
    public int getPollHeartbeatRetries() {
        int retries = POLL_RETRIES_DEFAULT;
        try {
            retries = Integer.getInteger(
                HsqlDatabaseProperties.system_lockfile_poll_retries_property,
                retries).intValue();
        } catch (Exception e) {}
        if (retries < 1) {
            retries = 1;
        }
        return retries;
    }
    public long getPollHeartbeatInterval() {
        int  retries  = getPollHeartbeatRetries();
        long interval = 10 + (HEARTBEAT_INTERVAL_PADDED / retries);
        try {
            interval = Long.getLong(POLL_INTERVAL_PROPERTY,
                                    interval).longValue();
        } catch (Exception e) {}
        if (interval <= 0) {
            interval = 10 + (HEARTBEAT_INTERVAL_PADDED / retries);
        }
        return interval;
    }
    private final void pollHeartbeat()
    throws LockFile.FileSecurityException,
           LockFile.LockHeldExternallyException,
           LockFile.UnexpectedFileNotFoundException,
           LockFile.UnexpectedEndOfFileException,
           LockFile.UnexpectedFileIOException, LockFile.WrongLengthException,
           LockFile.WrongMagicException {
        boolean                success  = false;
        int                    retries  = getPollHeartbeatRetries();
        long                   interval = getPollHeartbeatInterval();
        LockFile.BaseException reason   = null;
        for (int i = retries; i > 0; i--) {
            try {
                checkHeartbeat(true);    
                success = true;
                break;
            } catch (LockFile.BaseException ex) {
                reason = ex;
            }
            try {
                Thread.sleep(interval);
            } catch (InterruptedException ex) {
                break;
            }
        }
        if (!success) {
            if (reason instanceof FileSecurityException) {
                throw (FileSecurityException) reason;
            } else if (reason instanceof LockHeldExternallyException) {
                throw (LockHeldExternallyException) reason;
            } else if (reason instanceof UnexpectedFileNotFoundException) {
                throw (UnexpectedFileNotFoundException) reason;
            } else if (reason instanceof UnexpectedEndOfFileException) {
                throw (UnexpectedEndOfFileException) reason;
            } else if (reason instanceof UnexpectedFileIOException) {
                throw (UnexpectedFileIOException) reason;
            } else if (reason instanceof WrongLengthException) {
                throw (WrongLengthException) reason;
            } else if (reason instanceof WrongMagicException) {
                throw (WrongMagicException) reason;
            }
        }
    }
    public final boolean tryLock()
    throws LockFile.FileSecurityException,
           LockFile.LockHeldExternallyException,
           LockFile.UnexpectedFileNotFoundException,
           LockFile.UnexpectedEndOfFileException,
           LockFile.UnexpectedFileIOException, LockFile.WrongLengthException,
           LockFile.WrongMagicException {
        if (this.locked) {
            return true;
        }
        try {
            pollHeartbeat();
            openRAF();
            doOptionalLockActions();
            writeMagic();
            writeHeartbeat();
            FileUtil.getFileUtil().deleteOnExit(file);
            this.locked = true;
            startHeartbeat();
        } finally {
            if (!locked) {
                doOptionalReleaseActions();
                try {
                    closeRAF();
                } catch (Exception ex) {
                }
            }
        }
        return this.locked;
    }
    public final boolean tryRelease()
    throws LockFile.FileSecurityException, LockFile.UnexpectedFileIOException {
        boolean released = !locked;
        if (released) {
            return true;
        }
        stopHeartbeat();
        doOptionalReleaseActions();
        UnexpectedFileIOException closeRAFReason = null;
        FileSecurityException     securityReason = null;
        try {
            try {
                closeRAF();
            } catch (UnexpectedFileIOException ex) {
                closeRAFReason = ex;
            }
            try {
                Thread.sleep(100);
            } catch (Exception ex) {
            }
            try {
                released = file.delete();
            } catch (SecurityException ex) {
                securityReason = new FileSecurityException(this, "tryRelease",
                        ex);
            }
        } finally {
            this.locked = false;
        }
        if (closeRAFReason != null) {
            throw closeRAFReason;
        } else if (securityReason != null) {
            throw securityReason;
        }
        return released;
    }
    protected final void finalize() throws Throwable {
        this.tryRelease();
    }
    private final class HeartbeatRunner implements Runnable {
        public void run() {
            try {
                LockFile.this.writeHeartbeat();
            } catch (Throwable t) {
                Error.printSystemOut(t.toString());
            }
        }
    }
    public abstract static class BaseException extends Exception {
        private final LockFile lockFile;
        private final String   inMethod;
        public BaseException(final LockFile lockFile, final String inMethod) {
            super();
            if (lockFile == null) {
                throw new NullPointerException("lockFile");
            }
            if (inMethod == null) {
                throw new NullPointerException("inMethod");
            }
            this.lockFile = lockFile;
            this.inMethod = inMethod;
        }
        public String getMessage() {    
            return "lockFile: " + lockFile + " method: " + inMethod;
        }
        public String getInMethod() {
            return this.inMethod;
        }
        public LockFile getLockFile() {
            return this.lockFile;
        }
    }
    public static final class FileCanonicalizationException
    extends BaseException {
        private final IOException reason;
        public FileCanonicalizationException(final LockFile lockFile,
                                             final String inMethod,
                                             final IOException reason) {
            super(lockFile, inMethod);
            this.reason = reason;
        }
        public IOException getReason() {
            return this.reason;
        }
        public String getMessage() {    
            return super.getMessage() + " reason: " + reason;
        }
    }
    public static final class FileSecurityException extends BaseException {
        private final SecurityException reason;
        public FileSecurityException(final LockFile lockFile,
                                     final String inMethod,
                                     final SecurityException reason) {
            super(lockFile, inMethod);
            this.reason = reason;
        }
        public SecurityException getReason() {
            return this.reason;
        }
        public String getMessage() {    
            return super.getMessage() + " reason: " + reason;
        }
    }
    public static final class LockHeldExternallyException
    extends BaseException {
        private final long read;
        private final long heartbeat;
        public LockHeldExternallyException(final LockFile lockFile,
                                           final String inMethod,
                                           final long read,
                                           final long heartbeat) {
            super(lockFile, inMethod);
            this.read      = read;
            this.heartbeat = heartbeat;
        }
        public long getHeartbeat() {
            return this.heartbeat;
        }
        public long getRead() {
            return this.read;
        }
        public String getMessage() {    
            return super.getMessage() + " read: "
                   + HsqlDateTime.getTimestampString(this.read)
                   + " heartbeat - read: " + (this.heartbeat - this.read)
                   + " ms.";
        }
    }
    public static final class UnexpectedEndOfFileException
    extends BaseException {
        private final EOFException reason;
        public UnexpectedEndOfFileException(final LockFile lockFile,
                                            final String inMethod,
                                            final EOFException reason) {
            super(lockFile, inMethod);
            this.reason = reason;
        }
        public EOFException getReason() {
            return this.reason;
        }
        public String getMessage() {    
            return super.getMessage() + " reason: " + reason;
        }
    }
    public static final class UnexpectedFileIOException extends BaseException {
        private final IOException reason;
        public UnexpectedFileIOException(final LockFile lockFile,
                                         final String inMethod,
                                         final IOException reason) {
            super(lockFile, inMethod);
            this.reason = reason;
        }
        public IOException getReason() {
            return this.reason;
        }
        public String getMessage() {    
            return super.getMessage() + " reason: " + reason;
        }
    }
    public static final class UnexpectedFileNotFoundException
    extends BaseException {
        private final FileNotFoundException reason;
        public UnexpectedFileNotFoundException(
                final LockFile lockFile, final String inMethod,
                final FileNotFoundException reason) {
            super(lockFile, inMethod);
            this.reason = reason;
        }
        public FileNotFoundException getReason() {
            return this.reason;
        }
        public String getMessage() {    
            return super.getMessage() + " reason: " + reason;
        }
    }
    public static final class WrongLengthException extends BaseException {
        private final long length;
        public WrongLengthException(final LockFile lockFile,
                                    final String inMethod, final long length) {
            super(lockFile, inMethod);
            this.length = length;
        }
        public long getLength() {
            return this.length;
        }
        public String getMessage() {    
            return super.getMessage() + " length: " + length;
        }
    }
    public static final class WrongMagicException extends BaseException {
        private final byte[] magic;
        public WrongMagicException(final LockFile lockFile,
                                   final String inMethod, final byte[] magic) {
            super(lockFile, inMethod);
            this.magic = magic;
        }
        public String getMessage() {    
            String message = super.getMessage() + " magic: ";
            message = message + ((magic == null) ? "null"
                                                 : "'"
                                                   + StringConverter.byteArrayToHexString(magic)
                                                   + "'");
            return message;
        }
        public byte[] getMagic() {
            return (magic == null) ? null
                                   : (byte[]) this.magic.clone();
        }
    }
}