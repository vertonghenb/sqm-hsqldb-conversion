package org.hsqldb.server;
import java.io.IOException;
import java.util.Locale;
import org.hsqldb.ColumnBase;
import org.hsqldb.lib.DataOutputStream;
import org.hsqldb.result.ResultMetaData;
public class OdbcUtil {
    static void validateInputPacketSize(OdbcPacketInputStream p)
    throws RecoverableOdbcFailure {
        int remaining = -1;
        try {
            remaining = p.available();
        } catch (IOException ioe) {
        }
        if (remaining < 1) {
            return;
        }
        throw new RecoverableOdbcFailure(
            "Client supplied bad length for " + p.packetType
            + " packet.  " + remaining + " bytes available after processing",
            "Bad length for " + p.packetType
            + " packet.  " + remaining + " extra bytes", "08P01");
    }
    static String echoBackReplyString(String inCommand, int retval) {
        String uc = inCommand.trim().toUpperCase(Locale.ENGLISH);
        int firstWhiteSpace;
        for (firstWhiteSpace = 0; firstWhiteSpace < uc.length();
            firstWhiteSpace++) {
            if (Character.isWhitespace(uc.charAt(firstWhiteSpace))) {
                break;
            }
        }
        StringBuffer replyString = new StringBuffer(
            uc.substring(0, firstWhiteSpace));
        String keyword = replyString.toString();
        if (keyword.equals("UPDATE") || keyword.equals("DELETE")) {
            replyString.append(" " + retval);
        } else if (keyword.equals("CREATE") || keyword.equals("DROP")) {
            int wordStart;
            for (wordStart = firstWhiteSpace; wordStart < uc.length();
                wordStart++) {
                if (!Character.isWhitespace(uc.charAt(wordStart))) {
                    break;
                }
            }
            int wordEnd;
            for (wordEnd = wordStart; wordEnd < uc.length();
                wordEnd++) {
                if (!Character.isWhitespace(uc.charAt(wordEnd))) {
                    break;
                }
            }
            replyString.append(" " + uc.substring(wordStart, wordEnd));
        } else if (keyword.equals("INSERT")) {
            replyString.append(" " + 0 + ' ' + retval);
        }
        return replyString.toString();
    }
    static void writeParam(
    String key, String val, DataOutputStream hOutStream) throws IOException {
        OdbcPacketOutputStream alertPacket =
            OdbcPacketOutputStream.newOdbcPacketOutputStream();
        alertPacket.write(key);
        alertPacket.write(val);
        alertPacket.xmit('S', hOutStream);
        alertPacket.close();
    }
    static final int ODBC_SM_DATABASE = 64;
    static final int ODBC_SM_USER = 32;
    static final int ODBC_SM_OPTIONS = 64;
    static final int ODBC_SM_UNUSED = 64;
    static final int ODBC_SM_TTY = 64;
    static final int ODBC_AUTH_REQ_PASSWORD = 3;
    static final int ODBC_AUTH_REQ_OK = 0;
    static void alertClient(int severity, String message,
    DataOutputStream hOutStream) throws IOException {
        alertClient(severity, message, null, hOutStream);
    }
    static void alertClient(int severity, String message,
    String sqlStateCode, DataOutputStream hOutStream) throws IOException {
        if (sqlStateCode == null) {
            sqlStateCode = "XX000";
        }
        if (!odbcSeverityMap.containsKey(severity)) {
            throw new IllegalArgumentException(
                "Unknown severity value (" + severity + ')');
        }
        OdbcPacketOutputStream alertPacket =
            OdbcPacketOutputStream.newOdbcPacketOutputStream();
        alertPacket.write("S" + odbcSeverityMap.get(severity));
        if (severity < ODBC_SEVERITY_NOTICE) {
            alertPacket.write("C" + sqlStateCode);
        }
        alertPacket.write("M" + message);
        alertPacket.writeByte(0);
        alertPacket.xmit((severity < ODBC_SEVERITY_NOTICE) ? 'E' : 'N',
                hOutStream);
        alertPacket.close();
    }
    static String[][] hardcodedParams = new String[][] {
        new String[] { "client_encoding", "SQL_ASCII" },
        new String[] { "DateStyle", "ISO, MDY" },
        new String[] { "integer_datetimes", "on" },
        new String[] { "is_superuser", "on" },
        new String[] { "server_encoding", "SQL_ASCII" },
        new String[] { "server_version", "8.3.1" },
        new String[] { "session_authorization", "blaine" },
        new String[] { "standard_conforming_strings", "off" },
        new String[] { "TimeZone", "US/Eastern" },
    };
    static final int ODBC_SIMPLE_MODE = 0;
    static final int ODBC_EXTENDED_MODE = 1;
    static final int ODBC_EXT_RECOVER_MODE = 2;
    static final int ODBC_SEVERITY_FATAL = 1;
    static final int ODBC_SEVERITY_ERROR = 2;
    static final int ODBC_SEVERITY_PANIC = 3;
    static final int ODBC_SEVERITY_WARNING = 4;
    static final int ODBC_SEVERITY_NOTICE = 5;
    static final int ODBC_SEVERITY_DEBUG = 6;
    static final int ODBC_SEVERITY_INFO = 7;
    static final int ODBC_SEVERITY_LOG = 8;
    static org.hsqldb.lib.IntKeyHashMap odbcSeverityMap =
        new org.hsqldb.lib.IntKeyHashMap();
    static {
        odbcSeverityMap.put(ODBC_SEVERITY_FATAL, "FATAL");
        odbcSeverityMap.put(ODBC_SEVERITY_ERROR, "ERROR");
        odbcSeverityMap.put(ODBC_SEVERITY_PANIC, "PANIC");
        odbcSeverityMap.put(ODBC_SEVERITY_WARNING, "WARNING");
        odbcSeverityMap.put(ODBC_SEVERITY_NOTICE, "NOTICE");
        odbcSeverityMap.put(ODBC_SEVERITY_DEBUG, "DEBUG");
        odbcSeverityMap.put(ODBC_SEVERITY_INFO, "INFO");
        odbcSeverityMap.put(ODBC_SEVERITY_LOG, "LOG");
    }
    static String revertMungledPreparedQuery(String inQuery) {
        return inQuery.replaceAll("\\$\\d+", "?");
    }
    public static int getTableOidForColumn(int colIndex, ResultMetaData md) {
        if (!md.isTableColumn(colIndex)) {
            return 0;
        }
        ColumnBase col = md.columns[colIndex];
        int hashCode = (col.getSchemaNameString() + '.'
            + col.getTableNameString()).hashCode();
        if (hashCode < 0) {
            hashCode *= -1;
        }
        return hashCode;
    }
    public static short getIdForColumn(int colIndex, ResultMetaData md) {
        if (!md.isTableColumn(colIndex)) {
            return 0;
        }
        short hashCode =
            (short) md.getGeneratedColumnNames()[colIndex].hashCode();
        if (hashCode < 0) {
            hashCode *= -1;
        }
        return hashCode;
    }
    public static String hexCharsToOctalOctets(String hexChars) {
        int chars = hexChars.length();
        if (chars != (chars / 2) * 2) {
            throw new IllegalArgumentException("Hex character lists contains "
                + "an odd number of characters: " + chars);
        }
        StringBuffer sb = new StringBuffer();
        char c;
        int octet;
        for (int i = 0; i < chars; i++) {
            octet = 0;
            c = hexChars.charAt(i);
            if (c >= 'a' && c <= 'f') {
                octet += 10 + c - 'a';
            } else if (c >= 'A' && c <= 'F') {
                octet += 10 + c - 'A';
            } else if (c >= '0' && c <= '9') {
                octet += c - '0';
            } else {
                throw new IllegalArgumentException(
                    "Non-hex character in input at offset " + i + ": " + c);
            }
            octet = octet << 4;
            c = hexChars.charAt(++i);
            if (c >= 'a' && c <= 'f') {
                octet += 10 + c - 'a';
            } else if (c >= 'A' && c <= 'F') {
                octet += 10 + c - 'A';
            } else if (c >= '0' && c <= '9') {
                octet += c - '0';
            } else {
                throw new IllegalArgumentException(
                    "Non-hex character in input at offset " + i + ": " + c);
            }
            sb.append('\\');
            sb.append((char) ('0' + (octet >> 6)));
            sb.append((char) ('0' + ((octet >> 3) & 7)));
            sb.append((char) ('0' + (octet & 7)));
        }
        return sb.toString();
    }
    public static void main(String[] sa) {
        System.out.println("(" + OdbcUtil.hexCharsToOctalOctets(sa[0]) + ')');
    }
}