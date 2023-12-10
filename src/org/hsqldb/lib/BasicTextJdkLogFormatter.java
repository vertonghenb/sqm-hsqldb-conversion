package org.hsqldb.lib;
import java.io.StringWriter;
import java.io.PrintWriter;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;
import java.text.SimpleDateFormat;
import java.util.Date;
public class BasicTextJdkLogFormatter extends Formatter {
    protected boolean withTime = true;
    public static String LS = System.getProperty("line.separator");
    protected SimpleDateFormat sdf =
            new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
    public BasicTextJdkLogFormatter(boolean withTime) {
        this.withTime = withTime;
    }
    public BasicTextJdkLogFormatter() {
    }
    public String format(LogRecord record) {
        StringBuilder sb = new StringBuilder();
        if (withTime) {
            sb.append(sdf.format(new Date(record.getMillis())) + "  ");
        }
        sb.append(record.getLevel() + "  " + formatMessage(record));
        if (record.getThrown() != null) {
            StringWriter sw = new StringWriter();
            record.getThrown().printStackTrace(new PrintWriter(sw));
            sb.append(LS + sw);
        }
        return sb.toString() + LS;
    }
}