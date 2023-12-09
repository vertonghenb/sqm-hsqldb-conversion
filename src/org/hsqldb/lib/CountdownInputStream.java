


package org.hsqldb.lib;

import java.io.IOException;
import java.io.InputStream;




public final class CountdownInputStream extends InputStream {

    private long        m_count;
    private InputStream m_input;

    public CountdownInputStream(final InputStream is) {
        m_input = is;
    }

    public int read() throws IOException {

        if (m_count <= 0) {
            return -1;
        }

        final int b = m_input.read();

        if (b >= 0) {
            m_count--;
        }

        return b;
    }

    public int read(final byte[] buf) throws IOException {

        if (buf == null) {
            throw new NullPointerException();
        } 

        if (m_count <= 0) {
            return -1;
        }

        int len = buf.length;

        if (len > m_count) {
            len = (int) m_count;
        }

        final int r = m_input.read(buf, 0, len);

        if (r > 0) {
            m_count -= r;
        }

        return r;
    }

    public int read(final byte[] buf, final int off,
                    int len) throws IOException {

        if (buf == null) {
            throw new NullPointerException();
        } 

        if (m_count <= 0) {
            return -1;
        }

        if (len > m_count) {
            len = (int) m_count;
        }

        final int r = m_input.read(buf, off, len);

        if (r > 0) {
            m_count -= r;
        }

        return r;
    }

    public void close() throws IOException {
        m_input.close();
    }

    public int available() throws IOException {
        return Math.min(m_input.available(),
                        (int) Math.min(Integer.MAX_VALUE, m_count));
    }

    public long skip(long count) throws IOException {
        return (count <= 0) ? 0
                            : m_input.skip(Math.min(m_count, count));
    }

    public long getCount() {
        return m_count;
    }

    public void setCount(long count) {
        m_count = count;
    }
}
