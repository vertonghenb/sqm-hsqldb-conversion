


package org.hsqldb.lib;

import java.io.InputStream;
import java.io.OutputStream;


public interface FileAccess {

    int ELEMENT_READ         = 1;
    int ELEMENT_SEEKABLEREAD = 3;
    int ELEMENT_WRITE        = 4;
    int ELEMENT_READWRITE    = 7;
    int ELEMENT_TRUNCATE     = 8;

    InputStream openInputStreamElement(java.lang.String streamName)
    throws java.io.IOException;

    OutputStream openOutputStreamElement(java.lang.String streamName)
    throws java.io.IOException;

    boolean isStreamElement(java.lang.String elementName);

    void createParentDirs(java.lang.String filename);

    void removeElement(java.lang.String filename);

    void renameElement(java.lang.String oldName, java.lang.String newName);

    public interface FileSync {
        void sync() throws java.io.IOException;
    }

    FileSync getFileSync(OutputStream os) throws java.io.IOException;
}
