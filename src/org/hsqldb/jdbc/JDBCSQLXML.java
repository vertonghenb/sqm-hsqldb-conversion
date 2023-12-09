


package org.hsqldb.jdbc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.CharArrayReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.StringReader;
import java.io.Writer;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import javax.xml.bind.util.JAXBResult;
import javax.xml.bind.util.JAXBSource;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.FactoryConfigurationError;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.TransformerFactoryConfigurationError;
import javax.xml.transform.dom.DOMResult;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.sax.SAXResult;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.stax.StAXResult;
import javax.xml.transform.stax.StAXSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.ClosableByteArrayOutputStream;
import org.hsqldb.lib.StringConverter;
import org.w3c.dom.DOMException;
import org.w3c.dom.DOMImplementation;
import org.w3c.dom.Document;
import org.w3c.dom.DocumentType;
import org.w3c.dom.Element;
import org.w3c.dom.EntityReference;
import org.w3c.dom.Node;
import org.w3c.dom.ProcessingInstruction;
import org.w3c.dom.Text;
import org.w3c.dom.bootstrap.DOMImplementationRegistry;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;




public class JDBCSQLXML implements SQLXML {

    private static String domFeatures = "XML 3.0 Traversal +Events 2.0";
    private static DOMImplementation         domImplementation;
    private static DOMImplementationRegistry domImplementationRegistry;
    private static ThreadPoolExecutor        executorService;
    private static Transformer               identityTransformer;
    private static TransformerFactory        transformerFactory;

    
    private static final Charset                utf8Charset;
    private static ArrayBlockingQueue<Runnable> workQueue;

    static {
        Charset charset = null;

        try {
            charset = Charset.forName("UTF8");
        } catch (Exception e) {
        }
        utf8Charset = charset;
    }

    
    private SAX2DOMBuilder builder;

    
    private boolean closed;

    

    
    private volatile byte[] gzdata;

    
    private InputStream inputStream;

    
    private ClosableByteArrayOutputStream outputStream;

    
    private DOMResult domResult;

    
    private String publicId;

    
    private boolean readable;

    
    private String systemId;

    
    private boolean writable;

    
    protected JDBCSQLXML() {
        setReadable(false);
        setWritable(true);
    }

    
    protected JDBCSQLXML(byte[] bytes) throws SQLException {
        this(bytes, null);
    }

    
    protected JDBCSQLXML(char[] chars) throws SQLException {
        this(chars, 0, chars.length, null);
    }

    
    protected JDBCSQLXML(Document document) throws SQLException {
        this(new DOMSource(document));
    }

    
    protected JDBCSQLXML(InputStream inputStream) throws SQLException {
        this(inputStream, null);
    }

    
    protected JDBCSQLXML(Reader reader) throws SQLException {
        this(reader, null);
    }

    
    public JDBCSQLXML(Source source) throws SQLException {
        init(source);
    }

    
    protected JDBCSQLXML(String string) throws SQLException {
        this(new StreamSource(new StringReader(string)));
    }

    
    protected JDBCSQLXML(byte[] bytes, String systemId) throws SQLException {
        this(new StreamSource(new ByteArrayInputStream(bytes), systemId));
    }

    protected JDBCSQLXML(char[] chars, String systemId) throws SQLException {
        this(chars, 0, chars.length, systemId);
    }

    
    protected JDBCSQLXML(InputStream inputStream,
                         String systemId) throws SQLException {
        this(new StreamSource(inputStream, systemId));
    }

    
    protected JDBCSQLXML(Reader reader, String systemId) throws SQLException {
        this(new StreamSource(reader, systemId));
    }

    
    protected JDBCSQLXML(String string, String systemId) throws SQLException {
        this(new StreamSource(new StringReader(string), systemId));
    }

    
    protected JDBCSQLXML(byte[] bytes, boolean clone, String systemId,
                         String publicId) throws SQLException {

        this.setGZipData(clone ? bytes.clone()
                               : bytes);

        this.systemId = systemId;
        this.publicId = publicId;
    }

    protected JDBCSQLXML(char[] chars, int offset, int length,
                         String systemId) throws SQLException {
        this(new StreamSource(new CharArrayReader(chars, offset, length),
                              systemId));
    }

    
    public void free() throws SQLException {
        close();
    }

    
    public synchronized InputStream getBinaryStream() throws SQLException {

        checkClosed();
        checkReadable();

        InputStream rval = getBinaryStreamImpl();

        setReadable(false);
        setWritable(false);

        return rval;
    }

    
    public synchronized OutputStream setBinaryStream() throws SQLException {

        checkClosed();
        checkWritable();

        OutputStream rval = setBinaryStreamImpl();

        setWritable(false);
        setReadable(true);

        return rval;
    }

    
    public synchronized Reader getCharacterStream() throws SQLException {

        checkClosed();
        checkReadable();

        Reader reader = getCharacterStreamImpl();

        setReadable(false);
        setWritable(false);

        return reader;
    }

    
    public synchronized Writer setCharacterStream() throws SQLException {

        checkClosed();
        checkWritable();

        Writer writer = setCharacterStreamImpl();

        setReadable(true);
        setWritable(false);

        return writer;
    }

    
    public synchronized String getString() throws SQLException {

        checkClosed();
        checkReadable();

        String value = getStringImpl();

        setReadable(false);
        setWritable(false);

        return value;
    }

    
    public synchronized void setString(String value) throws SQLException {

        if (value == null) {
            throw Util.nullArgument("value");
        }
        checkWritable();
        setStringImpl(value);
        setReadable(true);
        setWritable(false);
    }

    
    @SuppressWarnings("unchecked")
    public synchronized <T extends Source>T getSource(
            Class<T> sourceClass) throws SQLException {

        checkClosed();
        checkReadable();

        final Source source = getSourceImpl(sourceClass);

        setReadable(false);
        setWritable(false);

        return (T) source;
    }

    
    public synchronized <T extends Result>T setResult(
            Class<T> resultClass) throws SQLException {

        checkClosed();
        checkWritable();

        final T result = createResult(resultClass);

        setReadable(true);
        setWritable(false);

        return result;
    }

    
    protected static ExecutorService getExecutorService() {

        if (JDBCSQLXML.executorService == null) {
            int      corePoolSize    = 1;
            int      maximumPoolSize = 10;
            long     keepAliveTime   = 1;
            TimeUnit unit            = TimeUnit.SECONDS;

            JDBCSQLXML.workQueue = new ArrayBlockingQueue<Runnable>(10);
            JDBCSQLXML.executorService = new ThreadPoolExecutor(corePoolSize,
                    maximumPoolSize, keepAliveTime, unit, workQueue);
        }

        return executorService;
    }

    
    protected static TransformerFactory getTransformerFactory() throws SQLException {

        if (JDBCSQLXML.transformerFactory == null) {
            try {
                JDBCSQLXML.transformerFactory =
                    TransformerFactory.newInstance();
            } catch (TransformerFactoryConfigurationError ex) {
                throw Exceptions.transformFailed(ex);
            }
        }

        return JDBCSQLXML.transformerFactory;
    }

    
    protected static Transformer getIdentityTransformer() throws SQLException {

        if (JDBCSQLXML.identityTransformer == null) {
            try {
                JDBCSQLXML.identityTransformer =
                    getTransformerFactory().newTransformer();
            } catch (TransformerConfigurationException ex) {
                throw Exceptions.transformFailed(ex);
            }
        }

        return JDBCSQLXML.identityTransformer;
    }

    
    protected static DOMImplementationRegistry getDOMImplementationRegistry() throws SQLException {

        if (domImplementationRegistry == null) {
            try {
                domImplementationRegistry =
                    DOMImplementationRegistry.newInstance();
            } catch (ClassCastException ex) {
                throw Exceptions.domInstantiation(ex);
            } catch (InstantiationException ex) {
                throw Exceptions.domInstantiation(ex);
            } catch (ClassNotFoundException ex) {
                throw Exceptions.domInstantiation(ex);
            } catch (IllegalAccessException ex) {
                throw Exceptions.domInstantiation(ex);
            }
        }

        return domImplementationRegistry;
    }

    
    protected static DOMImplementation getDOMImplementation() throws SQLException {

        if (domImplementation == null) {
            domImplementation =
                getDOMImplementationRegistry().getDOMImplementation(
                    domFeatures);
        }

        if (domImplementation == null) {
            Exception ex = new RuntimeException("Not supported: "
                + domFeatures);

            throw Exceptions.domInstantiation(ex);
        }

        return domImplementation;
    }

    
    protected static Document createDocument(String namespaceURI,
            String qualifiedName, DocumentType docType) throws SQLException {

        try {
            return getDOMImplementation().createDocument(namespaceURI,
                    qualifiedName, docType);
        } catch (DOMException ex) {
            throw Exceptions.domInstantiation(ex);
        }
    }

    
    protected static Document createDocument() throws SQLException {
        return createDocument(null, null, null);
    }

    
    protected void init(Source source) throws SQLException {

        if (source == null) {
            throw Util.nullArgument("source");
        }

        Transformer           transformer =
            JDBCSQLXML.getIdentityTransformer();
        StreamResult          result      = new StreamResult();
        ByteArrayOutputStream baos        = new ByteArrayOutputStream();
        GZIPOutputStream      gzos;

        try {
            gzos = new GZIPOutputStream(baos);
        } catch (IOException ex) {
            throw Exceptions.transformFailed(ex);
        }
        result.setOutputStream(gzos);

        try {
            transformer.transform(source, result);
        } catch (TransformerException ex) {
            throw Exceptions.transformFailed(ex);
        }

        try {
            gzos.close();
        } catch (IOException ex) {
            throw Exceptions.transformFailed(ex);
        }

        byte[] data = baos.toByteArray();

        setGZipData(data);
        setReadable(true);
        setWritable(false);
    }

    
    protected void setGZipData(byte[] data) throws SQLException {

        if (data == null) {
            throw Util.nullArgument("data");
        }
        this.gzdata = data;
    }

    
    protected byte[] gZipData() {
        return this.gzdata;
    }

    
    protected byte[] getGZipData() throws SQLException {

        byte[] bytes = gZipData();

        if (bytes != null) {
            return bytes;
        }

        if (this.domResult != null) {
            DOMSource source = new DOMSource(domResult.getNode(),
                domResult.getSystemId());
            OutputStream os     = setBinaryStreamImpl();
            StreamResult result = new StreamResult(os);

            try {
                JDBCSQLXML.identityTransformer.transform(source, result);
            } catch (TransformerException ex) {
                throw Exceptions.transformFailed(ex);
            }

            try {
                os.close();
            } catch (IOException ex) {
                throw Exceptions.transformFailed(ex);
            }
        }

        if (this.outputStream == null) {
            throw Exceptions.notReadable("No Data.");
        } else if (!this.outputStream.isClosed()) {
            throw Exceptions.notReadable(
                "Stream used for writing must be closed but is still open.");
        } else if (this.outputStream.isFreed()) {
            throw Exceptions.notReadable(
                "Stream used for writing was freed and is no longer valid.");
        }

        try {
            setGZipData(this.outputStream.toByteArray());

            return gZipData();
        } catch (IOException ex) {
            throw Exceptions.notReadable();
        } finally {
            this.freeOutputStream();
        }
    }

    
    protected synchronized void close() {

        this.closed = true;

        setReadable(false);
        setWritable(false);
        freeOutputStream();
        freeInputStream();
        freeDomResult();

        this.gzdata = null;
    }

    
    protected void freeInputStream() {

        if (this.inputStream != null) {
            try {
                this.inputStream.close();
            } catch (IOException ex) {

                
            } finally {
                this.inputStream = null;
            }
        }
    }

    
    protected void freeOutputStream() {

        if (this.outputStream != null) {
            try {
                this.outputStream.free();
            } catch (IOException ex) {

                
            }
            this.outputStream = null;
        }
    }

    
    protected synchronized void checkClosed() throws SQLException {

        if (this.closed) {
            throw Exceptions.inFreedState();
        }
    }

    
    protected synchronized void checkReadable() throws SQLException {

        if (!this.isReadable()) {
            throw Exceptions.notReadable();
        }
    }

    
    protected synchronized void setReadable(boolean readable) {
        this.readable = readable;
    }

    
    protected synchronized void checkWritable() throws SQLException {

        if (!this.isWritable()) {
            throw Exceptions.notWritable();
        }
    }

    
    protected synchronized void setWritable(boolean writable) {
        this.writable = writable;
    }

    
    public synchronized boolean isReadable() {
        return this.readable;
    }

    
    public synchronized boolean isWritable() {
        return this.writable;
    }

    
    protected InputStream getBinaryStreamImpl() throws SQLException {

        try {
            byte[]               data = getGZipData();
            ByteArrayInputStream bais = new ByteArrayInputStream(data);

            return new GZIPInputStream(bais);
        } catch (IOException ex) {
            throw Exceptions.transformFailed(ex);
        }
    }

    
    protected Reader getCharacterStreamImpl() throws SQLException {
        return new InputStreamReader(getBinaryStreamImpl());
    }

    
    protected String getStringImpl() throws SQLException {

        try {
            return StringConverter.inputStreamToString(getBinaryStreamImpl(),
                    "US-ASCII");
        } catch (IOException ex) {
            throw Exceptions.transformFailed(ex);
        }
    }

    
    protected OutputStream setBinaryStreamImpl() throws SQLException {

        this.outputStream = new ClosableByteArrayOutputStream();

        try {
            return new GZIPOutputStream(this.outputStream);
        } catch (IOException ex) {
            this.outputStream = null;

            throw Exceptions.resultInstantiation(ex);
        }
    }

    
    protected Writer setCharacterStreamImpl() throws SQLException {
        return new OutputStreamWriter(setBinaryStreamImpl());
    }

    
    protected void setStringImpl(String value) throws SQLException {
        init(new StreamSource(new StringReader(value)));
    }

    
    protected <T extends Source>T getSourceImpl(
            Class<T> sourceClass) throws SQLException {

        if (JAXBSource.class.isAssignableFrom(sourceClass)) {

            
            
            
            
            
            
        } else if (StreamSource.class.isAssignableFrom(sourceClass)) {
            return createStreamSource(sourceClass);
        } else if ((sourceClass == null)
                   || DOMSource.class.isAssignableFrom(sourceClass)) {
            return createDOMSource(sourceClass);
        } else if (SAXSource.class.isAssignableFrom(sourceClass)) {
            return createSAXSource(sourceClass);
        } else if (StAXSource.class.isAssignableFrom(sourceClass)) {
            return createStAXSource(sourceClass);
        }

        throw Util.invalidArgument("sourceClass: " + sourceClass);
    }

    
    @SuppressWarnings("unchecked")
    protected <T extends Source>T createStreamSource(
            Class<T> sourceClass) throws SQLException {

        StreamSource source = null;

        try {
            source = (sourceClass == null) ? new StreamSource()
                    : (StreamSource) sourceClass.newInstance();
        } catch (SecurityException ex) {
            throw Exceptions.sourceInstantiation(ex);
        } catch (InstantiationException ex) {
            throw Exceptions.sourceInstantiation(ex);
        } catch (IllegalAccessException ex) {
            throw Exceptions.sourceInstantiation(ex);
        } catch (ClassCastException ex) {
            throw Exceptions.sourceInstantiation(ex);
        }

        Reader reader = getCharacterStreamImpl();

        source.setReader(reader);

        return (T) source;
    }

    
    @SuppressWarnings("unchecked")
    protected <T extends Source>T createDOMSource(
            Class<T> sourceClass) throws SQLException {

        DOMSource source = null;

        try {
            source = (sourceClass == null) ? new DOMSource()
                    : (DOMSource) sourceClass.newInstance();
        } catch (SecurityException ex) {
            throw Exceptions.sourceInstantiation(ex);
        } catch (IllegalAccessException ex) {
            throw Exceptions.sourceInstantiation(ex);
        } catch (InstantiationException ex) {
            throw Exceptions.sourceInstantiation(ex);
        } catch (ClassCastException ex) {
            throw Exceptions.sourceInstantiation(ex);
        }

        Transformer  transformer  = JDBCSQLXML.getIdentityTransformer();
        InputStream  stream       = this.getBinaryStreamImpl();
        StreamSource streamSource = new StreamSource();
        DOMResult    result       = new DOMResult();

        streamSource.setInputStream(stream);

        try {
            transformer.transform(streamSource, result);
        } catch (TransformerException ex) {
            throw Exceptions.transformFailed(ex);
        }
        source.setNode(result.getNode());
        source.setSystemId(result.getSystemId());

        return (T) source;
    }

    
    @SuppressWarnings("unchecked")
    protected <T extends Source>T createSAXSource(
            Class<T> sourceClass) throws SQLException {

        SAXSource source = null;

        try {
            source = (sourceClass == null) ? new SAXSource()
                    : (SAXSource) sourceClass.newInstance();
        } catch (SecurityException ex) {
            throw Exceptions.sourceInstantiation(ex);
        } catch (InstantiationException ex) {
            throw Exceptions.sourceInstantiation(ex);
        } catch (IllegalAccessException ex) {
            throw Exceptions.sourceInstantiation(ex);
        } catch (ClassCastException ex) {
            throw Exceptions.sourceInstantiation(ex);
        }

        Reader      reader      = getCharacterStreamImpl();
        InputSource inputSource = new InputSource(reader);

        source.setInputSource(inputSource);

        return (T) source;
    }

    
    @SuppressWarnings("unchecked")
    protected <T extends Source>T createStAXSource(
            Class<T> sourceClass) throws SQLException {

        StAXSource      source      = null;
        Constructor     sourceCtor  = null;
        Reader          reader      = null;
        XMLInputFactory factory     = null;
        XMLEventReader  eventReader = null;

        try {
            factory = XMLInputFactory.newInstance();
        } catch (FactoryConfigurationError ex) {
            throw Exceptions.sourceInstantiation(ex);
        }

        try {
            sourceCtor =
                (sourceClass == null)
                ? StAXSource.class.getConstructor(XMLEventReader.class)
                : sourceClass.getConstructor(XMLEventReader.class);
        } catch (SecurityException ex) {
            throw Exceptions.sourceInstantiation(ex);
        } catch (NoSuchMethodException ex) {
            throw Exceptions.sourceInstantiation(ex);
        }
        reader = getCharacterStreamImpl();

        try {
            eventReader = factory.createXMLEventReader(reader);
        } catch (XMLStreamException ex) {
            throw Exceptions.sourceInstantiation(ex);
        }

        try {
            source = (StAXSource) sourceCtor.newInstance(eventReader);
        } catch (SecurityException ex) {
            throw Exceptions.sourceInstantiation(ex);
        } catch (IllegalArgumentException ex) {
            throw Exceptions.sourceInstantiation(ex);
        } catch (IllegalAccessException ex) {
            throw Exceptions.sourceInstantiation(ex);
        } catch (InstantiationException ex) {
            throw Exceptions.sourceInstantiation(ex);
        } catch (InvocationTargetException ex) {
            throw Exceptions.sourceInstantiation(ex.getTargetException());
        } catch (ClassCastException ex) {
            throw Exceptions.sourceInstantiation(ex);
        }

        return (T) source;
    }

    
    protected <T extends Result>T createResult(
            Class<T> resultClass) throws SQLException {

        checkWritable();
        setWritable(false);
        setReadable(true);

        if (JAXBResult.class.isAssignableFrom(resultClass)) {

            
            
            
            
            
            
        } else if ((resultClass == null)
                   || StreamResult.class.isAssignableFrom(resultClass)) {
            return createStreamResult(resultClass);
        } else if (DOMResult.class.isAssignableFrom(resultClass)) {
            return createDOMResult(resultClass);
        } else if (SAXResult.class.isAssignableFrom(resultClass)) {
            return createSAXResult(resultClass);
        } else if (StAXResult.class.isAssignableFrom(resultClass)) {
            return createStAXResult(resultClass);
        }

        throw Util.invalidArgument("resultClass: " + resultClass);
    }

    


    protected <T extends Result>T createStreamResult(
            Class<T> resultClass) throws SQLException {

        StreamResult result = null;

        try {
            result = (resultClass == null) ? new StreamResult()
                    : (StreamResult) resultClass.newInstance();
        } catch (SecurityException ex) {
            throw Exceptions.resultInstantiation(ex);
        } catch (InstantiationException ex) {
            throw Exceptions.resultInstantiation(ex);
        } catch (IllegalAccessException ex) {
            throw Exceptions.resultInstantiation(ex);
        } catch (ClassCastException ex) {
            throw Exceptions.resultInstantiation(ex);
        }

        OutputStream stream = setBinaryStreamImpl();

        result.setOutputStream(stream);

        return (T) result;
    }

    
    @SuppressWarnings("unchecked")
    protected <T extends Result>T createDOMResult(
            Class<T> resultClass) throws SQLException {

        try {
            T result = (resultClass == null) ? ((T) new DOMResult())
                    : resultClass.newInstance();

            this.domResult = (DOMResult) result;

            return result;
        } catch (SecurityException ex) {
            throw Exceptions.resultInstantiation(ex);
        } catch (InstantiationException ex) {
            throw Exceptions.resultInstantiation(ex);
        } catch (IllegalAccessException ex) {
            throw Exceptions.resultInstantiation(ex);
        } catch (ClassCastException ex) {
            throw Exceptions.resultInstantiation(ex);
        }
    }

    
    @SuppressWarnings("unchecked")
    protected <T extends Result>T createSAXResult(
            Class<T> resultClass) throws SQLException {

        SAXResult result = null;

        try {
            result = (resultClass == null) ? new SAXResult()
                    : (SAXResult) resultClass.newInstance();
        } catch (SecurityException ex) {
            throw Exceptions.resultInstantiation(ex);
        } catch (InstantiationException ex) {
            throw Exceptions.resultInstantiation(ex);
        } catch (IllegalAccessException ex) {
            throw Exceptions.resultInstantiation(ex);
        } catch (ClassCastException ex) {
            throw Exceptions.resultInstantiation(ex);
        }

        SAX2DOMBuilder handler = null;

        try {
            handler = new SAX2DOMBuilder();
        } catch (ParserConfigurationException ex) {
            throw Exceptions.resultInstantiation(ex);
        }
        this.domResult = new DOMResult();

        result.setHandler(handler);
        this.domResult.setNode(handler.getDocument());

        return (T) result;
    }

    
    @SuppressWarnings("unchecked")
    protected <T extends Result>T createStAXResult(
            Class<T> resultClass) throws SQLException {

        StAXResult result = null;

        try {
            this.domResult =
                new DOMResult((new SAX2DOMBuilder()).getDocument());

            XMLOutputFactory factory = XMLOutputFactory.newInstance();
            XMLStreamWriter xmlStreamWriter =
                factory.createXMLStreamWriter(this.domResult);

            if (resultClass == null || resultClass == StAXResult.class) {
                result = new StAXResult(xmlStreamWriter);
            } else {
                Constructor ctor =
                    resultClass.getConstructor(XMLStreamWriter.class);

                result = (StAXResult) ctor.newInstance(xmlStreamWriter);
            }
        } catch (ParserConfigurationException ex) {
            throw Exceptions.resultInstantiation(ex);
        } catch (SecurityException ex) {
            throw Exceptions.resultInstantiation(ex);
        } catch (IllegalArgumentException ex) {
            throw Exceptions.resultInstantiation(ex);
        } catch (IllegalAccessException ex) {
            throw Exceptions.resultInstantiation(ex);
        } catch (InvocationTargetException ex) {
            throw Exceptions.resultInstantiation(ex.getTargetException());
        } catch (FactoryConfigurationError ex) {
            throw Exceptions.resultInstantiation(ex);
        } catch (InstantiationException ex) {
            throw Exceptions.resultInstantiation(ex);
        } catch (NoSuchMethodException ex) {
            throw Exceptions.resultInstantiation(ex);
        } catch (XMLStreamException ex) {
            throw Exceptions.resultInstantiation(ex);
        }

        return (T) result;
    }

    protected void freeDomResult() {
        this.domResult = null;
    }

    
    protected static class Exceptions {

        
        private Exceptions() {
        }

        
        static SQLException domInstantiation(Throwable cause) {

            Exception ex = (cause instanceof Exception) ? (Exception) cause
                    : new Exception(cause);

            return Util.sqlException(ErrorCode.GENERAL_ERROR,
                                     "SQLXML DOM instantiation failed: "
                                     + cause, ex);
        }

        
        static SQLException sourceInstantiation(Throwable cause) {

            Exception ex = (cause instanceof Exception) ? (Exception) cause
                    : new Exception(cause);

            return Util.sqlException(ErrorCode.GENERAL_ERROR,
                                     "SQLXML Source instantiation failed: "
                                     + cause, ex);
        }

        
        static SQLException resultInstantiation(Throwable cause) {

            Exception ex = (cause instanceof Exception) ? (Exception) cause
                    : new Exception(cause);

            return Util.sqlException(ErrorCode.GENERAL_ERROR,
                                     "SQLXML Result instantiation failed: "
                                     + cause, ex);
        }

        
        static SQLException parseFailed(Throwable cause) {

            Exception ex = (cause instanceof Exception) ? (Exception) cause
                    : new Exception(cause);

            return Util.sqlException(ErrorCode.GENERAL_ERROR,
                                     "parse failed: " + cause, ex);
        }

        
        static SQLException transformFailed(Throwable cause) {

            Exception ex = (cause instanceof Exception) ? (Exception) cause
                    : new Exception(cause);

            return Util.sqlException(ErrorCode.GENERAL_ERROR,
                                     "transform failed: " + cause, ex);
        }

        
        static SQLException notReadable() {
            return Util.sqlException(ErrorCode.GENERAL_IO_ERROR,
                                     "SQLXML in not readable state");
        }

        
        static SQLException notReadable(String reason) {

            return Util.sqlException(ErrorCode.GENERAL_IO_ERROR,
                                     "SQLXML in not readable state: "
                                     + reason);
        }

        
        static SQLException notWritable() {
            return Util.sqlException(ErrorCode.GENERAL_IO_ERROR,
                                     "SQLXML in not writable state");
        }

        
        static SQLException directUpdateByLocatorNotSupported() {
            return Util.sqlException(ErrorCode.X_0A000,
                                     "SQLXML direct update by locator");
        }

        
        static SQLException inFreedState() {
            return Util.sqlException(ErrorCode.GENERAL_ERROR,
                                     "SQLXML in freed state");
        }
    }

    

    
    protected static class SAX2DOMBuilder implements ContentHandler,
            Closeable {

        
        private boolean closed;

        
        private Element currentElement;

        

        
        private Node currentNode;

        
        private Document document;

        
        private Locator locator;

        
        public SAX2DOMBuilder() throws ParserConfigurationException {

            DocumentBuilderFactory documentBuilderFactory;
            DocumentBuilder        documentBuilder;

            documentBuilderFactory = DocumentBuilderFactory.newInstance();

            documentBuilderFactory.setValidating(false);
            documentBuilderFactory.setNamespaceAware(true);

            documentBuilder  = documentBuilderFactory.newDocumentBuilder();
            this.document    = documentBuilder.newDocument();
            this.currentNode = this.document;
        }

        
        public void setDocumentLocator(Locator locator) {
            this.locator = locator;
        }

        
        public Locator getDocumentLocator() {
            return this.locator;
        }

        
        public void startDocument() throws SAXException {
            checkClosed();
        }

        
        public void endDocument() throws SAXException {
            checkClosed();
            close();
        }

        
        public void startPrefixMapping(String prefix,
                                       String uri) throws SAXException {
            checkClosed();
        }

        
        public void endPrefixMapping(String prefix) throws SAXException {
            checkClosed();
        }

        
        public void startElement(String uri, String localName, String qName,
                                 Attributes atts) throws SAXException {

            checkClosed();

            Element element;

            if ((uri == null) || (uri.length() == 0)) {
                element = getDocument().createElement(qName);
            } else {
                element = getDocument().createElementNS(uri, qName);
            }

            if (atts != null) {
                for (int i = 0; i < atts.getLength(); i++) {
                    String attrURI   = atts.getURI(i);
                    String attrQName = atts.getQName(i);
                    String attrValue = atts.getValue(i);

                    if ((attrURI == null) || (attrURI.length() == 0)) {
                        element.setAttribute(attrQName, attrValue);
                    } else {
                        element.setAttributeNS(attrURI, attrQName, attrValue);
                    }
                }
            }
            getCurrentNode().appendChild(element);
            setCurrentNode(element);

            if (getCurrentElement() == null) {
                setCurrentElement(element);
            }
        }

        
        public void endElement(String uri, String localName,
                               String qName) throws SAXException {
            checkClosed();
            setCurrentNode(getCurrentNode().getParentNode());
        }

        
        public void characters(char[] ch, int start,
                               int length) throws SAXException {

            checkClosed();

            Node   node = getCurrentNode().getLastChild();
            String s    = new String(ch, start, length);

            if ((node != null) && (node.getNodeType() == Node.TEXT_NODE)) {
                ((Text) node).appendData(s);
            } else {
                Text text = getDocument().createTextNode(s);

                getCurrentNode().appendChild(text);
            }
        }

        
        public void ignorableWhitespace(char[] ch, int start,
                                        int length) throws SAXException {
            characters(ch, start, length);
        }

        
        public void processingInstruction(String target,
                String data) throws SAXException {

            checkClosed();

            ProcessingInstruction processingInstruction;

            processingInstruction =
                getDocument().createProcessingInstruction(target, data);

            getCurrentNode().appendChild(processingInstruction);
        }

        
        public void skippedEntity(String name) throws SAXException {

            checkClosed();

            EntityReference entityReference =
                getDocument().createEntityReference(name);

            getCurrentNode().appendChild(entityReference);
        }

        
        public void close() {
            this.closed = true;
        }

        
        public void free() {

            close();

            this.document       = null;
            this.currentElement = null;
            this.currentNode    = null;
            this.locator        = null;
        }

        
        public boolean isClosed() {
            return this.closed;
        }

        
        protected void checkClosed() throws SAXException {

            if (isClosed()) {
                throw new SAXException("content handler is closed.");    
            }
        }

        
        public Document getDocument() {
            return this.document;
        }

        
        protected Element getCurrentElement() {
            return this.currentElement;
        }

        
        protected void setCurrentElement(Element element) {
            this.currentElement = element;
        }

        
        protected Node getCurrentNode() {
            return this.currentNode;
        }

        
        protected void setCurrentNode(Node node) {
            this.currentNode = node;
        }
    }

    
    public static class SAX2XMLStreamWriter implements ContentHandler,
            Closeable {

        
        private List<QualifiedName> namespaces =
            new ArrayList<QualifiedName>();

        
        private boolean closed;

        
        private Locator locator;

        
        private XMLStreamWriter writer;

        
        public SAX2XMLStreamWriter(XMLStreamWriter writer) {

            if (writer == null) {
                throw new NullPointerException("writer");
            }
            this.writer = writer;
        }

        
        public void startDocument() throws SAXException {

            checkClosed();

            try {
                this.writer.writeStartDocument();
            } catch (XMLStreamException e) {
                throw new SAXException(e);
            }
        }

        
        public void endDocument() throws SAXException {

            checkClosed();

            try {
                this.writer.writeEndDocument();
                this.writer.flush();
            } catch (XMLStreamException e) {
                throw new SAXException(e);
            }
        }

        
        public void characters(char[] ch, int start,
                               int length) throws SAXException {

            checkClosed();

            try {
                this.writer.writeCharacters(ch, start, length);
            } catch (XMLStreamException e) {
                throw new SAXException(e);
            }
        }

        
        public void startElement(String namespaceURI, String localName,
                                 String qName,
                                 Attributes atts) throws SAXException {

            checkClosed();

            try {
                int    qi     = qName.indexOf(':');
                String prefix = (qi > 0) ? qName.substring(0, qi)
                        : "";

                this.writer.writeStartElement(prefix, localName, namespaceURI);

                int length = namespaces.size();

                for (int i = 0; i < length; i++) {
                    QualifiedName ns = namespaces.get(i);

                    this.writer.writeNamespace(ns.prefix, ns.namespaceName);
                }
                namespaces.clear();

                length = atts.getLength();

                for (int i = 0; i < length; i++) {
                    this.writer.writeAttribute(atts.getURI(i),
                            atts.getLocalName(i), atts.getValue(i));
                }
            } catch (XMLStreamException e) {
                throw new SAXException(e);
            }
        }

        
        public void endElement(String namespaceURI, String localName,
                               String qName) throws SAXException {

            checkClosed();

            try {
                this.writer.writeEndElement();
            } catch (XMLStreamException e) {
                throw new SAXException(e);
            }
        }

        
        public void startPrefixMapping(String prefix,
                                       String uri) throws SAXException {

            checkClosed();

            try {
                this.writer.setPrefix(prefix, uri);
                namespaces.add(new QualifiedName(prefix, uri));
            } catch (XMLStreamException e) {
                throw new SAXException(e);
            }
        }

        
        public void endPrefixMapping(String prefix) throws SAXException {

            checkClosed();

            
        }

        
        public void ignorableWhitespace(char[] ch, int start,
                                        int length) throws SAXException {
            characters(ch, start, length);
        }

        
        public void processingInstruction(String target,
                String data) throws SAXException {

            checkClosed();

            try {
                this.writer.writeProcessingInstruction(target, data);
            } catch (XMLStreamException e) {
                throw new SAXException(e);
            }
        }

        
        public void setDocumentLocator(Locator locator) {
            this.locator = locator;
        }

        
        public Locator getDocumentLocator() {
            return this.locator;
        }

        
        public void skippedEntity(String name) throws SAXException {

            checkClosed();

            
        }

        public void comment(char[] ch, int start,
                            int length) throws SAXException {

            checkClosed();

            try {
                this.writer.writeComment(new String(ch, start, length));
            } catch (XMLStreamException e) {
                throw new SAXException(e);
            }
        }

        public XMLStreamWriter getWriter() {
            return this.writer;
        }

        protected List<QualifiedName> getNamespaces() {
            return this.namespaces;
        }

        
        public void close() throws IOException {

            if (!this.closed) {
                this.closed = true;

                try {
                    this.writer.close();
                } catch (XMLStreamException e) {
                    throw new IOException(e);
                } finally {
                    this.writer     = null;
                    this.locator    = null;
                    this.namespaces = null;
                }
            }
        }

        
        public boolean isClosed() {
            return this.closed;
        }

        
        protected void checkClosed() throws SAXException {

            if (isClosed()) {
                throw new SAXException("content handler is closed.");    
            }
        }

        
        protected class QualifiedName {

            public final String namespaceName;
            public final String prefix;

            public QualifiedName(final String prefix,
                                 final String namespaceName) {
                this.prefix        = prefix;
                this.namespaceName = namespaceName;
            }
        }
    }
}
