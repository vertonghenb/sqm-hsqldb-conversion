package org.hsqldb.types;
import java.text.Collator;
import java.util.Locale;
import org.hsqldb.HsqlException;
import org.hsqldb.HsqlNameManager;
import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.SchemaObject;
import org.hsqldb.Session;
import org.hsqldb.SqlInvariants;
import org.hsqldb.Tokens;
import org.hsqldb.TypeInvariants;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.HashMap;
import org.hsqldb.lib.Iterator;
import org.hsqldb.lib.OrderedHashSet;
import org.hsqldb.lib.StringUtil;
import org.hsqldb.lib.java.JavaSystem;
import org.hsqldb.rights.Grantee;
public class Collation implements SchemaObject {
    public static final HashMap nameToJavaName    = new HashMap(101);
    public static final HashMap dbNameToJavaName  = new HashMap(101);
    public static final HashMap dbNameToCollation = new HashMap(11);
    static {
        nameToJavaName.put("Afrikaans", "af-ZA");
        nameToJavaName.put("Amharic", "am-ET");
        nameToJavaName.put("Arabic", "ar");
        nameToJavaName.put("Assamese", "as-IN");
        nameToJavaName.put("Azerbaijani_Latin", "az-AZ");
        nameToJavaName.put("Azerbaijani_Cyrillic", "az-cyrillic");
        nameToJavaName.put("Belarusian", "be-BY");
        nameToJavaName.put("Bulgarian", "bg-BG");
        nameToJavaName.put("Bengali", "bn-IN");
        nameToJavaName.put("Tibetan", "bo-CN");
        nameToJavaName.put("Bosnian", "bs-BA");
        nameToJavaName.put("Catalan", "ca-ES");
        nameToJavaName.put("Czech", "cs-CZ");
        nameToJavaName.put("Welsh", "cy-GB");
        nameToJavaName.put("Danish", "da-DK");
        nameToJavaName.put("German", "de-DE");
        nameToJavaName.put("Greek", "el-GR");
        nameToJavaName.put("Latin1_General", "en-US");
        nameToJavaName.put("English", "en-US");
        nameToJavaName.put("Spanish", "es-ES");
        nameToJavaName.put("Estonian", "et-EE");
        nameToJavaName.put("Basque", "eu");
        nameToJavaName.put("Finnish", "fi-FI");
        nameToJavaName.put("French", "fr-FR");
        nameToJavaName.put("Guarani", "gn-PY");
        nameToJavaName.put("Gujarati", "gu-IN");
        nameToJavaName.put("Hausa", "ha-NG");
        nameToJavaName.put("Hebrew", "he-IL");
        nameToJavaName.put("Hindi", "hi-IN");
        nameToJavaName.put("Croatian", "hr-HR");
        nameToJavaName.put("Hungarian", "hu-HU");
        nameToJavaName.put("Armenian", "hy-AM");
        nameToJavaName.put("Indonesian", "id-ID");
        nameToJavaName.put("Igbo", "ig-NG");
        nameToJavaName.put("Icelandic", "is-IS");
        nameToJavaName.put("Italian", "it-IT");
        nameToJavaName.put("Inuktitut", "iu-CA");
        nameToJavaName.put("Japanese", "ja-JP");
        nameToJavaName.put("Georgian", "ka-GE");
        nameToJavaName.put("Kazakh", "kk-KZ");
        nameToJavaName.put("Khmer", "km-KH");
        nameToJavaName.put("Kannada", "kn-IN");
        nameToJavaName.put("Korean", "ko-KR");
        nameToJavaName.put("Konkani", "kok-IN");
        nameToJavaName.put("Kashmiri", "ks");
        nameToJavaName.put("Kirghiz", "ky-KG");
        nameToJavaName.put("Lao", "lo-LA");
        nameToJavaName.put("Lithuanian", "lt-LT");
        nameToJavaName.put("Latvian", "lv-LV");
        nameToJavaName.put("Maori", "mi-NZ");
        nameToJavaName.put("Macedonian", "mk-MK");
        nameToJavaName.put("Malayalam", "ml-IN");
        nameToJavaName.put("Mongolian", "mn-MN");
        nameToJavaName.put("Manipuri", "mni-IN");
        nameToJavaName.put("Marathi", "mr-IN");
        nameToJavaName.put("Malay", "ms-MY");
        nameToJavaName.put("Maltese", "mt-MT");
        nameToJavaName.put("Burmese", "my-MM");
        nameToJavaName.put("Danish_Norwegian", "nb-NO");
        nameToJavaName.put("Nepali", "ne-NP");
        nameToJavaName.put("Dutch", "nl-NL");
        nameToJavaName.put("Norwegian", "nn-NO");
        nameToJavaName.put("Oriya", "or-IN");
        nameToJavaName.put("Punjabi", "pa-IN");
        nameToJavaName.put("Polish", "pl-PL");
        nameToJavaName.put("Pashto", "ps-AF");
        nameToJavaName.put("Portuguese", "pt-PT");
        nameToJavaName.put("Romanian", "ro-RO");
        nameToJavaName.put("Russian", "ru-RU");
        nameToJavaName.put("Sanskrit", "sa-IN");
        nameToJavaName.put("Sindhi", "sd-IN");
        nameToJavaName.put("Slovak", "sk-SK");
        nameToJavaName.put("Slovenian", "sl-SI");
        nameToJavaName.put("Somali", "so-SO");
        nameToJavaName.put("Albanian", "sq-AL");
        nameToJavaName.put("Serbian_Cyrillic", "sr-YU");
        nameToJavaName.put("Serbian_Latin", "sh-BA");
        nameToJavaName.put("Swedish", "sv-SE");
        nameToJavaName.put("Swahili", "sw-KE");
        nameToJavaName.put("Tamil", "ta-IN");
        nameToJavaName.put("Telugu", "te-IN");
        nameToJavaName.put("Tajik", "tg-TJ");
        nameToJavaName.put("Thai", "th-TH");
        nameToJavaName.put("Turkmen", "tk-TM");
        nameToJavaName.put("Tswana", "tn-BW");
        nameToJavaName.put("Turkish", "tr-TR");
        nameToJavaName.put("Tatar", "tt-RU");
        nameToJavaName.put("Ukrainian", "uk-UA");
        nameToJavaName.put("Urdu", "ur-PK");
        nameToJavaName.put("Uzbek_Latin", "uz-UZ");
        nameToJavaName.put("Venda", "ven-ZA");
        nameToJavaName.put("Vietnamese", "vi-VN");
        nameToJavaName.put("Yoruba", "yo-NG");
        nameToJavaName.put("Chinese", "zh-CN");
        nameToJavaName.put("Zulu", "zu-ZA");
        Iterator it = nameToJavaName.values().iterator();
        while (it.hasNext()) {
            String javaName = (String) it.next();
            String dbName = javaName.replace('-',
                                             '_').toUpperCase(Locale.ENGLISH);
            dbNameToJavaName.put(dbName, javaName);
        }
    }
    static final Collation defaultCollation = new Collation();
    static {
        defaultCollation.charset = TypeInvariants.SQL_TEXT;
    }
    final HsqlName name;
    Collator       collator;
    Locale         locale;
    boolean        equalIsIdentical = true;
    boolean        isFinal;
    Charset  charset;
    HsqlName sourceName;
    private Collation() {
        locale = Locale.ENGLISH;
        String language = locale.getDisplayLanguage(Locale.ENGLISH);
        name = HsqlNameManager.newInfoSchemaObjectName(language, true,
                SchemaObject.COLLATION);
        this.isFinal = true;
    }
    private Collation(String name, String language, String country) {
        locale           = new Locale(language, country);
        collator         = Collator.getInstance(locale);
        equalIsIdentical = false;
        this.name = HsqlNameManager.newInfoSchemaObjectName(name, true,
                SchemaObject.COLLATION);
        charset      = TypeInvariants.SQL_TEXT;
        this.isFinal = true;
    }
    public Collation(HsqlName name, Collation source, Charset charset) {
        this.name             = name;
        this.locale           = source.locale;
        this.collator         = source.collator;
        this.equalIsIdentical = source.equalIsIdentical;
        this.isFinal          = true;
        this.charset    = charset;
        this.sourceName = source.name;
    }
    public static Collation getDefaultInstance() {
        return defaultCollation;
    }
    public static Collation getDatabaseInstance() {
        Collation collation = new Collation();
        collation.isFinal = false;
        return collation;
    }
    public static org.hsqldb.lib.Iterator getCollationsIterator() {
        return nameToJavaName.keySet().iterator();
    }
    public static org.hsqldb.lib.Iterator getLocalesIterator() {
        return nameToJavaName.values().iterator();
    }
    public synchronized static Collation getCollation(String name) {
        Collation collation = (Collation) dbNameToCollation.get(name);
        if (collation != null) {
            return collation;
        }
        String javaName = (String) dbNameToJavaName.get(name);
        if (javaName == null) {
            javaName = (String) nameToJavaName.get(name);
            if (javaName == null) {
                throw Error.error(ErrorCode.X_42501, javaName);
            }
        }
        String[] parts    = StringUtil.split(javaName, "-");
        String   language = parts[0];
        String   country  = parts.length == 2 ? parts[1]
                                              : "";
        collation = new Collation(name, language, country);
        dbNameToCollation.put(name, collation);
        return collation;
    }
    public void setCollationAsLocale() {
        Locale locale   = Locale.getDefault();
        String language = locale.getDisplayLanguage(Locale.ENGLISH);
        try {
            setCollation(language);
        } catch (HsqlException e) {}
    }
    public void setCollation(String newName) {
        String jname = (String) Collation.nameToJavaName.get(newName);
        if (jname == null) {
            jname = (String) Collation.dbNameToJavaName.get(newName);
        }
        if (jname == null) {
            throw Error.error(ErrorCode.X_42501, newName);
        }
        if (isFinal) {
            throw Error.error(ErrorCode.X_42503, newName);
        }
        name.rename(newName, true);
        String[] parts    = StringUtil.split(jname, "-");
        String   language = parts[0];
        String   country  = parts.length == 2 ? parts[1]
                                              : "";
        locale           = new Locale(language, country);
        collator         = Collator.getInstance(locale);
        equalIsIdentical = false;
    }
    public boolean isEqualAlwaysIdentical() {
        return collator == null;
    }
    public int compare(String a, String b) {
        int i;
        if (collator == null) {
            i = a.compareTo(b);
        } else {
            i = collator.compare(a, b);
        }
        return (i == 0) ? 0
                        : (i < 0 ? -1
                                 : 1);
    }
    public int compareIgnoreCase(String a, String b) {
        int i;
        if (collator == null) {
            i = JavaSystem.compareIngnoreCase(a, b);
        } else {
            i = collator.compare(toUpperCase(a), toUpperCase(b));
        }
        return (i == 0) ? 0
                        : (i < 0 ? -1
                                 : 1);
    }
    public String toUpperCase(String s) {
        return s.toUpperCase(locale);
    }
    public String toLowerCase(String s) {
        return s.toLowerCase(locale);
    }
    public boolean isDefaultCollation() {
        return collator == null;
    }
    public boolean isObjectCollation() {
        return isFinal && collator != null;
    }
    public HsqlName getName() {
        return name;
    }
    public int getType() {
        return SchemaObject.COLLATION;
    }
    public HsqlName getSchemaName() {
        return name.schema;
    }
    public HsqlName getCatalogName() {
        return name.schema.schema;
    }
    public Grantee getOwner() {
        return name.schema.owner;
    }
    public OrderedHashSet getReferences() {
        return new OrderedHashSet();
    }
    public OrderedHashSet getComponents() {
        return null;
    }
    public void compile(Session session, SchemaObject parentObject) {}
    public String getSQL() {
        StringBuffer sb = new StringBuffer();
        sb.append(Tokens.T_CREATE).append(' ');
        sb.append(Tokens.T_COLLATION).append(' ');
        sb.append(name.getSchemaQualifiedStatementName()).append(' ');
        sb.append(Tokens.T_FOR).append(' ');
        sb.append(charset.name.getSchemaQualifiedStatementName()).append(' ');
        sb.append(Tokens.T_FROM).append(' ');
        sb.append(sourceName.statementName);
        return sb.toString();
    }
    public long getChangeTimestamp() {
        return 0;
    }
}