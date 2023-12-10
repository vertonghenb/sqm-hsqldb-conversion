package org.hsqldb.auth;
import java.io.IOException;
import java.io.File;
import java.io.FileInputStream;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.Hashtable;
import java.util.Properties;
import javax.naming.AuthenticationException;
import javax.naming.NamingException;
import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.directory.SearchResult;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.Attributes;
import javax.naming.directory.Attribute;
import javax.naming.ldap.StartTlsRequest;
import javax.naming.ldap.StartTlsResponse;
import javax.naming.ldap.LdapContext;
import javax.naming.ldap.InitialLdapContext;
import javax.naming.ldap.ExtendedRequest;
import javax.naming.ldap.ExtendedResponse;
import org.hsqldb.lib.FrameworkLogger;
public class LdapAuthBean implements AuthFunctionBean {
    private static FrameworkLogger logger =
            FrameworkLogger.getLog(LdapAuthBean.class);
    private Integer ldapPort;
    private String ldapHost, principalTemplate, saslRealm, parentDn;
    private Pattern roleSchemaValuePattern, accessValuePattern;
    private String initialContextFactory = "com.sun.jndi.ldap.LdapCtxFactory";
    private boolean tls;  
    private String mechanism = "SIMPLE";
    private String rdnAttribute = "uid";
    private boolean initialized;
    private String rolesSchemaAttribute, accessAttribute;
    protected String[] attributeUnion;
    public void setStartTls(boolean isTls) {
        this.tls = isTls;
    }
    public LdapAuthBean() {
    }
    public void setLdapPort(int ldapPort) {
        this.ldapPort = Integer.valueOf(ldapPort);
    }
    public void init() {
        if (ldapHost == null) {
            throw new IllegalStateException(
                    "Required property 'ldapHost' not set");
        }
        if (parentDn == null) {
            throw new IllegalStateException(
                    "Required property 'parentDn' not set");
        }
        if (initialContextFactory == null) {
            throw new IllegalStateException(
                    "Required property 'initialContextFactory' not set");
        }
        if (mechanism == null) {
            throw new IllegalStateException(
                    "Required property 'mechanism' not set");
        }
        if (rdnAttribute == null) {
            throw new IllegalStateException(
                    "Required property 'rdnAttribute' not set");
        }
        if (rolesSchemaAttribute == null && accessAttribute == null) {
            throw new IllegalStateException(
                    "You must set property 'rolesSchemaAttribute' "
                    + "and/or property 'accessAttribute'");
        }
        if (roleSchemaValuePattern != null && rolesSchemaAttribute == null) {
            throw new IllegalStateException(
                    "If property 'roleSchemaValuePattern' is set, then you "
                    + "must also set property 'rolesSchemaAttribute' to "
                    + "indicate which attribute to evalueate");
        }
        if (accessValuePattern != null && accessAttribute == null) {
            throw new IllegalStateException(
                    "If property 'accessValuePattern' is set, then you "
                    + "must also set property 'accessAttribute' to "
                    + "indicate which attribute to evalueate");
        }
        if (rolesSchemaAttribute != null && accessAttribute != null) {
            attributeUnion = new String[]
                    { rolesSchemaAttribute, accessAttribute };
        } else if (rolesSchemaAttribute != null) {
            attributeUnion = new String[] { rolesSchemaAttribute };
        } else {
            attributeUnion = new String[] { accessAttribute };
        }
        initialized = true;
    }
    public void setAccessValuePattern(Pattern accessValuePattern) {
        this.accessValuePattern = accessValuePattern;
    }
    public void setAccessValuePatternString(String patternString) {
        setAccessValuePattern(Pattern.compile(patternString));
    }
    public void setRoleSchemaValuePattern(Pattern roleSchemaValuePattern) {
        this.roleSchemaValuePattern = roleSchemaValuePattern;
    }
    public void setRoleSchemaValuePatternString(String patternString) {
        setRoleSchemaValuePattern(Pattern.compile(patternString));
    }
    public void setSecurityMechanism(String mechanism) {
        this.mechanism = mechanism;
    }
    public void setLdapHost(String ldapHost) {
        this.ldapHost = ldapHost;
    }
    public void setPrincipalTemplate(String principalTemplate) {
        this.principalTemplate = principalTemplate;
    }
    public void setInitialContextFactory(String initialContextFactory) {
        this.initialContextFactory = initialContextFactory;
    }
    public void setSaslRealm(String saslRealm) {
        this.saslRealm = saslRealm;
    }
    public void setParentDn(String parentDn) {
        this.parentDn = parentDn;
    }
    public void setRdnAttribute(String rdnAttribute) {
        this.rdnAttribute = rdnAttribute;
    }
    public void setRolesSchemaAttribute(String attribute) {
        rolesSchemaAttribute = attribute;
    }
    public void setAccessAttribute(String attribute) {
        accessAttribute = attribute;
    }
    public String[] authenticate(String userName, String password)
            throws DenyException {
        if (!initialized) {
            throw new IllegalStateException(
                "You must invoke the 'init' method to initialize the "
                + LdapAuthBean.class.getName() + " instance.");
        }
        Hashtable env = new Hashtable(5, 0.75f);
        env.put(Context.INITIAL_CONTEXT_FACTORY, initialContextFactory);
        env.put(Context.PROVIDER_URL, "ldap://" + ldapHost
                + ((ldapPort == null) ? "" : (":" + ldapPort)));
        StartTlsResponse tlsResponse = null;
        LdapContext ctx = null;
        try {
            ctx = new InitialLdapContext(env, null);
            if (tls) {
                tlsResponse = (StartTlsResponse) ctx.extendedOperation(
                        new StartTlsRequest());
                tlsResponse.negotiate();
            }
            ctx.addToEnvironment(Context.SECURITY_AUTHENTICATION, mechanism);
            ctx.addToEnvironment(Context.SECURITY_PRINCIPAL,
                  ((principalTemplate == null)
                  ? userName
                  : principalTemplate.replace("${username}", userName)));
            ctx.addToEnvironment(Context.SECURITY_CREDENTIALS, password);
            if (saslRealm != null) {
                env.put("java.naming.security.sasl.realm", saslRealm);
            }
            NamingEnumeration<SearchResult> sRess = null;
            try {
                sRess = ctx.search(parentDn,
                        new BasicAttributes(rdnAttribute, userName),
                        attributeUnion);
            } catch (AuthenticationException ae) {
                throw new DenyException();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            if (!sRess.hasMore()) {
                throw new DenyException();
            }
            SearchResult sRes = sRess.next();
            if (sRess.hasMore()) {
                throw new RuntimeException("> 1 result");
            }
            Attributes attrs = sRes.getAttributes();
            if (accessAttribute != null) {
                Attribute attribute =  attrs.get(accessAttribute);
                if (attribute == null) {
                    throw new DenyException();
                }
                if (attribute.size() != 1) {
                    throw new RuntimeException("Access attribute '"
                            + accessAttribute + "' has unexpected value count: "
                            + attribute.size());
                }
                if (accessValuePattern != null) {
                    Object accessValue = attribute.get(0);
                    if (accessValue == null) {
                        throw new RuntimeException(
                                "Access Attr. value is null");
                    }
                    if (!(accessValue instanceof String)) {
                        throw new RuntimeException("Access Attr. value "
                                + "not a String: "
                                + accessValue.getClass().getName());
                    }
                    if (!accessValuePattern.matcher(
                            (String) accessValue).matches()) {
                        throw new DenyException();
                    }
                }
            }
            if (rolesSchemaAttribute == null) {
                return null;
            }
            List<String> returns = new ArrayList<String>();
            Attribute attribute =  attrs.get(rolesSchemaAttribute);
            if (attribute != null) {
                int valCount = attribute.size();
                Matcher matcher;
                Object oneVal;
                for (int i = 0; i < valCount; i++) {
                    oneVal = attribute.get(i);
                    if (oneVal == null) {
                        throw new RuntimeException(
                                "R/S Attr value #" + i + " is null");
                    }
                    if (!(oneVal instanceof String)) {
                        throw new RuntimeException(
                                "R/S Attr value #" + i + " not a String: "
                                + oneVal.getClass().getName());
                    }
                    if (roleSchemaValuePattern == null) {
                        returns.add((String) oneVal);
                    } else {
                        matcher = roleSchemaValuePattern.matcher(
                                (String) oneVal);
                        if (matcher.matches()) {
                            returns.add((matcher.groupCount() > 0)
                                    ? matcher.group(1)
                                    : (String) oneVal);
                        }
                    }
                }
            }
            if (returns.size() < 1) {
                if (accessAttribute == null) {
                    throw new DenyException();
                }
                return new String[0];
            }
            return returns.toArray(new String[0]);
        } catch (DenyException de) {
            throw de;
        } catch (RuntimeException re) {
            throw re;
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        } catch (NamingException ne) {
            throw new RuntimeException(ne);
        } finally {
            if (tlsResponse != null) try {
                tlsResponse.close();
            } catch (IOException ioe) {
                logger.error("Failed to close TLS Response", ioe);
            }
            if (ctx != null) try {
                ctx.close();
            } catch (NamingException ne) {
                logger.error("Failed to close LDAP Context", ne);
            }
        }
    }
    public static void main(String[] sa) throws IOException {
        if (sa.length != 3) {
            throw new IllegalArgumentException(
                    "SYNTAX:  java " + AuthBeanMultiplexer.class.getName()
                    + " path/to/file.properties <USERNAME> <PASSWORD>");
        }
        File file = new File(sa[0]);
        if (!file.isFile()) {
            throw new IllegalArgumentException(
                    "Not a file: " + file.getAbsolutePath());
        }
        Properties p = new Properties();
        p.load(new FileInputStream(file));
        String trustStore = p.getProperty("trustStore");
        String startTlsString = p.getProperty("startTls");
        String ldapPortString = p.getProperty("ldapPort");
        String roleSchemaValuePatternString =
                p.getProperty("roleSchemaValuePattern");
        String accessValuePatternString = p.getProperty("accessValuePattern");
        String securityMechanism = p.getProperty("securityMechanism");
        String ldapHost = p.getProperty("ldapHost");
        String principalTemplate = p.getProperty("principalTemplate");
        String initialContextFactory = p.getProperty("initialContextFactory");
        String saslRealm = p.getProperty("saslRealm");
        String parentDn = p.getProperty("parentDn");
        String rdnAttribute = p.getProperty("rdnAttribute");
        String rolesSchemaAttribute = p.getProperty("rolesSchemaAttribute");
        String accessAttribute = p.getProperty("accessAttribute");
        if (trustStore != null) {
            if (!(new File(trustStore)).isFile()) {
                throw new IllegalArgumentException(
                        "Specified trust store is not a file: " + trustStore);
            }
            System.setProperty("javax.net.ssl.trustStore", trustStore);
        }
        LdapAuthBean bean = new LdapAuthBean();
        if (startTlsString != null) {
            bean.setStartTls(Boolean.parseBoolean(startTlsString));
        }
        if (ldapPortString != null) {
            bean.setLdapPort(Integer.parseInt(ldapPortString));
        }
        if (roleSchemaValuePatternString != null) {
            bean.setRoleSchemaValuePatternString(roleSchemaValuePatternString);
        }
        if (accessValuePatternString != null) {
            bean.setAccessValuePatternString(accessValuePatternString);
        }
        if (securityMechanism != null) {
            bean.setSecurityMechanism(securityMechanism);
        }
        if (ldapHost != null) {
            bean.setLdapHost(ldapHost);
        }
        if (principalTemplate != null) {
            bean.setPrincipalTemplate(principalTemplate);
        }
        if (initialContextFactory != null) {
            bean.setInitialContextFactory(initialContextFactory);
        }
        if (saslRealm != null) {
            bean.setSaslRealm(saslRealm);
        }
        if (parentDn != null) {
            bean.setParentDn(parentDn);
        }
        if (rdnAttribute != null) {
            bean.setRdnAttribute(rdnAttribute);
        }
        if (rolesSchemaAttribute != null) {
            bean.setRolesSchemaAttribute(rolesSchemaAttribute);
        }
        if (accessAttribute != null) {
            bean.setAccessAttribute(accessAttribute);
        }
        bean.init();
        String[] res = null;
        try {
            res = bean.authenticate(sa[1], sa[2]);
        } catch (DenyException de) {
            System.out.println("<DENY ACCESS>");
            return;
        }
        if (res == null) {
            System.out.println("<ALLOW ACCESS w/ local Roles+Schema>");
        } else {
            System.out.println(Integer.toString(res.length)
                    + " Roles/Schema: " + Arrays.toString(res));
        }
    }
}