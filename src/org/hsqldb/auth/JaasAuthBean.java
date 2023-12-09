


package org.hsqldb.auth;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.Subject;
import java.security.Principal;
import org.hsqldb.lib.FrameworkLogger;


public class JaasAuthBean implements AuthFunctionBean {
    private static FrameworkLogger logger =
            FrameworkLogger.getLog(JaasAuthBean.class);

    private boolean initialized;
    private String applicationKey;
    private Pattern roleSchemaValuePattern;
    private boolean roleSchemaViaCredential;

    public JaasAuthBean() {
        
    }

    
    public void setRoleSchemaViaCredential(boolean roleSchemaViaCredential) {
        this.roleSchemaViaCredential = roleSchemaViaCredential;
    }

    
    public void init() {
        if (applicationKey == null) {
            throw new IllegalStateException(
                    "Required property 'applicationKey' not set");
        }
        if (roleSchemaViaCredential && roleSchemaValuePattern == null) {
            throw new IllegalStateException(
                    "Properties 'roleSchemaViaCredential' and "
                    + "'roleSchemaValuePattern' are mutually exclusive.  "
                    + "If you want JaasAuthBean to manage roles or schemas, "
                    + "you must set property 'roleSchemaValuePattern'.");
        }
        initialized = true;
    }

    
    public void setApplicationKey(String applicationKey) {
        this.applicationKey = applicationKey;
    }

    
    public void setRoleSchemaValuePattern(Pattern roleSchemaValuePattern) {
        this.roleSchemaValuePattern = roleSchemaValuePattern;
    }

    
    public void setRoleSchemaValuePatternString(String patternString) {
        setRoleSchemaValuePattern(Pattern.compile(patternString));
    }

    public static class UPCallbackHandler implements CallbackHandler {
        private String u;
        private char[] p;

        public UPCallbackHandler(String u, String pString) {
            this.u = u;
            p = pString.toCharArray();
        }

        public void handle(Callback[] callbacks)
                throws UnsupportedCallbackException {
            boolean didSetName = false;
            boolean didSetPassword = false;
            for (Callback cb : callbacks)
                if (cb instanceof NameCallback) {
                    ((NameCallback) cb).setName(u);
                    didSetName = true;
                } else if (cb instanceof PasswordCallback) {
                    ((PasswordCallback) cb).setPassword(p);
                    didSetPassword = true;
                } else {
                    throw new UnsupportedCallbackException(cb,
                            "Unsupported Callback type: "
                            + cb.getClass().getName());
                }
            if (!didSetName)
                throw new IllegalStateException(
                        "Supplied Callbacks does not include a NameCallback");
            if (!didSetPassword)
                throw new IllegalStateException("Supplied Callbacks "
                        + "does not include a PasswordCallback");
        }
    }

    
    public String[] authenticate(String userName, String password)
            throws DenyException {
        if (!initialized) {
            throw new IllegalStateException(
                "You must invoke the 'init' method to initialize the "
                + JaasAuthBean.class.getName() + " instance.");
        }
        try {
            LoginContext lc =
                new LoginContext(applicationKey,
                        new UPCallbackHandler(userName, password));
            try {
                lc.login();
            } catch (LoginException le) {
                
                
                logger.finer("JSSE backend denying access:  " + le);
                throw new DenyException();
            }
            try {
                if (roleSchemaValuePattern == null) {
                    return null;
                }
                int i = 0;
                Matcher m = null;
                List<String> rsCandidates = new ArrayList<String>();
                List<String> rsList = new ArrayList<String>();
                Subject s = lc.getSubject();
                if (roleSchemaViaCredential) {
                    for (Object cred :
                                new ArrayList(s.getPublicCredentials())) {
                        rsCandidates.add(cred.toString());
                    }
                } else {
                    for (Principal p :
                            new ArrayList<Principal>(s.getPrincipals())) {
                        rsCandidates.add(p.getName());
                    }
                }
                logger.finer(Integer.toString(rsCandidates.size())
                            + " candidate " + (roleSchemaViaCredential
                            ? "Credentials" : "Principals"));
                for (String candid : rsCandidates) {
                    m = roleSchemaValuePattern.matcher(candid);
                    if (m.matches()) {
                        logger.finer("    +" + ++i + ": "
                                + ((m.groupCount() > 0) ? m.group(1) : candid));
                        rsList.add((m.groupCount() > 0) ? m.group(1) : candid);
                    } else {
                        logger.finer("    -" + ++i + ": " + candid);
                    }
                }
                return rsList.toArray(new String[0]);
            } finally {
                lc.logout();
            }
        } catch (LoginException le) {
            logger.severe("System JaasAuthBean failure", le);
            throw new RuntimeException(le);  
        } catch (RuntimeException re) {
            logger.severe("System JaasAuthBean failure", re);
            throw re;
        }
    }
}
