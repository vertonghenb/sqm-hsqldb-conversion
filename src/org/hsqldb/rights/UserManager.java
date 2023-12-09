


package org.hsqldb.rights;

import org.hsqldb.Database;
import org.hsqldb.HsqlException;
import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.Routine;
import org.hsqldb.Schema;
import org.hsqldb.SchemaObject;
import org.hsqldb.Session;
import org.hsqldb.SqlInvariants;
import org.hsqldb.Tokens;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.HashMappedList;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.result.Result;


public final class UserManager {

    
    private HashMappedList userList;
    private GranteeManager granteeManager;

    
    Routine pwCheckFunction;
    Routine extAuthenticationFunction;

    
    public UserManager(Database database) {
        granteeManager = database.getGranteeManager();
        userList       = new HashMappedList();
    }

    
    public User createUser(HsqlName name, String password, boolean isDigest) {

        
        
        User user = granteeManager.addUser(name);

        user.setPassword(password, isDigest);

        boolean success = userList.add(name.name, user);

        if (!success) {
            throw Error.error(ErrorCode.X_28503, name.statementName);
        }

        return user;
    }

    public void setPassword(Session session, User user, String password,
                            boolean isDigest) {

        if (!isDigest && !checkComplexity(session, password)) {
            throw Error.error(ErrorCode.PASSWORD_COMPLEXITY);
        }

        
        user.setPassword(password, isDigest);
    }

    public boolean checkComplexity(Session session, String password) {

        if (session == null || pwCheckFunction == null) {
            return true;
        }

        Result result = pwCheckFunction.invoke(session,
                                               new Object[]{ password }, null,
                                               false);
        Boolean check = (Boolean) result.getValueObject();

        if (check == null || !check.booleanValue()) {
            return false;
        }

        return true;
    }

    
    public void dropUser(String name) {

        boolean reservedUser = GranteeManager.isReserved(name);

        if (reservedUser) {
            throw Error.error(ErrorCode.X_28502, name);
        }

        boolean result = granteeManager.removeGrantee(name);

        if (!result) {
            throw Error.error(ErrorCode.X_28501, name);
        }

        User user = (User) userList.remove(name);

        if (user == null) {
            throw Error.error(ErrorCode.X_28501, name);
        }
    }

    public void createFirstUser(String username, String password) {

        boolean isQuoted = true;

        if (username.equalsIgnoreCase("SA")) {
            username = "SA";
            isQuoted = false;
        }

        HsqlName name =
            granteeManager.database.nameManager.newHsqlName(username,
                isQuoted, SchemaObject.GRANTEE);
        User user = createUser(name, password, false);

        user.isLocalOnly = true;

        granteeManager.grant(name.name, SqlInvariants.DBA_ADMIN_ROLE_NAME,
                             granteeManager.getDBARole());
    }

    
    public User getUser(String name, String password) {

        if (name == null) {
            name = "";
        }

        if (password == null) {
            password = "";
        }

        User    user    = (User) userList.get(name);
        boolean isLocal = user != null && user.isLocalOnly;

        if (extAuthenticationFunction == null || isLocal) {
            user = get(name);

            user.checkPassword(password);

            return user;
        }

        
        Result result =
            extAuthenticationFunction.invokeJavaMethodDirect(new String[] {
            granteeManager.database.getUniqueName(), name, password
        });

        if (result.isError()) {
            throw Error.error(ErrorCode.X_28501, result.getMainString());
        }

        Object[] roles = (Object[]) result.getValueObject();

        if (user == null) {
            HsqlName hsqlName =
                granteeManager.database.nameManager.newHsqlName(name, true,
                    SchemaObject.GRANTEE);

            user                = createUser(hsqlName, "", false);
            user.isExternalOnly = true;
        }

        if (roles == null) {
            user.updateAllRights();

            return user;
        }

        
        user.clearPrivileges();

        
        for (int i = 0; i < roles.length; i++) {
            try {
                Grantee role = granteeManager.getRole((String) roles[i]);

                user.grant(role);
            } catch (HsqlException e) {}
        }

        user.updateAllRights();

        for (int i = 0; i < roles.length; i++) {
            Schema schema = granteeManager.database.schemaManager.findSchema(
                (String) roles[i]);

            if (schema != null) {
                user.setInitialSchema(schema.getName());

                break;
            }
        }

        return user;
    }

    
    public HashMappedList getUsers() {
        return userList;
    }

    public boolean exists(String name) {
        return userList.get(name) == null ? false
                                          : true;
    }

    
    public User get(String name) {

        User user = (User) userList.get(name);

        if (user == null) {
            throw Error.error(ErrorCode.X_28501, name);
        }

        return user;
    }

    
    public HsqlArrayList listVisibleUsers(Session session) {

        HsqlArrayList list;
        User          user;
        boolean       isAdmin;
        String        sessionName;
        String        userName;

        list        = new HsqlArrayList();
        isAdmin     = session.isAdmin();
        sessionName = session.getUsername();

        if (userList == null || userList.size() == 0) {
            return list;
        }

        for (int i = 0; i < userList.size(); i++) {
            user = (User) userList.get(i);

            if (user == null) {
                continue;
            }

            userName = user.getName().getNameString();

            if (isAdmin) {
                list.add(user);
            } else if (sessionName.equals(userName)) {
                list.add(user);
            }
        }

        return list;
    }

    
    public User getSysUser() {
        return GranteeManager.systemAuthorisation;
    }

    public synchronized void removeSchemaReference(String schemaName) {

        for (int i = 0; i < userList.size(); i++) {
            User     user   = (User) userList.get(i);
            HsqlName schema = user.getInitialSchema();

            if (schema == null) {
                continue;
            }

            if (schemaName.equals(schema.name)) {
                user.setInitialSchema(null);
            }
        }
    }

    public void setPasswordCheckFunction(Routine function) {
        pwCheckFunction = function;
    }

    public void setExtAuthenticationFunction(Routine function) {
        extAuthenticationFunction = function;
    }

    public String[] getInitialSchemaSQL() {

        HsqlArrayList list = new HsqlArrayList(userList.size());

        for (int i = 0; i < userList.size(); i++) {
            User user = (User) userList.get(i);

            if (user.isSystem) {
                continue;
            }

            HsqlName name = user.getInitialSchema();

            if (name == null) {
                continue;
            }

            list.add(user.getInitialSchemaSQL());
        }

        String[] array = new String[list.size()];

        list.toArray(array);

        return array;
    }

    public String[] getAuthenticationSQL() {

        HsqlArrayList list = new HsqlArrayList();
        String[]      array;

        if (pwCheckFunction != null) {
            StringBuffer sb = new StringBuffer();

            sb.append(Tokens.T_SET).append(' ').append(Tokens.T_DATABASE);
            sb.append(' ').append(Tokens.T_PASSWORD).append(' ');
            sb.append(Tokens.T_CHECK).append(' ').append(Tokens.T_FUNCTION);
            sb.append(' ');
            sb.append(pwCheckFunction.getSQLBodyDefinition());
            list.add(sb.toString());
        }

        if (extAuthenticationFunction != null) {
            StringBuffer sb = new StringBuffer();

            sb.append(Tokens.T_SET).append(' ').append(Tokens.T_DATABASE);
            sb.append(' ').append(Tokens.T_AUTHENTICATION).append(' ');
            sb.append(Tokens.T_FUNCTION).append(' ');
            sb.append(extAuthenticationFunction.getSQLBodyDefinition());
            list.add(sb.toString());
        }

        array = new String[list.size()];

        list.toArray(array);

        return array;
    }
}
