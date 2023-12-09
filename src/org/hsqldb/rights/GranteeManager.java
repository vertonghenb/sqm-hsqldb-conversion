


package org.hsqldb.rights;

import org.hsqldb.Database;
import org.hsqldb.HsqlNameManager;
import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.Routine;
import org.hsqldb.RoutineSchema;
import org.hsqldb.SchemaObject;
import org.hsqldb.SqlInvariants;
import org.hsqldb.Tokens;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.Collection;
import org.hsqldb.lib.HashMappedList;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.IntValueHashMap;
import org.hsqldb.lib.Iterator;
import org.hsqldb.lib.OrderedHashSet;
import org.hsqldb.lib.Set;


public class GranteeManager {

    
    static User systemAuthorisation;

    static {
        HsqlName name = HsqlNameManager.newSystemObjectName(
            SqlInvariants.SYSTEM_AUTHORIZATION_NAME, SchemaObject.GRANTEE);

        systemAuthorisation          = new User(name, null);
        systemAuthorisation.isSystem = true;

        systemAuthorisation.setAdminDirect();
        systemAuthorisation.setInitialSchema(
            SqlInvariants.SYSTEM_SCHEMA_HSQLNAME);

        SqlInvariants.INFORMATION_SCHEMA_HSQLNAME.owner = systemAuthorisation;
        SqlInvariants.SYSTEM_SCHEMA_HSQLNAME.owner      = systemAuthorisation;
        SqlInvariants.LOBS_SCHEMA_HSQLNAME.owner        = systemAuthorisation;
        SqlInvariants.SQLJ_SCHEMA_HSQLNAME.owner        = systemAuthorisation;
    }

    
    private HashMappedList map = new HashMappedList();

    
    private HashMappedList roleMap = new HashMappedList();

    
    Database database;

    
    Grantee publicRole;

    
    Grantee dbaRole;

    
    Grantee schemaRole;

    
    Grantee changeAuthRole;

    
    public GranteeManager(Database database) {

        this.database = database;



        addRole(
            this.database.nameManager.newHsqlName(
                SqlInvariants.PUBLIC_ROLE_NAME, false, SchemaObject.GRANTEE));

        publicRole          = getRole(SqlInvariants.PUBLIC_ROLE_NAME);
        publicRole.isPublic = true;

        addRole(
            this.database.nameManager.newHsqlName(
                SqlInvariants.DBA_ADMIN_ROLE_NAME, false,
                SchemaObject.GRANTEE));

        dbaRole = getRole(SqlInvariants.DBA_ADMIN_ROLE_NAME);

        dbaRole.setAdminDirect();
        addRole(
            this.database.nameManager.newHsqlName(
                SqlInvariants.SCHEMA_CREATE_ROLE_NAME, false,
                SchemaObject.GRANTEE));

        schemaRole = getRole(SqlInvariants.SCHEMA_CREATE_ROLE_NAME);

        addRole(
            this.database.nameManager.newHsqlName(
                SqlInvariants.CHANGE_AUTH_ROLE_NAME, false,
                SchemaObject.GRANTEE));

        changeAuthRole = getRole(SqlInvariants.CHANGE_AUTH_ROLE_NAME);
    }

    static final IntValueHashMap rightsStringLookup = new IntValueHashMap(7);

    static {
        rightsStringLookup.put(Tokens.T_ALL, GrantConstants.ALL);
        rightsStringLookup.put(Tokens.T_SELECT, GrantConstants.SELECT);
        rightsStringLookup.put(Tokens.T_UPDATE, GrantConstants.UPDATE);
        rightsStringLookup.put(Tokens.T_DELETE, GrantConstants.DELETE);
        rightsStringLookup.put(Tokens.T_INSERT, GrantConstants.INSERT);
        rightsStringLookup.put(Tokens.T_EXECUTE, GrantConstants.EXECUTE);
        rightsStringLookup.put(Tokens.T_USAGE, GrantConstants.USAGE);
        rightsStringLookup.put(Tokens.T_REFERENCES, GrantConstants.REFERENCES);
        rightsStringLookup.put(Tokens.T_TRIGGER, GrantConstants.TRIGGER);
    }

    public Grantee getDBARole() {
        return dbaRole;
    }

    public static Grantee getSystemRole() {
        return systemAuthorisation;
    }

    
    public void grant(OrderedHashSet granteeList, SchemaObject dbObject,
                      Right right, Grantee grantor, boolean withGrantOption) {

        if (dbObject instanceof RoutineSchema) {
            SchemaObject[] routines =
                ((RoutineSchema) dbObject).getSpecificRoutines();

            grant(granteeList, routines, right, grantor, withGrantOption);

            return;
        }

        HsqlName name = dbObject.getName();

        if (dbObject instanceof Routine) {
            name = ((Routine) dbObject).getSpecificName();
        }

        if (!grantor.isGrantable(dbObject, right)) {
            throw Error.error(ErrorCode.X_0L000,
                              grantor.getName().getNameString());
        }

        if (grantor.isAdmin()) {
            grantor = dbObject.getOwner();
        }

        checkGranteeList(granteeList);

        for (int i = 0; i < granteeList.size(); i++) {
            Grantee grantee = get((String) granteeList.get(i));

            grantee.grant(name, right, grantor, withGrantOption);

            if (grantee.isRole) {
                updateAllRights(grantee);
            }
        }
    }

    public void grant(OrderedHashSet granteeList, SchemaObject[] routines,
                      Right right, Grantee grantor, boolean withGrantOption) {

        boolean granted = false;

        for (int i = 0; i < routines.length; i++) {
            if (!grantor.isGrantable(routines[i], right)) {
                continue;
            }

            grant(granteeList, routines[i], right, grantor, withGrantOption);

            granted = true;
        }

        if (!granted) {
            throw Error.error(ErrorCode.X_0L000,
                              grantor.getName().getNameString());
        }
    }

    public void checkGranteeList(OrderedHashSet granteeList) {

        for (int i = 0; i < granteeList.size(); i++) {
            String  name    = (String) granteeList.get(i);
            Grantee grantee = get(name);

            if (grantee == null) {
                throw Error.error(ErrorCode.X_28501, name);
            }

            if (isImmutable(name)) {
                throw Error.error(ErrorCode.X_28502, name);
            }

            if (grantee instanceof User && ((User) grantee).isExternalOnly) {
                throw Error.error(ErrorCode.X_28000, name);
            }
        }
    }

    
    public void grant(String granteeName, String roleName, Grantee grantor) {

        Grantee grantee = get(granteeName);

        if (grantee == null) {
            throw Error.error(ErrorCode.X_28501, granteeName);
        }

        if (isImmutable(granteeName)) {
            throw Error.error(ErrorCode.X_28502, granteeName);
        }

        Grantee role = getRole(roleName);

        if (role == null) {
            throw Error.error(ErrorCode.X_0P000, roleName);
        }

        if (role == grantee) {
            throw Error.error(ErrorCode.X_0P501, granteeName);
        }

        
        
        
        if (role.hasRole(grantee)) {

            

            
            throw Error.error(ErrorCode.X_0P501, roleName);
        }

        if (!grantor.isGrantable(role)) {
            throw Error.error(ErrorCode.X_0L000,
                              grantor.getName().getNameString());
        }

        grantee.grant(role);
        grantee.updateAllRights();

        if (grantee.isRole) {
            updateAllRights(grantee);
        }
    }

    public void checkRoleList(String granteeName, OrderedHashSet roleList,
                              Grantee grantor, boolean grant) {

        Grantee grantee = get(granteeName);

        for (int i = 0; i < roleList.size(); i++) {
            String  roleName = (String) roleList.get(i);
            Grantee role     = getRole(roleName);

            if (role == null) {
                throw Error.error(ErrorCode.X_0P000, roleName);
            }

            if (roleName.equals(SqlInvariants.SYSTEM_AUTHORIZATION_NAME)
                    || roleName.equals(SqlInvariants.PUBLIC_ROLE_NAME)) {
                throw Error.error(ErrorCode.X_28502, roleName);
            }

            if (grant) {
                if (grantee.getDirectRoles().contains(role)) {

                    
                    throw Error.error(ErrorCode.X_0P000, granteeName);
                }
            } else {
                if (!grantee.getDirectRoles().contains(role)) {

                    
                    throw Error.error(ErrorCode.X_0P000, roleName);
                }
            }

            if (!grantor.isAdmin()) {
                throw Error.error(ErrorCode.X_0L000,
                                  grantor.getName().getNameString());
            }
        }
    }

    public void grantSystemToPublic(SchemaObject object, Right right) {
        publicRole.grant(object.getName(), right, systemAuthorisation, true);
    }

    
    public void revoke(String granteeName, String roleName, Grantee grantor) {

        if (!grantor.isAdmin()) {
            throw Error.error(ErrorCode.X_42507);
        }

        Grantee grantee = get(granteeName);

        if (grantee == null) {
            throw Error.error(ErrorCode.X_28000, granteeName);
        }

        Grantee role = (Grantee) roleMap.get(roleName);

        grantee.revoke(role);
        grantee.updateAllRights();

        if (grantee.isRole) {
            updateAllRights(grantee);
        }
    }

    
    public void revoke(OrderedHashSet granteeList, SchemaObject dbObject,
                       Right rights, Grantee grantor, boolean grantOption,
                       boolean cascade) {

        if (dbObject instanceof RoutineSchema) {
            SchemaObject[] routines =
                ((RoutineSchema) dbObject).getSpecificRoutines();

            revoke(granteeList, routines, rights, grantor, grantOption,
                   cascade);

            return;
        }

        HsqlName name = dbObject.getName();

        if (dbObject instanceof Routine) {
            name = ((Routine) dbObject).getSpecificName();
        }

        if (!grantor.isFullyAccessibleByRole(name)) {
            throw Error.error(ErrorCode.X_42501, dbObject.getName().name);
        }

        if (grantor.isAdmin()) {
            grantor = dbObject.getOwner();
        }

        for (int i = 0; i < granteeList.size(); i++) {
            String  granteeName = (String) granteeList.get(i);
            Grantee g           = get(granteeName);

            if (g == null) {
                throw Error.error(ErrorCode.X_28501, granteeName);
            }

            if (isImmutable(granteeName)) {
                throw Error.error(ErrorCode.X_28502, granteeName);
            }
        }

        for (int i = 0; i < granteeList.size(); i++) {
            String  granteeName = (String) granteeList.get(i);
            Grantee g           = get(granteeName);

            g.revoke(dbObject, rights, grantor, grantOption);
            g.updateAllRights();

            if (g.isRole) {
                updateAllRights(g);
            }
        }
    }

    public void revoke(OrderedHashSet granteeList, SchemaObject[] routines,
                       Right rights, Grantee grantor, boolean grantOption,
                       boolean cascade) {

        for (int i = 0; i < routines.length; i++) {
            revoke(granteeList, routines[i], rights, grantor, grantOption,
                   cascade);
        }
    }

    
    void removeEmptyRole(Grantee role) {

        for (int i = 0; i < map.size(); i++) {
            Grantee grantee = (Grantee) map.get(i);

            grantee.roles.remove(role);
        }
    }

    
    public void removeDbObject(HsqlName name) {

        for (int i = 0; i < map.size(); i++) {
            Grantee g = (Grantee) map.get(i);

            g.revokeDbObject(name);
        }
    }

    public void removeDbObjects(OrderedHashSet nameSet) {

        Iterator it = nameSet.iterator();

        while (it.hasNext()) {
            HsqlName name = (HsqlName) it.next();

            for (int i = 0; i < map.size(); i++) {
                Grantee g = (Grantee) map.get(i);

                g.revokeDbObject(name);
            }
        }
    }

    
    void updateAllRights(Grantee role) {

        for (int i = 0; i < map.size(); i++) {
            Grantee grantee = (Grantee) map.get(i);

            if (grantee.isRole) {
                grantee.updateNestedRoles(role);
            }
        }

        for (int i = 0; i < map.size(); i++) {
            Grantee grantee = (Grantee) map.get(i);

            if (!grantee.isRole) {
                grantee.updateAllRights();
            }
        }
    }

    
    public boolean removeGrantee(String name) {

        
        if (isReserved(name)) {
            return false;
        }

        Grantee g = (Grantee) map.remove(name);

        if (g == null) {
            return false;
        }

        g.clearPrivileges();
        updateAllRights(g);

        if (g.isRole) {
            roleMap.remove(name);
            removeEmptyRole(g);
        }

        return true;
    }

    
    public Grantee addRole(HsqlName name) {

        if (map.containsKey(name.name)) {
            throw Error.error(ErrorCode.X_28503, name.name);
        }

        if (SqlInvariants.isLobsSchemaName(name.name)
                || SqlInvariants.isSystemSchemaName(name.name)) {
            throw Error.error(ErrorCode.X_28502, name.name);
        }

        Grantee g = new Grantee(name, this);

        g.isRole = true;

        map.put(name.name, g);
        roleMap.add(name.name, g);

        return g;
    }

    public User addUser(HsqlName name) {

        if (map.containsKey(name.name)) {
            throw Error.error(ErrorCode.X_28503, name.name);
        }

        if (SqlInvariants.isLobsSchemaName(name.name)
                || SqlInvariants.isSystemSchemaName(name.name)) {
            throw Error.error(ErrorCode.X_28502, name.name);
        }

        User g = new User(name, this);

        map.put(name.name, g);

        return g;
    }

    
    boolean isGrantee(String name) {
        return map.containsKey(name);
    }

    public static int getCheckSingleRight(String right) {

        int r = getRight(right);

        if (r != 0) {
            return r;
        }

        throw Error.error(ErrorCode.X_42581, right);
    }

    
    public static int getRight(String right) {
        return rightsStringLookup.get(right, 0);
    }

    public Grantee get(String name) {
        return (Grantee) map.get(name);
    }

    public Collection getGrantees() {
        return map.values();
    }

    public static boolean validRightString(String rightString) {
        return getRight(rightString) != 0;
    }

    public static boolean isImmutable(String name) {

        return name.equals(SqlInvariants.SYSTEM_AUTHORIZATION_NAME)
               || name.equals(SqlInvariants.DBA_ADMIN_ROLE_NAME)
               || name.equals(SqlInvariants.SCHEMA_CREATE_ROLE_NAME)
               || name.equals(SqlInvariants.CHANGE_AUTH_ROLE_NAME);
    }

    public static boolean isReserved(String name) {

        return name.equals(SqlInvariants.SYSTEM_AUTHORIZATION_NAME)
               || name.equals(SqlInvariants.DBA_ADMIN_ROLE_NAME)
               || name.equals(SqlInvariants.SCHEMA_CREATE_ROLE_NAME)
               || name.equals(SqlInvariants.CHANGE_AUTH_ROLE_NAME)
               || name.equals(SqlInvariants.PUBLIC_ROLE_NAME);
    }

    
    public void dropRole(String name) {

        if (!isRole(name)) {
            throw Error.error(ErrorCode.X_0P000, name);
        }

        if (GranteeManager.isReserved(name)) {
            throw Error.error(ErrorCode.X_42507);
        }

        removeGrantee(name);
    }

    public Set getRoleNames() {
        return roleMap.keySet();
    }

    public Collection getRoles() {
        return roleMap.values();
    }

    
    public Grantee getRole(String name) {

        Grantee g = (Grantee) roleMap.get(name);

        if (g == null) {
            throw Error.error(ErrorCode.X_0P000, name);
        }

        return g;
    }

    public boolean isRole(String name) {
        return roleMap.containsKey(name);
    }

    public String[] getSQL() {

        HsqlArrayList list = new HsqlArrayList();

        
        Iterator it = getRoles().iterator();

        while (it.hasNext()) {
            Grantee grantee = (Grantee) it.next();

            
            if (!GranteeManager.isReserved(
                    grantee.getName().getNameString())) {
                list.add(grantee.getSQL());
            }
        }

        
        it = getGrantees().iterator();

        for (; it.hasNext(); ) {
            Grantee grantee = (Grantee) it.next();

            if (grantee instanceof User) {
                if (((User) grantee).isExternalOnly) {
                    continue;
                }

                list.add(grantee.getSQL());

                if (((User) grantee).isLocalOnly) {
                    list.add(((User) grantee).getLocalUserSQL());
                }
            }
        }

        String[] array = new String[list.size()];

        list.toArray(array);

        return array;
    }

    public String[] getRightstSQL() {

        HsqlArrayList list     = new HsqlArrayList();
        Iterator      grantees = getGrantees().iterator();

        while (grantees.hasNext()) {
            Grantee grantee = (Grantee) grantees.next();
            String  name    = grantee.getName().getNameString();

            
            if (GranteeManager.isImmutable(name)) {
                continue;
            }

            if (grantee instanceof User && ((User) grantee).isExternalOnly) {
                continue;
            }

            HsqlArrayList subList = grantee.getRightsSQL();

            list.addAll(subList);
        }

        String[] array = new String[list.size()];

        list.toArray(array);

        return array;
    }
}
