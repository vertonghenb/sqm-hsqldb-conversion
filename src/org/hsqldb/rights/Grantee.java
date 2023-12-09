


package org.hsqldb.rights;

import org.hsqldb.HsqlNameManager.HsqlName;
import org.hsqldb.NumberSequence;
import org.hsqldb.Routine;
import org.hsqldb.SchemaObject;
import org.hsqldb.Session;
import org.hsqldb.Table;
import org.hsqldb.Tokens;
import org.hsqldb.error.Error;
import org.hsqldb.error.ErrorCode;
import org.hsqldb.lib.HashMap;
import org.hsqldb.lib.HashSet;
import org.hsqldb.lib.HsqlArrayList;
import org.hsqldb.lib.Iterator;
import org.hsqldb.lib.MultiValueHashMap;
import org.hsqldb.lib.OrderedHashSet;
import org.hsqldb.lib.Set;
import org.hsqldb.types.Type;


public class Grantee implements SchemaObject {

    boolean isRole;

    
    private boolean isAdminDirect = false;

    
    private boolean isAdmin = false;

    
    boolean isSchemaCreator = false;

    
    boolean isPublic = false;

    
    boolean isSystem = false;

    
    protected HsqlName granteeName;

    
    private MultiValueHashMap directRightsMap;

    
    HashMap fullRightsMap;

    
    OrderedHashSet roles;

    
    private MultiValueHashMap grantedRightsMap;

    
    protected GranteeManager granteeManager;

    
    protected Right ownerRights;

    
    Grantee(HsqlName name, GranteeManager man) {

        fullRightsMap       = new HashMap();
        directRightsMap     = new MultiValueHashMap();
        grantedRightsMap    = new MultiValueHashMap();
        granteeName         = name;
        granteeManager      = man;
        roles               = new OrderedHashSet();
        ownerRights         = new Right();
        ownerRights.isFull  = true;
        ownerRights.grantor = GranteeManager.systemAuthorisation;
        ownerRights.grantee = this;
    }

    public int getType() {
        return SchemaObject.GRANTEE;
    }

    public HsqlName getName() {
        return granteeName;
    }

    public HsqlName getSchemaName() {
        return null;
    }

    public HsqlName getCatalogName() {
        return null;
    }

    public Grantee getOwner() {
        return null;
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

        sb.append(Tokens.T_CREATE).append(' ').append(Tokens.T_ROLE);
        sb.append(' ').append(granteeName.statementName);

        return sb.toString();
    }

    public long getChangeTimestamp() {
        return 0;
    }

    public boolean isRole() {
        return isRole;
    }

    public boolean isSystem() {
        return isSystem;
    }

    
    public OrderedHashSet getDirectRoles() {
        return roles;
    }

    
    public OrderedHashSet getAllRoles() {

        OrderedHashSet set = getGranteeAndAllRoles();

        
        set.remove(this);

        return set;
    }

    public OrderedHashSet getGranteeAndAllRoles() {

        OrderedHashSet set = new OrderedHashSet();

        addGranteeAndRoles(set);

        return set;
    }

    public OrderedHashSet getGranteeAndAllRolesWithPublic() {

        OrderedHashSet set = new OrderedHashSet();

        addGranteeAndRoles(set);
        set.add(granteeManager.publicRole);

        return set;
    }

    public boolean isAccessible(HsqlName name, int privilegeType) {

        if (isFullyAccessibleByRole(name)) {
            return true;
        }

        Right right = (Right) fullRightsMap.get(name);

        if (right == null) {
            return false;
        }

        return right.canAccess(privilegeType);
    }

    
    public boolean isAccessible(SchemaObject object) {
        return isAccessible(object.getName());
    }

    public boolean isAccessible(HsqlName name) {

        if (isFullyAccessibleByRole(name)) {
            return true;
        }

        Right right = (Right) fullRightsMap.get(name);

        if (right != null && !right.isEmpty()) {
            return true;
        }

        if (!isPublic) {
            return granteeManager.publicRole.isAccessible(name);
        }

        return false;
    }

    
    private OrderedHashSet addGranteeAndRoles(OrderedHashSet set) {

        Grantee candidateRole;

        set.add(this);

        for (int i = 0; i < roles.size(); i++) {
            candidateRole = (Grantee) roles.get(i);

            if (!set.contains(candidateRole)) {
                candidateRole.addGranteeAndRoles(set);
            }
        }

        return set;
    }

    private boolean hasRoleDirect(Grantee role) {
        return roles.contains(role);
    }

    public boolean hasRole(Grantee role) {
        return getAllRoles().contains(role);
    }

    
    void grant(HsqlName name, Right right, Grantee grantor,
               boolean withGrant) {

        final Right grantableRights =
            ((Grantee) grantor).getAllGrantableRights(name);
        Right existingRight = null;

        if (right == Right.fullRights) {
            if (grantableRights.isEmpty()) {
                return;    
            }

            right = grantableRights;
        } else {
            if (!grantableRights.contains(right)) {
                throw Error.error(ErrorCode.X_0L000);
            }
        }

        Iterator it = directRightsMap.get(name);

        while (it.hasNext()) {
            Right existing = (Right) it.next();

            if (existing.grantor == grantor) {
                existingRight = existing;

                existingRight.add(right);

                break;
            }
        }

        if (existingRight == null) {
            existingRight         = right.duplicate();
            existingRight.grantor = grantor;
            existingRight.grantee = this;

            directRightsMap.put(name, existingRight);
        }

        if (withGrant) {
            if (existingRight.grantableRights == null) {
                existingRight.grantableRights = right.duplicate();
            } else {
                existingRight.grantableRights.add(right);
            }
        }

        if (!grantor.isSystem()) {

            
            ((Grantee) grantor).grantedRightsMap.put(name, existingRight);
        }

        updateAllRights();
    }

    
    void revoke(SchemaObject object, Right right, Grantee grantor,
                boolean grantOption) {

        HsqlName name = object.getName();

        if (object instanceof Routine) {
            name = ((Routine) object).getSpecificName();
        }

        Iterator it       = directRightsMap.get(name);
        Right    existing = null;

        while (it.hasNext()) {
            existing = (Right) it.next();

            if (existing.grantor == grantor) {
                break;
            }
        }

        if (existing == null) {
            return;
        }

        if (existing.grantableRights != null) {
            existing.grantableRights.remove(object, right);
        }

        if (grantOption) {
            return;
        }

        if (right.isFull) {
            directRightsMap.remove(name, existing);
            ((Grantee) grantor).grantedRightsMap.remove(name, existing);
            updateAllRights();

            return;
        }

        existing.remove(object, right);

        if (existing.isEmpty()) {
            directRightsMap.remove(name, existing);
            ((Grantee) grantor).grantedRightsMap.remove(name, existing);
        }

        updateAllRights();

        return;
    }

    
    void revokeDbObject(HsqlName name) {

        directRightsMap.remove(name);
        grantedRightsMap.remove(name);
        fullRightsMap.remove(name);
    }

    
    void clearPrivileges() {

        roles.clear();
        directRightsMap.clear();
        grantedRightsMap.clear();
        fullRightsMap.clear();

        isAdmin = false;
    }

    public OrderedHashSet getColumnsForAllPrivileges(SchemaObject object) {

        if (object instanceof Table) {
            Table table = (Table) object;

            if (isFullyAccessibleByRole(table.getName())) {
                return table.getColumnNameSet();
            }

            Right right = (Right) fullRightsMap.get(table.getName());

            return right == null ? Right.emptySet
                                 : right.getColumnsForAllRights(table);
        }

        return Right.emptySet;
    }

    public OrderedHashSet getAllDirectPrivileges(SchemaObject object) {

        if (object.getOwner() == this) {
            OrderedHashSet set = new OrderedHashSet();

            set.add(ownerRights);

            return set;
        }

        HsqlName name = object.getName();

        if (object instanceof Routine) {
            name = ((Routine) object).getSpecificName();
        }

        Iterator rights = directRightsMap.get(name);

        if (rights.hasNext()) {
            OrderedHashSet set = new OrderedHashSet();

            while (rights.hasNext()) {
                set.add(rights.next());
            }

            return set;
        }

        return Right.emptySet;
    }

    public OrderedHashSet getAllGrantedPrivileges(SchemaObject object) {

        HsqlName name = object.getName();

        if (object instanceof Routine) {
            name = ((Routine) object).getSpecificName();
        }

        Iterator rights = grantedRightsMap.get(name);

        if (rights.hasNext()) {
            OrderedHashSet set = new OrderedHashSet();

            while (rights.hasNext()) {
                set.add(rights.next());
            }

            return set;
        }

        return Right.emptySet;
    }

    
    public void checkSelect(SchemaObject object, boolean[] checkList) {

        if (object instanceof Table) {
            Table table = (Table) object;

            if (isFullyAccessibleByRole(table.getName())) {
                return;
            }

            Right right = (Right) fullRightsMap.get(table.getName());

            if (right != null && right.canSelect(table, checkList)) {
                return;
            }
        }

        throw Error.error(ErrorCode.X_42501, object.getName().name);
    }

    public void checkInsert(SchemaObject object, boolean[] checkList) {

        if (object instanceof Table) {
            Table table = (Table) object;

            if (isFullyAccessibleByRole(table.getName())) {
                return;
            }

            Right right = (Right) fullRightsMap.get(table.getName());

            if (right != null && right.canInsert(table, checkList)) {
                return;
            }
        }

        throw Error.error(ErrorCode.X_42501, object.getName().name);
    }

    public void checkUpdate(SchemaObject object, boolean[] checkList) {

        if (object instanceof Table) {
            Table table = (Table) object;

            if (isFullyAccessibleByRole(table.getName())) {
                return;
            }

            Right right = (Right) fullRightsMap.get(table.getName());

            if (right != null && right.canUpdate(table, checkList)) {
                return;
            }
        }

        throw Error.error(ErrorCode.X_42501, object.getName().name);
    }

    public void checkReferences(SchemaObject object, boolean[] checkList) {

        if (object instanceof Table) {
            Table table = (Table) object;

            if (isFullyAccessibleByRole(table.getName())) {
                return;
            }

            Right right = (Right) fullRightsMap.get(table.getName());

            if (right != null && right.canReference(table, checkList)) {
                return;
            }
        }

        throw Error.error(ErrorCode.X_42501, object.getName().name);
    }

    public void checkTrigger(SchemaObject object, boolean[] checkList) {

        if (object instanceof Table) {
            Table table = (Table) object;

            if (isFullyAccessibleByRole(table.getName())) {
                return;
            }

            Right right = (Right) fullRightsMap.get(table.getName());

            if (right != null && right.canReference(table, checkList)) {
                return;
            }
        }

        throw Error.error(ErrorCode.X_42501, object.getName().name);
    }

    public void checkDelete(SchemaObject object) {

        if (object instanceof Table) {
            Table table = (Table) object;

            if (isFullyAccessibleByRole(table.getName())) {
                return;
            }

            Right right = (Right) fullRightsMap.get(table.getName());

            if (right != null && right.canDelete()) {
                return;
            }
        }

        throw Error.error(ErrorCode.X_42501, object.getName().name);
    }

    public void checkAccess(SchemaObject object) {

        if (isFullyAccessibleByRole(object.getName())) {
            return;
        }

        HsqlName name = object.getName();

        if (object instanceof Routine) {
            name = ((Routine) object).getSpecificName();
        }

        Right right = (Right) fullRightsMap.get(name);

        if (right != null && !right.isEmpty()) {
            return;
        }

        throw Error.error(ErrorCode.X_42501, object.getName().name);
    }

    
    public void checkSchemaUpdateOrGrantRights(String schemaName) {

        if (!hasSchemaUpdateOrGrantRights(schemaName)) {
            throw Error.error(ErrorCode.X_42501, schemaName);
        }
    }

    
    public boolean hasSchemaUpdateOrGrantRights(String schemaName) {

        
        if (isAdmin()) {
            return true;
        }

        Grantee schemaOwner =
            granteeManager.database.schemaManager.toSchemaOwner(schemaName);

        
        if (schemaOwner == this) {
            return true;
        }

        
        if (hasRole(schemaOwner)) {
            return true;
        }

        return false;
    }

    public boolean isGrantable(SchemaObject object, Right right) {

        if (isFullyAccessibleByRole(object.getName())) {
            return true;
        }

        Right grantableRights = getAllGrantableRights(object.getName());

        return grantableRights.contains(right);
    }

    public boolean isGrantable(Grantee role) {
        return isAdmin;
    }

    public boolean isFullyAccessibleByRole(HsqlName name) {

        Grantee owner;

        if (isAdmin) {
            return true;
        }

        if (name.type == SchemaObject.SCHEMA) {
            owner = name.owner;
        } else if (name.schema == null) {
            return false;
        } else {
            owner = name.schema.owner;
        }

        if (owner == this) {
            return true;
        }

        if (hasRole(owner)) {
            return true;
        }

        return false;
    }

    
    public void checkAdmin() {

        if (!isAdmin()) {
            throw Error.error(ErrorCode.X_42507);
        }
    }

    
    public boolean isAdmin() {
        return isAdmin;
    }

    
    public boolean isSchemaCreator() {
        return isAdmin || hasRole(granteeManager.schemaRole);
    }

    
    public boolean canChangeAuthorisation() {
        return isAdmin || hasRole(granteeManager.changeAuthRole);
    }

    
    public boolean isPublic() {
        return isPublic;
    }

    
    public OrderedHashSet visibleGrantees() {

        OrderedHashSet grantees = new OrderedHashSet();
        GranteeManager gm       = granteeManager;

        if (isAdmin()) {
            grantees.addAll(gm.getGrantees());
        } else {
            grantees.add(this);

            Iterator it = getAllRoles().iterator();

            while (it.hasNext()) {
                grantees.add(it.next());
            }
        }

        return grantees;
    }

    public boolean hasNonSelectTableRight(SchemaObject table) {

        if (isFullyAccessibleByRole(table.getName())) {
            return true;
        }

        Right right = (Right) fullRightsMap.get(table.getName());

        if (right == null) {
            return false;
        }

        return right.canAcesssNonSelect();
    }

    public boolean hasColumnRights(SchemaObject table, int[] columnMap) {

        if (isFullyAccessibleByRole(table.getName())) {
            return true;
        }

        Right right = (Right) fullRightsMap.get(table.getName());

        if (right == null) {
            return false;
        }

        return right.canAccess((Table) table, columnMap);
    }

    
    void setAdminDirect() {
        isAdmin = isAdminDirect = true;
    }

    
    boolean updateNestedRoles(Grantee role) {

        boolean hasNested = false;

        if (role != this) {
            for (int i = 0; i < roles.size(); i++) {
                Grantee currentRole = (Grantee) roles.get(i);

                hasNested |= currentRole.updateNestedRoles(role);
            }
        }

        if (hasNested) {
            updateAllRights();
        }

        return hasNested || role == this;
    }

    

    
    void updateAllRights() {

        fullRightsMap.clear();

        isAdmin = isAdminDirect;

        for (int i = 0; i < roles.size(); i++) {
            Grantee currentRole = (Grantee) roles.get(i);

            addToFullRights(currentRole.fullRightsMap);

            isAdmin |= currentRole.isAdmin();
        }

        addToFullRights(directRightsMap);

        if (!isRole && !isPublic && !isSystem) {
            addToFullRights(granteeManager.publicRole.fullRightsMap);
        }
    }

    
    void addToFullRights(HashMap map) {

        Iterator it = map.keySet().iterator();

        while (it.hasNext()) {
            Object key      = it.next();
            Right  add      = (Right) map.get(key);
            Right  existing = (Right) fullRightsMap.get(key);

            if (existing == null) {
                existing = add.duplicate();

                fullRightsMap.put(key, existing);
            } else {
                existing.add(add);
            }

            if (add.grantableRights == null) {
                continue;
            }

            if (existing.grantableRights == null) {
                existing.grantableRights = add.grantableRights.duplicate();
            } else {
                existing.grantableRights.add(add.grantableRights);
            }
        }
    }

    
    private void addToFullRights(MultiValueHashMap map) {

        Iterator it = map.keySet().iterator();

        while (it.hasNext()) {
            Object   key      = it.next();
            Iterator values   = map.get(key);
            Right    existing = (Right) fullRightsMap.get(key);

            while (values.hasNext()) {
                Right add = (Right) values.next();

                if (existing == null) {
                    existing = add.duplicate();

                    fullRightsMap.put(key, existing);
                } else {
                    existing.add(add);
                }

                if (add.grantableRights == null) {
                    continue;
                }

                if (existing.grantableRights == null) {
                    existing.grantableRights = add.grantableRights.duplicate();
                } else {
                    existing.grantableRights.add(add.grantableRights);
                }
            }
        }
    }

    Right getAllGrantableRights(HsqlName name) {

        if (isAdmin) {
            return ((Grantee) name.schema.owner).ownerRights;
        }

        if (name.schema.owner == this) {
            return ownerRights;
        }

        if (roles.contains(name.schema.owner)) {
            return ((Grantee) name.schema.owner).ownerRights;
        }

        OrderedHashSet set = getAllRoles();

        for (int i = 0; i < set.size(); i++) {
            Grantee role = (Grantee) set.get(i);

            if (name.schema.owner == role) {
                return role.ownerRights;
            }
        }

        Right right = (Right) fullRightsMap.get(name);

        return right == null || right.grantableRights == null ? Right.noRights
                                                              : right
                                                              .grantableRights;
    }

    
    private MultiValueHashMap getRights() {

        
        return directRightsMap;
    }

    
    void grant(Grantee role) {
        roles.add(role);
    }

    
    void revoke(Grantee role) {

        if (!hasRoleDirect(role)) {
            throw Error.error(ErrorCode.X_0P503,
                              role.getName().getNameString());
        }

        roles.remove(role);
    }

    private String roleMapToString(OrderedHashSet roles) {

        StringBuffer sb = new StringBuffer();

        for (int i = 0; i < roles.size(); i++) {
            if (sb.length() > 0) {
                sb.append(',');
            }

            Grantee role = (Grantee) roles.get(i);

            sb.append(role.getName().getStatementName());
        }

        return sb.toString();
    }

    HsqlArrayList getRightsSQL() {

        HsqlArrayList list       = new HsqlArrayList();
        String        roleString = roleMapToString(roles);

        if (roleString.length() != 0) {
            StringBuffer sb = new StringBuffer(128);

            sb.append(Tokens.T_GRANT).append(' ').append(roleString);
            sb.append(' ').append(Tokens.T_TO).append(' ');
            sb.append(getName().getStatementName());
            list.add(sb.toString());
        }

        MultiValueHashMap rightsMap = getRights();
        Iterator          dbObjects = rightsMap.keySet().iterator();

        while (dbObjects.hasNext()) {
            Object   nameObject = dbObjects.next();
            Iterator rights     = rightsMap.get(nameObject);

            while (rights.hasNext()) {
                Right        right    = (Right) rights.next();
                StringBuffer sb       = new StringBuffer(128);
                HsqlName     hsqlname = (HsqlName) nameObject;

                switch (hsqlname.type) {

                    case SchemaObject.TABLE :
                    case SchemaObject.VIEW :
                        Table table =
                            granteeManager.database.schemaManager
                                .findUserTable(null, hsqlname.name,
                                               hsqlname.schema.name);

                        if (table != null) {
                            sb.append(Tokens.T_GRANT).append(' ');
                            sb.append(right.getTableRightsSQL(table));
                            sb.append(' ').append(Tokens.T_ON).append(' ');
                            sb.append(Tokens.T_TABLE).append(' ');
                            sb.append(
                                hsqlname.getSchemaQualifiedStatementName());
                        }
                        break;

                    case SchemaObject.SEQUENCE :
                        NumberSequence sequence =
                            (NumberSequence) granteeManager.database
                                .schemaManager
                                .findSchemaObject(hsqlname.name,
                                                  hsqlname.schema.name,
                                                  SchemaObject.SEQUENCE);

                        if (sequence != null) {
                            sb.append(Tokens.T_GRANT).append(' ');
                            sb.append(Tokens.T_USAGE);
                            sb.append(' ').append(Tokens.T_ON).append(' ');
                            sb.append(Tokens.T_SEQUENCE).append(' ');
                            sb.append(
                                hsqlname.getSchemaQualifiedStatementName());
                        }
                        break;

                    case SchemaObject.DOMAIN :
                        Type domain =
                            (Type) granteeManager.database.schemaManager
                                .findSchemaObject(hsqlname.name,
                                                  hsqlname.schema.name,
                                                  SchemaObject.DOMAIN);

                        if (domain != null) {
                            sb.append(Tokens.T_GRANT).append(' ');
                            sb.append(Tokens.T_USAGE);
                            sb.append(' ').append(Tokens.T_ON).append(' ');
                            sb.append(Tokens.T_DOMAIN).append(' ');
                            sb.append(
                                hsqlname.getSchemaQualifiedStatementName());
                        }
                        break;

                    case SchemaObject.TYPE :
                        Type type =
                            (Type) granteeManager.database.schemaManager
                                .findSchemaObject(hsqlname.name,
                                                  hsqlname.schema.name,
                                                  SchemaObject.DOMAIN);

                        if (type != null) {
                            sb.append(Tokens.T_GRANT).append(' ');
                            sb.append(Tokens.T_USAGE);
                            sb.append(' ').append(Tokens.T_ON).append(' ');
                            sb.append(Tokens.T_TYPE).append(' ');
                            sb.append(
                                hsqlname.getSchemaQualifiedStatementName());
                        }
                        break;

                    case SchemaObject.PROCEDURE :
                    case SchemaObject.FUNCTION :
                    case SchemaObject.SPECIFIC_ROUTINE :
                        SchemaObject routine =
                            (SchemaObject) granteeManager.database
                                .schemaManager
                                .findSchemaObject(hsqlname.name,
                                                  hsqlname.schema.name,
                                                  hsqlname.type);

                        if (routine != null) {
                            sb.append(Tokens.T_GRANT).append(' ');
                            sb.append(Tokens.T_EXECUTE).append(' ');
                            sb.append(Tokens.T_ON).append(' ');
                            sb.append(Tokens.T_SPECIFIC).append(' ');

                            if (routine.getType() == SchemaObject.PROCEDURE) {
                                sb.append(Tokens.T_PROCEDURE);
                            } else {
                                sb.append(Tokens.T_FUNCTION);
                            }

                            sb.append(' ');
                            sb.append(
                                hsqlname.getSchemaQualifiedStatementName());
                        }
                        break;
                }

                if (sb.length() == 0) {
                    continue;
                }

                sb.append(' ').append(Tokens.T_TO).append(' ');
                sb.append(getName().getStatementName());
                list.add(sb.toString());
            }
        }

        return list;
    }
}
