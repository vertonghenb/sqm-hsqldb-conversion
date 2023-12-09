


package org.hsqldb.lib;

import java.util.Set;
import java.util.HashSet;
import java.util.Enumeration;




public class ValidatingResourceBundle {
    protected boolean validated = false;
    protected Class<? extends Enum<?>> enumType;

    public static final int THROW_BEHAVIOR =
            RefCapablePropertyResourceBundle.THROW_BEHAVIOR;
    public static final int EMPTYSTRING_BEHAVIOR =
            RefCapablePropertyResourceBundle.EMPTYSTRING_BEHAVIOR;
    public static final int NOOP_BEHAVIOR =
            RefCapablePropertyResourceBundle.NOOP_BEHAVIOR;
    

    protected RefCapablePropertyResourceBundle wrappedRCPRB;

    public static String resourceKeyFor(Enum<?> enumKey) {
        return enumKey.name().replace('_', '.');
    }

    public ValidatingResourceBundle(
            String baseName, Class<? extends Enum<?>> enumType) {
        this.enumType = enumType;
        try {
            wrappedRCPRB = RefCapablePropertyResourceBundle.getBundle(baseName,
                    enumType.getClassLoader());
            validate();
        } catch (RuntimeException re) {
            System.err.println("Failed to initialize resource bundle: " + re);
            
            
            throw re;
        }
    }

    

    
    public String getString(Enum<?> key) {
        if (!enumType.isInstance(key))
            throw new IllegalArgumentException(
                    "Key is a " + key.getClass().getName() + ",not a "
                    + enumType.getName() + ":  " + key);
        return wrappedRCPRB.getString(key.toString());
    }

    
    public String getString(Enum<?> key, String... strings) {
        if (!enumType.isInstance(key))
            throw new IllegalArgumentException(
                    "Key is a " + key.getClass().getName() + ",not a "
                    + enumType.getName() + ":  " + key);
        return wrappedRCPRB.getString(
                key.toString(), strings, missingPosValueBehavior);
    }

    
    public String getExpandedString(Enum<?> key) {
        if (!enumType.isInstance(key))
            throw new IllegalArgumentException(
                    "Key is a " + key.getClass().getName() + ",not a "
                    + enumType.getName() + ":  " + key);
        return wrappedRCPRB.getExpandedString(key.toString(), missingPropertyBehavior);
    }

    
    public String getExpandedString(Enum<?> key, String... strings) {
        if (!enumType.isInstance(key))
            throw new IllegalArgumentException(
                    "Key is a " + key.getClass().getName() + ",not a "
                    + enumType.getName() + ":  " + key);
        return wrappedRCPRB.getExpandedString(key.toString(), strings,
                missingPropertyBehavior, missingPosValueBehavior);
    }

    private int missingPropertyBehavior = THROW_BEHAVIOR;
    private int missingPosValueBehavior = THROW_BEHAVIOR;

    
    public void setMissingPropertyBehavior(int missingPropertyBehavior) {
        this.missingPropertyBehavior = missingPropertyBehavior;
    }
    
    public void setMissingPosValueBehavior(int missingPosValueBehavior) {
        this.missingPosValueBehavior = missingPosValueBehavior;
    }

    public int getMissingPropertyBehavior() {
        return missingPropertyBehavior;
    }
    public int getMissingPosValueBehavior() {
        return missingPosValueBehavior;
    }

    public void validate() {
        String val;
        if (validated) return;
        validated = true;
        Set<String> resKeysFromEls = new HashSet<String>();
        for (Enum<?> e : enumType.getEnumConstants())
            resKeysFromEls.add(e.toString());
        Enumeration<String> allKeys = wrappedRCPRB.getKeys();
        while (allKeys.hasMoreElements()) {
            
            
            
            val = allKeys.nextElement();
            wrappedRCPRB.getString(val);  
            
            resKeysFromEls.remove(val);
        }
        if (resKeysFromEls.size() > 0)
            throw new RuntimeException(
                    "Resource Bundle pre-validation failed.  "
                    + "Missing property with key:  " + resKeysFromEls);
    }

    
    public String getString(Enum<?> key, int i1) {
        if (!enumType.isInstance(key))
            throw new IllegalArgumentException(
                    "Key is a " + key.getClass().getName() + ",not a "
                    + enumType.getName() + ":  " + key);
        return getString(key, new String[] {Integer.toString(i1)});
    }
    public String getString(Enum<?> key, int i1, int i2) {
        if (!enumType.isInstance(key))
            throw new IllegalArgumentException(
                    "Key is a " + key.getClass().getName() + ",not a "
                    + enumType.getName() + ":  " + key);
        return getString(key, new String[] {
            Integer.toString(i1), Integer.toString(i2)
        });
    }
    public String getString(Enum<?> key, int i1, int i2, int i3) {
        if (!enumType.isInstance(key))
            throw new IllegalArgumentException(
                    "Key is a " + key.getClass().getName() + ",not a "
                    + enumType.getName() + ":  " + key);
        return getString(key, new String[] {
            Integer.toString(i1), Integer.toString(i2), Integer.toString(i3)
        });
    }
    public String getString(Enum<?> key, int i1, String s2) {
        if (!enumType.isInstance(key))
            throw new IllegalArgumentException(
                    "Key is a " + key.getClass().getName() + ",not a "
                    + enumType.getName() + ":  " + key);
        return getString(key, new String[] {
            Integer.toString(i1), s2
        });
    }
    public String getString(Enum<?> key, String s1, int i2) {
        if (!enumType.isInstance(key))
            throw new IllegalArgumentException(
                    "Key is a " + key.getClass().getName() + ",not a "
                    + enumType.getName() + ":  " + key);
        return getString(key, new String[] {
            s1, Integer.toString(i2)
        });
    }

    public String getString(Enum<?> key, int i1, int i2, String s3) {
        if (!enumType.isInstance(key))
            throw new IllegalArgumentException(
                    "Key is a " + key.getClass().getName() + ",not a "
                    + enumType.getName() + ":  " + key);
        return getString(key, new String[] {
            Integer.toString(i1), Integer.toString(i2), s3
        });
    }
    public String getString(Enum<?> key, int i1, String s2, int i3) {
        if (!enumType.isInstance(key))
            throw new IllegalArgumentException(
                    "Key is a " + key.getClass().getName() + ",not a "
                    + enumType.getName() + ":  " + key);
        return getString(key, new String[] {
            Integer.toString(i1), s2, Integer.toString(i3)
        });
    }
    public String getString(Enum<?> key, String s1, int i2, int i3) {
        if (!enumType.isInstance(key))
            throw new IllegalArgumentException(
                    "Key is a " + key.getClass().getName() + ",not a "
                    + enumType.getName() + ":  " + key);
        return getString(key, new String[] {
            s1, Integer.toString(i2), Integer.toString(i3)
        });
    }
    public String getString(Enum<?> key, int i1, String s2, String s3) {
        if (!enumType.isInstance(key))
            throw new IllegalArgumentException(
                    "Key is a " + key.getClass().getName() + ",not a "
                    + enumType.getName() + ":  " + key);
        return getString(key, new String[] {
            Integer.toString(i1), s2, s3
        });
    }
    public String getString(Enum<?> key, String s1, String s2, int i3) {
        if (!enumType.isInstance(key))
            throw new IllegalArgumentException(
                    "Key is a " + key.getClass().getName() + ",not a "
                    + enumType.getName() + ":  " + key);
        return getString(key, new String[] {
            s1, s2, Integer.toString(i3)
        });
    }
    public String getString(Enum<?> key, String s1, int i2, String s3) {
        if (!enumType.isInstance(key))
            throw new IllegalArgumentException(
                    "Key is a " + key.getClass().getName() + ",not a "
                    + enumType.getName() + ":  " + key);
        return getString(key, new String[] {
            s1, Integer.toString(i2), s3
        });
    }
}
