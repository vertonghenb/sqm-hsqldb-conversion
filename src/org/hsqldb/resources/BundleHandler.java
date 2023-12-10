package org.hsqldb.resources;
import java.lang.reflect.Method;
import java.util.Locale;
import java.util.MissingResourceException;
import java.util.ResourceBundle;
import org.hsqldb.lib.HashMap;
import org.hsqldb.lib.HsqlArrayList;
public final class BundleHandler {
    private static final Object mutex = new Object();
    private static Locale locale = Locale.getDefault();
    private static HashMap bundleHandleMap = new HashMap();
    private static HsqlArrayList bundleList = new HsqlArrayList();
    private static final String prefix = "org/hsqldb/resources/";
    private static final Method newGetBundleMethod = getNewGetBundleMethod();
    private BundleHandler() {}
    public static Locale getLocale() {
        synchronized (mutex) {
            return locale;
        }
    }
    public static void setLocale(Locale l) throws IllegalArgumentException {
        synchronized (mutex) {
            if (l == null) {
                throw new IllegalArgumentException("null locale");
            }
            locale = l;
        }
    }
    public static int getBundleHandle(String name, ClassLoader cl) {
        Integer        bundleHandle;
        ResourceBundle bundle;
        String         bundleName;
        String         bundleKey;
        bundleName = prefix + name;
        synchronized (mutex) {
            bundleKey    = locale.toString() + bundleName;
            bundleHandle = (Integer) bundleHandleMap.get(bundleKey);
            if (bundleHandle == null) {
                bundle = getBundle(bundleName, locale, cl);
                bundleList.add(bundle);
                bundleHandle = new Integer(bundleList.size() - 1);
                bundleHandleMap.put(bundleKey, bundleHandle);
            }
        }
        return bundleHandle == null ? -1
                                    : bundleHandle.intValue();
    }
    public static String getString(int handle, String key) {
        ResourceBundle bundle;
        String         s;
        synchronized (mutex) {
            if (handle < 0 || handle >= bundleList.size() || key == null) {
                bundle = null;
            } else {
                bundle = (ResourceBundle) bundleList.get(handle);
            }
        }
        if (bundle == null) {
            s = null;
        } else {
            try {
                s = bundle.getString(key);
            } catch (Exception e) {
                s = null;
            }
        }
        return s;
    }
    private static Method getNewGetBundleMethod() {
        Class   clazz;
        Class[] args;
        clazz = ResourceBundle.class;
        args  = new Class[] {
            String.class, Locale.class, ClassLoader.class
        };
        try {
            return clazz.getMethod("getBundle", args);
        } catch (Exception e) {
            return null;
        }
    }
    public static ResourceBundle getBundle(String name, Locale locale,
                                           ClassLoader cl)
                                           throws NullPointerException,
                                               MissingResourceException {
        if (cl == null) {
            return ResourceBundle.getBundle(name, locale);
        } else if (newGetBundleMethod == null) {
            return ResourceBundle.getBundle(name, locale);
        } else {
            try {
                return (ResourceBundle) newGetBundleMethod.invoke(null,
                        new Object[] {
                    name, locale, cl
                });
            } catch (Exception e) {
                return ResourceBundle.getBundle(name, locale);
            }
        }
    }
}