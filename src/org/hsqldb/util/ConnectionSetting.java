


package org.hsqldb.util;


public class ConnectionSetting implements java.io.Serializable {

    private String name, driver, url, user, pw;

    String getName() {
        return name;
    }

    String getDriver() {
        return driver;
    }

    String getUrl() {
        return url;
    }

    String getUser() {
        return user;
    }

    String getPassword() {
        return pw;
    }

    
    private ConnectionSetting() {}
    ;

    ConnectionSetting(String name, String driver, String url, String user,
                      String pw) {

        this.name   = name;
        this.driver = driver;
        this.url    = url;
        this.user   = user;
        this.pw     = pw;
    }

    public boolean equals(Object obj) {

        if (!(obj instanceof ConnectionSetting)) {
            return false;
        }

        ConnectionSetting other = (ConnectionSetting) obj;

        if (getName() == other.getName()) {
            return true;
        }

        if (getName() == null) {
            return false;
        }

        return getName().trim().equals(other.getName().trim());
    }

    public int hashCode() {
        return getName() == null ? 0
                                 : getName().trim().hashCode();
    }
}
