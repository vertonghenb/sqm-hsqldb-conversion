


package org.hsqldb.auth;


public interface AuthFunctionBean {
    
    public String[] authenticate(
            String userName, String password) throws Exception;
}
