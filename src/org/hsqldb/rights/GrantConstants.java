package org.hsqldb.rights;
public interface GrantConstants {
    int SELECT = 1 << 0;
    int DELETE = 1 << 1;
    int INSERT = 1 << 2;
    int UPDATE = 1 << 3;
    int USAGE = 1 << 4;
    int EXECUTE = 1 << 5;
    int REFERENCES = 1 << 6;
    int TRIGGER = 1 << 7;
    int ALL = SELECT | DELETE | INSERT | UPDATE | USAGE | EXECUTE;
}