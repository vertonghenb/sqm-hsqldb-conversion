package org.hsqldb.test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.hsqldb.server.ServerAcl;
import junit.framework.Test;
import java.io.IOException;
import java.io.File;
import java.io.PrintWriter;
import java.io.FileWriter;
import java.net.InetAddress;
import java.util.List;
import java.util.ArrayList;
public class TestAcl extends junit.framework.TestCase {
    private ServerAcl   aclDefault          = null;
    private ServerAcl[] aclPermitLocalhosts = null;
    private ServerAcl[] aclPermitLocalNets  = null;
    private ServerAcl[] aclDenyLocalNets    = null;
    private ServerAcl[] aclDenyLocalhosts   = null;
    private ServerAcl[] aclPermitAlls       = null;
    private ServerAcl[] aclDenyAlls         = null;
    private InetAddress localhostByName = InetAddress.getByName("localhost");
    private InetAddress localhostByAddr = InetAddress.getByAddress(new byte[] {
        127, 0, 0, 1
    });
    private InetAddress otherHostByAddr = InetAddress.getByAddress(new byte[] {
        1, 2, 3, 4
    });
    public TestAcl() throws IOException, ServerAcl.AclFormatException {
        commonSetup();
    }
    public TestAcl(String s) throws IOException, ServerAcl.AclFormatException {
        super(s);
        commonSetup();
    }
    private void commonSetup()
    throws IOException, ServerAcl.AclFormatException {
        boolean     verbose = System.getProperty("VERBOSE") != null;
        File        file;
        PrintWriter pw;
        List        acls = new ArrayList();
        file = File.createTempFile("zero", ".txt");
        file.deleteOnExit();
        (new FileWriter(file)).close();
        aclDefault = new ServerAcl(file);
        if (verbose) {
            aclDefault.setPrintWriter(new PrintWriter(System.out));
        }
        acls.clear();
        file = File.createTempFile("aclDenyAll1", ".txt");
        file.deleteOnExit();
        pw = new PrintWriter(new FileWriter(file));
        pw.println("# Deny all test ACL\n");
        pw.println("deny 0.0.0.0/0");
        pw.close();
        acls.add(new ServerAcl(file));
        aclDenyAlls = (ServerAcl[]) acls.toArray(new ServerAcl[0]);
        if (verbose) {
            for (int i = 0; i < aclDenyAlls.length; i++) {
                aclDenyAlls[i].setPrintWriter(new PrintWriter(System.out));
            }
        }
        acls.clear();
        file = File.createTempFile("aclPermitLocalhost1", ".txt");
        file.deleteOnExit();
        pw = new PrintWriter(new FileWriter(file));
        pw.println("# Permit Localhost test ACL\n");
        pw.println("permit 127.0.0.1");
        pw.close();
        acls.add(new ServerAcl(file));
        file = File.createTempFile("aclPermitLocalhost2", ".txt");
        file.deleteOnExit();
        pw = new PrintWriter(new FileWriter(file));
        pw.println("# Permit Localhost test ACL\n");
        pw.println("permit 127.0.0.1/32");
        pw.close();
        acls.add(new ServerAcl(file));
        aclPermitLocalhosts = (ServerAcl[]) acls.toArray(new ServerAcl[0]);
        if (verbose) {
            for (int i = 0; i < aclPermitLocalhosts.length; i++) {
                aclPermitLocalhosts[i].setPrintWriter(
                    new PrintWriter(System.out));
            }
        }
        acls.clear();
        file = File.createTempFile("aclPermitLocalNet1", ".txt");
        file.deleteOnExit();
        pw = new PrintWriter(new FileWriter(file));
        pw.println("# Permit Local Net test ACL\n");
        pw.println("permit 127.0.0.0/24");
        pw.close();
        acls.add(new ServerAcl(file));
        aclPermitLocalNets = (ServerAcl[]) acls.toArray(new ServerAcl[0]);
        if (verbose) {
            for (int i = 0; i < aclPermitLocalNets.length; i++) {
                aclPermitLocalNets[i].setPrintWriter(
                    new PrintWriter(System.out));
            }
        }
        acls.clear();
        file = File.createTempFile("aclDenyLocalNet1", ".txt");
        file.deleteOnExit();
        pw = new PrintWriter(new FileWriter(file));
        pw.println("# Deny Local Net test ACL\n");
        pw.println("deny 127.0.0.0/24");
        pw.println("allow 0.0.0.0/0");
        pw.close();
        acls.add(new ServerAcl(file));
        aclDenyLocalNets = (ServerAcl[]) acls.toArray(new ServerAcl[0]);
        if (verbose) {
            for (int i = 0; i < aclDenyLocalNets.length; i++) {
                aclDenyLocalNets[i].setPrintWriter(
                    new PrintWriter(System.out));
            }
        }
        acls.clear();
        file = File.createTempFile("aclDenyLocalhost1", ".txt");
        file.deleteOnExit();
        pw = new PrintWriter(new FileWriter(file));
        pw.println("# Deny Localhost test ACL\n");
        pw.println("deny 127.0.0.1/32");
        pw.println("allow 0.0.0.0/0");
        pw.close();
        acls.add(new ServerAcl(file));
        aclDenyLocalhosts = (ServerAcl[]) acls.toArray(new ServerAcl[0]);
        if (verbose) {
            for (int i = 0; i < aclDenyLocalhosts.length; i++) {
                aclDenyLocalhosts[i].setPrintWriter(
                    new PrintWriter(System.out));
            }
        }
        acls.clear();
        file = File.createTempFile("aclPermitAll1", ".txt");
        file.deleteOnExit();
        pw = new PrintWriter(new FileWriter(file));
        pw.println("# Permit all test ACL\n");
        pw.println("permit 0.0.0.0/0");
        pw.close();
        acls.add(new ServerAcl(file));
        aclPermitAlls = (ServerAcl[]) acls.toArray(new ServerAcl[0]);
        if (verbose) {
            for (int i = 0; i < aclPermitAlls.length; i++) {
                aclPermitAlls[i].setPrintWriter(new PrintWriter(System.out));
            }
        }
    }
    public static void main(String[] sa) {
            junit.textui.TestRunner runner = new junit.textui.TestRunner();
            junit.framework.TestResult result =
                runner.run(runner.getTest(TestAcl.class.getName()));
            System.exit(result.wasSuccessful() ? 0 : 1);
    }
    public void testDefaultWithNames() {
        assertFalse("Permitting access from localhost with default ACL",
                    aclDefault.permitAccess(localhostByName.getAddress()));
    }
    public void testDefaultWithIPs() {
        assertFalse("Permitting access from localhost with default ACL",
                    aclDefault.permitAccess(localhostByAddr.getAddress()));
        assertFalse("Permitting access from other host with default ACL",
                    aclDefault.permitAccess(otherHostByAddr.getAddress()));
    }
    public void testDenyAllWithNames() {
        ServerAcl acl;
        for (int i = 0; i < aclDenyAlls.length; i++) {
            acl = (ServerAcl) aclDenyAlls[i];
            assertFalse("Permitting access from localhost with deny-all ACL",
                        acl.permitAccess(localhostByName.getAddress()));
        }
    }
    public void testDenyAllWithIPs() {
        ServerAcl acl;
        for (int i = 0; i < aclDenyAlls.length; i++) {
            acl = (ServerAcl) aclDenyAlls[i];
            assertFalse("Permitting access from localhost with deny-all ACL",
                        acl.permitAccess(localhostByAddr.getAddress()));
            assertFalse("Permitting access from other host with deny-all ACL",
                        acl.permitAccess(otherHostByAddr.getAddress()));
        }
    }
    public void testLocalhostOnlyWithNames() {
        ServerAcl acl;
        for (int i = 0; i < aclPermitLocalhosts.length; i++) {
            acl = (ServerAcl) aclPermitLocalhosts[i];
            assertTrue(
                "Denying access from localhost with localhost-permit ACL",
                acl.permitAccess(localhostByName.getAddress()));
        }
    }
    public void testLocalhostOnlyWithIPs() {
        ServerAcl acl;
        for (int i = 0; i < aclPermitLocalhosts.length; i++) {
            acl = (ServerAcl) aclPermitLocalhosts[i];
            assertTrue(
                "Denying access from localhost with localhost-permit ACL",
                acl.permitAccess(localhostByAddr.getAddress()));
            assertFalse(
                "Permitting access from other host with localhost-permit ACL",
                acl.permitAccess(otherHostByAddr.getAddress()));
        }
    }
    public void testNoLocalhostOnlyWithNames() {
        ServerAcl acl;
        for (int i = 0; i < aclDenyLocalhosts.length; i++) {
            acl = (ServerAcl) aclDenyLocalhosts[i];
            assertFalse(
                "Permitting access from localhost with localhost-deny ACL",
                acl.permitAccess(localhostByName.getAddress()));
        }
    }
    public void testNoLocalhostOnlyWithIPs() {
        ServerAcl acl;
        for (int i = 0; i < aclDenyLocalhosts.length; i++) {
            acl = (ServerAcl) aclDenyLocalhosts[i];
            assertFalse(
                "Permitting access from localhost with localhost-deny ACL",
                acl.permitAccess(localhostByAddr.getAddress()));
            assertTrue(
                "Denying access from other host with localhost-deny ACL",
                acl.permitAccess(otherHostByAddr.getAddress()));
        }
    }
    public void testLocalNetOnlyWithNames() {
        ServerAcl acl;
        for (int i = 0; i < aclPermitLocalNets.length; i++) {
            acl = (ServerAcl) aclPermitLocalNets[i];
            assertTrue("Denying access from localNet with localNet-permit ACL",
                       acl.permitAccess(localhostByName.getAddress()));
        }
    }
    public void testLocalNetOnlyWithIPs() {
        ServerAcl acl;
        for (int i = 0; i < aclPermitLocalNets.length; i++) {
            acl = (ServerAcl) aclPermitLocalNets[i];
            assertTrue("Denying access from localNet with localNet-permit ACL",
                       acl.permitAccess(localhostByAddr.getAddress()));
            assertFalse(
                "Permitting access from other Net with localNet-permit ACL",
                acl.permitAccess(otherHostByAddr.getAddress()));
        }
    }
    public void testNoLocalNetOnlyWithNames() {
        ServerAcl acl;
        for (int i = 0; i < aclDenyLocalNets.length; i++) {
            acl = (ServerAcl) aclDenyLocalNets[i];
            assertFalse(
                "Permitting access from localNet with localNet-deny ACL",
                acl.permitAccess(localhostByName.getAddress()));
        }
    }
    public void testNoLocalNetOnlyWithIPs() {
        ServerAcl acl;
        for (int i = 0; i < aclDenyLocalNets.length; i++) {
            acl = (ServerAcl) aclDenyLocalNets[i];
            assertFalse(
                "Permitting access from localNet with localNet-deny ACL",
                acl.permitAccess(localhostByAddr.getAddress()));
            assertTrue("Denying access from other Net with localNet-deny ACL",
                       acl.permitAccess(otherHostByAddr.getAddress()));
        }
    }
    public static Test suite()
    throws IOException, ServerAcl.AclFormatException {
        TestSuite newSuite = new TestSuite();
        newSuite.addTest(new TestAcl("testDefaultWithNames"));
        newSuite.addTest(new TestAcl("testDefaultWithIPs"));
        newSuite.addTest(new TestAcl("testDenyAllWithNames"));
        newSuite.addTest(new TestAcl("testDenyAllWithIPs"));
        newSuite.addTest(new TestAcl("testLocalhostOnlyWithNames"));
        newSuite.addTest(new TestAcl("testLocalhostOnlyWithIPs"));
        newSuite.addTest(new TestAcl("testNoLocalhostOnlyWithNames"));
        newSuite.addTest(new TestAcl("testNoLocalhostOnlyWithIPs"));
        newSuite.addTest(new TestAcl("testLocalNetOnlyWithNames"));
        newSuite.addTest(new TestAcl("testLocalNetOnlyWithIPs"));
        newSuite.addTest(new TestAcl("testNoLocalNetOnlyWithNames"));
        newSuite.addTest(new TestAcl("testNoLocalNetOnlyWithIPs"));
        return newSuite;
    }
}