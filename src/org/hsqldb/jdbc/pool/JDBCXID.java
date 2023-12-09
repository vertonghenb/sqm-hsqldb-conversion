


package org.hsqldb.jdbc.pool;

import java.net.Inet4Address;
import java.util.Arrays;
import java.util.Random;

import javax.transaction.xa.Xid;


public class JDBCXID implements Xid {

    int formatID;
    byte[] txID;
    byte[] txBranch;
    
    int hash;
    boolean hashComputed;

    public int getFormatId() {
        return formatID;
    }

    public byte[] getGlobalTransactionId() {
        return txID;
    }

    public byte[] getBranchQualifier() {
        return txBranch;
    }

    public JDBCXID(int formatID, byte[] txID, byte[] txBranch) {

        this.formatID = formatID;
        this.txID = txID;
        this.txBranch = txBranch;
    }

    public int hashCode() {
        if (!hashComputed) {
            hash = 7;
            hash = 83 * hash + this.formatID;
            hash = 83 * hash + Arrays.hashCode(this.txID);
            hash = 83 * hash + Arrays.hashCode(this.txBranch);
            hashComputed = true;
        }
        return hash;
    }

    public boolean equals(Object other) {

        if (other instanceof Xid) {
            Xid o = (Xid) other;

            return formatID == o.getFormatId()
                    && Arrays.equals(txID, o.getGlobalTransactionId())
                    && Arrays.equals(txBranch, o.getBranchQualifier());
        }

        return false;
    }

    
    public String toString() {

        StringBuffer sb = new StringBuffer(512);
        
        sb.append("formatId=").append(getFormatId());
        
        sb.append(" globalTransactionId(").append(txID.length).append(")={0x");
        for (int i = 0; i < txID.length; i++) {
            final int hexVal = txID[i] & 0xFF;
            if (hexVal < 0x10) {
                sb.append("0").append(Integer.toHexString(txID[i] & 0xFF));
            }
            sb.append(Integer.toHexString(txID[i] & 0xFF));
        }
        
        sb.append("} branchQualifier(").append(txBranch.length).append("))={0x");
        for (int i = 0; i < txBranch.length; i++) {
            final int hexVal = txBranch[i] & 0xFF;
            if (hexVal < 0x10) {
                sb.append("0");
            }
            sb.append(Integer.toHexString(txBranch[i] & 0xFF));
        }
        sb.append("}");
        
        return sb.toString();
    }

    private static byte[] s_localIp = null;
    private static int s_txnSequenceNumber = 0;
    
    private static final int UXID_FORMAT_ID = 0xFEED;

    private static int nextTxnSequenceNumber() {
         s_txnSequenceNumber++;
        return  s_txnSequenceNumber;
    }

    private static byte[] getLocalIp() {
        if (null == s_localIp) {
            try {
                s_localIp = Inet4Address.getLocalHost().getAddress();
            } catch (Exception ex) {
                s_localIp = new byte[]{0x7F, 0x00, 0x00, 0x01};
            }
        }
        return s_localIp;
    }

    
    public static Xid getUniqueXid(final int threadId) {
        final Random random = new Random(System.currentTimeMillis());
        
        int txnSequenceNumberValue = nextTxnSequenceNumber();
        int threadIdValue = threadId;
        int randomValue = random.nextInt();
        
        byte[] globalTransactionId = new byte[MAXGTRIDSIZE];
        byte[] branchQualifier = new byte[MAXBQUALSIZE];
        byte[] localIp = getLocalIp();

        System.arraycopy(localIp, 0, globalTransactionId, 0, 4);
        System.arraycopy(localIp, 0, branchQualifier, 0, 4);

        
        
        
        for (int i = 0; i <= 3; i++) {
            globalTransactionId[i + 4] = (byte) (txnSequenceNumberValue % 0x100);
            branchQualifier[i + 4] = (byte) (txnSequenceNumberValue % 0x100);
            txnSequenceNumberValue >>= 8;
            globalTransactionId[i + 8] = (byte) (threadIdValue % 0x100);
            branchQualifier[i + 8] = (byte) (threadIdValue % 0x100);
            threadIdValue >>= 8;
            globalTransactionId[i + 12] = (byte) (randomValue % 0x100);
            branchQualifier[i + 12] = (byte) (randomValue % 0x100);
            randomValue >>= 8;
        }

        return new JDBCXID(UXID_FORMAT_ID, globalTransactionId, branchQualifier);
    }
}
