package org.apache.bookkeeper.client;

import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class LedgerHandleReadEntriesTest extends BookKeeperClusterTestCase {

    private final BookKeeper.DigestType defaultDigestType = BookKeeper.DigestType.CRC32;
    private final String password = "my_password";
    private final String validString = "It's not possible to test everything";
    private final byte[] data = validString.getBytes();
    private final int validOffset = 0;
    private final int validDataLength = data.length;
    // test parameters
    private long firstEntry;
    private long lastEntry;
    private int stateSize;
    private boolean expectedOutput;
    // SUT
    private LedgerHandle lh;

    public LedgerHandleReadEntriesTest(long firstEntry, long lastEntry, int stateSize, boolean expectedOutput){
        super(10);
        configure(firstEntry,lastEntry, stateSize, expectedOutput);
    }

    public void configure(long firstEntry, long lastEntry, int sizeOfState, boolean expectedOutput){
        this.firstEntry = firstEntry;
        this.lastEntry = lastEntry;
        this.stateSize = sizeOfState;
        this.expectedOutput = expectedOutput;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> getTestParameters() {
        return Arrays.asList(new Object[][]{
            //firstEntry    lastEntry   |State|     expectedOutput
            {0, 1, 1, false},
            {-1, 0, 1, false},
            {1, 0, 0, false},
            {0, 0, 1, true}
        });
    }

    @Before
    public void setup() {
        try {
            lh = bkc.createLedger(defaultDigestType, password.getBytes());
        } catch (BKException | InterruptedException e) {
            e.printStackTrace();
        }
        // generating state
        for (int i = 0; i < this.stateSize; i++) {
            try {
                lh.addEntry(data, validOffset, validDataLength);
            } catch (InterruptedException | BKException e) {
                e.printStackTrace();
            }
        }
    }


    @Test
    public void ledgerReadEntriesTest() {
        boolean outputTest = true;
        Enumeration<LedgerEntry> entries = null;
        try {
            entries = lh.readEntries(this.firstEntry, this.lastEntry);
        } catch (Exception e) {
            assertEquals(this.expectedOutput, false);
            return;
        }

        // looping on the got entries
        while (entries.hasMoreElements()) {
            outputTest = true;
            LedgerEntry entry = entries.nextElement();
            byte[] returned = entry.getEntry();
            String newString = new String(this.data);
            if (!newString.equals(new String(returned))) {
                outputTest = false;
            }
        }
        assertEquals(this.expectedOutput, outputTest);
    }

    @After
    public void cleanup() {
        try {
            lh.close();
        } catch (InterruptedException | BKException e) {
            e.printStackTrace();
        }
    }

}
