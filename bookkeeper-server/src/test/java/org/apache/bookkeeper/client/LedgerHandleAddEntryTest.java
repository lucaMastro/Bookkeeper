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

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class LedgerHandleAddEntryTest extends BookKeeperClusterTestCase {

    private static final BookKeeper.DigestType defaultDigestType = BookKeeper.DigestType.CRC32;
    private static final String validString = "It's not possible to test everything";
    private static final String emptyString = "";
    private static final String password = "my_password";

    // test parameters
    private byte[] data;
    private int offset;
    private int length;
    private boolean expectedOutput;
    // SUT
    private LedgerHandle ledgerHandle;

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        // {data, offset, length, expected_output}
        return Arrays.asList(new Object[][] {
                { validString.getBytes(), 1, 1, true },
                { validString.getBytes(), validString.length() - 1, 0, true },
                { validString.getBytes(), validString.length(), 0, true },
                { validString.getBytes(), validString.length() + 1, 0, false },
                { emptyString.getBytes(), 0, 0,true },
                { null, -1, -1, false },
                // added after mutation test analisys:
                { validString.getBytes(),validString.length(), 1, false },
        });
    }

    public LedgerHandleAddEntryTest(byte[] data, int offset, int length, boolean expected) throws InterruptedException, BKException {
        super(10);
        this.configure(data, offset, length, expected);
    }

    public void configure(byte[] data, int offset, int length, boolean output){
        this.data = data;
        this.offset = offset;
        this.length = length;
        this.expectedOutput = output;
    }


    @Test
    public void testAddLedgerEntry() {
        boolean outputTest = true;
        long new_entry_id = -1;
        try {
            new_entry_id = ledgerHandle.addEntry(this.data, this.offset, this.length);
        } catch (InterruptedException | BKException | IndexOutOfBoundsException | NullPointerException e) {
            outputTest = false;
        }

        assertEquals(this.expectedOutput, outputTest);

        // reading written value
        Enumeration<LedgerEntry> entries = null;
        try {
            entries = ledgerHandle.readEntries(new_entry_id, new_entry_id);
        } catch (InterruptedException | BKException e) {
            e.printStackTrace();
            return;
        }

        outputTest = true;
        LedgerEntry entry = entries.nextElement();
        byte[] returned = entry.getEntry();
        String newString = new String(data).substring(offset, offset + length);
        if (!newString.equals(new String(returned))) {
            outputTest = false;
        }
        assertEquals(this.expectedOutput, outputTest);
    }

    @After
    public void cleanup() {
        try {
            ledgerHandle.close();
        } catch (InterruptedException | BKException e) {
            e.printStackTrace();
        }
    }

    @Before
    public void setup() {
        try {
            ledgerHandle = bkc.createLedger(defaultDigestType, password.getBytes());
        } catch (BKException | InterruptedException e) {
            e.printStackTrace();
        }
    }

}
