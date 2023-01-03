package org.apache.bookkeeper.client;

import org.apache.bookkeeper.bookie.BookieImpl;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.zk.ZKMetadataDriverBase;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.util.BookKeeperConstants;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;



@RunWith(Parameterized.class)
public class BookKeeperAdminInitBookieTest extends BookKeeperClusterTestCase {
    // test params
    private ServerConfiguration serverConfiguration;
    private boolean deleteLedgerJournalDirs;
    private boolean expected;
    private boolean validConf;
    private boolean isBookiePresent;



    public BookKeeperAdminInitBookieTest(boolean validConf,
                                         boolean deleteLedgerJournalDirs,
                                         boolean isBookiePresent,
                                         boolean expected) {
        super(2);
        configure(validConf, deleteLedgerJournalDirs, isBookiePresent, expected);
    }

    private void configure(boolean validConf,
                           boolean deleteLedgerJournalDirs,
                           boolean isBookiePresent,
                           boolean expected) {
        this.deleteLedgerJournalDirs = deleteLedgerJournalDirs;
        this.expected = expected;
        this.isBookiePresent = isBookiePresent;
        this.validConf = validConf;
        }


    private void deleteDirs(File[] dirs) throws IOException {
        for (File dir : dirs) {
            //Delete the directory
            FileUtils.deleteDirectory(dir);
        }
    }



    @Before
    public void setUp() {

        try {
            super.setUp();

            if (this.validConf) {
                this.serverConfiguration = confByIndex(lastBookieIndex());

                if (this.deleteLedgerJournalDirs) {
                    File[] ledgerDirs = serverConfiguration.getLedgerDirs();
                    this.deleteDirs(ledgerDirs);
                }

                if (!this.isBookiePresent) {
                    this.killBookie(lastBookieIndex());
                    String path =
                            // returns the metadata service URI from Zookeeper
                            ZKMetadataDriverBase.resolveZkLedgersRootPath(serverConfiguration)
                                    + "/" + BookKeeperConstants.COOKIE_NODE
                                    //the bookie ID
                                    + "/" + BookieImpl.getBookieId(this.serverConfiguration).toString();
                    this.zkc.delete(path, -1);
                }
            }

        }catch (Exception e){
            e.printStackTrace();
        }
    }


    //Parameter association
    @Parameterized.Parameters
    public static Collection<?> getParameters(){

        return Arrays.asList(new Object[][] {
                // validConf, deleteLedgerJournalDirs, isBookiePresent, expected
                {true,  true, false, true },
                {false, true, true, false},
                {true,  true, true, false},
                {true, false, false, false},
                {true, false, false, false},
        });
    }


    @Test
    public void initBookie() {
        boolean output;


        try {
            //Try the method under test here
            output = BookKeeperAdmin.initBookie(serverConfiguration);
        } catch (Exception e) {
            output = false;
        }
        Assert.assertEquals(expected, output);

    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }

}
