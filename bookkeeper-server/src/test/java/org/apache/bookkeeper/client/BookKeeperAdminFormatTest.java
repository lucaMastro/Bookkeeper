package org.apache.bookkeeper.client;

import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import java.io.ByteArrayInputStream;
import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class BookKeeperAdminFormatTest extends BookKeeperClusterTestCase{


    private final BookKeeper.DigestType defaultDigestType = BookKeeper.DigestType.CRC32;

    private static final String password = "It's not possible to test everything";

    // test params
    private boolean isInteractive;
    private String promptAnswer;
    private boolean force;
    private boolean expectedOutput;
    // since the default conf is an attribute of the super class, I just keep trace if i should use it or not
    private boolean validConf;

    public BookKeeperAdminFormatTest(boolean validConf, boolean isInteractive, String promptAnswer, boolean force, boolean expectedOutput) {
        super(10);
        this.configure(validConf, isInteractive, promptAnswer, force, expectedOutput);
    }

    public void configure(boolean validConf, boolean isInteractive, String promptAnswer, boolean force, boolean expectedOutput){
        //test parameters
        this.validConf = validConf;
        this.isInteractive = isInteractive;
        this.promptAnswer = promptAnswer;
        this.force = force;
        this.expectedOutput = expectedOutput;
    }

    //Parameters association
    @Parameterized.Parameters
    public static Collection<?> getParameters(){
        return Arrays.asList(new Object[][] {

                // validConf, isInteractive, promptAnswer, force, expected
                {true, true, "y\n", false, true},
                {false, false, "", true, false},
                {true, true, "n\n", false, false},
                // to increase coverage
                {true, false, "", true, true},
        });
    }

    @Before
    public void setUp(){
        try {
            super.setUp();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Test
    public void formatTest() {

        boolean output;

        if (this.isInteractive) {
                // changing the input stream
                System.setIn(new ByteArrayInputStream(this.promptAnswer.getBytes(), 0, 2));
        }
        try{
            if (this.validConf) {
                // baseConf is super class protected attribute
                output = BookKeeperAdmin.format(this.baseConf, this.isInteractive, this.force);
            }
            else {
                output = BookKeeperAdmin.format(null, this.isInteractive, this.force);
            }
        } catch (Exception e) {
            output = false;
            e.printStackTrace();
        }

        Assert.assertEquals(expectedOutput, output);
    }

    @After
    public void tearDown() throws Exception{
        super.tearDown();
    }



}
