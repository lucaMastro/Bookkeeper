package org.apache.bookkeeper;

import org.apache.bookkeeper.proto.BKStats;
import org.junit.Assert;
import org.junit.Test;

public class TestProva {

    // comment for test automatic build

    @Test
    public void test_0(){
        BKStats.getInstance();
        int a = 3;
        Assert.assertEquals(a, 3);
    }
}
