package org.mapdb;

import org.junit.Assert;
import org.junit.Test;
import org.mapdb.impl.CC;

public class CCTest
{

    @Test
    public void concurency()
    {
        int i = 2;
        while( i < Integer.MAX_VALUE )
        {
            i = i * 2;
            if( i == CC.CONCURRENCY )
            {
                return;
            }
        }
        Assert.fail( "no power of two" );
    }
}
