package org.mapdb.impl;

import java.io.IOException;
import java.util.Map;
import org.junit.Test;
import org.mapdb.DB;
import org.mapdb.DBMaker;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CacheWeakSoftRefTest
{

    @Test
    public void weak_htree_inserts_delete()
        throws InterruptedException, IOException
    {
        DB db = DBMaker
            .newMemoryDB()
            .cacheWeakRefEnable()
            .make();
        testMap( db );
    }

    @Test
    public void soft_htree_inserts_delete()
        throws InterruptedException, IOException
    {
        DB db = DBMaker
            .newMemoryDB()
            .cacheSoftRefEnable()
            .make();
        testMap( db );
    }

    private void testMap( DB db )
        throws InterruptedException, IOException
    {
        Map<Integer, Integer> m = db.getHashMap( "name" );
        for( Integer i = 0; i < 1000; i++ )
        {
            m.put( i, i );
        }
        Caches.WeakSoftRef engine = (Caches.WeakSoftRef) db.getEngine();
        assertTrue( engine.items.size() != 0 );

        for( Integer i = 0; i < 1000; i++ )
        {
            Integer a = m.remove( i );
            assertEquals( i, a );
        }
        Thread t = engine.queueThread;
        db.close();
        int counter = 10000;
        while( Thread.State.TERMINATED != t.getState() && counter > 0 )
        {
            Thread.sleep( 1 );
            counter--;
        }
        assertEquals( Thread.State.TERMINATED, t.getState() );
    }
}
