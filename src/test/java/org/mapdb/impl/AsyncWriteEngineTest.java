package org.mapdb.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Before;
import org.junit.Test;
import org.mapdb.TestFile;
import org.mapdb.ValueSerializer;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author Jan Kotek
 */
@SuppressWarnings({ "unchecked", "rawtypes" })
public class AsyncWriteEngineTest extends TestFile
{
    AsyncWriteEngine engine;

    @Before
    public void reopenStore()
        throws IOException
    {
        assertNotNull( index );
        if( engine != null )
        {
            engine.close();
        }
        engine = new AsyncWriteEngine( new StoreDirect( fac ) );
    }

    @Test(timeout = 1000000)
    public void write_fetch_update_delete()
        throws IOException
    {
        long recid = engine.put( "aaa", SerializerBase.STRING_NOSIZE );
        assertEquals( "aaa", engine.get( recid, SerializerBase.STRING_NOSIZE ) );
        reopenStore();
        assertEquals( "aaa", engine.get( recid, SerializerBase.STRING_NOSIZE ) );
        engine.update( recid, "bbb", SerializerBase.STRING_NOSIZE );
        assertEquals( "bbb", engine.get( recid, SerializerBase.STRING_NOSIZE ) );
        reopenStore();
        assertEquals( "bbb", engine.get( recid, SerializerBase.STRING_NOSIZE ) );
    }

    @Test(timeout = 0xFFFF)
    public void concurrent_updates_test()
        throws InterruptedException, IOException
    {
        final int threadNum = 16;
        final int updates = 1000;
        final CountDownLatch latch = new CountDownLatch( threadNum );
        final Map<Integer, Long> recids = new ConcurrentHashMap<Integer, Long>();

        for( int i = 0; i < threadNum; i++ )
        {
            final int num = i;
            new Thread( new Runnable()
            {
                @Override
                public void run()
                {
                    long recid = engine.put( "START-", SerializerBase.STRING_NOSIZE );
                    recids.put( num, recid );
                    for( int i = 0; i < updates; i++ )
                    {
                        String str = engine.get( recid, SerializerBase.STRING_NOSIZE );
                        str += num + ",";
                        engine.update( recid, str, SerializerBase.STRING_NOSIZE );
                    }
                    latch.countDown();
                }
            } ).start();
        }

        latch.await();

        reopenStore();

        assertEquals( recids.size(), threadNum );
        for( int i = 0; i < threadNum; i++ )
        {
            long recid = recids.get( i );

            String expectedStr = "START-";
            for( int j = 0; j < updates; j++ )
            {
                expectedStr += i + ",";
            }

            String v = engine.get( recid, SerializerBase.STRING_NOSIZE );
            assertEquals( expectedStr, v );
        }
    }

    @Test(timeout = 1000000)
    public void async_commit()
    {
        final AtomicLong putCounter = new AtomicLong();
        StoreWAL t = new StoreWAL( fac )
        {
            @Override
            public <A> long put( A value, ValueSerializer<A> serializer )
            {
                putCounter.incrementAndGet();
                return super.put( value, serializer );
            }

            @Override
            public <A> void update( long recid, A value, ValueSerializer<A> serializer )
            {
                putCounter.incrementAndGet();
                super.update( recid, value, serializer );
            }
        };
        AsyncWriteEngine a = new AsyncWriteEngine( t );
        byte[] b = new byte[ 124 ];

        long max = 100;

        ArrayList<Long> l = new ArrayList<Long>();
        for( int i = 0; i < max; i++ )
        {
            long recid = a.put( b, SerializerBase.BASIC );
            l.add( recid );
        }
        //make commit just after bunch of records was added,
        // we need to test that all records made it into transaction log
        a.commit();
        assertEquals( max, putCounter.longValue() );
        assertTrue( a.writeCache.isEmpty() );
        a.close();

        //now reopen db and check ths
        t = new StoreWAL( fac );
        a = new AsyncWriteEngine( t );
        for( Long recid : l )
        {
            assertArrayEquals( b, (byte[]) a.get( recid, SerializerBase.BASIC ) );
        }
        a.close();
    }
}
