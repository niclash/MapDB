package org.mapdb;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ConcurrentMap;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mapdb.impl.binaryserializer.SerializerBase;
import org.mapdb.impl.UtilsTest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class Issue353Test
{

    private ConcurrentMap<byte[], byte[]> map;
    private DB db;
    private Random random = new Random();
    private static final int ITERATIONS = 40000;

    @Before
    public void setupDb()
    {
        db = DBMaker.newFileDB( UtilsTest.tempDbFile() ).closeOnJvmShutdown().mmapFileEnableIfSupported()
            .commitFileSyncDisable().transactionDisable().compressionEnable().freeSpaceReclaimQ( 0 ).make();
        HTreeMapMaker maker = db.createHashMap( "products" ).hasher( Hasher.BYTE_ARRAY )
            .valueSerializer( SerializerBase.BYTE_ARRAY ).keySerializer( SerializerBase.BYTE_ARRAY ).counterEnable();
        map = maker.makeOrGet();
    }

    @After
    public void shutdownDb()
        throws IOException
    {
        db.close();
    }

    @Test
    public void iterateKeySet()
    {
        db.commit();
        map.clear();
        db.commit();
        for( int i = 0; i < ITERATIONS; i++ )
        {
            map.put( createByteArrayForKey(), createByteArrayForValue() );
        }
        for( byte[] e : map.keySet() )
        {
            assertNotNull( map.get( e ) );
        }
        assertEquals( ITERATIONS, map.size() );
        map.clear();
        db.commit();
        assertEquals( 0, map.size() );
        for( byte[] e : map.keySet() )
        {
            fail();
        }
        map.put( createByteArrayForKey(), createByteArrayForValue() );
        db.commit();
        assertEquals( 1, map.size() );
        boolean found = false;
        for( byte[] e : map.keySet() )
        {
            if( found == true )
            {
                fail();
            }
            found = true;
        }
    }

    private byte[] createByteArrayForKey()
    {
        byte[] result = new byte[ 12 ];
        random.nextBytes( result );
        return result;
    }

    private byte[] createByteArrayForValue()
    {
        int size = random.nextInt( 300 ) + 200;
        byte[] result = new byte[ size ];
        random.nextBytes( result );
        return result;
    }
}