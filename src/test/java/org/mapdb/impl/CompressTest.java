package org.mapdb.impl;

import java.io.IOException;
import org.junit.Before;
import org.junit.Test;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.ValueSerializer;
import org.mapdb.impl.binaryserializer.SerializerBase;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CompressTest
{

    DB db;

    @Before
    public void init()
    {
        db = DBMaker
            .newMemoryDB()
            .cacheDisable()
            .compressionEnable()
            .make();
    }

    @Test
    public void check_instance()
        throws Exception
    {
        Store s = Store.forDB( db );
        assertTrue( s.compress );
    }

    @Test
    public void put_get_update()
        throws Exception
    {
        long recid = db.getEngine().put( "aaaa", SerializerBase.STRING_NOSIZE );
        assertEquals( "aaaa", db.getEngine().get( recid, SerializerBase.STRING_NOSIZE ) );
        db.getEngine().update( recid, "bbbb", SerializerBase.STRING_NOSIZE );
        assertEquals( "bbbb", db.getEngine().get( recid, SerializerBase.STRING_NOSIZE ) );
        db.getEngine().delete( recid, SerializerBase.STRING_NOSIZE );
        assertEquals( null, db.getEngine().get( recid, SerializerBase.STRING_NOSIZE ) );
    }

    @Test
    public void short_compression()
        throws Exception
    {
        byte[] b = new byte[]{ 1, 2, 3, 4, 5, 33, 3 };
        byte[] b2 = UtilsTest.clone( b, new CompressionWrapper<byte[]>( SerializerBase.BYTE_ARRAY ) );
        assertArrayEquals( b, b2 );
    }

    @Test
    public void large_compression()
        throws IOException
    {
        byte[] b = new byte[ 1024 ];
        b[ 0 ] = 1;
        b[ 4 ] = 5;
        b[ 1000 ] = 1;

        ValueSerializer<byte[]> ser = new CompressionWrapper<byte[]>( SerializerBase.BYTE_ARRAY );
        assertArrayEquals( b, UtilsTest.clone( b, ser ) );

        //check compressed size is actually smaller
        DataOutput2 out = new DataOutput2();
        ser.serialize( out, b );
        assertTrue( out.pos < 100 );
    }
}
