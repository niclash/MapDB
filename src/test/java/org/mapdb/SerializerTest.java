package org.mapdb;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import java.util.UUID;
import org.junit.Test;
import org.mapdb.impl.CompressionWrapper;
import org.mapdb.impl.DataOutput2;
import org.mapdb.impl.SerializerBase;
import org.mapdb.impl.SerializerBaseTest;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class SerializerTest
{

    @Test
    public void UUID2()
    {
        UUID u = UUID.randomUUID();
        assertEquals( u, SerializerBaseTest.clone2( u, SerializerBase.UUID ) );
    }

    @Test
    public void string_ascii()
    {
        String s = "adas9 asd9009asd";
        assertEquals( s, SerializerBaseTest.clone2( s, SerializerBase.STRING_ASCII ) );
        s = "";
        assertEquals( s, SerializerBaseTest.clone2( s, SerializerBase.STRING_ASCII ) );
        s = "    ";
        assertEquals( s, SerializerBaseTest.clone2( s, SerializerBase.STRING_ASCII ) );
    }

    @Test
    public void compression_wrapper()
        throws IOException
    {
        byte[] b = new byte[ 100 ];
        new Random().nextBytes( b );
        ValueSerializer<byte[]> ser = new CompressionWrapper( SerializerBase.BYTE_ARRAY );
        assertArrayEquals( b, SerializerBaseTest.clone2( b, ser ) );

        b = Arrays.copyOf( b, 10000 );
        assertArrayEquals( b, SerializerBaseTest.clone2( b, ser ) );

        DataOutput2 out = new DataOutput2();
        ser.serialize( out, b );
        assertTrue( out.pos < 1000 );
    }
}
