package org.mapdb;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class CompressTest{

    DB db;

    @Before public void init(){
        db = DBMaker
                .newMemoryDB()
                .cacheDisable()
                .compressionEnable()
                .make();
    }


    @Test
    public void check_instance() throws Exception {
        Store s = Store.forDB(db);
        assertTrue(s.compress);
    }


    @Test
    public void put_get_update() throws Exception {
        long recid = db.engine.put("aaaa", Serializer.STRING_NOSIZE);
        assertEquals("aaaa",db.engine.get(recid, Serializer.STRING_NOSIZE));
        db.engine.update(recid, "bbbb", Serializer.STRING_NOSIZE);
        assertEquals("bbbb",db.engine.get(recid, Serializer.STRING_NOSIZE));
        db.engine.delete(recid,Serializer.STRING_NOSIZE);
        assertEquals(null,db.engine.get(recid, Serializer.STRING_NOSIZE));

    }


    @Test
    public void short_compression() throws Exception {
        byte[] b = new byte[]{1,2,3,4,5,33,3};
        byte[] b2 = UtilsTest.clone(b, new Serializer.CompressionWrapper<byte[]>(Serializer.BYTE_ARRAY));
        assertArrayEquals(b,b2);
    }

    @Test public void large_compression() throws IOException {
        byte[]  b = new byte[1024];
        b[0] = 1;
        b[4] = 5;
        b[1000] = 1;

        Serializer<byte[]> ser = new Serializer.CompressionWrapper<byte[]>(Serializer.BYTE_ARRAY);
        assertArrayEquals(b, UtilsTest.clone(b, ser));

        //check compressed size is actually smaller
        DataIO.DataOutputByteArray out = new DataIO.DataOutputByteArray();
        ser.serialize(out,b);
        assertTrue(out.pos<100);
    }


}
