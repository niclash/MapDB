package org.mapdb.impl;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import org.junit.Test;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Engine;
import org.mapdb.Serialization2Bean;
import org.mapdb.Serialized2DerivedBean;
import org.mapdb.TestFile;
import org.mapdb.ValueSerializer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class Serialization2Test extends TestFile
{

    @Test
    public void test2()
        throws IOException
    {
        DB db = DBMaker.newFileDB( index ).cacheDisable().transactionDisable().make();

        Serialization2Bean processView = new Serialization2Bean();

        Map<Object, Object> map = db.getHashMap( "test2" );

        map.put( "abc", processView );

        db.commit();

        Serialization2Bean retProcessView = (Serialization2Bean) map.get( "abc" );
        assertEquals( processView, retProcessView );

        db.close();
    }

    @Test
    public void test2_engine()
        throws IOException
    {
        DB db = DBMaker.newFileDB( index ).cacheDisable().make();

        Serialization2Bean processView = new Serialization2Bean();

        long recid = db.getEngine().put( processView, (ValueSerializer<Object>) db.getDefaultSerializer() );

        db.commit();

        Serialization2Bean retProcessView = (Serialization2Bean) db.getEngine().get( recid, db.getDefaultSerializer() );
        assertEquals( processView, retProcessView );

        db.close();
    }

    @Test
    public void test3()
        throws IOException
    {

        Serialized2DerivedBean att = new Serialized2DerivedBean();
        DB db = DBMaker.newFileDB( index ).cacheDisable().make();

        Map<Object, Object> map = db.getHashMap( "test" );

        map.put( "att", att );
        db.commit();
        db.close();
        db = DBMaker.newFileDB( index ).cacheDisable().make();
        map = db.getHashMap( "test" );

        Serialized2DerivedBean retAtt = (Serialized2DerivedBean) map.get( "att" );
        assertEquals( att, retAtt );
    }

    static class AAA implements Serializable
    {

        private static final long serialVersionUID = 632633199013551846L;

        String test = "aa";
    }

    @Test
    public void testReopenWithDefrag()
        throws IOException
    {

        File f = UtilsTest.tempDbFile();

        DB db = DBMaker.newFileDB( f )
            .transactionDisable()
            .cacheDisable()
            .checksumEnable()
            .make();

        Map<Integer, AAA> map = db.getTreeMap( "test" );
        map.put( 1, new AAA() );

        db.compact();
        System.out.println( db.getEngine().get( Engine.CLASS_INFO_RECID, SerializerPojo.serializer ) );
        db.close();

        db = DBMaker.newFileDB( f )
            .transactionDisable()
            .cacheDisable()
            .checksumEnable()
            .make();

        map = db.getTreeMap( "test" );
        assertNotNull( map.get( 1 ) );
        assertEquals( map.get( 1 ).test, "aa" );

        db.close();
    }
}
