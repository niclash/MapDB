package org.mapdb;

import java.io.IOException;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.mapdb.impl.engine.DbImpl;
import org.mapdb.impl.binaryserializer.SerializerBase;
import org.mapdb.impl.StoreHeap;

import static org.junit.Assert.assertEquals;

/**
 * check that `IllegalAccessError` is thrown after DB was closed
 */
public abstract class ClosedThrowsExceptionTest
{

    abstract DB db();

    DB db;

    @Before
    public void init()
    {
        db = db();
    }

    static public class Def extends ClosedThrowsExceptionTest
    {
        @Override
        DB db()
        {
            return DBMaker.newMemoryDB().make();
        }
    }

    static public class Async extends ClosedThrowsExceptionTest
    {
        @Override
        DB db()
        {
            return DBMaker.newMemoryDB().asyncWriteEnable().make();
        }
    }

    static public class NoCache extends ClosedThrowsExceptionTest
    {
        @Override
        DB db()
        {
            return DBMaker.newMemoryDB().cacheDisable().make();
        }
    }

    static public class HardRefCache extends ClosedThrowsExceptionTest
    {
        @Override
        DB db()
        {
            return DBMaker.newMemoryDB().cacheHardRefEnable().make();
        }
    }

    static public class TX extends ClosedThrowsExceptionTest
    {
        @Override
        DB db()
        {
            return DBMaker.newMemoryDB().makeTxMaker().makeTx();
        }
    }

    static public class storeHeap extends ClosedThrowsExceptionTest
    {
        @Override
        DB db()
        {
            return new DbImpl( new StoreHeap() );
        }
    }

    @Test(expected = IllegalAccessError.class)
    public void closed_getHashMap()
        throws IOException
    {
        db.getHashMap( "test" );
        db.close();
        db.getHashMap( "test" );
    }

    @Test()
    public void closed_getNamed()
        throws IOException
    {
        db.getHashMap( "test" );
        db.close();
        assertEquals( null, db.getNameForObject( "test" ) );
    }

    @Test(expected = IllegalAccessError.class)
    public void closed_put()
        throws IOException
    {
        Map m = db.getHashMap( "test" );
        db.close();
        m.put( "aa", "bb" );
    }

    @Test(expected = IllegalAccessError.class)
    public void closed_remove()
        throws IOException
    {
        Map m = db.getHashMap( "test" );
        m.put( "aa", "bb" );
        db.close();
        m.remove( "aa" );
    }

    @Test
    public void closed_close()
        throws IOException
    {
        Map m = db.getHashMap( "test" );
        m.put( "aa", "bb" );
        db.close();
        db.close(); // Multiple close() should succeed!
    }

    @Test(expected = IllegalAccessError.class)
    public void closed_rollback()
        throws IOException
    {
        Map m = db.getHashMap( "test" );
        m.put( "aa", "bb" );
        db.close();
        db.rollback();
    }

    @Test(expected = IllegalAccessError.class)
    public void closed_commit()
        throws IOException
    {
        Map m = db.getHashMap( "test" );
        m.put( "aa", "bb" );
        db.close();
        db.commit();
    }

    @Test
    public void closed_is_closed()
        throws IOException
    {
        Map m = db.getHashMap( "test" );
        m.put( "aa", "bb" );
        db.close();
        assertEquals( true, db.isClosed() );
    }

    @Test(expected = IllegalAccessError.class)
    public void closed_engine_get()
        throws IOException
    {
        long recid = db.getEngine().put( "aa", SerializerBase.STRING );
        db.close();
        db.getEngine().get( recid, SerializerBase.STRING );
    }

    @Test(expected = IllegalAccessError.class)
    public void closed_engine_put()
        throws IOException
    {
        db.close();
        long recid = db.getEngine().put( "aa", SerializerBase.STRING );
    }

    @Test(expected = IllegalAccessError.class)
    public void closed_engine_update()
        throws IOException
    {
        long recid = db.getEngine().put( "aa", SerializerBase.STRING );
        db.close();
        db.getEngine().update( recid, "aax", SerializerBase.STRING );
    }

    @Test(expected = IllegalAccessError.class)
    public void closed_engine_delete()
        throws IOException
    {
        long recid = db.getEngine().put( "aa", SerializerBase.STRING );
        db.close();
        db.getEngine().delete( recid, SerializerBase.STRING );
    }
}
