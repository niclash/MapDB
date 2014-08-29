package org.mapdb.impl;

import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Engine;
import org.mapdb.HTreeMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class TxEngineTest
{

    TxEngine e;

    @Before
    public void init()
    {
        e = new TxEngine( new StoreWAL( Volume.memoryFactory( false, 0L, CC.VOLUME_SLICE_SHIFT ) ), false );
    }

    @Test
    public void update()
    {
        long recid = e.put( 111, SerializerBase.INTEGER );
        e.commit();
        Engine snapshot = e.snapshot();
        e.update( recid, 222, SerializerBase.INTEGER );
        assertEquals( Integer.valueOf( 111 ), snapshot.get( recid, SerializerBase.INTEGER ) );
    }

    @Test
    public void compareAndSwap()
    {
        long recid = e.put( 111, SerializerBase.INTEGER );
        e.commit();
        Engine snapshot = e.snapshot();
        e.compareAndSwap( recid, 111, 222, SerializerBase.INTEGER );
        assertEquals( Integer.valueOf( 111 ), snapshot.get( recid, SerializerBase.INTEGER ) );
    }

    @Test
    public void delete()
    {
        long recid = e.put( 111, SerializerBase.INTEGER );
        e.commit();
        Engine snapshot = e.snapshot();
        e.delete( recid, SerializerBase.INTEGER );
        assertEquals( Integer.valueOf( 111 ), snapshot.get( recid, SerializerBase.INTEGER ) );
    }

    @Test
    public void notExist()
    {
        Engine snapshot = e.snapshot();
        long recid = e.put( 111, SerializerBase.INTEGER );
        assertNull( snapshot.get( recid, SerializerBase.INTEGER ) );
    }

    @Test
    public void create_snapshot()
    {
        Engine e = DBMaker.newMemoryDB().snapshotEnable().makeEngine();
        Engine snapshot = TxEngine.createSnapshotFor( e );
        assertNotNull( snapshot );
    }

    @Test
    public void DB_snapshot()
    {
        DB db = DBMaker.newMemoryDB().snapshotEnable().asyncWriteFlushDelay( 100 ).transactionDisable().make();
        long recid = db.getEngine().put( "aa", SerializerBase.STRING_NOSIZE );
        DB db2 = db.snapshot();
        assertEquals( "aa", db2.getEngine().get( recid, SerializerBase.STRING_NOSIZE ) );
        db.getEngine().update( recid, "bb", SerializerBase.STRING_NOSIZE );
        assertEquals( "aa", db2.getEngine().get( recid, SerializerBase.STRING_NOSIZE ) );
    }

    @Test
    public void DB_snapshot2()
    {
        DB db = DBMaker.newMemoryDB().transactionDisable().snapshotEnable().make();
        long recid = db.getEngine().put( "aa", SerializerBase.STRING_NOSIZE );
        DB db2 = db.snapshot();
        assertEquals( "aa", db2.getEngine().get( recid, SerializerBase.STRING_NOSIZE ) );
        db.getEngine().update( recid, "bb", SerializerBase.STRING_NOSIZE );
        assertEquals( "aa", db2.getEngine().get( recid, SerializerBase.STRING_NOSIZE ) );
    }

    @Test
    public void BTreeMap_snapshot()
    {
        BTreeMap map =
            DBMaker.newMemoryDB().transactionDisable().snapshotEnable()
                .make().getTreeMap( "aaa" );
        map.put( "aa", "aa" );
        Map map2 = map.snapshot();
        map.put( "aa", "bb" );
        assertEquals( "aa", map2.get( "aa" ) );
    }

    @Test
    public void HTreeMap_snapshot()
    {
        HTreeMap map =
            DBMaker.newMemoryDB().transactionDisable().snapshotEnable()
                .make().getHashMap( "aaa" );
        map.put( "aa", "aa" );
        Map map2 = map.snapshot();
        map.put( "aa", "bb" );
        assertEquals( "aa", map2.get( "aa" ) );
    }

//    @Test public void test_stress(){
//        ExecutorService ex = Executors.newCachedThreadPool();
//
//        TxMaker tx = DBMaker.newMemoryDB().transactionDisable().makeTxMaker();
//
//        DB db = tx.makeTx();
//        final long recid =
//
//        final int threadNum = 32;
//        for(int i=0;i<threadNum;i++){
//            ex.execute(new Runnable() { @Override public void run() {
//
//            }});
//        }
//    }
}
