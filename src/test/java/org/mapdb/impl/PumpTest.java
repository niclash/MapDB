package org.mapdb.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import org.junit.Ignore;
import org.junit.Test;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Engine;
import org.mapdb.impl.binaryserializer.SerializerBase;
import org.mapdb.impl.btree.BTreeMapImpl;
import org.mapdb.impl.engine.DbImpl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@SuppressWarnings( { "rawtypes", "unchecked" } )
public class PumpTest
{

    @Test
    public void copy()
    {
        DB db1 = new DbImpl( new StoreHeap() );
        Map m = db1.getHashMap( "test" );
        for( int i = 0; i < 1000; i++ )
        {
            m.put( i, "aa" + i );
        }

        DbImpl db2 = (DbImpl) DBMaker.newMemoryDB().make();
        Pump.copy( db1, db2 );

        Map m2 = db2.getHashMap( "test" );
        for( int i = 0; i < 1000; i++ )
        {
            assertEquals( "aa" + i, m.get( i ) );
        }
    }

    DbImpl makeDB( int i )
    {
        switch( i )
        {
        case 0:
            return (DbImpl) DBMaker.newAppendFileDB( UtilsTest.tempDbFile() )
                .deleteFilesAfterClose()
                .snapshotEnable()
                .make();
        case 1:
            return (DbImpl) DBMaker.newMemoryDB().snapshotEnable().make();
        case 2:
            return (DbImpl) DBMaker.newMemoryDB().snapshotEnable().transactionDisable().make();
        case 3:
            return (DbImpl) DBMaker.newMemoryDB().snapshotEnable().makeTxMaker().makeTx();
        case 4:
            return new DbImpl( new StoreHeap() );
        }
        throw new IllegalArgumentException( "" + i );
    }

    final int dbmax = 5;

    @Test
    @Ignore
    public void copy_all_stores_simple()
    {
        for( int srcc = 0; srcc < dbmax; srcc++ )
        {
            for( int targetc = 0; targetc < dbmax; targetc++ )
            {
                try
                {

                    DbImpl src = makeDB( srcc );
                    DbImpl target = makeDB( targetc );

                    long recid1 = src.getEngine().put( "1", SerializerBase.STRING_NOSIZE );
                    long recid2 = src.getEngine().put( "2", SerializerBase.STRING_NOSIZE );

                    Pump.copy( src, target );

                    assertEquals( "1", target.getEngine().get( recid1, SerializerBase.STRING_NOSIZE ) );
                    assertEquals( "2", target.getEngine().get( recid2, SerializerBase.STRING_NOSIZE ) );
                    assertEquals( "1", src.getEngine().get( recid1, SerializerBase.STRING_NOSIZE ) );
                    assertEquals( "2", src.getEngine().get( recid2, SerializerBase.STRING_NOSIZE ) );

                    src.close();
                    target.close();
                }
                catch( Throwable e )
                {
                    throw new RuntimeException( "Failed with " + srcc + " - " + targetc, e );
                }
            }
        }
    }

    @Test
    @Ignore
    public void copy_all_stores()
    {
        for( int srcc = 0; srcc < dbmax; srcc++ )
        {
            for( int targetc = 0; targetc < dbmax; targetc++ )
            {
                try
                {

                    DB src = makeDB( srcc );
                    DbImpl target = makeDB( targetc );

                    Map m = src.getTreeMap( "test" );
                    for( int i = 0; i < 1000; i++ )
                    {
                        m.put( i, "99090adas d" + i );
                    }
                    src.commit();

                    Pump.copy( src, target );

                    assertEquals( src.getCatalog(), target.getCatalog() );
                    Map m2 = target.getTreeMap( "test" );
                    assertFalse( m2.isEmpty() );
                    assertEquals( m, m2 );
                    src.close();
                    target.close();
                }
                catch( Throwable e )
                {
                    throw new RuntimeException( "Failed with " + srcc + " - " + targetc, e );
                }
            }
        }
    }

    @Test
    @Ignore
    public void copy_all_stores_with_snapshot()
    {
        for( int srcc = 0; srcc < dbmax; srcc++ )
        {
            for( int targetc = 0; targetc < dbmax; targetc++ )
            {
                try
                {

                    DbImpl src = makeDB( srcc );
                    DbImpl target = makeDB( targetc );

                    Map m = src.getTreeMap( "test" );
                    for( int i = 0; i < 1000; i++ )
                    {
                        m.put( i, "99090adas d" + i );
                    }
                    src.commit();

                    DB srcSnapshot = src.snapshot();

                    for( int i = 0; i < 1000; i++ )
                    {
                        m.put( i, "aaaa" + i );
                    }

                    Pump.copy( srcSnapshot, target );

                    assertEquals( src.getCatalog(), target.getCatalog() );
                    Map m2 = target.getTreeMap( "test" );
                    assertFalse( m2.isEmpty() );
                    assertEquals( m, m2 );
                    src.close();
                    target.close();
                }
                catch( Throwable e )
                {
                    throw new RuntimeException( "Failed with " + srcc + " - " + targetc, e );
                }
            }
        }
    }

    @Test
    public void presort()
    {
        final Integer max = 10000;
        List<Integer> list = new ArrayList<Integer>( max );
        for( Integer i = 0; i < max; i++ )
        {
            list.add( i );
        }
        Collections.shuffle( list );

        Iterator<Integer> sorted = Pump.sort( list.iterator(), false, max / 20,
                                              BTreeMapImpl.COMPARABLE_COMPARATOR, SerializerBase.INTEGER );

        Integer counter = 0;
        while( sorted.hasNext() )
        {
            assertEquals( counter++, sorted.next() );
        }
        assertEquals( max, counter );
    }

    @Test
    public void presort_duplicates()
    {
        final Integer max = 10000;
        List<Integer> list = new ArrayList<Integer>( max );
        for( Integer i = 0; i < max; i++ )
        {
            list.add( i );
            list.add( i );
        }
        Collections.shuffle( list );

        Iterator<Integer> sorted = Pump.sort( list.iterator(), true, max / 20,
                                              BTreeMapImpl.COMPARABLE_COMPARATOR, SerializerBase.INTEGER );

        Integer counter = 0;
        while( sorted.hasNext() )
        {
            Object v = sorted.next();
            assertEquals( counter++, v );
        }
        assertEquals( max, counter );
    }

    @Test
    public void build_treeset()
    {
        final int max = 10000;
        List<Integer> list = new ArrayList<Integer>( max );
        for( Integer i = max - 1; i >= 0; i-- )
        {
            list.add( i );
        }

        Engine e = new StoreHeap();
        DB db = new DbImpl( e );

        Set s = db.createTreeSet( "test" )
            .nodeSize( 8 )
            .pumpSource( list.iterator() )
            .make();

        Iterator iter = s.iterator();

        Integer count = 0;
        while( iter.hasNext() )
        {
            assertEquals( count++, iter.next() );
        }

        for( Integer i : list )
        {
            assertTrue( "" + i, s.contains( i ) );
        }

        assertEquals( max, s.size() );
    }

    @Test
    public void build_treeset_ignore_duplicates()
    {
        final int max = 10000;
        List<Integer> list = new ArrayList<Integer>( max );
        for( Integer i = max - 1; i >= 0; i-- )
        {
            list.add( i );
            list.add( i );
        }

        Engine e = new StoreHeap();
        DB db = new DbImpl( e );

        Set s = db.createTreeSet( "test" )
            .nodeSize( 8 )
            .pumpSource( list.iterator() )
            .pumpIgnoreDuplicates()
            .make();

        Iterator iter = s.iterator();

        Integer count = 0;
        while( iter.hasNext() )
        {
            assertEquals( count++, iter.next() );
        }

        for( Integer i : list )
        {
            assertTrue( "" + i, s.contains( i ) );
        }

        assertEquals( max, s.size() );
    }

    @Test
    public void build_treemap()
    {
        final int max = 10000;
        List<Integer> list = new ArrayList<Integer>( max );
        for( Integer i = max - 1; i >= 0; i-- )
        {
            list.add( i );
        }

        Engine e = new StoreHeap();
        DB db = new DbImpl( e );

        Fun.Function1<Object, Integer> valueExtractor = new Fun.Function1<Object, Integer>()
        {
            @Override
            public Object run( Integer integer )
            {
                return integer * 100;
            }
        };

        Map s = db.createTreeMap( "test" )
            .nodeSize( 6 )
            .pumpSource( list.iterator(), valueExtractor )
            .make();

        Iterator iter = s.keySet().iterator();

        Integer count = 0;
        while( iter.hasNext() )
        {
            assertEquals( count++, iter.next() );
        }

        for( Integer i : list )
        {
            assertEquals( i * 100, s.get( i ) );
        }

        assertEquals( max, s.size() );
    }

    @Test
    public void build_treemap_ignore_dupliates()
    {
        final int max = 10000;
        List<Integer> list = new ArrayList<Integer>( max );
        for( Integer i = max - 1; i >= 0; i-- )
        {
            list.add( i );
            list.add( i );
        }

        Engine e = new StoreHeap();
        DB db = new DbImpl( e );

        Fun.Function1<Object, Integer> valueExtractor = new Fun.Function1<Object, Integer>()
        {
            @Override
            public Object run( Integer integer )
            {
                return integer * 100;
            }
        };

        Map s = db.createTreeMap( "test" )
            .nodeSize( 6 )
            .pumpSource( list.iterator(), valueExtractor )
            .pumpIgnoreDuplicates()
            .make();

        Iterator iter = s.keySet().iterator();

        Integer count = 0;
        while( iter.hasNext() )
        {
            assertEquals( count++, iter.next() );
        }

        for( Integer i : list )
        {
            assertEquals( i * 100, s.get( i ) );
        }

        assertEquals( max, s.size() );
    }

    @Test( expected = IllegalArgumentException.class )
    public void build_treemap_fails_with_unsorted()
    {
        List a = Arrays.asList( 1, 2, 3, 4, 4, 5 );
        DB db = new DbImpl( new StoreHeap() );
        db.createTreeSet( "test" ).pumpSource( a.iterator() ).make();
    }

    @Test( expected = IllegalArgumentException.class )
    public void build_treemap_fails_with_unsorted2()
    {
        List a = Arrays.asList( 1, 2, 3, 4, 3, 5 );
        DB db = new DbImpl( new StoreHeap() );
        db.createTreeSet( "test" ).pumpSource( a.iterator() ).make();
    }

    @Test
    public void uuid_reversed()
    {
        List<UUID> u = new ArrayList<UUID>();
        Random r = new Random();
        for( int i = 0; i < 1e6; i++ )
        {
            u.add( new UUID( r.nextLong(), r.nextLong() ) );
        }
        Set<UUID> sorted = new TreeSet<UUID>( Collections.reverseOrder( BTreeMapImpl.COMPARABLE_COMPARATOR ) );
        sorted.addAll( u );

        Iterator<UUID> iter = u.iterator();
        iter = Pump.sort( iter, false, 10000, Collections.reverseOrder( BTreeMapImpl.COMPARABLE_COMPARATOR ), SerializerBase.UUID );
        Iterator<UUID> iter2 = sorted.iterator();

        while( iter.hasNext() )
        {
            assertEquals( iter2.next(), iter.next() );
        }
        assertFalse( iter2.hasNext() );
    }

    @Test
    public void duplicates_reversed()
    {
        List<Long> u = new ArrayList<Long>();

        for( long i = 0; i < 1e6; i++ )
        {
            u.add( i );
            if( i % 100 == 0 )
            {
                for( int j = 0; j < 10; j++ )
                {
                    u.add( i );
                }
            }
        }

        Comparator c = Collections.reverseOrder( BTreeMapImpl.COMPARABLE_COMPARATOR );
        List<Long> sorted = new ArrayList<Long>( u );
        Collections.sort( sorted, c );

        Iterator<Long> iter = u.iterator();
        iter = Pump.sort( iter, false, 10000, c, SerializerBase.LONG );
        Iterator<Long> iter2 = sorted.iterator();

        while( iter.hasNext() )
        {
            assertEquals( iter2.next(), iter.next() );
        }
        assertFalse( iter2.hasNext() );
    }

    @Test
    public void merge()
    {
        Iterator i = Pump.merge(
            Arrays.asList( "a", "b" ).iterator(),
            Arrays.asList().iterator(),
            Arrays.asList( "c", "d" ).iterator(),
            Arrays.asList().iterator()
        );

        assertTrue( i.hasNext() );
        assertEquals( "a", i.next() );
        assertTrue( i.hasNext() );
        assertEquals( "b", i.next() );
        assertTrue( i.hasNext() );
        assertEquals( "c", i.next() );
        assertTrue( i.hasNext() );
        assertEquals( "d", i.next() );
        assertTrue( !i.hasNext() );
    }
}
