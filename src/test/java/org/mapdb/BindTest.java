package org.mapdb;

import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mapdb.impl.Fun.Function2;
import static org.mapdb.impl.Fun.Tuple2;
import static org.mapdb.impl.Fun.t2;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class BindTest
{

    BTreeMap<Integer, String> m;

    @Before
    public void init()
    {
        m = DBMaker.newMemoryDB().make().getTreeMap( "test" );
    }

    String[] split( String s )
    {
        if( s == null )
        {
            return null;
        }
        String[] ret = new String[ s.length() ];
        for( int i = 0; i < ret.length; i++ )
        {
            ret[ i ] = "" + s.charAt( i );
        }
        return ret;
    }

    @Test
    public void secondary_values()
    {
        m.put( 1, "jedna" );
        m.put( 2, "dve" );

        Set<Tuple2<Integer, String>> sec = new TreeSet<Tuple2<Integer, String>>();

        Bind.secondaryValues( m, sec, new Function2<String[], Integer, String>()
        {
            @Override
            public String[] run( Integer integer, String s )
            {
                return split( s );
            }
        } );

        //filled if empty
        assertEquals( 5 + 3, sec.size() );
        assert ( sec.contains( t2( 2, "d" ) ) );
        assert ( sec.contains( t2( 2, "v" ) ) );
        assert ( sec.contains( t2( 2, "e" ) ) );

        //old values preserved
        m.put( 2, "dvea" );
        assertEquals( 5 + 4, sec.size() );
        assert ( sec.contains( t2( 2, "d" ) ) );
        assert ( sec.contains( t2( 2, "v" ) ) );
        assert ( sec.contains( t2( 2, "e" ) ) );
        assert ( sec.contains( t2( 2, "a" ) ) );

        //old values deleted
        m.put( 2, "dva" );
        assertEquals( 5 + 3, sec.size() );
        assert ( sec.contains( t2( 2, "d" ) ) );
        assert ( sec.contains( t2( 2, "v" ) ) );
        assert ( sec.contains( t2( 2, "a" ) ) );

        //all removed on delete
        m.remove( 2 );
        assertEquals( 5, sec.size() );

        //all added on put
        m.put( 2, "dva" );
        assertEquals( 5 + 3, sec.size() );
        assert ( sec.contains( t2( 2, "d" ) ) );
        assert ( sec.contains( t2( 2, "v" ) ) );
        assert ( sec.contains( t2( 2, "a" ) ) );
    }

    @Test
    public void secondary_keys()
    {
        m.put( 1, "jedna" );
        m.put( 2, "dve" );

        Set<Tuple2<String, Integer>> sec = new TreeSet<Tuple2<String, Integer>>();

        Bind.secondaryKeys( m, sec, new Function2<String[], Integer, String>()
        {
            @Override
            public String[] run( Integer integer, String s )
            {
                return split( s );
            }
        } );

        //filled if empty
        assertEquals( 5 + 3, sec.size() );
        assert ( sec.contains( t2( "d", 2 ) ) );
        assert ( sec.contains( t2( "v", 2 ) ) );
        assert ( sec.contains( t2( "e", 2 ) ) );

        //old values preserved
        m.put( 2, "dvea" );
        assertEquals( 5 + 4, sec.size() );
        assert ( sec.contains( t2( "d", 2 ) ) );
        assert ( sec.contains( t2( "v", 2 ) ) );
        assert ( sec.contains( t2( "e", 2 ) ) );
        assert ( sec.contains( t2( "a", 2 ) ) );

        //old values deleted
        m.put( 2, "dva" );
        assertEquals( 5 + 3, sec.size() );
        assert ( sec.contains( t2( "d", 2 ) ) );
        assert ( sec.contains( t2( "v", 2 ) ) );
        assert ( sec.contains( t2( "a", 2 ) ) );

        //all removed on delete
        m.remove( 2 );
        assertEquals( 5, sec.size() );

        //all added on put
        m.put( 2, "dva" );
        assertEquals( 5 + 3, sec.size() );
        assert ( sec.contains( t2( "d", 2 ) ) );
        assert ( sec.contains( t2( "v", 2 ) ) );
        assert ( sec.contains( t2( "a", 2 ) ) );
    }

    @Test
    public void htreemap_listeners()
    {
        mapListeners( DBMaker.newMemoryDB().transactionDisable().make().getHashMap( "test" ) );
    }

    @Test
    public void btreemap_listeners()
    {
        mapListeners( DBMaker.newMemoryDB().transactionDisable().make().getTreeMap( "test" ) );
    }

    void mapListeners( Bind.MapWithModificationListener test )
    {
        final AtomicReference rkey = new AtomicReference();
        final AtomicReference roldVal = new AtomicReference();
        final AtomicReference rnewVal = new AtomicReference();

        test.modificationListenerAdd( new Bind.MapListener()
        {
            @Override
            public void update( Object key, Object oldVal, Object newVal )
            {
                rkey.set( key );
                roldVal.set( oldVal );
                rnewVal.set( newVal );
            }
        } );

        int max = (int) 1e6;
        Random r = new Random();
        for( int i = 0; i < max; i++ )
        {
            Integer k = r.nextInt( max / 100 );
            Integer v = k * 1000;
            Integer vold = null;

            if( test.containsKey( k ) )
            {
                vold = v * 10;
                test.put( k, vold );
            }

            test.put( k, v );
            assertEquals( k, rkey.get() );
            assertEquals( v, rnewVal.get() );
            assertEquals( vold, roldVal.get() );

            final int m = i % 20;
            if( m == 1 )
            {
                test.remove( k );
                assertEquals( k, rkey.get() );
                assertEquals( null, rnewVal.get() );
                assertEquals( v, roldVal.get() );
            }
            else if( m == 2 )
            {
                test.put( k, i * 20 );
                assertEquals( k, rkey.get() );
                assertEquals( i * 20, rnewVal.get() );
                assertEquals( v, roldVal.get() );
            }
            else if( m == 3 && !test.containsKey( i + 1 ) )
            {
                ( (ConcurrentMap) test ).putIfAbsent( i + 1, i + 2 );
                assertEquals( i + 1, rkey.get() );
                assertEquals( i + 2, rnewVal.get() );
                assertEquals( null, roldVal.get() );
            }
            else if( m == 4 )
            {
                ( (ConcurrentMap) test ).remove( k, v );
                assertEquals( k, rkey.get() );
                assertEquals( null, rnewVal.get() );
                assertEquals( v, roldVal.get() );
            }
            else if( m == 5 )
            {
                ( (ConcurrentMap) test ).replace( k, v, i * i );
                assertEquals( k, rkey.get() );
                assertEquals( i * i, rnewVal.get() );
                assertEquals( v, roldVal.get() );
            }
            else if( m == 5 )
            {
                ( (ConcurrentMap) test ).replace( k, i * i );
                assertEquals( k, rkey.get() );
                assertEquals( i * i, rnewVal.get() );
                assertEquals( v, roldVal.get() );
            }
        }
    }
}
