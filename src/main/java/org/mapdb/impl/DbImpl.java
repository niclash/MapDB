/*
 *  Copyright (c) 2012 Jan Kotek
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.mapdb.impl;

import java.io.Closeable;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import org.mapdb.BTreeMap;
import org.mapdb.BTreeMapMaker;
import org.mapdb.BTreeSetMaker;
import org.mapdb.DB;
import org.mapdb.Engine;
import org.mapdb.HTreeMap;
import org.mapdb.HTreeMapMaker;
import org.mapdb.HTreeSetMaker;
import org.mapdb.Hasher;
import org.mapdb.KeySerializer;
import org.mapdb.ValueSerializer;

/**
 * A database with easy access to named maps and other collections.
 *
 * @author Jan Kotek
 */
//TODO DB uses global lock, replace it with ReadWrite lock or fine grained locking.
@SuppressWarnings( "unchecked" )
public class DbImpl implements Closeable, DB
{

    protected final boolean strictDBGet;

    /**
     * Engine which provides persistence for this DB
     */
    protected Engine engine;
    /**
     * already loaded named collections. It is important to keep collections as singletons, because of 'in-memory' locking
     */
    protected Map<String, WeakReference<?>> namesInstanciated = new HashMap<String, WeakReference<?>>();

    protected Map<IdentityWrapper, String> namesLookup =
        Collections.synchronizedMap( //TODO remove synchronized map, after DB locking is resolved
                                     new HashMap<IdentityWrapper, String>() );

    /**
     * view over named records
     */
    protected SortedMap<String, Object> catalog;

    protected static class IdentityWrapper
    {

        final Object o;

        public IdentityWrapper( Object o )
        {
            this.o = o;
        }

        @Override
        public int hashCode()
        {
            return System.identityHashCode( o );
        }

        @Override
        public boolean equals( Object v )
        {
            return ( (IdentityWrapper) v ).o == o;
        }
    }

    /**
     * Construct new DB. It is just thin layer over {@link org.mapdb.Engine} which does the real work.
     *
     * @param engine
     */
    public DbImpl( final Engine engine )
    {
        this( engine, false, false );
    }

    public DbImpl( Engine engine, boolean strictDBGet, boolean disableLocks )
    {
        if( !( engine instanceof EngineWrapper ) )
        {
            //access to Store should be prevented after `close()` was called.
            //So for this we have to wrap raw Store into EngineWrapper
            engine = new EngineWrapper( engine );
        }
        this.engine = engine;
        this.strictDBGet = strictDBGet;
        engine.getSerializerPojo().setDb( this );
        reinit();
    }

    protected void reinit()
    {
        //open name dir
        catalog = BTreeMapImpl.preinitCatalog( this );
    }

    public <A> A catGet( String name, A init )
    {
        assert ( Thread.holdsLock( DbImpl.this ) );
        A ret = (A) catalog.get( name );
        return ret != null ? ret : init;
    }

    public <A> A catGet( String name )
    {
        assert ( Thread.holdsLock( DbImpl.this ) );
        return (A) catalog.get( name );
    }

    public <A> A catPut( String name, A value )
    {
        assert ( Thread.holdsLock( DbImpl.this ) );
        catalog.put( name, value );
        return value;
    }

    public <A> A catPut( String name, A value, A retValueIfNull )
    {
        assert ( Thread.holdsLock( DbImpl.this ) );
        if( value == null )
        {
            return retValueIfNull;
        }
        catalog.put( name, value );
        return value;
    }

    @Override public String getNameForObject( Object obj )
    {
        //TODO this method should be synchronized, but it causes deadlock.
        return namesLookup.get( new IdentityWrapper( obj ) );
    }

    @Override synchronized public <K, V> HTreeMap<K, V> getHashMap( String name )
    {
        return getHashMap( name, null );
    }

    @Override synchronized public <K, V> HTreeMap<K, V> getHashMap( String name, Fun.Function1<V, K> valueCreator )
    {
        checkNotClosed();
        HTreeMap<K, V> ret = (HTreeMap<K, V>) getFromWeakCollection( name );
        if( ret != null )
        {
            return ret;
        }
        String type = catGet( name + ".type", null );
        if( type == null )
        {
            checkShouldCreate( name );
            if( engine.isReadOnly() )
            {
                Engine e = new StoreHeap();
                new DbImpl( e ).getHashMap( "a" );
                return namedPut( name,
                                 new DbImpl( new EngineWrapper.ReadOnlyEngine( e ) ).getHashMap( "a" ) );
            }
            return createHashMap( name ).make();
        }

        //check type
        checkType( type, "HashMap" );
        //open existing map
        ret = new HTreeMapImpl<K, V>( engine,
                                  (Long) catGet( name + ".counterRecid" ),
                                  (Integer) catGet( name + ".hashSalt" ),
                                  (long[]) catGet( name + ".segmentRecids" ),
                                  catGet( name + ".keySerializer", getDefaultSerializer() ),
                                  catGet( name + ".valueSerializer", getDefaultSerializer() ),
                                  catGet( name + ".expireTimeStart", 0L ),
                                  catGet( name + ".expire", 0L ),
                                  catGet( name + ".expireAccess", 0L ),
                                  catGet( name + ".expireMaxSize", 0L ),
                                  catGet( name + ".expireStoreSize", 0L ),
                                  (long[]) catGet( name + ".expireHeads", null ),
                                  (long[]) catGet( name + ".expireTails", null ),
                                  valueCreator,
                                  catGet( name + ".hasher", Hasher.BASIC ),
                                  false );

        namedPut( name, ret );
        return ret;
    }

    public <V> V namedPut( String name, Object ret )
    {
        namesInstanciated.put( name, new WeakReference<Object>( ret ) );
        namesLookup.put( new IdentityWrapper( ret ), name );
        return (V) ret;
    }

    /**
     * Returns new builder for HashMap with given name
     *
     * @param name of map to create
     *
     * @return maker, call `.make()` to create map
     *
     * @throws IllegalArgumentException if name is already used
     */
    public HTreeMapMaker createHashMap( String name )
    {
        return new HTreeMapMakerImpl( name, this );
    }

    /**
     * Creates new HashMap with more specific arguments
     *
     * @return newly created map
     *
     * @throws IllegalArgumentException if name is already used
     */
    synchronized protected <K, V> HTreeMap<K, V> createHashMap( HTreeMapMakerImpl m )
    {
        String name = m.name;
        checkNameNotExists( name );

        long expireTimeStart = 0, expire = 0, expireAccess = 0, expireMaxSize = 0, expireStoreSize = 0;
        long[] expireHeads = null, expireTails = null;

        if( m.expire != 0 || m.expireAccess != 0 || m.expireMaxSize != 0 || m.expireStoreSize != 0 )
        {
            expireTimeStart = catPut( name + ".expireTimeStart", System.currentTimeMillis() );
            expire = catPut( name + ".expire", m.expire );
            expireAccess = catPut( name + ".expireAccess", m.expireAccess );
            expireMaxSize = catPut( name + ".expireMaxSize", m.expireMaxSize );
            expireStoreSize = catPut( name + ".expireStoreSize", m.expireStoreSize );
            expireHeads = new long[ 16 ];
            expireTails = new long[ 16 ];
            for( int i = 0; i < 16; i++ )
            {
                expireHeads[ i ] = engine.put( 0L, SerializerBase.LONG );
                expireTails[ i ] = engine.put( 0L, SerializerBase.LONG );
            }
            catPut( name + ".expireHeads", expireHeads );
            catPut( name + ".expireTails", expireHeads );
        }

        if( m.hasher != null )
        {
            catPut( name + ".hasher", m.hasher );
        }

        HTreeMap<K, V> ret = new HTreeMapImpl<K, V>( engine,
                                                 catPut( name + ".counterRecid", !m.counter ? 0L : engine.put( 0L, SerializerBase.LONG ) ),
                                                 catPut( name + ".hashSalt", new Random().nextInt() ),
                                                 catPut( name + ".segmentRecids", HTreeMapImpl.preallocateSegments( engine ) ),
                                                 catPut( name + ".keySerializer", m.keySerializer, getDefaultSerializer() ),
                                                 catPut( name + ".valueSerializer", m.valueSerializer, getDefaultSerializer() ),
                                                 expireTimeStart, expire, expireAccess, expireMaxSize, expireStoreSize, expireHeads, expireTails,
                                                 (Fun.Function1<V, K>) m.valueCreator, m.hasher, false

        );

        catalog.put( name + ".type", "HashMap" );
        namedPut( name, ret );
        return ret;
    }

    @Override synchronized public <K> Set<K> getHashSet( String name )
    {
        checkNotClosed();
        Set<K> ret = (Set<K>) getFromWeakCollection( name );
        if( ret != null )
        {
            return ret;
        }
        String type = catGet( name + ".type", null );
        if( type == null )
        {
            checkShouldCreate( name );
            if( engine.isReadOnly() )
            {
                Engine e = new StoreHeap();
                new DbImpl( e ).getHashSet( "a" );
                return namedPut( name,
                                 new DbImpl( new EngineWrapper.ReadOnlyEngine( e ) ).getHashSet( "a" ) );
            }
            return createHashSet( name ).makeOrGet();
        }

        //check type
        checkType( type, "HashSet" );
        //open existing map
        ret = new HTreeMapImpl<K, Object>( engine,
                                       (Long) catGet( name + ".counterRecid" ),
                                       (Integer) catGet( name + ".hashSalt" ),
                                       (long[]) catGet( name + ".segmentRecids" ),
                                       catGet( name + ".serializer", getDefaultSerializer() ),
                                       null, 0L, 0L, 0L, 0L, 0L, null, null, null,
                                       catGet( name + ".hasher", Hasher.BASIC ),
                                       false ).keySet();

        namedPut( name, ret );
        return ret;
    }

    /**
     * Creates new HashSet
     *
     * @param name of set to create
     */
    synchronized public HTreeSetMaker createHashSet( String name )
    {
        return new HTreeSetMakerImpl( this, name );
    }

    synchronized protected <K> Set<K> createHashSet( HTreeSetMakerImpl m )
    {
        String name = m.name;
        checkNameNotExists( name );

        long expireTimeStart = 0, expire = 0, expireAccess = 0, expireMaxSize = 0, expireStoreSize = 0;
        long[] expireHeads = null, expireTails = null;

        if( m.expire != 0 || m.expireAccess != 0 || m.expireMaxSize != 0 )
        {
            expireTimeStart = catPut( name + ".expireTimeStart", System.currentTimeMillis() );
            expire = catPut( name + ".expire", m.expire );
            expireAccess = catPut( name + ".expireAccess", m.expireAccess );
            expireMaxSize = catPut( name + ".expireMaxSize", m.expireMaxSize );
            expireStoreSize = catPut( name + ".expireStoreSize", m.expireStoreSize );
            expireHeads = new long[ 16 ];
            expireTails = new long[ 16 ];
            for( int i = 0; i < 16; i++ )
            {
                expireHeads[ i ] = engine.put( 0L, SerializerBase.LONG );
                expireTails[ i ] = engine.put( 0L, SerializerBase.LONG );
            }
            catPut( name + ".expireHeads", expireHeads );
            catPut( name + ".expireTails", expireHeads );
        }

        if( m.hasher != null )
        {
            catPut( name + ".hasher", m.hasher );
        }

        HTreeMap<K, Object> ret = new HTreeMapImpl<K, Object>( engine,
                                                           catPut( name + ".counterRecid", !m.counter ? 0L : engine.put( 0L, SerializerBase.LONG ) ),
                                                           catPut( name + ".hashSalt", new Random().nextInt() ),
                                                           catPut( name + ".segmentRecids", HTreeMapImpl.preallocateSegments( engine ) ),
                                                           catPut( name + ".serializer", m.serializer, getDefaultSerializer() ),
                                                           null,
                                                           expireTimeStart, expire, expireAccess, expireMaxSize, expireStoreSize, expireHeads, expireTails,
                                                           null, m.hasher, false

        );
        Set<K> ret2 = ret.keySet();

        catalog.put( name + ".type", "HashSet" );
        namedPut( name, ret2 );
        return ret2;
    }

    @Override synchronized public <K, V> BTreeMap<K, V> getTreeMap( String name )
    {
        checkNotClosed();
        BTreeMap<K, V> ret = (BTreeMap<K, V>) getFromWeakCollection( name );
        if( ret != null )
        {
            return ret;
        }
        String type = catGet( name + ".type", null );
        if( type == null )
        {
            checkShouldCreate( name );
            if( engine.isReadOnly() )
            {
                Engine e = new StoreHeap();
                new DbImpl( e ).getTreeMap( "a" );
                return namedPut( name,
                                 new DbImpl( new EngineWrapper.ReadOnlyEngine( e ) ).getTreeMap( "a" ) );
            }
            return createTreeMap( name ).make();
        }
        checkType( type, "TreeMap" );

        ret = new BTreeMapImpl<K, V>( engine,
                                  (Long) catGet( name + ".rootRecidRef" ),
                                  catGet( name + ".maxNodeSize", 32 ),
                                  catGet( name + ".valuesOutsideNodes", false ),
                                  catGet( name + ".counterRecid", 0L ),
                                  (KeySerializer) catGet( name + ".keySerializer", new BTreeKeySerializer.BasicKeySerializer( getDefaultSerializer() ) ),
                                  catGet( name + ".valueSerializer", getDefaultSerializer() ),
                                  catGet( name + ".comparator", BTreeMapImpl.COMPARABLE_COMPARATOR ),
                                  catGet( name + ".numberOfNodeMetas", 0 ),
                                  false
        );
        namedPut( name, ret );
        return ret;
    }

    /**
     * Returns new builder for TreeMap with given name
     *
     * @param name of map to create
     *
     * @return maker, call `.make()` to create map
     *
     * @throws IllegalArgumentException if name is already used
     */
    public BTreeMapMaker createTreeMap( String name )
    {
        return new BTreeMapMakerImpl( this, name );
    }

    synchronized protected <K, V> BTreeMap<K, V> createTreeMap( final BTreeMapMakerImpl m )
    {
        String name = m.name;
        checkNameNotExists( name );
        m.keySerializer = fillNulls( m.keySerializer );
        m.keySerializer = catPut( name + ".keySerializer", m.keySerializer, new BTreeKeySerializer.BasicKeySerializer( getDefaultSerializer() ) );
        m.valueSerializer = catPut( name + ".valueSerializer", m.valueSerializer, getDefaultSerializer() );
        if( m.comparator == null )
        {
            m.comparator = m.keySerializer.getComparator();
            if( m.comparator == null )
            {
                m.comparator = BTreeMapImpl.COMPARABLE_COMPARATOR;
            }
        }

        m.comparator = catPut( name + ".comparator", m.comparator );

        if( m.pumpPresortBatchSize != -1 && m.pumpSource != null )
        {
            Comparator presortComp = new Comparator()
            {

                @Override
                public int compare( Object o1, Object o2 )
                {
                    return -m.comparator.compare( m.pumpKeyExtractor.run( o1 ), m.pumpKeyExtractor.run( o2 ) );
                }
            };

            m.pumpSource = Pump.sort( m.pumpSource, m.pumpIgnoreDuplicates, m.pumpPresortBatchSize,
                                      presortComp, getDefaultSerializer() );
        }

        long counterRecid = !m.counter ? 0L : engine.put( 0L, SerializerBase.LONG );

        long rootRecidRef;
        if( m.pumpSource == null )
        {
            rootRecidRef = BTreeMapImpl.createRootRef( engine, m.keySerializer, m.valueSerializer, m.comparator, 0 );
        }
        else
        {
            rootRecidRef = Pump.buildTreeMap(
                (Iterator<K>) m.pumpSource,
                engine,
                (Fun.Function1<K, K>) m.pumpKeyExtractor,
                (Fun.Function1<V, K>) m.pumpValueExtractor,
                m.pumpIgnoreDuplicates, m.nodeSize,
                m.valuesOutsideNodes,
                counterRecid,
                m.keySerializer,
                m.valueSerializer,
                m.comparator );
        }

        BTreeMap<K, V> ret = new BTreeMapImpl<K, V>( engine,
                                                 catPut( name + ".rootRecidRef", rootRecidRef ),
                                                 catPut( name + ".maxNodeSize", m.nodeSize ),
                                                 catPut( name + ".valuesOutsideNodes", m.valuesOutsideNodes ),
                                                 catPut( name + ".counterRecid", counterRecid ),
                                                 m.keySerializer,
                                                 m.valueSerializer,
                                                 m.comparator,
                                                 catPut( m.name + ".numberOfNodeMetas", 0 ),
                                                 false
        );
        catalog.put( name + ".type", "TreeMap" );
        namedPut( name, ret );
        return ret;
    }

    /**
     * Replace nulls in tuple serializers with default (Comparable) values
     *
     * @param keySerializer with nulls
     *
     * @return keySerializers which does not contain any nulls
     */
    protected <K> KeySerializer<K> fillNulls( KeySerializer<K> keySerializer )
    {
        if( keySerializer == null )
        {
            return null;
        }
        if( keySerializer instanceof BTreeKeySerializer.Tuple2KeySerializer )
        {
            BTreeKeySerializer.Tuple2KeySerializer<?, ?> s =
                (BTreeKeySerializer.Tuple2KeySerializer<?, ?>) keySerializer;
            return new BTreeKeySerializer.Tuple2KeySerializer( s, getDefaultSerializer() );
        }
        if( keySerializer instanceof BTreeKeySerializer.Tuple3KeySerializer )
        {
            BTreeKeySerializer.Tuple3KeySerializer<?, ?, ?> s =
                (BTreeKeySerializer.Tuple3KeySerializer<?, ?, ?>) keySerializer;
            return new BTreeKeySerializer.Tuple3KeySerializer( s, getDefaultSerializer());
        }
        if( keySerializer instanceof BTreeKeySerializer.Tuple4KeySerializer )
        {
            BTreeKeySerializer.Tuple4KeySerializer<?, ?, ?, ?> s =
                (BTreeKeySerializer.Tuple4KeySerializer<?, ?, ?, ?>) keySerializer;
            return new BTreeKeySerializer.Tuple4KeySerializer(s, getDefaultSerializer());
        }

        if( keySerializer instanceof BTreeKeySerializer.Tuple5KeySerializer )
        {
            BTreeKeySerializer.Tuple5KeySerializer<?, ?, ?, ?, ?> s =
                (BTreeKeySerializer.Tuple5KeySerializer<?, ?, ?, ?, ?>) keySerializer;
            return new BTreeKeySerializer.Tuple5KeySerializer( s, getDefaultSerializer() );
        }

        if( keySerializer instanceof BTreeKeySerializer.Tuple6KeySerializer )
        {
            BTreeKeySerializer.Tuple6KeySerializer<?, ?, ?, ?, ?, ?> s =
                (BTreeKeySerializer.Tuple6KeySerializer<?, ?, ?, ?, ?, ?>) keySerializer;
            return new BTreeKeySerializer.Tuple6KeySerializer(s, getDefaultSerializer() );
        }

        return keySerializer;
    }

    @Override public SortedMap<String, Object> getCatalog()
    {
        return catalog;
    }

    @Override synchronized public <K> NavigableSet<K> getTreeSet( String name )
    {
        checkNotClosed();
        NavigableSet<K> ret = (NavigableSet<K>) getFromWeakCollection( name );
        if( ret != null )
        {
            return ret;
        }
        String type = catGet( name + ".type", null );
        if( type == null )
        {
            checkShouldCreate( name );
            if( engine.isReadOnly() )
            {
                Engine e = new StoreHeap();
                new DbImpl( e ).getTreeSet( "a" );
                return namedPut( name,
                                 new DbImpl( new EngineWrapper.ReadOnlyEngine( e ) ).getTreeSet( "a" ) );
            }
            return createTreeSet( name ).make();
        }
        checkType( type, "TreeSet" );

        ret = new BTreeMapImpl<K, Object>( engine,
                                       (Long) catGet( name + ".rootRecidRef" ),
                                       catGet( name + ".maxNodeSize", 32 ),
                                       false,
                                       catGet( name + ".counterRecid", 0L ),
                                       (KeySerializer) catGet( name + ".keySerializer", new BTreeKeySerializer.BasicKeySerializer( getDefaultSerializer() ) ),
                                       null,
                                       catGet( name + ".comparator", BTreeMapImpl.COMPARABLE_COMPARATOR ),
                                       catGet( name + ".numberOfNodeMetas", 0 ),
                                       false
        ).keySet();

        namedPut( name, ret );
        return ret;
    }

    /**
     * Creates new TreeSet.
     *
     * @param name of set to create
     *
     * @return maker used to construct set
     *
     * @throws IllegalArgumentException if name is already used
     */
    synchronized public BTreeSetMaker createTreeSet( String name )
    {
        return new BTreeSetMakerImpl( this, name );
    }

    synchronized public <K> NavigableSet<K> createTreeSet( BTreeSetMakerImpl m )
    {
        checkNameNotExists( m.name );
        m.serializer = fillNulls( m.serializer );
        m.serializer = catPut( m.name + ".keySerializer", m.serializer, new BTreeKeySerializer.BasicKeySerializer( getDefaultSerializer() ) );
        if( m.comparator == null )
        {
            m.comparator = m.serializer.getComparator();
            if( m.comparator == null )
            {
                m.comparator = BTreeMapImpl.COMPARABLE_COMPARATOR;
            }
        }
        m.comparator = catPut( m.name + ".comparator", m.comparator );

        if( m.pumpPresortBatchSize != -1 )
        {
            m.pumpSource = Pump.sort( m.pumpSource, m.pumpIgnoreDuplicates, m.pumpPresortBatchSize, Collections.reverseOrder( m.comparator ), getDefaultSerializer() );
        }

        long counterRecid = !m.counter ? 0L : engine.put( 0L, SerializerBase.LONG );
        long rootRecidRef;

        if( m.pumpSource == null )
        {
            rootRecidRef = BTreeMapImpl.createRootRef( engine, m.serializer, null, m.comparator, 0 );
        }
        else
        {
            rootRecidRef = Pump.buildTreeMap(
                (Iterator<Object>) m.pumpSource,
                engine,
                Fun.extractNoTransform(),
                null,
                m.pumpIgnoreDuplicates,
                m.nodeSize,
                false,
                counterRecid,
                (BTreeKeySerializer<Object>) m.serializer,
                null,
                m.comparator );
        }

        NavigableSet<K> ret = new BTreeMapImpl<K, Object>(
            engine,
            catPut( m.name + ".rootRecidRef", rootRecidRef ),
            catPut( m.name + ".maxNodeSize", m.nodeSize ),
            false,
            catPut( m.name + ".counterRecid", counterRecid ),
            (BTreeKeySerializer<K>) m.serializer,
            null,
            (Comparator<K>) m.comparator,
            catPut( m.name + ".numberOfNodeMetas", 0 ),
            false
        ).keySet();
        catalog.put( m.name + ".type", "TreeSet" );
        namedPut( m.name, ret );
        return ret;
    }

    @Override synchronized public <E> BlockingQueue<E> getQueue( String name )
    {
        checkNotClosed();
        Queues.Queue<E> ret = (Queues.Queue<E>) getFromWeakCollection( name );
        if( ret != null )
        {
            return ret;
        }
        String type = catGet( name + ".type", null );
        if( type == null )
        {
            checkShouldCreate( name );
            if( engine.isReadOnly() )
            {
                Engine e = new StoreHeap();
                new DbImpl( e ).getQueue( "a" );
                return namedPut( name,
                                 new DbImpl( new EngineWrapper.ReadOnlyEngine( e ) ).getQueue( "a" ) );
            }
            return createQueue( name, null, true );
        }
        checkType( type, "Queue" );

        ret = new Queues.Queue<E>( engine,
                                   (ValueSerializer<E>) catGet( name + ".serializer", getDefaultSerializer() ),
                                   (Long) catGet( name + ".headRecid" ),
                                   (Long) catGet( name + ".tailRecid" ),
                                   (Boolean) catGet( name + ".useLocks" )
        );

        namedPut( name, ret );
        return ret;
    }

    @Override synchronized public <E> BlockingQueue<E> createQueue( String name, ValueSerializer<E> serializer, boolean useLocks )
    {
        checkNameNotExists( name );

        long node = engine.put( Queues.SimpleQueue.Node.EMPTY, new Queues.SimpleQueue.NodeSerializer( serializer ) );
        long headRecid = engine.put( node, SerializerBase.LONG );
        long tailRecid = engine.put( node, SerializerBase.LONG );

        Queues.Queue<E> ret = new Queues.Queue<E>( engine,
                                                   catPut( name + ".serializer", serializer, getDefaultSerializer() ),
                                                   catPut( name + ".headRecid", headRecid ),
                                                   catPut( name + ".tailRecid", tailRecid ),
                                                   catPut( name + ".useLocks", useLocks )
        );
        catalog.put( name + ".type", "Queue" );
        namedPut( name, ret );
        return ret;
    }

    @Override synchronized public <E> BlockingQueue<E> getStack( String name )
    {
        checkNotClosed();
        Queues.Stack<E> ret = (Queues.Stack<E>) getFromWeakCollection( name );
        if( ret != null )
        {
            return ret;
        }
        String type = catGet( name + ".type", null );
        if( type == null )
        {
            checkShouldCreate( name );
            if( engine.isReadOnly() )
            {
                Engine e = new StoreHeap();
                new DbImpl( e ).getStack( "a" );
                return namedPut( name,
                                 new DbImpl( new EngineWrapper.ReadOnlyEngine( e ) ).getStack( "a" ) );
            }
            return createStack( name, null, true );
        }

        checkType( type, "Stack" );

        ret = new Queues.Stack<E>( engine,
                                   (ValueSerializer<E>) catGet( name + ".serializer", getDefaultSerializer() ),
                                   (Long) catGet( name + ".headRecid" ),
                                   (Boolean) catGet( name + ".useLocks" )
        );

        namedPut( name, ret );
        return ret;
    }

    @Override synchronized public <E> BlockingQueue<E> createStack( String name, ValueSerializer<E> serializer, boolean useLocks )
    {
        checkNameNotExists( name );

        long node = engine.put( Queues.SimpleQueue.Node.EMPTY, new Queues.SimpleQueue.NodeSerializer( serializer ) );
        long headRecid = engine.put( node, SerializerBase.LONG );

        Queues.Stack<E> ret = new Queues.Stack<E>( engine,
                                                   catPut( name + ".serializer", serializer, getDefaultSerializer() ),
                                                   catPut( name + ".headRecid", headRecid ),
                                                   catPut( name + ".useLocks", useLocks )
        );
        catalog.put( name + ".type", "Stack" );
        namedPut( name, ret );
        return ret;
    }

    @Override synchronized public <E> BlockingQueue<E> getCircularQueue( String name )
    {
        checkNotClosed();
        BlockingQueue<E> ret = (BlockingQueue<E>) getFromWeakCollection( name );
        if( ret != null )
        {
            return ret;
        }
        String type = catGet( name + ".type", null );
        if( type == null )
        {
            checkShouldCreate( name );
            if( engine.isReadOnly() )
            {
                Engine e = new StoreHeap();
                new DbImpl( e ).getCircularQueue( "a" );
                return namedPut( name,
                                 new DbImpl( new EngineWrapper.ReadOnlyEngine( e ) ).getCircularQueue( "a" ) );
            }
            return createCircularQueue( name, null, 1024 );
        }

        checkType( type, "CircularQueue" );

        ret = new Queues.CircularQueue<E>( engine,
                                           (ValueSerializer<E>) catGet( name + ".serializer", getDefaultSerializer() ),
                                           (Long) catGet( name + ".headRecid" ),
                                           (Long) catGet( name + ".headInsertRecid" ),
                                           (Long) catGet( name + ".size" )
        );

        namedPut( name, ret );
        return ret;
    }

    @Override synchronized public <E> BlockingQueue<E> createCircularQueue( String name,
                                                                  ValueSerializer<E> serializer,
                                                                  long size
    )
    {
        checkNameNotExists( name );
        if( serializer == null )
        {
            serializer = getDefaultSerializer();
        }

//        long headerRecid = engine.put(0L, Serializer.LONG);
        //insert N Nodes empty nodes into a circle
        long prevRecid = 0;
        long firstRecid = 0;
        ValueSerializer<Queues.SimpleQueue.Node<E>> nodeSer = new Queues.SimpleQueue.NodeSerializer<E>( serializer );
        for( long i = 0; i < size; i++ )
        {
            Queues.SimpleQueue.Node<E> n = new Queues.SimpleQueue.Node<E>( prevRecid, null );
            prevRecid = engine.put( n, nodeSer );
            if( firstRecid == 0 )
            {
                firstRecid = prevRecid;
            }
        }
        //update first node to point to last recid
        engine.update( firstRecid, new Queues.SimpleQueue.Node<E>( prevRecid, null ), nodeSer );

        long headRecid = engine.put( prevRecid, SerializerBase.LONG );
        long headInsertRecid = engine.put( prevRecid, SerializerBase.LONG );

        Queues.CircularQueue<E> ret = new Queues.CircularQueue<E>( engine,
                                                                   catPut( name + ".serializer", serializer ),
                                                                   catPut( name + ".headRecid", headRecid ),
                                                                   catPut( name + ".headInsertRecid", headInsertRecid ),
                                                                   catPut( name + ".size", size )
        );
        catalog.put( name + ".type", "CircularQueue" );
        namedPut( name, ret );
        return ret;
    }

    @Override synchronized public Atomic.Long createAtomicLong( String name, long initValue )
    {
        checkNameNotExists( name );
        long recid = engine.put( initValue, SerializerBase.LONG );
        Atomic.Long ret = new Atomic.Long( engine,
                                           catPut( name + ".recid", recid )
        );
        catalog.put( name + ".type", "AtomicLong" );
        namedPut( name, ret );
        return ret;
    }

    @Override synchronized public Atomic.Long getAtomicLong( String name )
    {
        checkNotClosed();
        Atomic.Long ret = (Atomic.Long) getFromWeakCollection( name );
        if( ret != null )
        {
            return ret;
        }
        String type = catGet( name + ".type", null );
        if( type == null )
        {
            checkShouldCreate( name );
            if( engine.isReadOnly() )
            {
                Engine e = new StoreHeap();
                new DbImpl( e ).getAtomicLong( "a" );
                return namedPut( name,
                                 new DbImpl( new EngineWrapper.ReadOnlyEngine( e ) ).getAtomicLong( "a" ) );
            }
            return createAtomicLong( name, 0L );
        }
        checkType( type, "AtomicLong" );

        ret = new Atomic.Long( engine, (Long) catGet( name + ".recid" ) );
        namedPut( name, ret );
        return ret;
    }

    @Override synchronized public Atomic.Integer createAtomicInteger( String name, int initValue )
    {
        checkNameNotExists( name );
        long recid = engine.put( initValue, SerializerBase.INTEGER );
        Atomic.Integer ret = new Atomic.Integer( engine,
                                                 catPut( name + ".recid", recid )
        );
        catalog.put( name + ".type", "AtomicInteger" );
        namedPut( name, ret );
        return ret;
    }

    @Override synchronized public Atomic.Integer getAtomicInteger( String name )
    {
        checkNotClosed();
        Atomic.Integer ret = (Atomic.Integer) getFromWeakCollection( name );
        if( ret != null )
        {
            return ret;
        }
        String type = catGet( name + ".type", null );
        if( type == null )
        {
            checkShouldCreate( name );
            if( engine.isReadOnly() )
            {
                Engine e = new StoreHeap();
                new DbImpl( e ).getAtomicInteger( "a" );
                return namedPut( name,
                                 new DbImpl( new EngineWrapper.ReadOnlyEngine( e ) ).getAtomicInteger( "a" ) );
            }
            return createAtomicInteger( name, 0 );
        }
        checkType( type, "AtomicInteger" );

        ret = new Atomic.Integer( engine, (Long) catGet( name + ".recid" ) );
        namedPut( name, ret );
        return ret;
    }

    @Override synchronized public Atomic.Boolean createAtomicBoolean( String name, boolean initValue )
    {
        checkNameNotExists( name );
        long recid = engine.put( initValue, SerializerBase.BOOLEAN );
        Atomic.Boolean ret = new Atomic.Boolean( engine,
                                                 catPut( name + ".recid", recid )
        );
        catalog.put( name + ".type", "AtomicBoolean" );
        namedPut( name, ret );
        return ret;
    }

    @Override synchronized public Atomic.Boolean getAtomicBoolean( String name )
    {
        checkNotClosed();
        Atomic.Boolean ret = (Atomic.Boolean) getFromWeakCollection( name );
        if( ret != null )
        {
            return ret;
        }
        String type = catGet( name + ".type", null );
        if( type == null )
        {
            checkShouldCreate( name );
            if( engine.isReadOnly() )
            {
                Engine e = new StoreHeap();
                new DbImpl( e ).getAtomicBoolean( "a" );
                return namedPut( name,
                                 new DbImpl( new EngineWrapper.ReadOnlyEngine( e ) ).getAtomicBoolean( "a" ) );
            }
            return createAtomicBoolean( name, false );
        }
        checkType( type, "AtomicBoolean" );

        ret = new Atomic.Boolean( engine, (Long) catGet( name + ".recid" ) );
        namedPut( name, ret );
        return ret;
    }

    public void checkShouldCreate( String name )
    {
        if( strictDBGet )
        {
            throw new NoSuchElementException( "No record with this name was found: " + name );
        }
    }

    @Override synchronized public Atomic.String createAtomicString( String name, String initValue )
    {
        checkNameNotExists( name );
        if( initValue == null )
        {
            throw new IllegalArgumentException( "initValue may not be null" );
        }
        long recid = engine.put( initValue, SerializerBase.STRING_NOSIZE );
        Atomic.String ret = new Atomic.String( engine,
                                               catPut( name + ".recid", recid )
        );
        catalog.put( name + ".type", "AtomicString" );
        namedPut( name, ret );
        return ret;
    }

    @Override synchronized public Atomic.String getAtomicString( String name )
    {
        checkNotClosed();
        Atomic.String ret = (Atomic.String) getFromWeakCollection( name );
        if( ret != null )
        {
            return ret;
        }
        String type = catGet( name + ".type", null );
        if( type == null )
        {
            checkShouldCreate( name );
            if( engine.isReadOnly() )
            {
                Engine e = new StoreHeap();
                new DbImpl( e ).getAtomicString( "a" );
                return namedPut( name,
                                 new DbImpl( new EngineWrapper.ReadOnlyEngine( e ) ).getAtomicString( "a" ) );
            }
            return createAtomicString( name, "" );
        }
        checkType( type, "AtomicString" );

        ret = new Atomic.String( engine, (Long) catGet( name + ".recid" ) );
        namedPut( name, ret );
        return ret;
    }

    @Override synchronized public <E> Atomic.Var<E> createAtomicVar( String name, E initValue, ValueSerializer<E> serializer )
    {
        checkNameNotExists( name );
        if( serializer == null )
        {
            serializer = getDefaultSerializer();
        }
        long recid = engine.put( initValue, serializer );
        Atomic.Var ret = new Atomic.Var( engine,
                                         catPut( name + ".recid", recid ),
                                         catPut( name + ".serializer", serializer )
        );
        catalog.put( name + ".type", "AtomicVar" );
        namedPut( name, ret );
        return ret;
    }

    @Override synchronized public <E> Atomic.Var<E> getAtomicVar( String name )
    {
        checkNotClosed();
        Atomic.Var ret = (Atomic.Var) getFromWeakCollection( name );
        if( ret != null )
        {
            return ret;
        }
        String type = catGet( name + ".type", null );
        if( type == null )
        {
            checkShouldCreate( name );
            if( engine.isReadOnly() )
            {
                Engine e = new StoreHeap();
                new DbImpl( e ).getAtomicVar( "a" );
                return namedPut( name,
                                 new DbImpl( new EngineWrapper.ReadOnlyEngine( e ) ).getAtomicVar( "a" ) );
            }
            return createAtomicVar( name, null, getDefaultSerializer() );
        }
        checkType( type, "AtomicVar" );

        ret = new Atomic.Var( engine, (Long) catGet( name + ".recid" ), (ValueSerializer) catGet( name + ".serializer" ) );
        namedPut( name, ret );
        return ret;
    }

    @Override synchronized public <E> E get( String name )
    {
        String type = catGet( name + ".type" );
        if( type == null )
        {
            return null;
        }
        if( "HashMap".equals( type ) )
        {
            return (E) getHashMap( name );
        }
        if( "HashSet".equals( type ) )
        {
            return (E) getHashSet( name );
        }
        if( "TreeMap".equals( type ) )
        {
            return (E) getTreeMap( name );
        }
        if( "TreeSet".equals( type ) )
        {
            return (E) getTreeSet( name );
        }
        if( "AtomicBoolean".equals( type ) )
        {
            return (E) getAtomicBoolean( name );
        }
        if( "AtomicInteger".equals( type ) )
        {
            return (E) getAtomicInteger( name );
        }
        if( "AtomicLong".equals( type ) )
        {
            return (E) getAtomicLong( name );
        }
        if( "AtomicString".equals( type ) )
        {
            return (E) getAtomicString( name );
        }
        if( "AtomicVar".equals( type ) )
        {
            return (E) getAtomicVar( name );
        }
        if( "Queue".equals( type ) )
        {
            return (E) getQueue( name );
        }
        if( "Stack".equals( type ) )
        {
            return (E) getStack( name );
        }
        if( "CircularQueue".equals( type ) )
        {
            return (E) getCircularQueue( name );
        }
        throw new AssertionError( "Unknown type: " + name );
    }

    @Override synchronized public boolean exists( String name )
    {
        return catGet( name + ".type" ) != null;
    }

    @Override synchronized public void delete( String name )
    {
        Object r = get( name );
        if( r instanceof Atomic.Boolean )
        {
            engine.delete( ( (Atomic.Boolean) r ).recid, SerializerBase.BOOLEAN );
        }
        else if( r instanceof Atomic.Integer )
        {
            engine.delete( ( (Atomic.Integer) r ).recid, SerializerBase.INTEGER );
        }
        else if( r instanceof Atomic.Long )
        {
            engine.delete( ( (Atomic.Long) r ).recid, SerializerBase.LONG );
        }
        else if( r instanceof Atomic.String )
        {
            engine.delete( ( (Atomic.String) r ).recid, SerializerBase.STRING_NOSIZE );
        }
        else if( r instanceof Atomic.Var )
        {
            engine.delete( ( (Atomic.Var) r ).recid, ( (Atomic.Var) r ).serializer );
        }
        else if( r instanceof Queue )
        {
            //drain queue
            Queue q = (Queue) r;
            while( q.poll() != null )
            {
                //do nothing
            }
        }
        else if( r instanceof HTreeMap || r instanceof HTreeMapImpl.KeySet )
        {
            HTreeMapImpl m = ( r instanceof HTreeMapImpl ) ? (HTreeMapImpl) r : (HTreeMapImpl) ((HTreeMapImpl.KeySet) r ).parent();
            m.clear();
            //delete segments
            for( long segmentRecid : m.segmentRecids )
            {
                engine.delete( segmentRecid, HTreeMapImpl.DIR_SERIALIZER );
            }
        }
        else if( r instanceof BTreeMap || r instanceof BTreeMapImpl.KeySet )
        {
            BTreeMapImpl m = ( r instanceof BTreeMapImpl ) ? (BTreeMapImpl) r : (BTreeMapImpl) ( (BTreeMapImpl.KeySet) r ).m;

            //TODO on BTreeMap recursively delete all nodes
            m.clear();

            if( m.counter != null )
            {
                engine.delete( m.counter.recid, SerializerBase.LONG );
            }
        }

        for( String n : catalog.keySet() )
        {
            if( !n.startsWith( name ) )
            {
                continue;
            }
            String suffix = n.substring( name.length() );
            if( suffix.charAt( 0 ) == '.' && suffix.length() > 1 && !suffix.substring( 1 ).contains( "." ) )
            {
                catalog.remove( n );
            }
        }
        namesInstanciated.remove( name );
        namesLookup.remove( new IdentityWrapper( r ) );
    }

    @Override synchronized public Map<String, Object> getAll()
    {
        TreeMap<String, Object> ret = new TreeMap<String, Object>();

        for( String name : catalog.keySet() )
        {
            if( !name.endsWith( ".type" ) )
            {
                continue;
            }
            name = name.substring( 0, name.length() - 5 );
            ret.put( name, get( name ) );
        }

        return Collections.unmodifiableMap( ret );
    }

    @Override synchronized public void rename( String oldName, String newName )
    {
        if( oldName.equals( newName ) )
        {
            return;
        }

        Map<String, Object> sub = catalog.tailMap( oldName );
        List<String> toRemove = new ArrayList<String>();

        for( String param : sub.keySet() )
        {
            if( !param.startsWith( oldName ) )
            {
                break;
            }

            String suffix = param.substring( oldName.length() );
            catalog.put( newName + suffix, catalog.get( param ) );
            toRemove.add( param );
        }
        if( toRemove.isEmpty() )
        {
            throw new NoSuchElementException( "Could not rename, name does not exist: " + oldName );
        }

        WeakReference old = namesInstanciated.remove( oldName );
        if( old != null )
        {
            Object old2 = old.get();
            if( old2 != null )
            {
                namesLookup.remove( new IdentityWrapper( old2 ) );
                namedPut( newName, old2 );
            }
        }
        for( String param : toRemove )
        {
            catalog.remove( param );
        }
    }

    @Override public void checkNameNotExists( String name )
    {
        if( catalog.get( name + ".type" ) != null )
        {
            throw new IllegalArgumentException( "Name already used: " + name );
        }
    }

    /**
     * Closes database.
     * All other methods will throw 'IllegalAccessError' after this method was called.
     * <p/>
     * !! it is necessary to call this method before JVM exits!!
     */
    synchronized public void close()
    {
        if( engine == null )
        {
            return;
        }
        engine.close();
        //dereference db to prevent memory leaks
        engine = EngineWrapper.CLOSED;
        namesInstanciated = Collections.unmodifiableMap( new HashMap() );
        namesLookup = Collections.unmodifiableMap( new HashMap() );
    }

    /**
     * All collections are weakly referenced to prevent two instances of the same collection in memory.
     * This is mainly for locking, two instances of the same lock would not simply work.
     */
    public Object getFromWeakCollection( String name )
    {
        WeakReference<?> r = namesInstanciated.get( name );
        if( r == null )
        {
            return null;
        }
        Object o = r.get();
        if( o == null )
        {
            namesInstanciated.remove( name );
        }
        return o;
    }

    @Override public void checkNotClosed()
    {
        if( engine == null )
        {
            throw new IllegalAccessError( "DB was already closed" );
        }
    }

    @Override public synchronized boolean isClosed()
    {
        return engine == null || engine.isClosed();
    }

    @Override synchronized public void commit()
    {
        checkNotClosed();
        engine.commit();
    }

    @Override synchronized public void rollback()
    {
        checkNotClosed();
        engine.rollback();
    }

    @Override synchronized public void compact()
    {
        engine.compact();
    }

    @Override synchronized public DB snapshot()
    {
        Engine snapshot = TxEngine.createSnapshotFor( engine );
        return new DbImpl( snapshot );
    }

    /**
     * @return default serializer used in this DB, it handles POJO and other stuff.
     */
    public ValueSerializer getDefaultSerializer()
    {
        return engine.getSerializerPojo();
    }

    @Override public Engine getEngine()
    {
        return engine;
    }

    public void checkType( String type, String expected )
    {
        if( !expected.equals( type ) )
        {
            throw new IllegalArgumentException( "Wrong type: " + type );
        }
    }
}
