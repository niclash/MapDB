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
package org.mapdb.impl.htree;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.mapdb.Atomic;
import org.mapdb.Bind;
import org.mapdb.Engine;
import org.mapdb.HTreeMap;
import org.mapdb.Hasher;
import org.mapdb.SerializerFactory;
import org.mapdb.ValueSerializer;
import org.mapdb.impl.CC;
import org.mapdb.impl.Fun;
import org.mapdb.impl.Store;
import org.mapdb.impl.engine.TxEngine;
import org.mapdb.impl.binaryserializer.DirValueSerializer;
import org.mapdb.impl.binaryserializer.LinkedNodeValueSerializer;
import org.mapdb.impl.binaryserializer.SerializerBase;

/**
 * Thread safe concurrent HashMap
 * <p/>
 * This map uses full 32bit hash from beginning, There is no initial load factor and rehash.
 * Technically it is not hash table, but hash tree with nodes expanding when they become full.
 * <p/>
 * This map is suitable for number of records  1e9 and over.
 * Larger number of records will increase hash collisions and performance
 * will degrade linearly with number of records (separate chaining).
 * <p/>
 * Concurrent scalability is achieved by splitting HashMap into 16 segments, each with separate lock.
 * Very similar to {@link java.util.concurrent.ConcurrentHashMap}
 *
 * @author Jan Kotek
 */
@SuppressWarnings( { "unchecked", "rawtypes" } )
public class HTreeMapImpl<K, V> extends AbstractMap<K, V> implements HTreeMap<K,V>
{

    protected static final Logger LOG = Logger.getLogger( HTreeMap.class.getName() );

    protected static final int BUCKET_OVERFLOW = 4;

    protected static final int DIV8 = 3;
    protected static final int MOD8 = 0x7;

    /**
     * is this a Map or Set?  if false, entries do not have values, only keys are allowed
     */
    protected final boolean hasValues;

    /**
     * Salt added to hash before rehashing, so it is harder to trigger hash collision attack.
     */
    protected final int hashSalt;

    protected final Atomic.Long counter;

//    protected final KeySerializer<K> keySerializer;
//    protected final ValueSerializer<V> valueSerializer;
    protected final SerializerFactory serializerFactory;

    protected final Hasher<K> hasher;

    protected final Engine engine;

    protected final boolean expireFlag;
    protected final long expireTimeStart;
    protected final long expire;
    protected final boolean expireAccessFlag;
    protected final long expireAccess;
    protected final long expireMaxSize;
    protected final long expireStoreSize;
    protected final boolean expireMaxSizeFlag;

    protected final long[] expireHeads;
    protected final long[] expireTails;

    protected final Fun.Function1<V, K> valueCreator;

    protected final CountDownLatch closeLatch = new CountDownLatch( 2 );

    protected final Runnable closeListener = new Runnable()
    {
        @Override
        public void run()
        {
            if( closeLatch.getCount() > 1 )
            {
                closeLatch.countDown();
            }

            try
            {
                closeLatch.await();
            }
            catch( InterruptedException e )
            {
                throw new RuntimeException( e );
            }

            HTreeMapImpl.this.engine.closeListenerUnregister( HTreeMapImpl.this.closeListener );
        }
    };
    private final Class<K> keyType;
    private final Class<V> valueType;

    protected final ValueSerializer<HTreeLinkedNode<K, V>> LN_SERIALIZER;

    public static final ValueSerializer<long[][]> DIR_SERIALIZER = new DirValueSerializer();

    /**
     * list of segments, this is immutable
     */
    public final long[] segmentRecids;

    protected final ReentrantReadWriteLock[] segmentLocks = new ReentrantReadWriteLock[ 16 ];

    {
        for( int i = 0; i < 16; i++ )
        {
            segmentLocks[ i ] = new ReentrantReadWriteLock( CC.FAIR_LOCKS );
        }
    }

    /**
     * Opens HTreeMap
     */
    public HTreeMapImpl( Engine engine, long counterRecid, int hashSalt, long[] segmentRecids,
                     Class<K> keyType, Class<V> valueType, SerializerFactory serializerFactory,
                     long expireTimeStart, long expire, long expireAccess, long expireMaxSize, long expireStoreSize,
                     long[] expireHeads, long[] expireTails, Fun.Function1<V, K> valueCreator,
                     Hasher hasher, boolean disableLocks, boolean hasValues
    )
    {
        this.keyType = keyType;
        this.valueType = valueType;
        this.serializerFactory = serializerFactory;
        this.hasValues = hasValues;
        if( counterRecid < 0 )
        {
            throw new IllegalArgumentException();
        }
        if( engine == null )
        {
            throw new NullPointerException();
        }
        if( segmentRecids == null )
        {
            throw new NullPointerException();
        }

        if( segmentRecids.length != 16 )
        {
            throw new IllegalArgumentException();
        }

        this.engine = engine;
        this.hashSalt = hashSalt;
        this.segmentRecids = Arrays.copyOf( segmentRecids, 16 );
        this.hasher = hasher != null ? hasher : Hasher.BASIC;
        if( expire == 0 && expireAccess != 0 )
        {
            expire = expireAccess;
        }
        if( expireMaxSize != 0 && counterRecid == 0 )
        {
            throw new IllegalArgumentException( "expireMaxSize must have counter enabled" );
        }

        this.expireFlag = expire != 0L || expireAccess != 0L || expireMaxSize != 0 || expireStoreSize != 0;
        this.expire = expire;
        this.expireTimeStart = expireTimeStart;
        this.expireAccessFlag = expireAccess != 0L || expireMaxSize != 0 || expireStoreSize != 0;
        this.expireAccess = expireAccess;
        this.expireHeads = expireHeads == null ? null : Arrays.copyOf( expireHeads, 16 );
        this.expireTails = expireTails == null ? null : Arrays.copyOf( expireTails, 16 );
        this.expireMaxSizeFlag = expireMaxSize != 0;
        this.expireMaxSize = expireMaxSize;
        this.expireStoreSize = expireStoreSize;
        this.valueCreator = valueCreator;

        if( counterRecid != 0 )
        {
            this.counter = new Atomic.Long( engine, counterRecid, serializerFactory.createValueSerializer( java.lang.Long.class ) );
            Bind.size( this, counter );
        }
        else
        {
            this.counter = null;
        }

        if( expireFlag )
        {
            Thread t = new Thread( new HTreeExpireRunnable( this ), "HTreeMap expirator" );
            t.setDaemon( true );
            t.start();

            engine.closeListenerRegister( closeListener );
        }
        LN_SERIALIZER = new LinkedNodeValueSerializer<K, V>(serializerFactory.createKeySerializer( keyType ),
                                                            serializerFactory.createValueSerializer( valueType ),
                                                            expireFlag, hasValues );
    }

    public static long[] preallocateSegments( Engine engine )
    {
        //prealocate segmentRecids, so we dont have to lock on those latter
        long[] ret = new long[ 16 ];
        for( int i = 0; i < 16; i++ )
        {
            ret[ i ] = engine.put( new long[ 16 ][], DIR_SERIALIZER );
        }
        return ret;
    }

    @Override
    public boolean containsKey( final Object o )
    {
        return getPeek( o ) != null;
    }

    @Override
    public int size()
    {
        long size = sizeLong();
        if( size > Integer.MAX_VALUE )
        {
            return Integer.MAX_VALUE;
        }
        return (int) size;
    }

    @Override
    public long sizeLong()
    {
        if( counter != null )
        {
            return counter.get();
        }

        long counter = 0;

        //search tree, until we find first non null
        for( int i = 0; i < 16; i++ )
        {
            try
            {
                segmentLocks[ i ].readLock().lock();

                final long dirRecid = segmentRecids[ i ];
                counter += recursiveDirCount( dirRecid );
            }
            finally
            {
                segmentLocks[ i ].readLock().unlock();
            }
        }

        return counter;
    }

    private long recursiveDirCount( final long dirRecid )
    {
        long[][] dir = engine.get( dirRecid, DIR_SERIALIZER );
        long counter = 0;
        for( long[] subdir : dir )
        {
            if( subdir == null )
            {
                continue;
            }
            for( long recid : subdir )
            {
                if( recid == 0 )
                {
                    continue;
                }
                if( ( recid & 1 ) == 0 )
                {
                    //reference to another subdir
                    recid = recid >>> 1;
                    counter += recursiveDirCount( recid );
                }
                else
                {
                    //reference to linked list, count it
                    recid = recid >>> 1;
                    while( recid != 0 )
                    {
                        HTreeLinkedNode n = engine.get( recid, LN_SERIALIZER );
                        if( n != null )
                        {
                            counter++;
                            recid = n.next;
                        }
                        else
                        {
                            recid = 0;
                        }
                    }
                }
            }
        }
        return counter;
    }

    @Override
    public boolean isEmpty()
    {
        //search tree, until we find first non null
        for( int i = 0; i < 16; i++ )
        {
            try
            {
                segmentLocks[ i ].readLock().lock();

                long dirRecid = segmentRecids[ i ];
                long[][] dir = engine.get( dirRecid, DIR_SERIALIZER );
                for( long[] d : dir )
                {
                    if( d != null )
                    {
                        return false;
                    }
                }
            }
            finally
            {
                segmentLocks[ i ].readLock().unlock();
            }
        }

        return true;
    }

    @Override
    public V get( final Object o )
    {
        if( o == null )
        {
            return null;
        }
        final int h = hash( o );
        final int segment = h >>> 28;

        final Lock lock = expireAccessFlag ? segmentLocks[ segment ].writeLock() : segmentLocks[ segment ].readLock();
        lock.lock();
        HTreeLinkedNode<K, V> ln;
        try
        {
            ln = getInner( o, h, segment );
            if( ln == null )
            {
                return null;
            }
            if( expireAccessFlag )
            {
                expireLinkBump( segment, ln.expireLinkNodeRecid, true );
            }
        }
        finally
        {
            lock.unlock();
        }
        if( valueCreator == null || ln.value != null )
        {
            return ln.value;
        }

        V value = valueCreator.run( (K) o );
        V prevVal = putIfAbsent( (K) o, value );
        if( prevVal != null )
        {
            return prevVal;
        }
        return value;
    }

    protected HTreeLinkedNode<K, V> getInner( Object o, int h, int segment )
    {
        long recid = segmentRecids[ segment ];
        for( int level = 3; level >= 0; level-- )
        {
            long[][] dir = engine.get( recid, DIR_SERIALIZER );
            if( dir == null )
            {
                return null;
            }
            int slot = ( h >>> ( level * 7 ) ) & 0x7F;
            assert ( slot < 128 );
            if( dir[ slot >>> DIV8 ] == null )
            {
                return null;
            }
            recid = dir[ slot >>> DIV8 ][ slot & MOD8 ];
            if( recid == 0 )
            {
                return null;
            }
            if( ( recid & 1 ) != 0 )
            { //last bite indicates if referenced record is LinkedNode
                recid = recid >>> 1;
                while( true )
                {
                    HTreeLinkedNode<K, V> ln = engine.get( recid, LN_SERIALIZER );
                    if( ln == null )
                    {
                        return null;
                    }
                    if( hasher.equals( ln.key, (K) o ) )
                    {
                        assert ( hash( ln.key ) == h );
                        return ln;
                    }
                    if( ln.next == 0 )
                    {
                        return null;
                    }
                    recid = ln.next;
                }
            }

            recid = recid >>> 1;
        }

        return null;
    }

    @Override
    public V put( final K key, final V value )
    {
        if( key == null )
        {
            throw new IllegalArgumentException( "null key" );
        }

        if( value == null )
        {
            throw new IllegalArgumentException( "null value" );
        }

        final int h = hash( key );
        final int segment = h >>> 28;
        segmentLocks[ segment ].writeLock().lock();
        try
        {

            return putInner( key, value, h, segment );
        }
        finally
        {
            segmentLocks[ segment ].writeLock().unlock();
        }
    }

    private V putInner( K key, V value, int h, int segment )
    {
        long dirRecid = segmentRecids[ segment ];

        int level = 3;
        while( true )
        {
            long[][] dir = engine.get( dirRecid, DIR_SERIALIZER );
            final int slot = ( h >>> ( 7 * level ) ) & 0x7F;
            assert ( slot <= 127 );

            if( dir == null )
            {
                //create new dir
                dir = new long[ 16 ][];
            }

            if( dir[ slot >>> DIV8 ] == null )
            {
                dir = Arrays.copyOf( dir, 16 );
                dir[ slot >>> DIV8 ] = new long[ 8 ];
            }

            int counter = 0;
            long recid = dir[ slot >>> DIV8 ][ slot & MOD8 ];

            if( recid != 0 )
            {
                if( ( recid & 1 ) == 0 )
                {
                    dirRecid = recid >>> 1;
                    level--;
                    continue;
                }
                recid = recid >>> 1;

                //traverse linked list, try to replace previous value
                HTreeLinkedNode<K, V> ln = engine.get( recid, LN_SERIALIZER );

                while( ln != null )
                {
                    if( hasher.equals( ln.key, key ) )
                    {
                        //found, replace value at this node
                        V oldVal = ln.value;
                        ln = new HTreeLinkedNode<K, V>( ln.next, ln.expireLinkNodeRecid, ln.key, value );
                        engine.update( recid, ln, LN_SERIALIZER );
                        if( expireFlag )
                        {
                            expireLinkBump( segment, ln.expireLinkNodeRecid, false );
                        }
                        notify( key, oldVal, value );
                        return oldVal;
                    }
                    recid = ln.next;
                    ln = recid == 0 ? null : engine.get( recid, LN_SERIALIZER );
                    counter++;
                }
                //key was not found at linked list, so just append it to beginning
            }

            //check if linked list has overflow and needs to be expanded to new dir level
            if( counter >= BUCKET_OVERFLOW && level >= 1 )
            {
                long[][] nextDir = new long[ 16 ][];

                {
                    final long expireNodeRecid = expireFlag ? engine.preallocate() : 0L;
                    final HTreeLinkedNode<K, V> node = new HTreeLinkedNode<K, V>( 0, expireNodeRecid, key, value );
                    final long newRecid = engine.put( node, LN_SERIALIZER );
                    //add newly inserted record
                    int pos = ( h >>> ( 7 * ( level - 1 ) ) ) & 0x7F;
                    nextDir[ pos >>> DIV8 ] = new long[ 8 ];
                    nextDir[ pos >>> DIV8 ][ pos & MOD8 ] = ( newRecid << 1 ) | 1;
                    if( expireFlag )
                    {
                        expireLinkAdd( segment, expireNodeRecid, newRecid, h );
                    }
                }

                //redistribute linked bucket into new dir
                long nodeRecid = dir[ slot >>> DIV8 ][ slot & MOD8 ] >>> 1;
                while( nodeRecid != 0 )
                {
                    HTreeLinkedNode<K, V> n = engine.get( nodeRecid, LN_SERIALIZER );
                    final long nextRecid = n.next;
                    int pos = ( hash( n.key ) >>> ( 7 * ( level - 1 ) ) ) & 0x7F;
                    if( nextDir[ pos >>> DIV8 ] == null )
                    {
                        nextDir[ pos >>> DIV8 ] = new long[ 8 ];
                    }
                    n = new HTreeLinkedNode<K, V>( nextDir[ pos >>> DIV8 ][ pos & MOD8 ] >>> 1, n.expireLinkNodeRecid, n.key, n.value );
                    nextDir[ pos >>> DIV8 ][ pos & MOD8 ] = ( nodeRecid << 1 ) | 1;
                    engine.update( nodeRecid, n, LN_SERIALIZER );
                    nodeRecid = nextRecid;
                }

                //insert nextDir and update parent dir
                long nextDirRecid = engine.put( nextDir, DIR_SERIALIZER );
                int parentPos = ( h >>> ( 7 * level ) ) & 0x7F;
                dir = Arrays.copyOf( dir, 16 );
                dir[ parentPos >>> DIV8 ] = Arrays.copyOf( dir[ parentPos >>> DIV8 ], 8 );
                dir[ parentPos >>> DIV8 ][ parentPos & MOD8 ] = ( nextDirRecid << 1 ) | 0;
                engine.update( dirRecid, dir, DIR_SERIALIZER );
                notify( key, null, value );
                return null;
            }
            else
            {
                // record does not exist in linked list, so create new one
                recid = dir[ slot >>> DIV8 ][ slot & MOD8 ] >>> 1;
                final long expireNodeRecid = expireFlag ? engine.put( HTreeExpireLinkNode.EMPTY, HTreeExpireLinkNode.SERIALIZER ) : 0L;

                final long newRecid = engine.put( new HTreeLinkedNode<K, V>( recid, expireNodeRecid, key, value ), LN_SERIALIZER );
                dir = Arrays.copyOf( dir, 16 );
                dir[ slot >>> DIV8 ] = Arrays.copyOf( dir[ slot >>> DIV8 ], 8 );
                dir[ slot >>> DIV8 ][ slot & MOD8 ] = ( newRecid << 1 ) | 1;
                engine.update( dirRecid, dir, DIR_SERIALIZER );
                if( expireFlag )
                {
                    expireLinkAdd( segment, expireNodeRecid, newRecid, h );
                }
                notify( key, null, value );
                return null;
            }
        }
    }

    @Override
    public V remove( Object key )
    {

        final int h = hash( key );
        final int segment = h >>> 28;
        segmentLocks[ segment ].writeLock().lock();
        try
        {
            return removeInternal( key, segment, h, true );
        }
        finally
        {
            segmentLocks[ segment ].writeLock().unlock();
        }
    }

    protected V removeInternal( Object key, int segment, int h, boolean removeExpire )
    {
        final long[] dirRecids = new long[ 4 ];
        int level = 3;
        dirRecids[ level ] = segmentRecids[ segment ];

        assert ( segment == h >>> 28 );

        while( true )
        {
            long[][] dir = engine.get( dirRecids[ level ], DIR_SERIALIZER );
            final int slot = ( h >>> ( 7 * level ) ) & 0x7F;
            assert ( slot <= 127 );

            if( dir == null )
            {
                //create new dir
                dir = new long[ 16 ][];
            }

            if( dir[ slot >>> DIV8 ] == null )
            {
                dir = Arrays.copyOf( dir, 16 );
                dir[ slot >>> DIV8 ] = new long[ 8 ];
            }

//                int counter = 0;
            long recid = dir[ slot >>> DIV8 ][ slot & MOD8 ];

            if( recid != 0 )
            {
                if( ( recid & 1 ) == 0 )
                {
                    level--;
                    dirRecids[ level ] = recid >>> 1;
                    continue;
                }
                recid = recid >>> 1;

                //traverse linked list, try to remove node
                HTreeLinkedNode<K, V> ln = engine.get( recid, LN_SERIALIZER );
                HTreeLinkedNode<K, V> prevLn = null;
                long prevRecid = 0;
                while( ln != null )
                {
                    if( hasher.equals( ln.key, (K) key ) )
                    {
                        //remove from linkedList
                        if( prevLn == null )
                        {
                            //referenced directly from dir
                            if( ln.next == 0 )
                            {
                                recursiveDirDelete( h, level, dirRecids, dir, slot );
                            }
                            else
                            {
                                dir = Arrays.copyOf( dir, 16 );
                                dir[ slot >>> DIV8 ] = Arrays.copyOf( dir[ slot >>> DIV8 ], 8 );
                                dir[ slot >>> DIV8 ][ slot & MOD8 ] = ( ln.next << 1 ) | 1;
                                engine.update( dirRecids[ level ], dir, DIR_SERIALIZER );
                            }
                        }
                        else
                        {
                            //referenced from LinkedNode
                            prevLn = new HTreeLinkedNode<K, V>( ln.next, prevLn.expireLinkNodeRecid, prevLn.key, prevLn.value );
                            engine.update( prevRecid, prevLn, LN_SERIALIZER );
                        }
                        //found, remove this node
                        assert ( hash( ln.key ) == h );
                        engine.delete( recid, LN_SERIALIZER );
                        if( removeExpire && expireFlag )
                        {
                            expireLinkRemove( segment, ln.expireLinkNodeRecid );
                        }
                        notify( (K) key, ln.value, null );
                        return ln.value;
                    }
                    prevRecid = recid;
                    prevLn = ln;
                    recid = ln.next;
                    ln = recid == 0 ? null : engine.get( recid, LN_SERIALIZER );
//                        counter++;
                }
                //key was not found at linked list, so it does not exist
                return null;
            }
            //recid is 0, so entry does not exist
            return null;
        }
    }

    private void recursiveDirDelete( int h, int level, long[] dirRecids, long[][] dir, int slot )
    {
        //was only item in linked list, so try to collapse the dir
        dir = Arrays.copyOf( dir, 16 );
        dir[ slot >>> DIV8 ] = Arrays.copyOf( dir[ slot >>> DIV8 ], 8 );
        dir[ slot >>> DIV8 ][ slot & MOD8 ] = 0;
        //one record was zeroed out, check if subarray can be collapsed to null
        boolean allZero = true;
        for( long l : dir[ slot >>> DIV8 ] )
        {
            if( l != 0 )
            {
                allZero = false;
                break;
            }
        }
        if( allZero )
        {
            dir[ slot >>> DIV8 ] = null;
        }
        allZero = true;
        for( long[] l : dir )
        {
            if( l != null )
            {
                allZero = false;
                break;
            }
        }

        if( allZero )
        {
            //delete from parent dir
            if( level == 3 )
            {
                //parent is segment, recid of this dir can not be modified,  so just update to null
                engine.update( dirRecids[ level ], new long[ 16 ][], DIR_SERIALIZER );
            }
            else
            {
                engine.delete( dirRecids[ level ], DIR_SERIALIZER );

                final long[][] parentDir = engine.get( dirRecids[ level + 1 ], DIR_SERIALIZER );
                final int parentPos = ( h >>> ( 7 * ( level + 1 ) ) ) & 0x7F;
                recursiveDirDelete( h, level + 1, dirRecids, parentDir, parentPos );
                //parentDir[parentPos>>>DIV8][parentPos&MOD8] = 0;
                //engine.update(dirRecids[level + 1],parentDir,DIR_SERIALIZER);

            }
        }
        else
        {
            engine.update( dirRecids[ level ], dir, DIR_SERIALIZER );
        }
    }

    @Override
    public void clear()
    {
        for( int i = 0; i < 16; i++ )
        {
            try
            {
                segmentLocks[ i ].writeLock().lock();

                final long dirRecid = segmentRecids[ i ];
                recursiveDirClear( dirRecid );

                //set dir to null, as segment recid is immutable
                engine.update( dirRecid, new long[ 16 ][], DIR_SERIALIZER );

                if( expireFlag )
                {
                    while( expireLinkRemoveLast( i ) != null )
                    {
                    } //TODO speedup remove all
                }
            }
            finally
            {
                segmentLocks[ i ].writeLock().unlock();
            }
        }
    }

    private void recursiveDirClear( final long dirRecid )
    {
        final long[][] dir = engine.get( dirRecid, DIR_SERIALIZER );
        if( dir == null )
        {
            return;
        }
        for( long[] subdir : dir )
        {
            if( subdir == null )
            {
                continue;
            }
            for( long recid : subdir )
            {
                if( recid == 0 )
                {
                    continue;
                }
                if( ( recid & 1 ) == 0 )
                {
                    //another dir
                    recid = recid >>> 1;
                    //recursively remove dir
                    recursiveDirClear( recid );
                    engine.delete( recid, DIR_SERIALIZER );
                }
                else
                {
                    //linked list to delete
                    recid = recid >>> 1;
                    while( recid != 0 )
                    {
                        HTreeLinkedNode n = engine.get( recid, LN_SERIALIZER );
                        engine.delete( recid, LN_SERIALIZER );
                        notify( (K) n.key, (V) n.value, null );
                        recid = n.next;
                    }
                }
            }
        }
    }

    @Override
    public boolean containsValue( Object value )
    {
        for( V v : values() )
        {
            if( v.equals( value ) )
            {
                return true;
            }
        }
        return false;
    }

    private final Set<K> _keySet = new HTreeKeySet( this );

    @SuppressWarnings( "NullableProblems" )
    @Override
    public Set<K> keySet()
    {
        return _keySet;
    }

    private final Collection<V> _values = new ValuesCollection<K,V>( this );

    @SuppressWarnings( "NullableProblems" )
    @Override
    public Collection<V> values()
    {
        return _values;
    }

    private final Set<Entry<K, V>> _entrySet = new AbstractSet<Entry<K, V>>()
    {

        @Override
        public int size()
        {
            return HTreeMapImpl.this.size();
        }

        @Override
        public boolean isEmpty()
        {
            return HTreeMapImpl.this.isEmpty();
        }

        @Override
        public boolean contains( Object o )
        {
            if( o instanceof Entry )
            {
                Entry e = (Entry) o;
                Object val = HTreeMapImpl.this.get( e.getKey() );
                return val != null && val.equals( e.getValue() );
            }
            else
            {
                return false;
            }
        }

        @SuppressWarnings( "NullableProblems" )
        @Override
        public Iterator<Entry<K, V>> iterator()
        {
            return new HTreeEntryIterator( HTreeMapImpl.this );
        }

        @Override
        public boolean add( Entry<K, V> kvEntry )
        {
            K key = kvEntry.getKey();
            V value = kvEntry.getValue();
            if( key == null || value == null )
            {
                throw new NullPointerException();
            }
            HTreeMapImpl.this.put( key, value );
            return true;
        }

        @Override
        public boolean remove( Object o )
        {
            if( o instanceof Entry )
            {
                Entry e = (Entry) o;
                Object key = e.getKey();
                if( key == null )
                {
                    return false;
                }
                return HTreeMapImpl.this.remove( key, e.getValue() );
            }
            return false;
        }

        @Override
        public void clear()
        {
            HTreeMapImpl.this.clear();
        }
    };

    @Override
    public Set<Entry<K, V>> entrySet()
    {
        return _entrySet;
    }

    protected int hash( final Object key )
    {
        int h = hasher.hashCode( (K) key ) ^ hashSalt;
        h ^= ( h >>> 20 ) ^ ( h >>> 12 );
        return h ^ ( h >>> 7 ) ^ ( h >>> 4 );
    }

    @Override
    public V putIfAbsent( K key, V value )
    {
        if( key == null || value == null )
        {
            throw new NullPointerException();
        }

        final int h = HTreeMapImpl.this.hash( key );
        final int segment = h >>> 28;
        try
        {
            segmentLocks[ segment ].writeLock().lock();

            HTreeLinkedNode<K, V> ln = HTreeMapImpl.this.getInner( key, h, segment );
            if( ln == null )
            {
                return put( key, value );
            }
            else
            {
                return ln.value;
            }
        }
        finally
        {
            segmentLocks[ segment ].writeLock().unlock();
        }
    }

    @Override
    public boolean remove( Object key, Object value )
    {
        if( key == null || value == null )
        {
            throw new NullPointerException();
        }
        final int h = HTreeMapImpl.this.hash( key );
        final int segment = h >>> 28;
        try
        {
            segmentLocks[ segment ].writeLock().lock();

            HTreeLinkedNode otherVal = getInner( key, h, segment );
            if( otherVal != null && otherVal.value.equals( value ) )
            {
                removeInternal( key, segment, h, true );
                return true;
            }
            else
            {
                return false;
            }
        }
        finally
        {
            segmentLocks[ segment ].writeLock().unlock();
        }
    }

    @Override
    public boolean replace( K key, V oldValue, V newValue )
    {
        if( key == null || oldValue == null || newValue == null )
        {
            throw new NullPointerException();
        }
        final int h = HTreeMapImpl.this.hash( key );
        final int segment = h >>> 28;
        try
        {
            segmentLocks[ segment ].writeLock().lock();

            HTreeLinkedNode<K, V> ln = getInner( key, h, segment );
            if( ln != null && ln.value.equals( oldValue ) )
            {
                putInner( key, newValue, h, segment );
                return true;
            }
            else
            {
                return false;
            }
        }
        finally
        {
            segmentLocks[ segment ].writeLock().unlock();
        }
    }

    @Override
    public V replace( K key, V value )
    {
        if( key == null || value == null )
        {
            throw new NullPointerException();
        }
        final int h = HTreeMapImpl.this.hash( key );
        final int segment = h >>> 28;
        try
        {
            segmentLocks[ segment ].writeLock().lock();

            if( getInner( key, h, segment ) != null )
            {
                return putInner( key, value, h, segment );
            }
            else
            {
                return null;
            }
        }
        finally
        {
            segmentLocks[ segment ].writeLock().unlock();
        }
    }

    protected void expireLinkAdd( int segment, long expireNodeRecid, long keyRecid, int hash )
    {
        assert ( segmentLocks[ segment ].writeLock().isHeldByCurrentThread() );
        assert ( expireNodeRecid > 0 );
        assert ( keyRecid > 0 );

        long time = expire == 0 ? 0 : expire + System.currentTimeMillis() - expireTimeStart;
        long head = engine.get( expireHeads[ segment ], SerializerBase.LONG );
        if( head == 0 )
        {
            //insert new
            HTreeExpireLinkNode n = new HTreeExpireLinkNode( 0, 0, keyRecid, time, hash );
            engine.update( expireNodeRecid, n, HTreeExpireLinkNode.SERIALIZER );
            engine.update( expireHeads[ segment ], expireNodeRecid, SerializerBase.LONG );
            engine.update( expireTails[ segment ], expireNodeRecid, SerializerBase.LONG );
        }
        else
        {
            //insert new head
            HTreeExpireLinkNode n = new HTreeExpireLinkNode( head, 0, keyRecid, time, hash );
            engine.update( expireNodeRecid, n, HTreeExpireLinkNode.SERIALIZER );

            //update old head to have new head as next
            HTreeExpireLinkNode oldHead = engine.get( head, HTreeExpireLinkNode.SERIALIZER );
            oldHead = oldHead.copyNext( expireNodeRecid );
            engine.update( head, oldHead, HTreeExpireLinkNode.SERIALIZER );

            //and update head
            engine.update( expireHeads[ segment ], expireNodeRecid, SerializerBase.LONG );
        }
    }

    protected void expireLinkBump( int segment, long nodeRecid, boolean access )
    {
        assert ( segmentLocks[ segment ].writeLock().isHeldByCurrentThread() );

        HTreeExpireLinkNode n = engine.get( nodeRecid, HTreeExpireLinkNode.SERIALIZER );
        long newTime =
            access ?
            ( expireAccess == 0 ? 0 : expireAccess + System.currentTimeMillis() - expireTimeStart ) :
            ( expire == 0 ? 0 : expire + System.currentTimeMillis() - expireTimeStart );

        //TODO optimize bellow, but what if there is only size limit?
        //if(n.time>newTime) return; // older time greater than new one, do not update

        if( n.next == 0 )
        {
            //already head, so just update time
            n = n.copyTime( newTime );
            engine.update( nodeRecid, n, HTreeExpireLinkNode.SERIALIZER );
        }
        else
        {
            //update prev so it points to next
            if( n.prev != 0 )
            {
                //not a tail
                HTreeExpireLinkNode prev = engine.get( n.prev, HTreeExpireLinkNode.SERIALIZER );
                prev = prev.copyNext( n.next );
                engine.update( n.prev, prev, HTreeExpireLinkNode.SERIALIZER );
            }
            else
            {
                //yes tail, so just update it to point to next
                engine.update( expireTails[ segment ], n.next, SerializerBase.LONG );
            }

            //update next so it points to prev
            HTreeExpireLinkNode next = engine.get( n.next, HTreeExpireLinkNode.SERIALIZER );
            next = next.copyPrev( n.prev );
            engine.update( n.next, next, HTreeExpireLinkNode.SERIALIZER );

            //TODO optimize if oldHead==next

            //now insert node as new head
            long oldHeadRecid = engine.get( expireHeads[ segment ], SerializerBase.LONG );
            HTreeExpireLinkNode oldHead = engine.get( oldHeadRecid, HTreeExpireLinkNode.SERIALIZER );
            oldHead = oldHead.copyNext( nodeRecid );
            engine.update( oldHeadRecid, oldHead, HTreeExpireLinkNode.SERIALIZER );
            engine.update( expireHeads[ segment ], nodeRecid, SerializerBase.LONG );

            n = new HTreeExpireLinkNode( oldHeadRecid, 0, n.keyRecid, newTime, n.hash );
            engine.update( nodeRecid, n, HTreeExpireLinkNode.SERIALIZER );
        }
    }

    protected HTreeExpireLinkNode expireLinkRemoveLast( int segment )
    {
        assert ( segmentLocks[ segment ].writeLock().isHeldByCurrentThread() );

        long tail = engine.get( expireTails[ segment ], SerializerBase.LONG );
        if( tail == 0 )
        {
            return null;
        }

        HTreeExpireLinkNode n = engine.get( tail, HTreeExpireLinkNode.SERIALIZER );
        if( n.next == 0 )
        {
            //update tail and head
            engine.update( expireHeads[ segment ], 0L, SerializerBase.LONG );
            engine.update( expireTails[ segment ], 0L, SerializerBase.LONG );
        }
        else
        {
            //point tail to next record
            engine.update( expireTails[ segment ], n.next, SerializerBase.LONG );
            //update next record to have zero prev
            HTreeExpireLinkNode next = engine.get( n.next, HTreeExpireLinkNode.SERIALIZER );
            next = next.copyPrev( 0L );
            engine.update( n.next, next, HTreeExpireLinkNode.SERIALIZER );
        }

        engine.delete( tail, HTreeExpireLinkNode.SERIALIZER );
        return n;
    }

    protected HTreeExpireLinkNode expireLinkRemove( int segment, long nodeRecid )
    {
        assert ( segmentLocks[ segment ].writeLock().isHeldByCurrentThread() );

        HTreeExpireLinkNode n = engine.get( nodeRecid, HTreeExpireLinkNode.SERIALIZER );
        engine.delete( nodeRecid, HTreeExpireLinkNode.SERIALIZER );
        if( n.next == 0 && n.prev == 0 )
        {
            engine.update( expireHeads[ segment ], 0L, SerializerBase.LONG );
            engine.update( expireTails[ segment ], 0L, SerializerBase.LONG );
        }
        else if( n.next == 0 )
        {
            HTreeExpireLinkNode prev = engine.get( n.prev, HTreeExpireLinkNode.SERIALIZER );
            prev = prev.copyNext( 0 );
            engine.update( n.prev, prev, HTreeExpireLinkNode.SERIALIZER );
            engine.update( expireHeads[ segment ], n.prev, SerializerBase.LONG );
        }
        else if( n.prev == 0 )
        {
            HTreeExpireLinkNode next = engine.get( n.next, HTreeExpireLinkNode.SERIALIZER );
            next = next.copyPrev( 0 );
            engine.update( n.next, next, HTreeExpireLinkNode.SERIALIZER );
            engine.update( expireTails[ segment ], n.next, SerializerBase.LONG );
        }
        else
        {
            HTreeExpireLinkNode next = engine.get( n.next, HTreeExpireLinkNode.SERIALIZER );
            next = next.copyPrev( n.prev );
            engine.update( n.next, next, HTreeExpireLinkNode.SERIALIZER );

            HTreeExpireLinkNode prev = engine.get( n.prev, HTreeExpireLinkNode.SERIALIZER );
            prev = prev.copyNext( n.next );
            engine.update( n.prev, prev, HTreeExpireLinkNode.SERIALIZER );
        }

        return n;
    }

    /**
     * Return given value, without updating cache statistics if `expireAccess()` is true
     * It also does not use `valueCreator` if value is not found (always returns null if not found)
     *
     * @param key key to lookup
     *
     * @return value associated with key or null
     */
    @Override public V getPeek( final Object key )
    {
        if( key == null )
        {
            return null;
        }
        final int h = hash( key );
        final int segment = h >>> 28;

        final Lock lock = segmentLocks[ segment ].readLock();
        lock.lock();

        try
        {
            HTreeLinkedNode<K, V> ln = getInner( key, h, segment );
            if( ln == null )
            {
                return null;
            }
            return ln.value;
        }
        finally
        {
            lock.unlock();
        }
    }

    /**
     * Returns maximal (newest) expiration timestamp
     */
    @Override public long getMaxExpireTime()
    {
        if( !expireFlag )
        {
            return 0;
        }
        long ret = 0;
        for( int segment = 0; segment < 16; segment++ )
        {
            segmentLocks[ segment ].readLock().lock();
            try
            {
                long head = engine.get( expireHeads[ segment ], SerializerBase.LONG );
                if( head == 0 )
                {
                    continue;
                }
                HTreeExpireLinkNode ln = engine.get( head, HTreeExpireLinkNode.SERIALIZER );
                if( ln == null || ln.time == 0 )
                {
                    continue;
                }
                ret = Math.max( ret, ln.time + expireTimeStart );
            }
            finally
            {
                segmentLocks[ segment ].readLock().unlock();
            }
        }
        return ret;
    }

    /**
     * Returns minimal (oldest) expiration timestamp
     */
    @Override public long getMinExpireTime()
    {
        if( !expireFlag )
        {
            return 0;
        }
        long ret = Long.MAX_VALUE;
        for( int segment = 0; segment < 16; segment++ )
        {
            segmentLocks[ segment ].readLock().lock();
            try
            {
                long tail = engine.get( expireTails[ segment ], SerializerBase.LONG );
                if( tail == 0 )
                {
                    continue;
                }
                HTreeExpireLinkNode ln = engine.get( tail, HTreeExpireLinkNode.SERIALIZER );
                if( ln == null || ln.time == 0 )
                {
                    continue;
                }
                ret = Math.min( ret, ln.time + expireTimeStart );
            }
            finally
            {
                segmentLocks[ segment ].readLock().unlock();
            }
        }
        if( ret == Long.MAX_VALUE )
        {
            ret = 0;
        }
        return ret;
    }

    /**
     * Make readonly snapshot view of current Map. Snapshot is immutable and not affected by modifications made by other threads.
     * Useful if you need consistent view on Map.
     * <p>
     * Maintaining snapshot have some overhead, underlying Engine is closed after Map view is GCed.
     * Please make sure to release reference to this Map view, so snapshot view can be garbage collected.
     *
     * @return snapshot
     */
    @Override public Map<K, V> snapshot()
    {
        Engine snapshot = TxEngine.createSnapshotFor( engine );
        return new HTreeMapImpl<K, V>( snapshot, counter == null ? 0 : counter.getRecid(),
                                   hashSalt, segmentRecids, keyType, valueType, serializerFactory, 0L, 0L, 0L, 0L, 0L, null, null, null, null, false, true );
    }

    protected void expirePurge()
    {
        if( !expireFlag )
        {
            return;
        }

        long removePerSegment = 0;
        if( expireMaxSizeFlag )
        {
            long size = counter.get();
            if( size > expireMaxSize )
            {
                removePerSegment = 1 + ( size - expireMaxSize ) / 16;
                if( LOG.isLoggable( Level.FINE ) )
                {
                    LOG.log( Level.FINE, "HTreeMap expirator expireMaxSize, will remove {0,number,integer} entries per segment",
                             removePerSegment );
                }
            }
        }

        if( expireStoreSize != 0 && removePerSegment == 0 )
        {
            Store store = Store.forEngine( engine );
            long storeSize = store.getCurrSize() - store.getFreeSize();
            if( expireStoreSize < storeSize )
            {
                removePerSegment = 640;
                if( LOG.isLoggable( Level.FINE ) )
                {
                    LOG.log( Level.FINE, "HTreeMap store store size ({0,number,integer}) over limit," +
                                         "will remove {2,number,integer} entries per segment",
                             new Object[]{ storeSize, removePerSegment } );
                }
            }
        }

        long counter = 0;
        for( int seg = 0; seg < 16; seg++ )
        {
            if( closeLatch.getCount() < 2 )
            {
                return;
            }
            counter += expirePurgeSegment( seg, removePerSegment );
        }
        if( LOG.isLoggable( Level.FINE ) )
        {
            LOG.log( Level.FINE, "HTreeMap expirator removed {0,number,integer}", counter );
        }
    }

    protected long expirePurgeSegment( int seg, long removePerSegment )
    {
        segmentLocks[ seg ].writeLock().lock();
        try
        {
//            expireCheckSegment(seg);
            long recid = engine.get( expireTails[ seg ], SerializerBase.LONG );
            long counter = 0;
            HTreeExpireLinkNode last = null, n = null;
            while( recid != 0 )
            {
                n = engine.get( recid, HTreeExpireLinkNode.SERIALIZER );
                assert ( n != HTreeExpireLinkNode.EMPTY );
                assert ( n.hash >>> 28 == seg );

                final boolean remove = ++counter < removePerSegment ||
                                       ( ( expire != 0 || expireAccess != 0 ) && n.time + expireTimeStart < System.currentTimeMillis() );

                if( remove )
                {
                    engine.delete( recid, HTreeExpireLinkNode.SERIALIZER );
                    HTreeLinkedNode<K, V> ln = engine.get( n.keyRecid, LN_SERIALIZER );
                    removeInternal( ln.key, seg, n.hash, false );
                }
                else
                {
                    break;
                }
                last = n;
                recid = n.next;
            }
            // patch linked list
            if( last == null )
            {
                //no items removed
            }
            else if( recid == 0 )
            {
                //all items were taken, so zero items
                engine.update( expireTails[ seg ], 0L, SerializerBase.LONG );
                engine.update( expireHeads[ seg ], 0L, SerializerBase.LONG );
            }
            else
            {
                //update tail to point to next item
                engine.update( expireTails[ seg ], recid, SerializerBase.LONG );
                //and update next item to point to tail
                n = engine.get( recid, HTreeExpireLinkNode.SERIALIZER );
                n = n.copyPrev( 0 );
                engine.update( recid, n, HTreeExpireLinkNode.SERIALIZER );
            }
            return counter;
//            expireCheckSegment(seg);
        }
        finally
        {
            segmentLocks[ seg ].writeLock().unlock();
        }
    }

    protected void expireCheckSegment( int segment )
    {
        long current = engine.get( expireTails[ segment ], SerializerBase.LONG );
        if( current == 0 )
        {
            if( engine.get( expireHeads[ segment ], SerializerBase.LONG ) != 0 )
            {
                throw new AssertionError( "head not 0" );
            }
            return;
        }

        long prev = 0;
        while( current != 0 )
        {
            HTreeExpireLinkNode curr = engine.get( current, HTreeExpireLinkNode.SERIALIZER );
            assert ( curr.prev == prev ) : "wrong prev " + curr.prev + " - " + prev;
            prev = current;
            current = curr.next;
        }
        if( engine.get( expireHeads[ segment ], SerializerBase.LONG ) != prev )
        {
            throw new AssertionError( "wrong head" );
        }
    }

    protected final Object modListenersLock = new Object();

    protected Bind.MapListener<K, V>[] modListeners = new Bind.MapListener[ 0 ];

    @Override
    public void modificationListenerAdd( Bind.MapListener<K, V> listener )
    {
        synchronized( modListenersLock )
        {
            Bind.MapListener<K, V>[] modListeners2 =
                Arrays.copyOf( modListeners, modListeners.length + 1 );
            modListeners2[ modListeners2.length - 1 ] = listener;
            modListeners = modListeners2;
        }
    }

    @Override
    public void modificationListenerRemove( Bind.MapListener<K, V> listener )
    {
        synchronized( modListenersLock )
        {
            for( int i = 0; i < modListeners.length; i++ )
            {
                if( modListeners[ i ] == listener )
                {
                    modListeners[ i ] = null;
                }
            }
        }
    }

    protected void notify( K key, V oldValue, V newValue )
    {
        assert ( segmentLocks[ hash( key ) >>> 28 ].isWriteLockedByCurrentThread() );
        Bind.MapListener<K, V>[] modListeners2 = modListeners;
        for( Bind.MapListener<K, V> listener : modListeners2 )
        {
            if( listener != null )
            {
                listener.update( key, oldValue, newValue );
            }
        }
    }

    /**
     * Closes underlying storage and releases all resources.
     * Used mostly with temporary collections where engine is not accessible.
     */
    public void close()
    {
        engine.close();
    }

    @Override public Engine getEngine()
    {
        return engine;
    }
}
