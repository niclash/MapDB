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

/*
 * NOTE: some code (and javadoc) used in this class
 * comes from JSR-166 group with following copyright:
 *
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */

package org.mapdb.impl.btree;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.LockSupport;
import org.mapdb.Atomic;
import org.mapdb.BTreeMap;
import org.mapdb.Bind;
import org.mapdb.Engine;
import org.mapdb.KeySerializer;
import org.mapdb.SerializerFactory;
import org.mapdb.ValueSerializer;
import org.mapdb.impl.CC;
import org.mapdb.impl.binaryserializer.InternalSerializers;
import org.mapdb.impl.engine.DbImpl;
import org.mapdb.impl.Fun;
import org.mapdb.impl.engine.TxEngine;
import org.mapdb.impl.binaryserializer.BTreeKeySerializer;
import org.mapdb.impl.binaryserializer.SerializerBase;
import org.mapdb.impl.longmap.LongConcurrentHashMap;
import org.mapdb.impl.longmap.LongMap;

/**
 * A scalable concurrent {@link java.util.concurrent.ConcurrentNavigableMap} implementation.
 * The map is sorted according to the {@linkplain Comparable natural
 * ordering} of its keys, or by a {@link java.util.Comparator} provided at map
 * creation time.
 * <p/>
 * Insertion, removal,
 * update, and access operations safely execute concurrently by
 * multiple threads.  Iterators are <i>weakly consistent</i>, returning
 * elements reflecting the state of the map at some point at or since
 * the creation of the iterator.  They do <em>not</em> throw {@link
 * java.util.ConcurrentModificationException}, and may proceed concurrently with
 * other operations. Ascending key ordered views and their iterators
 * are faster than descending ones.
 * <p/>
 * It is possible to obtain <i>consistent</i> iterator by using <code>snapshot()</code>
 * method.
 * <p/>
 * All <tt>Map.Entry</tt> pairs returned by methods in this class
 * and its views represent snapshots of mappings at the time they were
 * produced. They do <em>not</em> support the <tt>Entry.setValue</tt>
 * method. (Note however that it is possible to change mappings in the
 * associated map using <tt>put</tt>, <tt>putIfAbsent</tt>, or
 * <tt>replace</tt>, depending on exactly which effect you need.)
 * <p/>
 * This collection has optional size counter. If this is enabled Map size is
 * kept in {@link org.mapdb.Atomic.Long} variable. Keeping counter brings considerable
 * overhead on inserts and removals.
 * If the size counter is not enabled the <tt>size</tt> method is <em>not</em> a constant-time operation.
 * Determining the current number of elements requires a traversal of the elements.
 * <p/>
 * Additionally, the bulk operations <tt>putAll</tt>, <tt>equals</tt>, and
 * <tt>clear</tt> are <em>not</em> guaranteed to be performed
 * atomically. For example, an iterator operating concurrently with a
 * <tt>putAll</tt> operation might view only some of the added
 * elements. NOTE: there is an optional
 * <p/>
 * This class and its views and iterators implement all of the
 * <em>optional</em> methods of the {@link java.util.Map} and {@link java.util.Iterator}
 * interfaces. Like most other concurrent collections, this class does
 * <em>not</em> permit the use of <tt>null</tt> keys or values because some
 * null return values cannot be reliably distinguished from the absence of
 * elements.
 * <p/>
 * Theoretical design of BTreeMap is based on <a href="http://www.cs.cornell.edu/courses/cs4411/2009sp/blink.pdf">paper</a>
 * from Philip L. Lehman and S. Bing Yao. More practical aspects of BTreeMap implementation are based on <a href="http://www.doc.ic.ac.uk/~td202/">notes</a>
 * and <a href="http://www.doc.ic.ac.uk/~td202/btree/">demo application</a> from Thomas Dinsdale-Young.
 * B-Linked-Tree used here does not require locking for read. Updates and inserts locks only one, two or three nodes.
 * <p/>
 * This B-Linked-Tree structure does not support removal well, entry deletion does not collapse tree nodes. Massive
 * deletion causes empty nodes and performance lost. There is workaround in form of compaction process, but it is not
 * implemented yet.
 *
 * @author Jan Kotek
 * @author some parts by Doug Lea and JSR-166 group
 *
 *         TODO links to BTree papers are not working anymore.
 */
@SuppressWarnings( { "unchecked", "rawtypes" } )
public class BTreeMapImpl<K, V> extends AbstractMap<K, V>
    implements BTreeMap<K, V>
{

    @SuppressWarnings( "rawtypes" )
    public static final Comparator COMPARABLE_COMPARATOR = new Comparator<Comparable>()
    {
        @Override
        final public int compare( final Comparable o1, final Comparable o2 )
        {
            return o1.compareTo( o2 );
        }
    };

    public static final Object EMPTY = new Object();

    protected static final int B_TREE_NODE_LEAF_LR = 180;
    protected static final int B_TREE_NODE_LEAF_L = 181;
    protected static final int B_TREE_NODE_LEAF_R = 182;
    protected static final int B_TREE_NODE_LEAF_C = 183;
    protected static final int B_TREE_NODE_DIR_LR = 184;
    protected static final int B_TREE_NODE_DIR_L = 185;
    protected static final int B_TREE_NODE_DIR_R = 186;
    protected static final int B_TREE_NODE_DIR_C = 187;

    /**
     * recid under which reference to rootRecid is stored
     */
    protected final long rootRecidRef;

    /**
     * holds node level locks
     */
    protected final LongConcurrentHashMap<Thread> nodeLocks = new LongConcurrentHashMap<Thread>();

    /**
     * maximal node size allowed in this BTree
     */
    protected final int maxNodeSize;

    /**
     * DB Engine in which entries are persisted
     */
    protected final Engine engine;

    /**
     * is this a Map or Set?  if false, entries do not have values, only keys are allowed
     */
    protected final boolean hasValues;

    /**
     * store values as part of BTree nodes
     */
    protected final boolean valsOutsideNodes;

    protected final List<Long> leftEdges;

    private final BTreeKeySet keySet;

    private final BTreeEntrySet entrySet = new BTreeEntrySet( this );

    private final BTreeValues values = new BTreeValues( this );

    private final ConcurrentNavigableMap<K, V> descendingMap = new BTreeDescendingMap( this, null, true, null, false );

    public final Atomic.Long counter;

    protected final int numberOfNodeMetas;

    protected final ValueSerializer<BTreeNode> nodeSerializer;
    private final SerializerFactory serializerFactory;
    private final Class<V> valueType;
    private final Class<K> keyType;

    /**
     * Constructor used to create new BTreeMap.
     *
     * @param engine            used for persistence
     * @param rootRecidRef      reference to root recid
     * @param maxNodeSize       maximal BTree Node size. Node will split if number of entries is higher
     * @param valsOutsideNodes  Store Values outside of BTree Nodes in separate record?
     * @param counterRecid      recid under which `Atomic.Long` is stored, or `0` for no counter
     * @param numberOfNodeMetas number of meta records associated with each BTree node
     * @param disableLocks      makes class thread-unsafe but bit faster
     */
    public BTreeMapImpl( Engine engine, long rootRecidRef, int maxNodeSize, boolean valsOutsideNodes, long counterRecid,
                         Class<K> keyType, Class<V> valueType, SerializerFactory serializerFactory,
                         int numberOfNodeMetas, boolean disableLocks, boolean hasValues
    )
    {
        this.keyType = keyType;
        this.valueType = valueType;
        this.serializerFactory = serializerFactory;
        if( maxNodeSize % 2 != 0 )
        {
            throw new IllegalArgumentException( "maxNodeSize must be dividable by 2" );
        }
        if( maxNodeSize < 6 )
        {
            throw new IllegalArgumentException( "maxNodeSize too low" );
        }
        if( maxNodeSize > 126 )
        {
            throw new IllegalArgumentException( "maxNodeSize too high" );
        }
        if( rootRecidRef <= 0 || counterRecid < 0 || numberOfNodeMetas < 0 )
        {
            throw new IllegalArgumentException();
        }

        this.rootRecidRef = rootRecidRef;
        this.hasValues = hasValues;
        this.valsOutsideNodes = valsOutsideNodes;
        this.engine = engine;
        this.maxNodeSize = maxNodeSize;
        this.numberOfNodeMetas = numberOfNodeMetas;

        this.nodeSerializer = new BTreeNodeSerializer( valsOutsideNodes, serializerFactory.createKeySerializer( keyType ),
                                                       serializerFactory.createValueSerializer( valueType ),
                                                       numberOfNodeMetas );

        this.keySet = new BTreeKeySet( this, hasValues );

        if( counterRecid != 0 )
        {
            this.counter = new Atomic.Long( engine, counterRecid, serializerFactory.createValueSerializer( java.lang.Long.class ) );
            Bind.size( this, counter );
        }
        else
        {
            this.counter = null;
        }

        //load left edge refs
        ArrayList leftEdges2 = new ArrayList<Long>();
        long r = engine.get( rootRecidRef, InternalSerializers.LONG );
        for(; ; )
        {
            BTreeNode n = engine.get( r, nodeSerializer );
            leftEdges2.add( r );
            if( n.isLeaf() )
            {
                break;
            }
            r = n.child()[ 0 ];
        }
        Collections.reverse( leftEdges2 );
        leftEdges = new CopyOnWriteArrayList<Long>( leftEdges2 );
    }

    /**
     * hack used for DB Catalog
     */
    public SortedMap<String, Object> preinitCatalog( DbImpl db )
    {

        Long rootRef = db.getEngine().get( Engine.CATALOG_RECID, InternalSerializers.LONG );

        if( rootRef == null )
        {
            if( db.getEngine().isReadOnly() )
            {
                return Collections.unmodifiableSortedMap( new TreeMap<String, Object>() );
            }

            BTreeNodeSerializer rootSerializer = new BTreeNodeSerializer( false, BTreeKeySerializer.STRING,
                                                                          db.getDefaultSerializer(), 0 );
            BTreeNode root = new BTreeLeafNode( new Object[]{ null, null }, new Object[]{ }, 0 );
            rootRef = db.getEngine().put( root, rootSerializer );
            db.getEngine().update( Engine.CATALOG_RECID, rootRef, InternalSerializers.LONG );
            db.getEngine().commit();
        }
        return new BTreeMapImpl<String, Object>( db.getEngine(), Engine.CATALOG_RECID, 32, false, 0,
                                                 java.lang.String.class, Object.class, serializerFactory,
                                                 0, false, hasValues );
    }

    /**
     * creates empty root node and returns recid of its reference
     */
    public static long createRootRef( Engine engine,
                                      KeySerializer keySer,
                                      ValueSerializer valueSer,
                                      int numberOfNodeMetas
    )
    {
        final BTreeLeafNode emptyRoot = new BTreeLeafNode( new Object[]{ null, null }, new Object[]{ }, 0 );
        //empty root is serializer simpler way, so we can use dummy values
        long rootRecidVal = engine.put( emptyRoot, new BTreeNodeSerializer( false, keySer, valueSer, numberOfNodeMetas ) );
        return engine.put( rootRecidVal, InternalSerializers.LONG );
    }

    /**
     * Find the first children node with a key equal or greater than the given key.
     * If all items are smaller it returns `keys.length`
     */
    protected final int findChildren( final K key, final K[] keys )
    {
        int left = 0;
        if( keys[ 0 ] == null )
        {
            left++;
        }
        int right = keys[ keys.length - 1 ] == null ? keys.length - 1 : keys.length;

        int middle;

        // binary search
        while( true )
        {
            middle = ( left + right ) / 2;
            if( keys[ middle ] == null )
            {
                return middle; //null is positive infinitive
            }
            if( getKeySerializer().getComparator().compare( keys[ middle ], key ) < 0 )
            {
                left = middle + 1;
            }
            else
            {
                right = middle;
            }
            if( left >= right )
            {
                return right;
            }
        }
    }

    @Override
    public V get( Object key )
    {
        return (V) get( (K) key, true );
    }

    protected V get( K key, boolean expandValue )
    {
        if( key == null )
        {
            throw new NullPointerException();
        }
        K v = (K) key;
        long current = engine.get( rootRecidRef, InternalSerializers.LONG ); //get root

        BTreeNode<K, V> A = engine.get( current, nodeSerializer );

        //dive until  leaf
        while( !A.isLeaf() )
        {
            current = nextDir( (BTreeDirNode) A, v );
            A = engine.get( current, nodeSerializer );
        }

        //now at leaf level
        BTreeLeafNode<K, V> leaf = (BTreeLeafNode) A;
        int pos = findChildren( v, leaf.keys );
        while( pos == leaf.keys.length )
        {
            //follow next link on leaf until necessary
            leaf = (BTreeLeafNode) engine.get( leaf.next, nodeSerializer );
            pos = findChildren( v, leaf.keys );
        }

        if( pos == leaf.keys.length - 1 )
        {
            return null; //last key is always deleted
        }
        //finish search
        if( leaf.keys[ pos ] != null && 0 == getKeySerializer().getComparator().compare( v, leaf.keys[ pos ] ) )
        {
            V ret = leaf.vals[ pos - 1 ];
            return expandValue ? valExpand( ret ) : ret;
        }
        else
        {
            return null;
        }
    }

    protected V valExpand( Object ret )
    {
        if( valsOutsideNodes && ret != null )
        {
            long recid = ( (BTreeValRef) ret ).recid;
            ret = engine.get( recid, serializerFactory.createValueSerializer( valueType ) );
        }
        return (V) ret;
    }

    protected final long nextDir( BTreeDirNode<K, V> d, K key )
    {
        int pos = findChildren( key, d.keys ) - 1;
        if( pos < 0 )
        {
            pos = 0;
        }
        return d.child[ pos ];
    }

    @Override
    public V put( K key, V value )
    {
        if( key == null || value == null )
        {
            throw new NullPointerException();
        }
        return put2( key, value, false );
    }

    protected V put2( final K key, final V value2, final boolean putOnlyIfAbsent )
    {
        K v = key;
        if( v == null )
        {
            throw new IllegalArgumentException( "null key" );
        }
        if( value2 == null )
        {
            throw new IllegalArgumentException( "null value" );
        }

        V value = value2;
        if( valsOutsideNodes )
        {
            long recid = engine.put( value2, serializerFactory.createValueSerializer( valueType ) );
            value = (V) new BTreeValRef( recid );
        }

        int stackPos = -1;
        long[] stackVals = new long[ 4 ];

        final long rootRecid = engine.get( rootRecidRef, InternalSerializers.LONG );
        long current = rootRecid;

        BTreeNode<K, V> A = engine.get( current, nodeSerializer );
        while( !A.isLeaf() )
        {
            long t = current;
            current = nextDir( (BTreeDirNode) A, v );
            assert ( current > 0 ) : A;
            if( current == A.child()[ A.child().length - 1 ] )
            {
                //is link, do nothing
            }
            else
            {
                //stack push t
                stackPos++;
                if( stackVals.length == stackPos ) //grow if needed
                {
                    stackVals = Arrays.copyOf( stackVals, stackVals.length * 2 );
                }
                stackVals[ stackPos ] = t;
            }
            A = engine.get( current, nodeSerializer );
        }
        int level = 1;

        long p = 0;
        try
        {
            while( true )
            {
                boolean found;
                do
                {
                    lock( nodeLocks, current );
                    found = true;
                    A = engine.get( current, nodeSerializer );
                    int pos = findChildren( v, A.keys() );
                    //check if keys is already in tree
                    if( pos < A.keys().length - 1 && v != null && A.keys()[ pos ] != null &&
                        0 == getKeySerializer().getComparator().compare( v, A.keys()[ pos ] ) )
                    {
                        //yes key is already in tree
                        Object oldVal = A.vals()[ pos - 1 ];
                        if( putOnlyIfAbsent )
                        {
                            //is not absent, so quit
                            unlock( nodeLocks, current );
                            if( CC.PARANOID )
                            {
                                assertNoLocks( nodeLocks );
                            }
                            return valExpand( oldVal );
                        }
                        //insert new
                        Object[] vals = Arrays.copyOf( A.vals(), A.vals().length );
                        vals[ pos - 1 ] = value;

                        A = new BTreeLeafNode( Arrays.copyOf( A.keys(), A.keys().length ), vals, ( (BTreeLeafNode) A ).next );
                        assert ( nodeLocks.get( current ) == Thread.currentThread() );
                        engine.update( current, A, nodeSerializer );
                        //already in here
                        V ret = valExpand( oldVal );
                        notify( key, ret, value2 );
                        unlock( nodeLocks, current );
                        if( CC.PARANOID )
                        {
                            assertNoLocks( nodeLocks );
                        }
                        return ret;
                    }

                    //if v > highvalue(a)
                    if( A.highKey() != null && getKeySerializer().getComparator().compare( v, A.highKey() ) > 0 )
                    {
                        //follow link until necessary
                        unlock( nodeLocks, current );
                        found = false;
                        int pos2 = findChildren( v, A.keys() );
                        while( A != null && pos2 == A.keys().length )
                        {
                            //TODO lock?
                            long next = A.next();

                            if( next == 0 )
                            {
                                break;
                            }
                            current = next;
                            A = engine.get( current, nodeSerializer );
                            pos2 = findChildren( v, A.keys() );
                        }
                    }
                }
                while( !found );

                // can be new item inserted into A without splitting it?
                if( A.keys().length - ( A.isLeaf() ? 2 : 1 ) < maxNodeSize )
                {
                    int pos = findChildren( v, A.keys() );
                    Object[] keys = arrayPut( A.keys(), pos, v );

                    if( A.isLeaf() )
                    {
                        Object[] vals = arrayPut( A.vals(), pos - 1, value );
                        BTreeLeafNode n = new BTreeLeafNode( keys, vals, ( (BTreeLeafNode) A ).next );
                        assert ( nodeLocks.get( current ) == Thread.currentThread() );
                        engine.update( current, n, nodeSerializer );
                    }
                    else
                    {
                        assert ( p != 0 );
                        long[] child = arrayLongPut( A.child(), pos, p );
                        BTreeDirNode d = new BTreeDirNode( keys, child );
                        assert ( nodeLocks.get( current ) == Thread.currentThread() );
                        engine.update( current, d, nodeSerializer );
                    }

                    notify( key, null, value2 );
                    unlock( nodeLocks, current );
                    if( CC.PARANOID )
                    {
                        assertNoLocks( nodeLocks );
                    }
                    return null;
                }
                else
                {
                    //node is not safe, it requires splitting
                    final int pos = findChildren( v, A.keys() );
                    final Object[] keys = arrayPut( A.keys(), pos, v );
                    final Object[] vals = ( A.isLeaf() ) ? arrayPut( A.vals(), pos - 1, value ) : null;
                    final long[] child = A.isLeaf() ? null : arrayLongPut( A.child(), pos, p );
                    final int splitPos = keys.length / 2;
                    BTreeNode B;
                    if( A.isLeaf() )
                    {
                        Object[] vals2 = Arrays.copyOfRange( vals, splitPos, vals.length );

                        B = new BTreeLeafNode(
                            Arrays.copyOfRange( keys, splitPos, keys.length ),
                            vals2,
                            ( (BTreeLeafNode) A ).next );
                    }
                    else
                    {
                        B = new BTreeDirNode( Arrays.copyOfRange( keys, splitPos, keys.length ),
                                              Arrays.copyOfRange( child, splitPos, keys.length ) );
                    }
                    long q = engine.put( B, nodeSerializer );
                    if( A.isLeaf() )
                    {  //  splitPos+1 is there so A gets new high  value (key)
                        Object[] keys2 = Arrays.copyOf( keys, splitPos + 2 );
                        keys2[ keys2.length - 1 ] = keys2[ keys2.length - 2 ];
                        Object[] vals2 = Arrays.copyOf( vals, splitPos );

                        //TODO check high/low keys overlap
                        A = new BTreeLeafNode( keys2, vals2, q );
                    }
                    else
                    {
                        long[] child2 = Arrays.copyOf( child, splitPos + 1 );
                        child2[ splitPos ] = q;
                        A = new BTreeDirNode( Arrays.copyOf( keys, splitPos + 1 ), child2 );
                    }
                    assert ( nodeLocks.get( current ) == Thread.currentThread() );
                    engine.update( current, A, nodeSerializer );

                    if( ( current != rootRecid ) )
                    { //is not root
                        unlock( nodeLocks, current );
                        p = q;
                        v = (K) A.highKey();
                        level = level + 1;
                        if( stackPos != -1 )
                        { //if stack is not empty
                            current = stackVals[ stackPos-- ];
                        }
                        else
                        {
                            //current := the left most node at level
                            current = leftEdges.get( level - 1 );
                        }
                        assert ( current > 0 );
                    }
                    else
                    {
                        BTreeNode R = new BTreeDirNode(
                            new Object[]{ A.keys()[ 0 ], A.highKey(), B.isLeaf() ? null : B.highKey() },
                            new long[]{ current, q, 0 } );

                        lock( nodeLocks, rootRecidRef );
                        unlock( nodeLocks, current );
                        long newRootRecid = engine.put( R, nodeSerializer );

                        assert ( nodeLocks.get( rootRecidRef ) == Thread.currentThread() );
                        engine.update( rootRecidRef, newRootRecid, InternalSerializers.LONG );
                        //add newRootRecid into leftEdges
                        leftEdges.add( newRootRecid );

                        notify( key, null, value2 );
                        unlock( nodeLocks, rootRecidRef );
                        if( CC.PARANOID )
                        {
                            assertNoLocks( nodeLocks );
                        }
                        return null;
                    }
                }
            }
        }
        catch( RuntimeException e )
        {
            unlockAll( nodeLocks );
            throw e;
        }
        catch( Exception e )
        {
            unlockAll( nodeLocks );
            throw new RuntimeException( e );
        }
    }

    @Override
    public V remove( Object key )
    {
        return remove2( (K) key, null );
    }

    private V remove2( final K key, final V value )
    {
        long current = engine.get( rootRecidRef, InternalSerializers.LONG );

        BTreeNode<K, V> A = engine.get( current, nodeSerializer );
        while( !A.isLeaf() )
        {
            current = nextDir( (BTreeDirNode<K, V>) A, key );
            A = engine.get( current, nodeSerializer );
        }

        try
        {
            while( true )
            {

                lock( nodeLocks, current );
                A = engine.get( current, nodeSerializer );
                int pos = findChildren( key, A.keys() );
                if( pos < A.keys().length && key != null && A.keys()[ pos ] != null &&
                    0 == getKeySerializer().getComparator().compare( key, A.keys()[ pos ] ) )
                {
                    //check for last node which was already deleted
                    if( pos == A.keys().length - 1 && value == null )
                    {
                        unlock( nodeLocks, current );
                        return null;
                    }

                    //delete from node
                    Object oldVal = A.vals()[ pos - 1 ];
                    oldVal = valExpand( oldVal );
                    if( value != null && !value.equals( oldVal ) )
                    {
                        unlock( nodeLocks, current );
                        return null;
                    }

                    Object[] keys2 = new Object[ A.keys().length - 1 ];
                    System.arraycopy( A.keys(), 0, keys2, 0, pos );
                    System.arraycopy( A.keys(), pos + 1, keys2, pos, keys2.length - pos );

                    Object[] vals2 = new Object[ A.vals().length - 1 ];
                    System.arraycopy( A.vals(), 0, vals2, 0, pos - 1 );
                    System.arraycopy( A.vals(), pos, vals2, pos - 1, vals2.length - ( pos - 1 ) );

                    A = new BTreeLeafNode( keys2, vals2, ( (BTreeLeafNode) A ).next );
                    assert ( nodeLocks.get( current ) == Thread.currentThread() );
                    engine.update( current, A, nodeSerializer );
                    notify( (K) key, (V) oldVal, null );
                    unlock( nodeLocks, current );
                    return (V) oldVal;
                }
                else
                {
                    unlock( nodeLocks, current );
                    //follow link until necessary
                    if( A.highKey() != null && getKeySerializer().getComparator().compare( key, A.highKey() ) > 0 )
                    {
                        int pos2 = findChildren( key, A.keys() );
                        while( pos2 == A.keys().length )
                        {
                            //TODO lock?
                            current = ( (BTreeLeafNode) A ).next;
                            A = engine.get( current, nodeSerializer );
                        }
                    }
                    else
                    {
                        return null;
                    }
                }
            }
        }
        catch( RuntimeException e )
        {
            unlockAll( nodeLocks );
            throw e;
        }
        catch( Exception e )
        {
            unlockAll( nodeLocks );
            throw new RuntimeException( e );
        }
    }

    @Override
    public void clear()
    {
        Iterator iter = keyIterator();
        while( iter.hasNext() )
        {
            iter.next();
            iter.remove();
        }
    }

    protected Entry<K, V> makeEntry( Object key, Object value )
    {
        assert ( !( value instanceof BTreeValRef ) );
        return new SimpleImmutableEntry<K, V>( (K) key, (V) value );
    }

    @Override
    public boolean isEmpty()
    {
        return !keyIterator().hasNext();
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

        long size = 0;
        BTreeIterator iter = new BTreeIterator( this );
        while( iter.hasNext() )
        {
            iter.advance();
            size++;
        }
        return size;
    }

    @Override
    public V putIfAbsent( K key, V value )
    {
        if( key == null || value == null )
        {
            throw new NullPointerException();
        }
        return put2( key, value, true );
    }

    @Override
    public boolean remove( Object key, Object value )
    {
        if( key == null )
        {
            throw new NullPointerException();
        }
        if( value == null )
        {
            return false;
        }
        return remove2( (K) key, (V) value ) != null;
    }

    @Override
    public boolean replace( final K key, final V oldValue, final V newValue )
    {
        if( key == null || oldValue == null || newValue == null )
        {
            throw new NullPointerException();
        }

        long current = engine.get( rootRecidRef, InternalSerializers.LONG );

        BTreeNode<K, V> node = engine.get( current, nodeSerializer );
        //dive until leaf is found
        while( !node.isLeaf() )
        {
            current = nextDir( (BTreeDirNode) node, key );
            node = engine.get( current, nodeSerializer );
        }

        lock( nodeLocks, current );
        try
        {

            BTreeLeafNode<K, V> leaf = (BTreeLeafNode) engine.get( current, nodeSerializer );
            int pos = findChildren( key, leaf.keys );

            while( pos == leaf.keys.length )
            {
                //follow leaf link until necessary
                lock( nodeLocks, leaf.next );
                unlock( nodeLocks, current );
                current = leaf.next;
                leaf = (BTreeLeafNode) engine.get( current, nodeSerializer );
                pos = findChildren( key, leaf.keys );
            }

            boolean ret = false;
            if( key != null && leaf.keys[ pos ] != null &&
                getKeySerializer().getComparator().compare( key, leaf.keys[ pos ] ) == 0 )
            {
                Object val = leaf.vals[ pos - 1 ];
                val = valExpand( val );
                if( oldValue.equals( val ) )
                {
                    Object[] vals = Arrays.copyOf( leaf.vals, leaf.vals.length );
                    notify( key, oldValue, newValue );
                    vals[ pos - 1 ] = newValue;
                    if( valsOutsideNodes )
                    {
                        long recid = engine.put( newValue, serializerFactory.createValueSerializer( valueType ) );
                        vals[ pos - 1 ] = new BTreeValRef( recid );
                    }

                    leaf = new BTreeLeafNode( Arrays.copyOf( leaf.keys, leaf.keys.length ), vals, leaf.next );

                    assert ( nodeLocks.get( current ) == Thread.currentThread() );
                    engine.update( current, leaf, nodeSerializer );

                    ret = true;
                }
            }
            unlock( nodeLocks, current );
            return ret;
        }
        catch( RuntimeException e )
        {
            unlockAll( nodeLocks );
            throw e;
        }
        catch( Exception e )
        {
            unlockAll( nodeLocks );
            throw new RuntimeException( e );
        }
    }

    @Override
    public V replace( final K key, final V value )
    {
        if( key == null || value == null )
        {
            throw new NullPointerException();
        }
        long current = engine.get( rootRecidRef, InternalSerializers.LONG );

        BTreeNode<K, V> node = engine.get( current, nodeSerializer );
        //dive until leaf is found
        while( !node.isLeaf() )
        {
            current = nextDir( (BTreeDirNode) node, key );
            node = engine.get( current, nodeSerializer );
        }

        lock( nodeLocks, current );
        try
        {

            BTreeLeafNode<K, V> leaf = (BTreeLeafNode) engine.get( current, nodeSerializer );
            int pos = findChildren( key, leaf.keys );

            while( pos == leaf.keys.length )
            {
                //follow leaf link until necessary
                lock( nodeLocks, leaf.next );
                unlock( nodeLocks, current );
                current = leaf.next;
                leaf = (BTreeLeafNode) engine.get( current, nodeSerializer );
                pos = findChildren( key, leaf.keys );
            }

            Object ret = null;
            if( key != null && leaf.keys()[ pos ] != null &&
                0 == getKeySerializer().getComparator().compare( key, leaf.keys[ pos ] ) )
            {
                Object[] vals = Arrays.copyOf( leaf.vals, leaf.vals.length );
                Object oldVal = vals[ pos - 1 ];
                ret = valExpand( oldVal );
                notify( key, (V) ret, value );
                vals[ pos - 1 ] = value;
                if( valsOutsideNodes && value != null )
                {
                    long recid = engine.put( value, serializerFactory.createValueSerializer( valueType ) );
                    vals[ pos - 1 ] = new BTreeValRef( recid );
                }

                leaf = new BTreeLeafNode( Arrays.copyOf( leaf.keys, leaf.keys.length ), vals, leaf.next );
                assert ( nodeLocks.get( current ) == Thread.currentThread() );
                engine.update( current, leaf, nodeSerializer );
            }
            unlock( nodeLocks, current );
            return (V) ret;
        }
        catch( RuntimeException e )
        {
            unlockAll( nodeLocks );
            throw e;
        }
        catch( Exception e )
        {
            unlockAll( nodeLocks );
            throw new RuntimeException( e );
        }
    }

    @Override
    public Comparator<? super K> comparator()
    {
        return getKeySerializer().getComparator();
    }

    @Override
    public Entry<K, V> firstEntry()
    {
        final long rootRecid = engine.get( rootRecidRef, InternalSerializers.LONG );
        BTreeNode n = engine.get( rootRecid, nodeSerializer );
        while( !n.isLeaf() )
        {
            n = engine.get( n.child()[ 0 ], nodeSerializer );
        }
        BTreeLeafNode l = (BTreeLeafNode) n;
        //follow link until necessary
        while( l.keys.length == 2 )
        {
            if( l.next == 0 )
            {
                return null;
            }
            l = (BTreeLeafNode) engine.get( l.next, nodeSerializer );
        }
        return makeEntry( l.keys[ 1 ], valExpand( l.vals[ 0 ] ) );
    }

    @Override
    public Entry<K, V> pollFirstEntry()
    {
        while( true )
        {
            Entry<K, V> e = firstEntry();
            if( e == null || remove( e.getKey(), e.getValue() ) )
            {
                return e;
            }
        }
    }

    @Override
    public Entry<K, V> pollLastEntry()
    {
        while( true )
        {
            Entry<K, V> e = lastEntry();
            if( e == null || remove( e.getKey(), e.getValue() ) )
            {
                return e;
            }
        }
    }

    protected Entry<K, V> findSmaller( K key, boolean inclusive )
    {
        if( key == null )
        {
            throw new NullPointerException();
        }
        final long rootRecid = engine.get( rootRecidRef, InternalSerializers.LONG );
        BTreeNode n = engine.get( rootRecid, nodeSerializer );

        Entry<K, V> k = findSmallerRecur( n, key, inclusive );
        if( k == null || ( k.getValue() == null ) )
        {
            return null;
        }
        return k;
    }

    private Entry<K, V> findSmallerRecur( BTreeNode<K, V> n, K key, boolean inclusive )
    {
        final boolean leaf = n.isLeaf();
        final int start = leaf ? n.keys().length - 2 : n.keys().length - 1;
        final int end = leaf ? 1 : 0;
        final int res = inclusive ? 1 : 0;
        for( int i = start; i >= end; i-- )
        {
            final K key2 = n.keys()[ i ];
            int comp = ( key2 == null ) ? -1 : getKeySerializer().getComparator().compare( key2, key );
            if( comp < res )
            {
                if( leaf )
                {
                    return key2 == null ? null :
                           makeEntry( key2, valExpand( n.vals()[ i - 1 ] ) );
                }
                else
                {
                    final long recid = n.child()[ i ];
                    if( recid == 0 )
                    {
                        continue;
                    }
                    BTreeNode n2 = engine.get( recid, nodeSerializer );
                    Entry<K, V> ret = findSmallerRecur( n2, key, inclusive );
                    if( ret != null )
                    {
                        return ret;
                    }
                }
            }
        }

        return null;
    }

    @Override
    public Entry<K, V> lastEntry()
    {
        final long rootRecid = engine.get( rootRecidRef, InternalSerializers.LONG );
        BTreeNode n = engine.get( rootRecid, nodeSerializer );
        Entry e = lastEntryRecur( n );
        if( e != null && e.getValue() == null )
        {
            return null;
        }
        return e;
    }

    private Entry<K, V> lastEntryRecur( BTreeNode n )
    {
        if( n.isLeaf() )
        {
            //follow next node if available
            if( n.next() != 0 )
            {
                BTreeNode n2 = engine.get( n.next(), nodeSerializer );
                Entry<K, V> ret = lastEntryRecur( n2 );
                if( ret != null )
                {
                    return ret;
                }
            }

            //iterate over keys to find last non null key
            for( int i = n.keys().length - 2; i > 0; i-- )
            {
                Object k = n.keys()[ i ];
                if( k != null && n.vals().length > 0 )
                {
                    Object val = valExpand( n.vals()[ i - 1 ] );
                    if( val != null )
                    {
                        return makeEntry( k, val );
                    }
                }
            }
        }
        else
        {
            //dir node, dive deeper
            for( int i = n.child().length - 1; i >= 0; i-- )
            {
                long childRecid = n.child()[ i ];
                if( childRecid == 0 )
                {
                    continue;
                }
                BTreeNode n2 = engine.get( childRecid, nodeSerializer );
                Entry<K, V> ret = lastEntryRecur( n2 );
                if( ret != null )
                {
                    return ret;
                }
            }
        }
        return null;
    }

    @Override
    public Entry<K, V> lowerEntry( K key )
    {
        if( key == null )
        {
            throw new NullPointerException();
        }
        return findSmaller( key, false );
    }

    @Override
    public K lowerKey( K key )
    {
        Entry<K, V> n = lowerEntry( key );
        return ( n == null ) ? null : n.getKey();
    }

    @Override
    public Entry<K, V> floorEntry( K key )
    {
        if( key == null )
        {
            throw new NullPointerException();
        }
        return findSmaller( key, true );
    }

    @Override
    public K floorKey( K key )
    {
        Entry<K, V> n = floorEntry( key );
        return ( n == null ) ? null : n.getKey();
    }

    @Override
    public Entry<K, V> ceilingEntry( K key )
    {
        if( key == null )
        {
            throw new NullPointerException();
        }
        return findLarger( key, true );
    }

    protected Entry<K, V> findLarger( final K key, boolean inclusive )
    {
        if( key == null )
        {
            return null;
        }

        long current = engine.get( rootRecidRef, InternalSerializers.LONG );

        BTreeNode<K, V> A = engine.get( current, nodeSerializer );

        //dive until  leaf
        while( !A.isLeaf() )
        {
            current = nextDir( (BTreeDirNode) A, key );
            A = engine.get( current, nodeSerializer );
        }

        //now at leaf level
        BTreeLeafNode<K, V> leaf = (BTreeLeafNode) A;
        //follow link until first matching node is found
        final int comp = inclusive ? 1 : 0;
        while( true )
        {
            for( int i = 1; i < leaf.keys.length - 1; i++ )
            {
                if( leaf.keys[ i ] == null )
                {
                    continue;
                }

                if( getKeySerializer().getComparator().compare( key, leaf.keys[ i ] ) < comp )
                {
                    return makeEntry( leaf.keys[ i ], valExpand( leaf.vals[ i - 1 ] ) );
                }
            }
            if( leaf.next == 0 )
            {
                return null; //reached end
            }
            leaf = (BTreeLeafNode) engine.get( leaf.next, nodeSerializer );
        }
    }

    protected Fun.Tuple2<Integer, BTreeLeafNode<K, V>> findLargerNode( final K key, boolean inclusive )
    {
        if( key == null )
        {
            return null;
        }

        long current = engine.get( rootRecidRef, InternalSerializers.LONG );

        BTreeNode<K, V> A = engine.get( current, nodeSerializer );

        //dive until  leaf
        while( !A.isLeaf() )
        {
            current = nextDir( (BTreeDirNode) A, key );
            A = engine.get( current, nodeSerializer );
        }

        //now at leaf level
        BTreeLeafNode<K, V> leaf = (BTreeLeafNode) A;
        //follow link until first matching node is found
        final int comp = inclusive ? 1 : 0;
        while( true )
        {
            for( int i = 1; i < leaf.keys.length - 1; i++ )
            {
                if( leaf.keys[ i ] == null )
                {
                    continue;
                }

                if( getKeySerializer().getComparator().compare( key, leaf.keys[ i ] ) < comp )
                {
                    return Fun.t2( i, leaf );
                }
            }
            if( leaf.next == 0 )
            {
                return null; //reached end
            }
            leaf = (BTreeLeafNode) engine.get( leaf.next, nodeSerializer );
        }
    }

    @Override
    public K ceilingKey( K key )
    {
        if( key == null )
        {
            throw new NullPointerException();
        }
        Entry<K, V> n = ceilingEntry( key );
        return ( n == null ) ? null : n.getKey();
    }

    @Override
    public Entry<K, V> higherEntry( K key )
    {
        if( key == null )
        {
            throw new NullPointerException();
        }
        return findLarger( key, false );
    }

    @Override
    public K higherKey( K key )
    {
        if( key == null )
        {
            throw new NullPointerException();
        }
        Entry<K, V> n = higherEntry( key );
        return ( n == null ) ? null : n.getKey();
    }

    @Override
    public boolean containsKey( Object key )
    {
        if( key == null )
        {
            throw new NullPointerException();
        }
        return get( (K) key, false ) != null;
    }

    @Override
    public boolean containsValue( Object value )
    {
        if( value == null )
        {
            throw new NullPointerException();
        }
        Iterator<V> valueIter = valueIterator();
        while( valueIter.hasNext() )
        {
            if( value.equals( valueIter.next() ) )
            {
                return true;
            }
        }
        return false;
    }

    @Override
    public K firstKey()
    {
        Entry<K, V> e = firstEntry();
        if( e == null )
        {
            throw new NoSuchElementException();
        }
        return e.getKey();
    }

    @Override
    public K lastKey()
    {
        Entry<K, V> e = lastEntry();
        if( e == null )
        {
            throw new NoSuchElementException();
        }
        return e.getKey();
    }

    @Override
    public ConcurrentNavigableMap<K, V> subMap( K fromKey, boolean fromInclusive, K toKey, boolean toInclusive )
    {
        if( fromKey == null || toKey == null )
        {
            throw new NullPointerException();
        }
        return new BTreeSubMap<K, V>
            ( this, fromKey, fromInclusive, toKey, toInclusive );
    }

    @Override
    public ConcurrentNavigableMap<K, V> headMap( K toKey, boolean inclusive )
    {
        if( toKey == null )
        {
            throw new NullPointerException();
        }
        return new BTreeSubMap<K, V>
            ( this, null, false, toKey, inclusive );
    }

    @Override
    public ConcurrentNavigableMap<K, V> tailMap( K fromKey, boolean inclusive )
    {
        if( fromKey == null )
        {
            throw new NullPointerException();
        }
        return new BTreeSubMap<K, V>
            ( this, fromKey, inclusive, null, false );
    }

    @Override
    public ConcurrentNavigableMap<K, V> subMap( K fromKey, K toKey )
    {
        return subMap( fromKey, true, toKey, false );
    }

    @Override
    public ConcurrentNavigableMap<K, V> headMap( K toKey )
    {
        return headMap( toKey, false );
    }

    @Override
    public ConcurrentNavigableMap<K, V> tailMap( K fromKey )
    {
        return tailMap( fromKey, true );
    }

    Iterator<K> keyIterator()
    {
        return new BTreeKeyIterator( this );
    }

    Iterator<V> valueIterator()
    {
        return new BTreeValueIterator( this );
    }

    Iterator<Entry<K, V>> entryIterator()
    {
        return new BTreeEntryIterator( this );
    }


    /* ---------------- View methods -------------- */

    @Override
    public NavigableSet<K> keySet()
    {
        return keySet;
    }

    @Override
    public NavigableSet<K> navigableKeySet()
    {
        return keySet;
    }

    @Override
    public Collection<V> values()
    {
        return values;
    }

    @Override
    public Set<Entry<K, V>> entrySet()
    {
        return entrySet;
    }

    @Override
    public ConcurrentNavigableMap<K, V> descendingMap()
    {
        return descendingMap;
    }

    @Override
    public NavigableSet<K> descendingKeySet()
    {
        return descendingMap.keySet();
    }

    static <E> List<E> toList( Collection<E> c )
    {
        // Using size() here would be a pessimization.
        List<E> list = new ArrayList<E>();
        for( E e : c )
        {
            list.add( e );
        }
        return list;
    }

    private KeySerializer<K> getKeySerializer()
    {
        return serializerFactory.createKeySerializer( keyType );
    }

    /**
     * Make readonly snapshot view of current Map. Snapshot is immutable and not affected by modifications made by other threads.
     * Useful if you need consistent view on Map.
     *
     * Maintaining snapshot have some overhead, underlying Engine is closed after Map view is GCed.
     * Please make sure to release reference to this Map view, so snapshot view can be garbage collected.
     *
     * @return snapshot
     */
    @Override
    public NavigableMap<K, V> snapshot()
    {
        Engine snapshot = TxEngine.createSnapshotFor( engine );

        return new BTreeMapImpl<K, V>( snapshot, rootRecidRef, maxNodeSize, valsOutsideNodes,
                                       counter == null ? 0L : counter.getRecid(),
                                       keyType, valueType, serializerFactory, numberOfNodeMetas, false, hasValues );
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
        assert ( !( oldValue instanceof BTreeValRef ) );
        assert ( !( newValue instanceof BTreeValRef ) );

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
    @Override
    public void close()
    {
        engine.close();
    }

    @Override
    public Engine getEngine()
    {
        return engine;
    }

    public void printTreeStructure()
    {
        final long rootRecid = engine.get( rootRecidRef, InternalSerializers.LONG );
        printRecur( this, rootRecid, "" );
    }

    private static void printRecur( BTreeMapImpl m, long recid, String s )
    {
        BTreeNode n = (BTreeNode) m.engine.get( recid, m.nodeSerializer );
        System.out.println( s + recid + "-" + n );
        if( !n.isLeaf() )
        {
            for( int i = 0; i < n.child().length - 1; i++ )
            {
                long recid2 = n.child()[ i ];
                if( recid2 != 0 )
                {
                    printRecur( m, recid2, s + "  " );
                }
            }
        }
    }

    protected static long[] arrayLongPut( final long[] array, final int pos, final long value )
    {
        final long[] ret = Arrays.copyOf( array, array.length + 1 );
        if( pos < array.length )
        {
            System.arraycopy( array, pos, ret, pos + 1, array.length - pos );
        }
        ret[ pos ] = value;
        return ret;
    }

    /**
     * expand array size by 1, and put value at given position. No items from original array are lost
     */
    protected static Object[] arrayPut( final Object[] array, final int pos, final Object value )
    {
        final Object[] ret = Arrays.copyOf( array, array.length + 1 );
        if( pos < array.length )
        {
            System.arraycopy( array, pos, ret, pos + 1, array.length - pos );
        }
        ret[ pos ] = value;
        return ret;
    }

    protected static void assertNoLocks( LongConcurrentHashMap<Thread> locks )
    {
        LongMap.LongMapIterator<Thread> i = locks.longMapIterator();
        Thread t = null;
        while( i.moveToNext() )
        {
            if( t == null )
            {
                t = Thread.currentThread();
            }
            if( i.value() == t )
            {
                throw new AssertionError( "Node " + i.key() + " is still locked" );
            }
        }
    }

    protected static void unlock( LongConcurrentHashMap<Thread> locks, final long recid )
    {
        final Thread t = locks.remove( recid );
        assert ( t == Thread.currentThread() ) : ( "unlocked wrong thread" );
    }

    protected static void unlockAll( LongConcurrentHashMap<Thread> locks )
    {
        final Thread t = Thread.currentThread();
        LongMap.LongMapIterator<Thread> iter = locks.longMapIterator();
        while( iter.moveToNext() )
        {
            if( iter.value() == t )
            {
                iter.remove();
            }
        }
    }

    protected static void lock( LongConcurrentHashMap<Thread> locks, long recid )
    {
        //feel free to rewrite, if you know better (more efficient) way

        final Thread currentThread = Thread.currentThread();
        //check node is not already locked by this thread
        assert ( locks.get( recid ) != currentThread ) : ( "node already locked by current thread: " + recid );

        while( locks.putIfAbsent( recid, currentThread ) != null )
        {
            LockSupport.parkNanos( 10 );
        }
    }
}
