package org.mapdb;

import java.io.Closeable;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.BlockingQueue;
import org.mapdb.impl.Atomic;
import org.mapdb.impl.Fun;

public interface DB extends Closeable
{
    /**
     * returns name for this object, if it has name and was instanciated by this DB
     */
    String getNameForObject( Object obj );

    /**
     * Opens existing or creates new Hash Tree Map.
     * This collection perform well under concurrent access.
     * Is best for large keys and large values.
     *
     * @param name of the map
     *
     * @return map
     */
    <K, V> HTreeMap<K, V> getHashMap( String name );

    /**
     * Opens existing or creates new Hash Tree Map.
     * This collection perform well under concurrent access.
     * Is best for large keys and large values.
     *
     * @param name         of map
     * @param valueCreator if value is not found, new is created and placed into map.
     *
     * @return map
     */
    <K, V> HTreeMap<K, V> getHashMap( String name, Fun.Function1<V, K> valueCreator );


    /**
     * Opens existing or creates new Hash Tree Set.
     *
     * @param name of the Set
     *
     * @return set
     */
    <K> Set<K> getHashSet( String name );

    /**
     * Opens existing or creates new B-linked-tree Map.
     * This collection performs well under concurrent access.
     * Only trade-off are deletes, which causes tree fragmentation.
     * It is ordered and best suited for small keys and values.
     *
     * @param name of map
     *
     * @return map
     */
    <K, V> BTreeMap<K, V> getTreeMap( String name );

    /**
     * Get Name Catalog.
     * It is metatable which contains information about named collections and records.
     * Each collection constructor takes number of parameters, this map contains those parameters.
     *
     * _Note:_ Do not modify this map, unless you know what you are doing!
     *
     * @return Name Catalog
     */
    SortedMap<String, Object> getCatalog();

    /**
     * Opens existing or creates new B-linked-tree Set.
     *
     * @param name of set
     *
     * @return set
     */
    <K> NavigableSet<K> getTreeSet( String name );

    <E> BlockingQueue<E> getQueue( String name );

    <E> BlockingQueue<E> createQueue( String name, ValueSerializer<E> serializer, boolean useLocks );

    <E> BlockingQueue<E> getStack( String name );

    <E> BlockingQueue<E> createStack( String name, ValueSerializer<E> serializer, boolean useLocks );

    <E> BlockingQueue<E> getCircularQueue( String name );

    <E> BlockingQueue<E> createCircularQueue( String name,
                                              ValueSerializer<E> serializer,
                                              long size
    );

    Atomic.Long createAtomicLong( String name, long initValue );

    Atomic.Long getAtomicLong( String name );

    Atomic.Integer createAtomicInteger( String name, int initValue );

    Atomic.Integer getAtomicInteger( String name );

    Atomic.Boolean createAtomicBoolean( String name, boolean initValue );

    Atomic.Boolean getAtomicBoolean( String name );

    Atomic.String createAtomicString( String name, String initValue );

    Atomic.String getAtomicString( String name );

    <E> Atomic.Var<E> createAtomicVar( String name, E initValue, ValueSerializer<E> serializer );

    <E> Atomic.Var<E> getAtomicVar( String name );

    /**
     * return record with given name or null if name does not exist
     */
    <E> E get( String name );

    boolean exists( String name );

    /**
     * delete record/collection with given name
     */
    void delete( String name );

    /**
     * return map of all named collections/records
     */
    Map<String, Object> getAll();

    /**
     * rename named record into newName
     *
     * @param oldName current name of record/collection
     * @param newName new name of record/collection
     *
     * @throws java.util.NoSuchElementException if oldName does not exist
     */
    void rename( String oldName, String newName );

    /**
     * Checks that object with given name does not exist yet.
     *
     * @param name to check
     *
     * @throws IllegalArgumentException if name is already used
     */
    void checkNameNotExists( String name );

    void checkNotClosed();

    /**
     * @return true if DB is closed and can no longer be used
     */
    boolean isClosed();

    /**
     * Commit changes made on collections loaded by this DB
     *
     * @see Engine#commit()
     */
    void commit();

    /**
     * Rollback changes made on collections loaded by this DB
     *
     * @see Engine#rollback()
     */
    void rollback();

    /**
     * Perform storage maintenance.
     * Typically compact underlying storage and reclaim unused space.
     * <p/>
     * NOTE: MapDB does not have smart defragmentation algorithms. So compaction usually recreates entire
     * store from scratch. This may require additional disk space.
     */
    void compact();

    /**
     * Make readonly snapshot view of DB and all of its collection
     * Collections loaded by this instance are not affected (are still mutable).
     * You have to load new collections from DB returned by this method
     *
     * @return readonly snapshot view
     */
    DB snapshot();

    /**
     * @return underlying engine which takes care of persistence for this DB.
     */
    Engine getEngine();

    /**
     * Returns new builder for HashMap with given name
     *
     * @param name of map to create
     *
     * @return maker, call `.make()` to create map
     *
     * @throws IllegalArgumentException if name is already used
     */
    HTreeMapMaker createHashMap( String name );

    /**
     * Returns new builder for TreeMap with given name
     *
     * @param name of map to create
     *
     * @return maker, call `.make()` to create map
     *
     * @throws IllegalArgumentException if name is already used
     */
    BTreeMapMaker createTreeMap( String name );

    /**
     * Creates new TreeSet.
     *
     * @param name of set to create
     *
     * @return maker used to construct set
     *
     * @throws IllegalArgumentException if name is already used
     */
    BTreeSetMaker createTreeSet( String name );

    ValueSerializer getDefaultSerializer();
}
