package org.mapdb;

import java.io.File;
import org.mapdb.impl.Fun;

public interface DBFactory
{
    DBFactory newHeapDB();

    DBFactory newMemoryDB();

    DBFactory newMemoryDirectDB();

    DBFactory newAppendFileDB( File file );

    DBFactory newFileDB( File file );

    /**
     * Transaction journal is enabled by default
     * You must call <b>DB.commit()</b> to save your changes.
     * It is possible to disable transaction journal for better write performance
     * In this case all integrity checks are sacrificed for faster speed.
     * <p/>
     * If transaction journal is disabled, all changes are written DIRECTLY into store.
     * You must call DB.close() method before exit,
     * otherwise your store <b>WILL BE CORRUPTED</b>
     *
     * @return this builder
     */
    DBFactory transactionDisable();

    /**
     * Install callback condition, which decides if some record is to be included in cache.
     * Condition should return `true` for every record which should be included
     *
     * This could be for example useful to include only BTree Directory Nodes and leave values and Leaf nodes outside of cache.
     *
     * !!! Warning:!!!
     *
     * Cache requires **consistent** true or false. Failing to do so will result in inconsitent cache and possible data corruption.
     *
     * Condition is also executed several times, so it must be very fast
     *
     * You should only use very simple logic such as `value instanceof SomeClass`.
     *
     * @return this builder
     */
    DBFactory cacheCondition( Fun.RecordCondition cacheCondition );

    /**
     * /**
     * Instance cache is enabled by default.
     * This greatly decreases serialization overhead and improves performance.
     * Call this method to disable instance cache, so an object will always be deserialized.
     * <p/>
     * This may workaround some problems
     *
     * @return this builder
     */
    DBFactory cacheDisable();

    /**
     * Enables unbounded hard reference cache.
     * This cache is good if you have lot of available memory.
     * <p/>
     * All fetched records are added to HashMap and stored with hard reference.
     * To prevent OutOfMemoryExceptions MapDB monitors free memory,
     * if it is bellow 25% cache is cleared.
     *
     * @return this builder
     */
    DBFactory cacheHardRefEnable();

    /**
     * Enables unbounded cache which uses <code>WeakReference</code>.
     * Items are removed from cache by Garbage Collector
     *
     * @return this builder
     */
    DBFactory cacheWeakRefEnable();

    /**
     * Enables unbounded cache which uses <code>SoftReference</code>.
     * Items are removed from cache by Garbage Collector
     *
     * @return this builder
     */
    DBFactory cacheSoftRefEnable();

    /**
     * Enables Least Recently Used cache. It is fixed size cache and it removes less used items to make space.
     *
     * @return this builder
     */
    DBFactory cacheLRUEnable();

    /**
     * Enables Memory Mapped Files, much faster storage option. However on 32bit JVM this mode could corrupt
     * your DB thanks to 4GB memory addressing limit.
     *
     * You may experience `java.lang.OutOfMemoryError: Map failed` exception on 32bit JVM, if you enable this
     * mode.
     */
    DBFactory mmapFileEnable();

    /**
     * Keeps small-frequently-used part of storage files memory mapped, but main area is accessed using Random Access File.
     *
     * This mode is good performance compromise between Memory Mapped Files and old slow Random Access Files.
     *
     * Index file is typically 5% of storage. It contains small frequently read values,
     * which is where memory mapped file excel.
     *
     * With this mode you will experience `java.lang.OutOfMemoryError: Map failed` exceptions on 32bit JVMs
     * eventually. But storage size limit is pushed to somewhere around 40GB.
     */
    DBFactory mmapFileEnablePartial();

    void assertNotInMemoryVolume();

    /**
     * Enable Memory Mapped Files only if current JVM supports it (is 64bit).
     */
    DBFactory mmapFileEnableIfSupported();

    /**
     * Set cache size. Interpretations depends on cache type.
     * For fixed size caches (such as FixedHashTable cache) it is maximal number of items in cache.
     * <p/>
     * For unbounded caches (such as HardRef cache) it is initial capacity of underlying table (HashMap).
     * <p/>
     * Default cache size is 32768.
     *
     * @param cacheSize new cache size
     *
     * @return this builder
     */
    DBFactory cacheSize( int cacheSize );

    /**
     * MapDB supports snapshots. `TxEngine` requires additional locking which has small overhead when not used.
     * Snapshots are disabled by default. This option switches the snapshots on.
     *
     * @return this builder
     */
    DBFactory snapshotEnable();

    /**
     * Enables mode where all modifications are queued and written into disk on Background Writer Thread.
     * So all modifications are performed in asynchronous mode and do not block.
     *
     * <p/>
     * Enabling this mode might increase performance for single threaded apps.
     *
     * @return this builder
     */
    DBFactory asyncWriteEnable();

    /**
     * Set flush interval for write cache, by default is 0
     * <p/>
     * When BTreeMap is constructed from ordered set, tree node size is increasing linearly with each
     * item added. Each time new key is added to tree node, its size changes and
     * storage needs to find new place. So constructing BTreeMap from ordered set leads to large
     * store fragmentation.
     * <p/>
     * Setting flush interval is workaround as BTreeMap node is always updated in memory (write cache)
     * and only final version of node is stored on disk.
     *
     * @param delay flush write cache every N miliseconds
     *
     * @return this builder
     */
    DBFactory asyncWriteFlushDelay( int delay );

    /**
     * Set size of async Write Queue. Default size is 32 000
     * <p/>
     * Using too large queue size can lead to out of memory exception.
     *
     * @param queueSize of queue
     *
     * @return this builder
     */
    DBFactory asyncWriteQueueSize( int queueSize );

    /**
     * MapDB is thread-safe by default.
     * Concurrency locking brings some overhead which is unnecessary for single-threaded applications.
     * This switch will disable concurrency locks and make MapDB thread-unsafe,
     * in exchange for small performance bonus in single-threaded use.
     *
     * Usafull for data imports etc.
     *
     * @return this builder
     */
    DBFactory concurrencyDisable();

    /**
     * Try to delete files after DB is closed.
     * File deletion may silently fail, especially on Windows where buffer needs to be unmapped file delete.
     *
     * @return this builder
     */
    DBFactory deleteFilesAfterClose();

    /**
     * Adds JVM shutdown hook and closes DB just before JVM;
     *
     * @return this builder
     */
    DBFactory closeOnJvmShutdown();

    /**
     * Enables record compression.
     * <p/>
     * Make sure you enable this every time you reopen store, otherwise record de-serialization fails unpredictably.
     *
     * @return this builder
     */
    DBFactory compressionEnable();

    /**
     * Adds CRC32 checksum at end of each record to check data integrity.
     * It throws 'IOException("Checksum does not match, data broken")' on de-serialization if data are corrupted
     * <p/>
     * Make sure you enable this every time you reopen store, otherwise record de-serialization fails.
     *
     * @return this builder
     */
    DBFactory checksumEnable();

    /**
     * DB Get methods such as {@link DB#getTreeMap(String)} or {@link DB#getAtomicLong(String)} auto create
     * new record with default values, if record with given name does not exist. This could be problem if you would like to enforce
     * stricter database schema. So this parameter disables record auto creation.
     *
     * If this set, `DB.getXX()` will throw an exception if given name does not exist, instead of creating new record (or collection)
     *
     * @return this builder
     */
    DBFactory strictDBGet();

    /**
     * Open store in read-only mode. Any modification attempt will throw
     * <code>UnsupportedOperationException("Read-only")</code>
     *
     * @return this builder
     */
    DBFactory readOnly();

    /**
     * Set free space reclaim Q.  It is value from 0 to 10, indicating how eagerly MapDB
     * searchs for free space inside store to reuse, before expanding store file.
     * 0 means that no free space will be reused and store file will just grow (effectively append only).
     * 10 means that MapDB tries really hard to reuse free space, even if it may hurt performance.
     * Default value is 5;
     *
     * @return this builder
     */
    DBFactory freeSpaceReclaimQ( int q );

    /**
     * Disables file sync on commit. This way transactions are preserved (rollback works),
     * but commits are not 'durable' and data may be lost if store is not properly closed.
     * File store will get properly synced when closed.
     * Disabling this will make commits faster.
     *
     * @return this builder
     */
    DBFactory commitFileSyncDisable();

    /**
     * Sets store size limit. Disk or memory space consumed be storage should not grow over this space.
     * Limit is not strict and does not apply to some parts such as index table. Actual store size might
     * be 10% or more bigger.
     *
     * @param maxSize maximal store size in GB
     *
     * @return this builder
     */
    DBFactory sizeLimit( double maxSize );

    /**
     * constructs DB using current settings
     */
    DB make();

    TxMaker makeTxMaker();

    /**
     * Encrypt storage using XTEA algorithm.
     * <p/>
     * XTEA is sound encryption algorithm. However implementation in MapDB was not peer-reviewed.
     * MapDB only encrypts records data, so attacker may see number of records and their sizes.
     * <p/>
     * Make sure you enable this every time you reopen store, otherwise record de-serialization fails unpredictably.
     *
     * @param password for encryption
     *
     * @return this builder
     */
    DBFactory encryptionEnable( String password );

    /**
     * constructs Engine using current settings
     */
    Engine makeEngine();
}
