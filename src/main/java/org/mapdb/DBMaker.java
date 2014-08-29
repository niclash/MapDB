package org.mapdb;

import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.util.NavigableSet;
import java.util.Set;
import org.mapdb.impl.DBMakerImpl;

public class DBMaker
{
    private static DBFactory factory;

    static
    {
        factory = new DBMakerImpl();
    }

    public static void setFactory( DBFactory _factory ){
        factory = _factory;
    }
    /**
     * Creates new in-memory database which stores all data on heap without serialization.
     * This mode should be very fast, but data will affect Garbage Collector the same way as traditional Java Collections.
     */
    public static DBFactory newHeapDB(){
        return factory.newHeapDB();
    }

    /** Creates new in-memory database. Changes are lost after JVM exits.
     * <p/>
     * This will use HEAP memory so Garbage Collector is affected.
     */
    public static DBFactory newMemoryDB(){
        return factory.newMemoryDB();
    }

    /** Creates new in-memory database. Changes are lost after JVM exits.
     * <p/>
     * This will use DirectByteBuffer outside of HEAP, so Garbage Collector is not affected
     */
    public static DBFactory newMemoryDirectDB(){
        return factory.newMemoryDirectDB();
    }

    /**
     * Creates or open append-only database stored in file.
     * This database uses format other than usual file db
     *
     * @param file
     * @return maker
     */
    public static DBFactory newAppendFileDB(File file) {
        return factory.newAppendFileDB( file );
    }


    /**
     * Create new BTreeMap backed by temporary file storage.
     * This is quick way to create 'throw away' collection.
     *
     * <p>Storage is created in temp folder and deleted on JVM shutdown
     */
    public static <K,V> BTreeMap<K,V> newTempTreeMap(){
        return newTempFileDB()
            .deleteFilesAfterClose()
            .closeOnJvmShutdown()
            .transactionDisable()
            .make()
            .getTreeMap("temp");
    }

    /**
     * Create new HTreeMap backed by temporary file storage.
     * This is quick way to create 'throw away' collection.
     *
     * <p>Storage is created in temp folder and deleted on JVM shutdown
     */
    public static <K,V> HTreeMap<K,V> newTempHashMap(){
        return newTempFileDB()
            .deleteFilesAfterClose()
            .closeOnJvmShutdown()
            .transactionDisable()
            .make()
            .getHashMap( "temp" );
    }

    /**
     * Create new TreeSet backed by temporary file storage.
     * This is quick way to create 'throw away' collection.
     *
     * <p>Storage is created in temp folder and deleted on JVM shutdown
     */
    public static <K> NavigableSet<K> newTempTreeSet(){
        return newTempFileDB()
            .deleteFilesAfterClose()
            .closeOnJvmShutdown()
            .transactionDisable()
            .make()
            .getTreeSet( "temp" );
    }

    /**
     * Create new HashSet backed by temporary file storage.
     * This is quick way to create 'throw away' collection.
     * <p>
     * Storage is created in temp folder and deleted on JVM shutdown
     */
    public static <K> Set<K> newTempHashSet(){
        return newTempFileDB()
            .deleteFilesAfterClose()
            .closeOnJvmShutdown()
            .transactionDisable()
            .make()
            .getHashSet( "temp" );
    }

    /**
     * Creates new database in temporary folder.
     */
    public static DBFactory newTempFileDB() {
        try {
            return newFileDB(File.createTempFile("mapdb-temp","db"));
        } catch (IOException e) {
            throw new IOError(e);
        }
    }

    /**
     * Creates new off-heap cache with maximal size in GBs.
     * Entries are removed from cache in most-recently-used fashion
     * if store becomes too big.
     *
     * This method uses off-heap direct ByteBuffers. See {@link java.nio.ByteBuffer#allocateDirect(int)}
     *
     * @param size maximal size of off-heap store in gigabytes.
     * @return map
     */
    public static <K,V> HTreeMap<K,V> newCacheDirect(double size){
        return newMemoryDirectDB()
            .transactionDisable()
            .make()
            .createHashMap( "cache" )
            .expireStoreSize(size)
            .counterEnable()
            .make();
    }

    /**
     * Creates new cache with maximal size in GBs.
     * Entries are removed from cache in most-recently-used fashion
     * if store becomes too big.
     *
     * This cache uses on-heap `byte[]`, but does not affect GC since objects are serialized into binary form.
     * This method uses  ByteBuffers backed by on-heap byte[]. See {@link java.nio.ByteBuffer#allocate(int)}
     *
     * @param size maximal size of off-heap store in gigabytes.
     * @return map
     */
    public static <K,V> HTreeMap<K,V> newCache(double size){
        return newMemoryDB()
            .transactionDisable()
            .make()
            .createHashMap( "cache" )
            .expireStoreSize( size )
            .counterEnable()
            .make();
    }

    /** Creates or open database stored in file. */
    public static DBFactory newFileDB(File file){
        return factory.newFileDB( file );
    }


}
