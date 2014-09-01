package examples;

import java.io.IOException;
import java.util.Random;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.impl.binaryserializer.SerializerBase;
import org.mapdb.impl.Store;

/**
 * This example shows how-to create off-heap cache,
 * where entries expire when maximal store size is reached.
 *
 * It also shows howto get basic statistics about store size.
 *
 * It is more advanced version of previous example.
 * It uses more settings, bypasses general serialization for best performance
 * and discussed other performance tunning
 */
public class CacheOffHeapAdvanced
{

    public static void main( String[] args )
        throws IOException
    {

        final double cacheSizeInGB = 1.0;

        //first create store
        DB db = DBMaker
            .newMemoryDirectDB()
            .transactionDisable()
                //some additional options for DB
                // .asyncWriteEnable()
                // .cacheSize(100000)
            .make();

        HTreeMap cache = db
            .createHashMap( "cache" )
            .expireStoreSize( cacheSizeInGB )
            .counterEnable() //disable this if cache.size() is not used
                //use proper serializers to and improve performance
            .keySerializer( SerializerBase.LONG )
            .valueSerializer( SerializerBase.BYTE_ARRAY )
            .make();

        //generates random key and values
        Random r = new Random();
        //used to print store statistics
        Store store = Store.forDB( db );

        // insert some stuff in cycle
        for( long counter = 1; counter < 1e8; counter++ )
        {
            long key = r.nextLong();
            byte[] value = new byte[ 1000 ];
            r.nextBytes( value );

            cache.put( key, value );

            if( counter % 1e5 == 0 )
            {
                System.out.printf( "Map size: %,d, counter %,d, curr store size: %,d, store free size: %,d\n",
                                   cache.sizeLong(), counter, store.getCurrSize(), store.getFreeSize() );
            }
        }

        // and release memory. Only necessary with `DBMaker.newCacheDirect()`
        cache.close();
    }
}
