package org.mapdb;

import java.io.File;
import org.junit.Test;
import org.mapdb.impl.Fun;
import org.mapdb.impl.UtilsTest;

public class Issue90Test
{

    @Test
    public void testCounter()
        throws Exception
    {
        File file = UtilsTest.tempDbFile();

        final DB mapDb = DBMaker.newAppendFileDB( file )
            .closeOnJvmShutdown()
            .compressionEnable()  //This is the cause of the exception. If compression is not used, no exception occurs.

            .cacheDisable()
            .make();
        final Atomic.Long myCounter = mapDb.getAtomicLong( "MyCounter" );

        final BTreeMap<String, Fun.Tuple2<String, Integer>> treeMap = mapDb.getTreeMap( "map" );
        Bind.size( treeMap, myCounter );

        for( int i = 0; i < 3; i++ )
        {
            treeMap.put( "key_" + i, new Fun.Tuple2<String, Integer>( "value_", i ) );
        }
    }
}