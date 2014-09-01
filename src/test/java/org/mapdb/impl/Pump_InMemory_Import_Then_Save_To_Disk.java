package org.mapdb.impl;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Random;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.impl.engine.DbImpl;

/**
 * This demonstrates using Data Pump to first create store in-memory at maximal speed,
 * and than copy the store into memory
 */
//TODO Pump between stores is disabled for now, copy this back to examples  once enabled
public class Pump_InMemory_Import_Then_Save_To_Disk
{

    public static void main( String[] args )
        throws IOException
    {
        if( 1 == 1 )
        {
            return;
        }

        //create inMemory store which does not use serialization,
        //and has speed comparable to `java.util` collections
        DB inMemory = new DbImpl( new StoreHeap() );
        Map m = inMemory.getTreeMap( "test" );

        Random r = new Random();
        //insert random stuff, keep on mind it needs to fit into memory
        for( int i = 0; i < 10000; i++ )
        {
            m.put( r.nextInt(), "dwqas" + i );
        }

        //now create on-disk store, it needs to be completely empty
        File targetFile = UtilsTest.tempDbFile();
        DbImpl target = (DbImpl) DBMaker.newFileDB( targetFile ).make();

        Pump.copy( inMemory, target );

        inMemory.close();
        target.close();
    }
}
