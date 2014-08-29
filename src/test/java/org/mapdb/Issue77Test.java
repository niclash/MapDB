package org.mapdb;

import java.io.File;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ConcurrentNavigableMap;
import org.junit.After;
import org.junit.Test;
import org.mapdb.impl.UtilsTest;

public class Issue77Test
{
    private Random random = new Random( 1 );
    private File dir = new File( UtilsTest.tempDbFile() + "aaa" );

    @Test
    public void run()
        throws IOException
    {
        create();
        read(); // UnsupportedOperationException
        read(); // InternalError
    }

    DB open( boolean readOnly )
    {
        // This works:
        // DBMaker maker = DBMaker.newFileDB(new File(dir + "/test"));
        // This is faster, but fails if read() is called for the second time:
        DBFactory maker = DBMaker.newAppendFileDB( new File( dir + "/test" ) );
        if( readOnly )
        {
            maker.readOnly();
        }
//        maker.randomAccessFileEnableIfNeeded();
        maker.closeOnJvmShutdown();
        DB db = maker.make(); // InternalError, UnsupportedOperationException
        return db;
    }

    void create()
        throws IOException
    {
        dir.mkdirs();
        DB db = open( false );
        ConcurrentNavigableMap<Integer, byte[]> map = db.getTreeMap( "bytes" );
        int n = 10;
        int m = 10;
        for( int i = 0; i < n; i++ )
        {
            map.put( i, getRandomData( m ) );
        }
        db.commit();
        db.close();
    }

    void read()
        throws IOException
    {
        DB db = open( true ); // InternalError, UnsupportedOperationException
        db.close();
    }

    byte[] getRandomData( int n )
    {
        byte[] c = new byte[ n ];
        random.nextBytes( c );
        return c;
    }

    @After
    public void cleanup()
    {
        for( File f : dir.listFiles() )
        {
            f.delete();
        }
    }
}
