package org.mapdb;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import org.junit.Test;

public class Issue312Test
{

    @Test
    public void test()
        throws IOException
    {
        File f = File.createTempFile( "mapdb", "test" );
        DB db = DBMaker.newFileDB( f )
            .mmapFileEnableIfSupported()
            .transactionDisable()
            .make();

        Map<Long, String> map = db.createTreeMap( "data" ).make();
        for( long i = 0; i < 100000; i++ )
        {
            map.put( i, i + "hi my friend " + i );
        }
        db.commit();
        db.close();

        db = DBMaker.newFileDB( f )
            .mmapFileEnableIfSupported()
            .transactionDisable()
            .readOnly()
            .make();
    }
}
