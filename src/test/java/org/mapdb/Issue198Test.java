package org.mapdb;

import java.io.IOException;
import org.junit.Test;
import org.mapdb.impl.UtilsTest;

public class Issue198Test
{

    @Test
    public void main()
        throws IOException
    {

        DB db = DBMaker.newFileDB( UtilsTest.tempDbFile() )
            .closeOnJvmShutdown()
                //.randomAccessFileEnable()
            .make();
        BTreeMap<Integer, Integer> map = db.createTreeMap( "testmap" ).makeOrGet();
        for( int i = 1; i <= 3000; ++i )
        {
            map.put( i, i );
        }
        db.commit();
        db.close();
    }
}
