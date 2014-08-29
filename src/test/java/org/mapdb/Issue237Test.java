package org.mapdb;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import org.junit.Test;
import org.mapdb.impl.UtilsTest;

import static org.junit.Assert.assertEquals;

public class Issue237Test
{

    File file = UtilsTest.tempDbFile();

    @Test
    public void testReopenAsync()
        throws InterruptedException, IOException
    {
        DB database = DBMaker.newFileDB( file ).asyncWriteEnable().make();
        testQueue( database );

        database = DBMaker.newFileDB( file ).asyncWriteEnable().make();
        testQueue( database );
    }

    @Test
    public void testReopenSync()
        throws InterruptedException, IOException
    {
        file.delete();

        DB database = DBMaker.newFileDB( file ).make();
        testQueue( database );

        database = DBMaker.newFileDB( file ).make();
        testQueue( database );
    }

    private void testQueue( DB database )
        throws InterruptedException, IOException
    {
        BlockingQueue<String> queue = database.getQueue( "test-queue" );
        queue.add( "test-value" );
        database.commit();
        assertEquals( queue.take(), "test-value" );
        database.commit();
        database.close();
    }
}