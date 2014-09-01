package org.mapdb;

import org.junit.Test;
import org.mapdb.impl.binaryserializer.SerializerBase;
import org.mapdb.impl.UtilsTest;

import static org.junit.Assert.assertEquals;

public class Issue112Test
{

    @Test(timeout = 10000)
    public void testDoubleCommit()
        throws Exception
    {
        final DB myTestDataFile = DBMaker.newFileDB( UtilsTest.tempDbFile() )
            .checksumEnable()
            .make();
        myTestDataFile.commit();
        myTestDataFile.commit();

        long recid = myTestDataFile.getEngine().put( "aa", SerializerBase.STRING_NOSIZE );
        myTestDataFile.commit();

        assertEquals( "aa", myTestDataFile.getEngine().get( recid, SerializerBase.STRING_NOSIZE ) );
    }
}
