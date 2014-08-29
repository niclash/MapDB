package org.mapdb.impl;

import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import org.mapdb.Engine;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class EngineWrapper_ImmutabilityCheckEngine
{

    @Test
    public void test()
    {
        Volume.Factory fab = Volume.memoryFactory( false, 0L, CC.VOLUME_SLICE_SHIFT );
        Engine e = new StoreDirect( fab );
        e = new EngineWrapper.ImmutabilityCheckEngine( e );

        List rec = new ArrayList();
        rec.add( "aa" );
        long recid = e.put( rec, SerializerBase.BASIC );
        rec.add( "bb" );

        try
        {
            e.update( recid, rec, SerializerBase.BASIC );
            fail( "should throw exception" );
        }
        catch( AssertionError ee )
        {
            assertTrue( ee.getMessage().startsWith( "Record instance was modified" ) );
        }

        try
        {
            e.close();
            fail( "should throw exception" );
        }
        catch( AssertionError ee )
        {
            assertTrue( ee.getMessage().startsWith( "Record instance was modified" ) );
        }
    }
}
