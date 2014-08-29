package org.mapdb;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import org.junit.Test;
import org.mapdb.impl.UtilsTest;

public class Issue247Test
{

    private Map getMap( DB db )
    {
        return db.createTreeMap( "test" )
            .counterEnable()
            .valuesOutsideNodesEnable()
            .makeOrGet();
    }

    @Test
    public void test()
        throws IOException
    {
        File f = UtilsTest.tempDbFile();
        DB db = DBMaker.newFileDB( f )
            .transactionDisable()
            .make();

        getMap( db );
        //db.commit();

        db.close();

        db = DBMaker.newFileDB( f )
            .readOnly()
            .make();
        getMap( db ).size();
    }
}
