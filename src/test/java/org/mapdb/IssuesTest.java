package org.mapdb;

import java.util.Map;
import org.junit.Test;
import org.mapdb.impl.UtilsTest;

public class IssuesTest
{

    @Test
    public void issue130()
    {
        DB db = DBMaker.newAppendFileDB( UtilsTest.tempDbFile() )
            .closeOnJvmShutdown()
            .make();

        Map store = db.getTreeMap( "collectionName" );
    }
}
