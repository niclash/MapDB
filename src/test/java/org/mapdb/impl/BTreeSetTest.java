package org.mapdb.impl;

import java.util.Collections;
import org.junit.Before;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class BTreeSetTest extends HTreeSetTest
{

    @Before
    public void setUp()
        throws Exception
    {

        hs = new BTreeMapImpl( engine, BTreeMapImpl.createRootRef( engine, BTreeKeySerializer.BASIC, null, BTreeMapImpl.COMPARABLE_COMPARATOR, 0 ),
                               6, false, 0, BTreeKeySerializer.BASIC, null,
                               BTreeMapImpl.COMPARABLE_COMPARATOR, 0, false ).keySet();

        Collections.addAll( hs, objArray );
    }
}
