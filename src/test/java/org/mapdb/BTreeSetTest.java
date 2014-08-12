package org.mapdb;


import org.junit.Before;

import java.util.Collections;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class BTreeSetTest extends HTreeSetTest{

	@Before
    public void setUp() throws Exception {

        hs = new BTreeMap(engine,BTreeMap.createRootRef(engine,BTreeKeySerializer.BASIC,null, 0),
                6,false,0, BTreeKeySerializer.BASIC,null,
                0,false).keySet();

        Collections.addAll(hs, objArray);
    }
}
