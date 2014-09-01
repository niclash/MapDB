/*******************************************************************************
 * Copyright 2010 Cees De Groot, Alex Boisvert, Jan Kotek
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package org.mapdb.impl;

import java.util.concurrent.ConcurrentMap;
import org.mapdb.ConcurrentMapInterfaceTest;
import org.mapdb.Engine;
import org.mapdb.impl.binaryserializer.BTreeKeySerializer;
import org.mapdb.impl.binaryserializer.SerializerBase;
import org.mapdb.impl.btree.BTreeMapImpl;

public class BTreeMapTest2 extends ConcurrentMapInterfaceTest<Integer, String>
{

    public BTreeMapTest2()
    {
        super( false, false, true, true, true, true, false );
    }

    Engine r;

    @Override
    protected void setUp()
        throws Exception
    {
        r = new StoreDirect( Volume.memoryFactory( false, 0L, CC.VOLUME_SLICE_SHIFT ) );
    }

    @Override public void tearDown()
    {
        r.close();
    }

    @Override
    protected Integer getKeyNotInPopulatedMap()
        throws UnsupportedOperationException
    {
        return -100;
    }

    @Override
    protected String getValueNotInPopulatedMap()
        throws UnsupportedOperationException
    {
        return "XYZ";
    }

    @Override
    protected String getSecondValueNotInPopulatedMap()
        throws UnsupportedOperationException
    {
        return "AAAA";
    }

    @Override
    protected ConcurrentMap<Integer, String> makeEmptyMap()
        throws UnsupportedOperationException
    {

        return new BTreeMapImpl( r, BTreeMapImpl.createRootRef( r, BTreeKeySerializer.BASIC, SerializerBase.BASIC, BTreeMapImpl.COMPARABLE_COMPARATOR, 0 ),
                                 6, false, 0, BTreeKeySerializer.BASIC, SerializerBase.BASIC,
                                 BTreeMapImpl.COMPARABLE_COMPARATOR, 0, false );
    }

    @Override
    protected ConcurrentMap<Integer, String> makePopulatedMap()
        throws UnsupportedOperationException
    {
        ConcurrentMap<Integer, String> map = makeEmptyMap();
        for( int i = 0; i < 100; i++ )
        {
            map.put( i, "aa" + i );
        }
        return map;
    }
}
