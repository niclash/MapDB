/*
 *  Copyright (c) 2012 Jan Kotek
 *  Copyright (c) 2014 Niclas Hedhman
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.mapdb;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;

/**
 * Provides serialization and deserialization
 *
 * @author Jan Kotek
 */
public interface KeySerializer<A>
{

    /**
     * Serialize the content of an object into a ObjectOutput
     *
     * @param out   ObjectOutput to save object into
     * @param keys  Keys to serialize
     * @param start The first key to be serialized in the array.
     * @param end   The last key to be serialized in the array
     */
    void serialize( DataOutput out, int start, int end, Object[] keys )
        throws IOException;

    /**
     * Deserialize the content of an object from a DataInput.
     *
     * @param in to read serialized data from
     * @param start the first key to be read and returned.
     * @param end the first key to be read and returned.
     * @return deserialized array of keys.
     *
     * @throws java.io.IOException
     */
    Object[] deserialize( DataInput in, int start, int end, int size )
        throws IOException;

    /**
     * @return a Comparator for comparing the keys.
     */
    Comparator<A> getComparator();
}
