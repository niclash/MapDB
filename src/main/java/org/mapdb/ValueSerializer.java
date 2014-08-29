/*
 *  Copyright (c) 2012 Jan Kotek
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

/**
 * Provides serialization and deserialization
 *
 * @author Jan Kotek
 */
public interface ValueSerializer<A>
{
    /**
     * Serialize the content of an object into a ObjectOutput
     *
     * @param out   ObjectOutput to save object into
     * @param value Object to serialize
     */
    public void serialize( DataOutput out, A value )
        throws IOException;

    /**
     * Deserialize the content of an object from a DataInput.
     *
     * @param in        to read serialized data from
     * @param available how many bytes are available in DataInput for reading, may be -1 (in streams) or 0 (null).
     *
     * @return deserialized object
     *
     * @throws java.io.IOException
     */
    public A deserialize( DataInput in, int available )
        throws IOException;

    /**
     * Data could be serialized into record with variable size or fixed size.
     * Some optimizations can be applied to serializers with fixed size
     *
     * @return fixed size or -1 for variable size
     */
    public int fixedSize();
}
