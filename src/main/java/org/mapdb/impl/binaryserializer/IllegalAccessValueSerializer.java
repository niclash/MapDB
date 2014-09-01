package org.mapdb.impl.binaryserializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class IllegalAccessValueSerializer implements org.mapdb.ValueSerializer<Object>
{
    @Override
    public void serialize(DataOutput out, Object value) throws IOException
    {
        throw new IllegalAccessError();
    }

    @Override
    public Object deserialize(DataInput in, int available) throws IOException {
        throw new IllegalAccessError();
    }

    @Override
    public int fixedSize() {
        return -1;
    }
}
