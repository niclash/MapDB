package org.mapdb.impl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.mapdb.ValueSerializer;

public class BooleanValueSerializer implements ValueSerializer<Boolean>
{
    @Override
    public void serialize(DataOutput out, Boolean value) throws IOException
    {
        out.writeBoolean(value);
    }

    @Override
    public Boolean deserialize(DataInput in, int available) throws IOException {
        if(available==0) return null;
        return in.readBoolean();
    }

    @Override
    public int fixedSize() {
        return 1;
    }
}
