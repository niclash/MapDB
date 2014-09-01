package org.mapdb.impl.binaryserializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.mapdb.ValueSerializer;

public class IntegerValueSerializer implements ValueSerializer<Integer>
{
    @Override
    public void serialize(DataOutput out, Integer value) throws IOException
    {
        out.writeInt(value);
    }

    @Override
    public Integer deserialize(DataInput in, int available) throws IOException {
        return in.readInt();
    }

    @Override
    public int fixedSize() {
        return 4;
    }
}
