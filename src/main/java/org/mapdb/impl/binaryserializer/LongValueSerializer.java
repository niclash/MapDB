package org.mapdb.impl.binaryserializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.mapdb.ValueSerializer;

public class LongValueSerializer implements ValueSerializer<Long>
{
    @Override
    public void serialize(DataOutput out, Long value) throws IOException
    {
        if(value != null)
            out.writeLong(value);
    }

    @Override
    public Long deserialize(DataInput in, int available) throws IOException {
        if(available==0) return null;
        return in.readLong();
    }

    @Override
    public int fixedSize() {
        return 8;
    }
}
