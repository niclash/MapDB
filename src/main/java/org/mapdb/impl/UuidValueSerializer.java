package org.mapdb.impl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.UUID;
import org.mapdb.ValueSerializer;

public class UuidValueSerializer implements ValueSerializer<UUID>
{
    @Override
    public void serialize(DataOutput out, UUID value) throws IOException
    {
        out.writeLong(value.getMostSignificantBits());
        out.writeLong(value.getLeastSignificantBits());
    }

    @Override
    public UUID deserialize(DataInput in, int available) throws IOException {
        return new UUID(in.readLong(), in.readLong());
    }

    @Override
    public int fixedSize() {
        return 16;
    }
}
