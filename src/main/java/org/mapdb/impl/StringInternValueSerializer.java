package org.mapdb.impl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.mapdb.ValueSerializer;

public class StringInternValueSerializer implements ValueSerializer<String>
{
    @Override
    public void serialize(DataOutput out, String value) throws IOException
    {
        out.writeUTF(value);
    }

    @Override
    public String deserialize(DataInput in, int available) throws IOException {
        return in.readUTF().intern();
    }

    @Override
    public int fixedSize() {
        return -1;
    }
}
