package org.mapdb.impl.binaryserializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.mapdb.ValueSerializer;

public class StringValueSerializer implements ValueSerializer<String>
{
    @Override
    public void serialize(DataOutput out, String value) throws IOException
    {
        out.writeUTF(value);
    }

    @Override
    public String deserialize(DataInput in) throws IOException {
        return in.readUTF();
    }

    @Override
    public int fixedSize() {
        return -1;
    }
}
