package org.mapdb.impl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.mapdb.ValueSerializer;

public class ByteValueSerializer implements ValueSerializer<byte[]>
{

    @Override
    public void serialize(DataOutput out, byte[] value) throws IOException
    {
        DataOutput2.packInt( out, value.length );
        out.write(value);
    }

    @Override
    public byte[] deserialize(DataInput in, int available) throws IOException {
        int size = DataInput2.unpackInt( in );
        byte[] ret = new byte[size];
        in.readFully(ret);
        return ret;
    }

    @Override
    public int fixedSize() {
        return -1;
    }
}
