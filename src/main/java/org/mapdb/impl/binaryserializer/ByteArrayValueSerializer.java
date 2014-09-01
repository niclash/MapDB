package org.mapdb.impl.binaryserializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.mapdb.ValueSerializer;

public class ByteArrayValueSerializer implements ValueSerializer<byte[]>
{

    @Override
    public void serialize(DataOutput out, byte[] value) throws IOException
    {
        if(value==null||value.length==0) return;
        out.write(value);
    }

    @Override
    public byte[] deserialize(DataInput in, int available) throws IOException {
        if(available==-1) throw new IllegalArgumentException("BYTE_ARRAY_NOSIZE does not work with collections.");
        if(available==0) return null;
        byte[] ret = new byte[available];
        in.readFully(ret);
        return ret;
    }

    @Override
    public int fixedSize() {
        return -1;
    }
}
