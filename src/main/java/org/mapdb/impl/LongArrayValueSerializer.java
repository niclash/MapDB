package org.mapdb.impl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.mapdb.ValueSerializer;

public class LongArrayValueSerializer implements ValueSerializer<long[]>
{

    @Override
    public void serialize(DataOutput out, long[] value) throws IOException
    {
        DataOutput2.packInt( out, value.length );
        for(long c:value){
            out.writeLong(c);
        }
    }

    @Override
    public long[] deserialize(DataInput in, int available) throws IOException {
        final int size = DataInput2.unpackInt( in );
        long[] ret = new long[size];
        for(int i=0;i<size;i++){
            ret[i] = in.readLong();
        }
        return ret;
    }

    @Override
    public int fixedSize() {
        return -1;
    }
}
