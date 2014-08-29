package org.mapdb.impl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.mapdb.ValueSerializer;

public class IntArrayValueSerializer implements ValueSerializer<int[]>
{

    @Override
    public void serialize(DataOutput out, int[] value) throws IOException
    {
        DataOutput2.packInt( out, value.length );
        for(int c:value){
            out.writeInt(c);
        }
    }

    @Override
    public int[] deserialize(DataInput in, int available) throws IOException {
        final int size = DataInput2.unpackInt( in );
        int[] ret = new int[size];
        for(int i=0;i<size;i++){
            ret[i] = in.readInt();
        }
        return ret;
    }

    @Override
    public int fixedSize() {
        return -1;
    }
}
