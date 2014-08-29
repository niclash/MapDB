package org.mapdb.impl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.mapdb.ValueSerializer;

public class CharArrayValueSerializer implements ValueSerializer<char[]>
{

    @Override
    public void serialize(DataOutput out, char[] value) throws IOException
    {
        DataOutput2.packInt( out, value.length );
        for(char c:value){
            out.writeChar(c);
        }
    }

    @Override
    public char[] deserialize(DataInput in, int available) throws IOException {
        final int size = DataInput2.unpackInt( in );
        char[] ret = new char[size];
        for(int i=0;i<size;i++){
            ret[i] = in.readChar();
        }
        return ret;
    }

    @Override
    public int fixedSize() {
        return -1;
    }
}
