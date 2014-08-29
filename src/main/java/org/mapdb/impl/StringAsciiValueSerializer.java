package org.mapdb.impl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.mapdb.ValueSerializer;

public class StringAsciiValueSerializer implements ValueSerializer<String>
{
    @Override
    public void serialize(DataOutput out, String value) throws IOException
    {
        char[] cc = new char[value.length()];
        //TODO does this really works? is not char 2 byte unsigned?
        value.getChars(0,cc.length,cc,0);
        DataOutput2.packInt( out, cc.length );
        for(char c:cc){
            out.write(c);
        }
    }

    @Override
    public String deserialize(DataInput in, int available) throws IOException {
        int size = DataInput2.unpackInt( in );
        char[] cc = new char[size];
        for(int i=0;i<size;i++){
            cc[i] = (char) in.readUnsignedByte();
        }
        return new String(cc);
    }

    @Override
    public int fixedSize() {
        return -1;
    }
}
