package org.mapdb.impl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.mapdb.ValueSerializer;

public class DoubleArrayValueSerializer implements ValueSerializer<double[]>
{

    @Override
    public void serialize(DataOutput out, double[] value) throws IOException
    {
        DataOutput2.packInt( out, value.length );
        for(double c:value){
            out.writeDouble(c);
        }
    }

    @Override
    public double[] deserialize(DataInput in, int available) throws IOException {
        final int size = DataInput2.unpackInt( in );
        double[] ret = new double[size];
        for(int i=0;i<size;i++){
            ret[i] = in.readDouble();
        }
        return ret;
    }

    @Override
    public int fixedSize() {
        return -1;
    }
}
