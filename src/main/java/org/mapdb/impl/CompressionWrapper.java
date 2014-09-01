package org.mapdb.impl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import org.mapdb.ValueSerializer;
import org.mapdb.impl.binaryserializer.FastArrayList;
import org.mapdb.impl.binaryserializer.SerializerBase;

/** wraps another serializer and (de)compresses its output/input*/
public final class CompressionWrapper<E> implements ValueSerializer<E>, Serializable
{

    private static final long serialVersionUID = 4440826457939614346L;
    protected final ValueSerializer<E> serializer;
    protected final ThreadLocal<CompressLZF> LZF = new ThreadLocal<CompressLZF>() {
            @Override protected CompressLZF initialValue() {
                return new CompressLZF();
            }
        };

    public CompressionWrapper( ValueSerializer<E> serializer ) {
        this.serializer = serializer;
    }

    /** used for deserialization */
    public CompressionWrapper( SerializerBase serializerBase,
                               DataInput is,
                               FastArrayList<Object> objectStack
    ) throws IOException
    {
        objectStack.add(this);
        this.serializer = (ValueSerializer<E>) serializerBase.deserialize(is,objectStack);
    }


    @Override
    public void serialize(DataOutput out, E value) throws IOException {
        DataOutput2 out2 = new DataOutput2();
        serializer.serialize(out2,value);

        byte[] tmp = new byte[out2.pos+41];
        int newLen;
        try{
            newLen = LZF.get().compress(out2.buf,out2.pos,tmp,0);
        }catch(IndexOutOfBoundsException e){
            newLen=0; //larger after compression
        }
        if(newLen>=out2.pos){
            //compression adds size, so do not compress
            DataOutput2.packInt(out,0);
            out.write(out2.buf,0,out2.pos);
            return;
        }

        DataOutput2.packInt(out, out2.pos+1); //unpacked size, zero indicates no compression
        out.write(tmp,0,newLen);
    }

    @Override
    public E deserialize(DataInput in, int available) throws IOException {
        final int unpackedSize = DataInput2.unpackInt(in)-1;
        if(unpackedSize==-1){
            //was not compressed
            return serializer.deserialize(in, available>0?available-1:available);
        }

        byte[] unpacked = new byte[unpackedSize];
        LZF.get().expand(in,unpacked,0,unpackedSize);
        DataIO.DataInputByteArray in2 = new DataIO.DataInputByteArray(unpacked);
        E ret =  serializer.deserialize(in2,unpackedSize);
        assert(in2.getPos()==unpackedSize): "data were not fully read";
        return ret;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        org.mapdb.impl.CompressionWrapper that = (org.mapdb.impl.CompressionWrapper) o;
        return serializer.equals(that.serializer);
    }

    @Override
    public int hashCode() {
        return serializer.hashCode();
    }

    @Override
    public int fixedSize() {
        return -1;
    }

}
