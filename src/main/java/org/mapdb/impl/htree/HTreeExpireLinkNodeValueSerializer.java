package org.mapdb.impl.htree;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.mapdb.ValueSerializer;
import org.mapdb.impl.DataInput2;
import org.mapdb.impl.DataOutput2;

class HTreeExpireLinkNodeValueSerializer<K,V> implements ValueSerializer<HTreeExpireLinkNode>
{
    @Override
    public void serialize( DataOutput out, HTreeExpireLinkNode value )
        throws IOException
    {
        if( value == HTreeExpireLinkNode.EMPTY )
        {
            return;
        }
        DataOutput2.packLong( out, value.prev );
        DataOutput2.packLong( out, value.next );
        DataOutput2.packLong( out, value.keyRecid );
        DataOutput2.packLong( out, value.time );
        out.writeInt( value.hash );
    }

    @Override
    public HTreeExpireLinkNode deserialize( DataInput in )
        throws IOException
    {
        return new HTreeExpireLinkNode(
            DataInput2.unpackLong( in ), DataInput2.unpackLong( in ), DataInput2.unpackLong( in ), DataInput2.unpackLong( in ),
            in.readInt()
        );
    }

    @Override
    public int fixedSize()
    {
        return -1;
    }
}
