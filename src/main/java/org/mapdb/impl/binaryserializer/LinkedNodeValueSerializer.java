package org.mapdb.impl.binaryserializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.mapdb.KeySerializer;
import org.mapdb.ValueSerializer;
import org.mapdb.impl.btree.BTreeMapImpl;
import org.mapdb.impl.DataInput2;
import org.mapdb.impl.DataOutput2;
import org.mapdb.impl.htree.HTreeLinkedNode;

public class LinkedNodeValueSerializer<K, V> implements ValueSerializer<HTreeLinkedNode<K, V>>
{
    private final KeySerializer<K> keySerializer;
    private final ValueSerializer<V> valueSerializer;
    private final boolean expireFlag;
    private final boolean hasValues;

    public LinkedNodeValueSerializer( KeySerializer<K> keySerializer,
                                      ValueSerializer<V> valueSerializer,
                                      boolean expireFlag,
                                      boolean hasValues
    )
    {
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.expireFlag = expireFlag;
        this.hasValues = hasValues;
    }

    @Override
    public void serialize( DataOutput out, HTreeLinkedNode<K, V> value )
        throws IOException
    {
        DataOutput2.packLong( out, value.next );
        if( expireFlag )
        {
            DataOutput2.packLong( out, value.expireLinkNodeRecid );
        }
        keySerializer.serialize( out, 0, 1, new Object[] { value.key } );
        if( hasValues )
        {
            valueSerializer.serialize( out, value.value );
        }
    }

    @Override
    public HTreeLinkedNode<K, V> deserialize( DataInput in )
        throws IOException
    {
        return new HTreeLinkedNode<K, V>(
            DataInput2.unpackLong( in ),
            expireFlag ? DataInput2.unpackLong( in ) : 0L,
            (K) keySerializer.deserialize( in, 0, 0, 1 )[0],
            hasValues ? valueSerializer.deserialize( in ) : (V) BTreeMapImpl.EMPTY
        );
    }

    @Override
    public int fixedSize()
    {
        return -1;
    }
}
