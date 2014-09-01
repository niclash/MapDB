package org.mapdb.impl.htree;

import org.mapdb.ValueSerializer;

final class HTreeExpireLinkNode<K,V>
{
    public final static HTreeExpireLinkNode EMPTY = new HTreeExpireLinkNode( 0, 0, 0, 0, 0 );

    public static final ValueSerializer<HTreeExpireLinkNode> SERIALIZER = new HTreeExpireLinkNodeValueSerializer();

    public final long prev;
    public final long next;
    public final long keyRecid;
    public final long time;
    public final int hash;

    public HTreeExpireLinkNode( long prev, long next, long keyRecid, long time, int hash )
    {
        this.prev = prev;
        this.next = next;
        this.keyRecid = keyRecid;
        this.time = time;
        this.hash = hash;
    }

    public HTreeExpireLinkNode copyNext( long next2 )
    {
        return new HTreeExpireLinkNode( prev, next2, keyRecid, time, hash );
    }

    public HTreeExpireLinkNode<K,V> copyPrev( long prev2 )
    {
        return new HTreeExpireLinkNode<K,V>( prev2, next, keyRecid, time, hash );
    }

    public HTreeExpireLinkNode<K,V> copyTime( long time2 )
    {
        return new HTreeExpireLinkNode<K,V>( prev, next, keyRecid, time2, hash );
    }
}
