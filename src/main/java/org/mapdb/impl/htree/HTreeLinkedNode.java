package org.mapdb.impl.htree;

/**
 * node which holds key-value pair
 */
public final class HTreeLinkedNode<K, V>
{

    public final long next;
    public final long expireLinkNodeRecid;

    public final K key;
    public final V value;

    public HTreeLinkedNode( final long next, long expireLinkNodeRecid, final K key, final V value )
    {
        this.key = key;
        this.expireLinkNodeRecid = expireLinkNodeRecid;
        this.value = value;
        this.next = next;
    }
}
