package org.mapdb.impl.btree;

import java.util.Arrays;

public final class BTreeLeafNode<K,V> implements BTreeNode<K,V>
{
    final K[] keys;
    final V[] vals;
    final long next;

    public BTreeLeafNode( K[] keys, V[] vals, long next )
    {
        this.keys = keys;
        this.vals = vals;
        this.next = next;
        assert ( vals == null || keys.length == vals.length + 2 );
    }

    @Override
    public boolean isLeaf()
    {
        return true;
    }

    @Override
    public K[] keys()
    {
        return keys;
    }

    @Override
    public V[] vals()
    {
        return vals;
    }

    @Override
    public K highKey()
    {
        return keys[ keys.length - 1 ];
    }

    @Override
    public long[] child()
    {
        return null;
    }

    @Override
    public long next()
    {
        return next;
    }

    @Override
    public String toString()
    {
        return "Leaf(K" + Arrays.toString( keys ) + ", V" + Arrays.toString( vals ) + ", L=" + next + ")";
    }
}
