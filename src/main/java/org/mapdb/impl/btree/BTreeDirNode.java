package org.mapdb.impl.btree;

import java.util.Arrays;
import java.util.List;

public final class BTreeDirNode<A,B> implements BTreeNode<A,B>
{
    final A[] keys;
    final long[] child;

    BTreeDirNode( A[] keys, long[] child )
    {
        this.keys = keys;
        this.child = child;
    }

    BTreeDirNode( A[] keys, List<Long> child )
    {
        this.keys = keys;
        this.child = new long[ child.size() ];
        for( int i = 0; i < child.size(); i++ )
        {
            this.child[ i ] = child.get( i );
        }
    }

    @Override
    public boolean isLeaf()
    {
        return false;
    }

    @Override
    public A[] keys()
    {
        return keys;
    }

    @Override
    public B[] vals()
    {
        return null;
    }

    @Override
    public A highKey()
    {
        return keys[ keys.length - 1 ];
    }

    @Override
    public long[] child()
    {
        return child;
    }

    @Override
    public long next()
    {
        return child[ child.length - 1 ];
    }

    @Override
    public String toString()
    {
        return "Dir(K" + Arrays.toString( keys ) + ", C" + Arrays.toString( child ) + ")";
    }
}
