package org.mapdb.impl.btree;

import org.mapdb.impl.Fun;
import org.mapdb.impl.binaryserializer.InternalSerializers;
import org.mapdb.impl.binaryserializer.SerializerBase;

class BTreeIterator
{
    final BTreeMapImpl m;

    BTreeLeafNode currentLeaf;
    Object lastReturnedKey;
    int currentPos;
    final Object hi;
    final boolean hiInclusive;

    /**
     * unbounded iterator
     */
    BTreeIterator( BTreeMapImpl m )
    {
        this.m = m;
        hi = null;
        hiInclusive = false;
        pointToStart();
    }

    /**
     * bounder iterator, args may be null for partially bounded
     */
    BTreeIterator( BTreeMapImpl m, Object lo, boolean loInclusive, Object hi, boolean hiInclusive )
    {
        this.m = m;
        if( lo == null )
        {
            pointToStart();
        }
        else
        {
            Fun.Tuple2<Integer, BTreeLeafNode> l = m.findLargerNode( lo, loInclusive );
            currentPos = l != null ? l.a : -1;
            currentLeaf = l != null ? l.b : null;
        }

        this.hi = hi;
        this.hiInclusive = hiInclusive;
        if( hi != null && currentLeaf != null )
        {
            //check in bounds
            Object key = currentLeaf.keys[ currentPos ];
            int c = m.keySerializer.getComparator().compare( key, hi );
            if( c > 0 || ( c == 0 && !hiInclusive ) )
            {
                //out of high bound
                currentLeaf = null;
                currentPos = -1;
            }
        }
    }

    private void pointToStart()
    {
        //find left-most leaf
        final long rootRecid = m.engine.get( m.rootRecidRef, InternalSerializers.LONG );
        BTreeNode node = (BTreeNode) m.engine.get( rootRecid, m.nodeSerializer );
        while( !node.isLeaf() )
        {
            node = (BTreeNode) m.engine.get( node.child()[ 0 ], m.nodeSerializer );
        }
        currentLeaf = (BTreeLeafNode) node;
        currentPos = 1;

        while( currentLeaf.keys.length == 2 )
        {
            //follow link until leaf is not empty
            if( currentLeaf.next == 0 )
            {
                currentLeaf = null;
                return;
            }
            currentLeaf = (BTreeLeafNode) m.engine.get( currentLeaf.next, m.nodeSerializer );
        }
    }

    public boolean hasNext()
    {
        return currentLeaf != null;
    }

    public void remove()
    {
        if( lastReturnedKey == null )
        {
            throw new IllegalStateException();
        }
        m.remove( lastReturnedKey );
        lastReturnedKey = null;
    }

    protected void advance()
    {
        if( currentLeaf == null )
        {
            return;
        }
        lastReturnedKey = currentLeaf.keys[ currentPos ];
        currentPos++;
        if( currentPos == currentLeaf.keys.length - 1 )
        {
            //move to next leaf
            if( currentLeaf.next == 0 )
            {
                currentLeaf = null;
                currentPos = -1;
                return;
            }
            currentPos = 1;
            currentLeaf = (BTreeLeafNode) m.engine.get( currentLeaf.next, m.nodeSerializer );
            while( currentLeaf.keys.length == 2 )
            {
                if( currentLeaf.next == 0 )
                {
                    currentLeaf = null;
                    currentPos = -1;
                    return;
                }
                currentLeaf = (BTreeLeafNode) m.engine.get( currentLeaf.next, m.nodeSerializer );
            }
        }
        if( hi != null && currentLeaf != null )
        {
            //check in bounds
            Object key = currentLeaf.keys[ currentPos ];
            int c = m.keySerializer.getComparator().compare( key, hi );
            if( c > 0 || ( c == 0 && !hiInclusive ) )
            {
                //out of high bound
                currentLeaf = null;
                currentPos = -1;
            }
        }
    }
}
