package org.mapdb.impl.btree;

import java.util.Iterator;
import java.util.NoSuchElementException;

class BTreeKeyIterator<K> extends BTreeIterator implements Iterator<K>
{

    BTreeKeyIterator( BTreeMapImpl m )
    {
        super( m );
    }

    BTreeKeyIterator( BTreeMapImpl m, Object lo, boolean loInclusive, Object hi, boolean hiInclusive )
    {
        super( m, lo, loInclusive, hi, hiInclusive );
    }

    @Override
    public K next()
    {
        if( currentLeaf == null )
        {
            throw new NoSuchElementException();
        }
        K ret = (K) currentLeaf.keys[ currentPos ];
        advance();
        return ret;
    }
}
