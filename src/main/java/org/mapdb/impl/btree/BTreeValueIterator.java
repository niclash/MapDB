package org.mapdb.impl.btree;

import java.util.Iterator;
import java.util.NoSuchElementException;

class BTreeValueIterator<V> extends BTreeIterator implements Iterator<V>
{

    BTreeValueIterator( BTreeMapImpl m )
    {
        super( m );
    }

    BTreeValueIterator( BTreeMapImpl m, Object lo, boolean loInclusive, Object hi, boolean hiInclusive )
    {
        super( m, lo, loInclusive, hi, hiInclusive );
    }

    @Override
    public V next()
    {
        if( currentLeaf == null )
        {
            throw new NoSuchElementException();
        }
        Object ret = currentLeaf.vals[ currentPos - 1 ];
        advance();
        return (V) m.valExpand( ret );
    }
}
