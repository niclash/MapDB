package org.mapdb.impl.btree;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

class BTreeEntryIterator<K, V> extends BTreeIterator implements Iterator<Map.Entry<K, V>>
{

    BTreeEntryIterator( BTreeMapImpl m )
    {
        super( m );
    }

    BTreeEntryIterator( BTreeMapImpl m, Object lo, boolean loInclusive, Object hi, boolean hiInclusive )
    {
        super( m, lo, loInclusive, hi, hiInclusive );
    }

    @Override
    public Map.Entry<K, V> next()
    {
        if( currentLeaf == null )
        {
            throw new NoSuchElementException();
        }
        K ret = (K) currentLeaf.keys[ currentPos ];
        Object val = currentLeaf.vals[ currentPos - 1 ];
        advance();
        return m.makeEntry( ret, m.valExpand( val ) );
    }
}
