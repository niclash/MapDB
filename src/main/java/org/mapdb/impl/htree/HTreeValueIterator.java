package org.mapdb.impl.htree;

import java.util.Iterator;
import java.util.NoSuchElementException;

class HTreeValueIterator<K, V> extends HashIterator<K, V> implements Iterator<V>
{

    HTreeValueIterator( HTreeMapImpl<K, V> hTreeMap )
    {
        super( hTreeMap );
    }

    @Override
    public V next()
    {
        if( currentLinkedList == null )
        {
            throw new NoSuchElementException();
        }
        V value = (V) currentLinkedList[ currentLinkedListPos ].value;
        moveToNext();
        return value;
    }
}
