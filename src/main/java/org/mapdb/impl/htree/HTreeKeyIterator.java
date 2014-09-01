package org.mapdb.impl.htree;

import java.util.Iterator;
import java.util.NoSuchElementException;

class HTreeKeyIterator<K,V> extends HashIterator<K,V> implements Iterator<K>
{

    HTreeKeyIterator( HTreeMapImpl<K, V> hTreeMap )
    {
        super( hTreeMap );
    }

    @Override
    public K next()
    {
        if( currentLinkedList == null )
        {
            throw new NoSuchElementException();
        }
        K key = (K) currentLinkedList[ currentLinkedListPos ].key;
        moveToNext();
        return key;
    }
}
