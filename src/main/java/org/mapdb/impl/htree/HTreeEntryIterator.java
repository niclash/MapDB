package org.mapdb.impl.htree;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

class HTreeEntryIterator<K,V> extends HashIterator<K,V> implements Iterator<Map.Entry<K, V>>
{

    private HTreeMapImpl<K,V> hTreeMap;

    public HTreeEntryIterator( HTreeMapImpl<K,V> hTreeMap )
    {
        super(hTreeMap);
        this.hTreeMap = hTreeMap;
    }

    @Override
    public Map.Entry<K, V> next()
    {
        if( currentLinkedList == null )
        {
            throw new NoSuchElementException();
        }
        K key = (K) currentLinkedList[ currentLinkedListPos ].key;
        moveToNext();
        return new HTreeEntry2<K,V>( hTreeMap, key );
    }
}
