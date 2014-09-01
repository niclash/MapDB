package org.mapdb.impl.htree;

import java.util.Map;

class HTreeEntry2<K,V> implements Map.Entry<K, V>
{

    private HTreeMapImpl<K,V> hTreeMap;
    private final K key;

    HTreeEntry2( HTreeMapImpl<K,V> hTreeMap, K key )
    {
        this.hTreeMap = hTreeMap;
        this.key = key;
    }

    @Override
    public K getKey()
    {
        return key;
    }

    @Override
    public V getValue()
    {
        return hTreeMap.get( key );
    }

    @Override
    public V setValue( V value )
    {
        return hTreeMap.put( key, value );
    }

    @Override
    public boolean equals( Object o )
    {
        return ( o instanceof Map.Entry ) && hTreeMap.hasher.equals( key, ( (Map.Entry<K,V>) o ).getKey() );
    }

    @Override
    public int hashCode()
    {
        final V value = hTreeMap.get( key );
        return ( key == null ? 0 : hTreeMap.hasher.hashCode( key ) ) ^
               ( value == null ? 0 : value.hashCode() );
    }
}
