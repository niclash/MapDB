package org.mapdb.impl.htree;

import java.util.AbstractCollection;
import java.util.Iterator;

class ValuesCollection<K, V> extends AbstractCollection<V>
{

    private HTreeMapImpl<K, V> hTreeMap;

    public ValuesCollection( HTreeMapImpl<K, V> hTreeMap )
    {
        this.hTreeMap = hTreeMap;
    }

    @Override
    public int size()
    {
        return hTreeMap.size();
    }

    @Override
    public boolean isEmpty()
    {
        return hTreeMap.isEmpty();
    }

    @Override
    public boolean contains( Object o )
    {
        return hTreeMap.containsValue( o );
    }

    @SuppressWarnings( "NullableProblems" )
    @Override
    public Iterator<V> iterator()
    {
        return new HTreeValueIterator<K,V>(hTreeMap);
    }
}
