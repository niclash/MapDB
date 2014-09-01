package org.mapdb.impl.btree;

import java.util.AbstractCollection;
import java.util.Iterator;
import java.util.concurrent.ConcurrentNavigableMap;

final class BTreeValues<E> extends AbstractCollection<E>
{
    private final ConcurrentNavigableMap<Object, E> m;

    BTreeValues( ConcurrentNavigableMap<Object, E> map )
    {
        m = map;
    }

    @Override
    public Iterator<E> iterator()
    {
        if( m instanceof BTreeMapImpl )
        {
            return ( (BTreeMapImpl<Object, E>) m ).valueIterator();
        }
        else if( m instanceof BTreeSubMap )
        {
            return ( (BTreeSubMap<Object, E>) m ).valueIterator();
        }
        else
        {
            return m.values().iterator();
        }
    }

    @Override
    public boolean isEmpty()
    {
        return m.isEmpty();
    }

    @Override
    public int size()
    {
        return m.size();
    }

    @Override
    public boolean contains( Object o )
    {
        return m.containsValue( o );
    }

    @Override
    public void clear()
    {
        m.clear();
    }

    @Override
    public Object[] toArray()
    {
        return BTreeMapImpl.toList( this ).toArray();
    }

    @Override
    public <T> T[] toArray( T[] a )
    {
        return BTreeMapImpl.toList( this ).toArray( a );
    }
}
