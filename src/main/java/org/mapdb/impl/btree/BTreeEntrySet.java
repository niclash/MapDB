package org.mapdb.impl.btree;

import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;

final class BTreeEntrySet<K, V> extends AbstractSet<Map.Entry<K, V>>
{
    private final ConcurrentNavigableMap<K, V> m;

    BTreeEntrySet( ConcurrentNavigableMap<K, V> map )
    {
        m = map;
    }

    @Override
    public Iterator<Map.Entry<K, V>> iterator()
    {
        if( m instanceof BTreeMapImpl )
        {
            return ( (BTreeMapImpl<K, V>) m ).entryIterator();
        }
        else if( m instanceof BTreeSubMap )
        {
            return ( (BTreeSubMap<K, V>) m ).entryIterator();
        }
        else if( m instanceof BTreeDescendingMap )
        {
            return ( (BTreeDescendingMap<K, V>) m ).entryIterator();
        }
        else
        {
            return m.entrySet().iterator();
        }
    }

    @Override
    public boolean contains( Object o )
    {
        if( !( o instanceof Map.Entry ) )
        {
            return false;
        }
        Map.Entry<K, V> e = (Map.Entry<K, V>) o;
        K key = e.getKey();
        if( key == null )
        {
            return false;
        }
        V v = m.get( key );
        return v != null && v.equals( e.getValue() );
    }

    @Override
    public boolean remove( Object o )
    {
        if( !( o instanceof Map.Entry ) )
        {
            return false;
        }
        Map.Entry<K, V> e = (Map.Entry<K, V>) o;
        K key = e.getKey();
        if( key == null )
        {
            return false;
        }
        return m.remove( key,
                         e.getValue() );
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
    public void clear()
    {
        m.clear();
    }

    @Override
    public boolean equals( Object o )
    {
        if( o == this )
        {
            return true;
        }
        if( !( o instanceof Set ) )
        {
            return false;
        }
        Collection<?> c = (Collection<?>) o;
        try
        {
            return containsAll( c ) && c.containsAll( this );
        }
        catch( ClassCastException unused )
        {
            return false;
        }
        catch( NullPointerException unused )
        {
            return false;
        }
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
