package org.mapdb.impl.btree;

import java.util.AbstractSet;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;

public final class BTreeKeySet<E> extends AbstractSet<E> implements NavigableSet<E>
{

    public final ConcurrentNavigableMap<E, Object> m;
    private final boolean hasValues;

    BTreeKeySet( ConcurrentNavigableMap<E, Object> map, boolean hasValues )
    {
        m = map;
        this.hasValues = hasValues;
    }

    @Override
    public int size()
    {
        return m.size();
    }

    @Override
    public boolean isEmpty()
    {
        return m.isEmpty();
    }

    @Override
    public boolean contains( Object o )
    {
        return m.containsKey( o );
    }

    @Override
    public boolean remove( Object o )
    {
        return m.remove( o ) != null;
    }

    @Override
    public void clear()
    {
        m.clear();
    }

    @Override
    public E lower( E e )
    {
        return m.lowerKey( e );
    }

    @Override
    public E floor( E e )
    {
        return m.floorKey( e );
    }

    @Override
    public E ceiling( E e )
    {
        return m.ceilingKey( e );
    }

    @Override
    public E higher( E e )
    {
        return m.higherKey( e );
    }

    @Override
    public Comparator<? super E> comparator()
    {
        return m.comparator();
    }

    @Override
    public E first()
    {
        return m.firstKey();
    }

    @Override
    public E last()
    {
        return m.lastKey();
    }

    @Override
    public E pollFirst()
    {
        Map.Entry<E, Object> e = m.pollFirstEntry();
        return e == null ? null : e.getKey();
    }

    @Override
    public E pollLast()
    {
        Map.Entry<E, Object> e = m.pollLastEntry();
        return e == null ? null : e.getKey();
    }

    @Override
    public Iterator<E> iterator()
    {
        if( m instanceof BTreeMapImpl )
        {
            return ( (BTreeMapImpl<E, Object>) m ).keyIterator();
        }
        else if( m instanceof BTreeSubMap )
        {
            return ( (BTreeSubMap<E, Object>) m ).keyIterator();
        }
        else if( m instanceof BTreeDescendingMap )
        {
            return ( (BTreeDescendingMap<E, Object>) m ).keyIterator();
        }
        else
        {
            return m.keySet().iterator();
        }
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

    @Override
    public Iterator<E> descendingIterator()
    {
        return descendingSet().iterator();
    }

    @Override
    public NavigableSet<E> subSet( E fromElement,
                                   boolean fromInclusive,
                                   E toElement,
                                   boolean toInclusive
    )
    {
        return new BTreeKeySet<E>( m.subMap( fromElement, fromInclusive,
                                        toElement, toInclusive ), hasValues );
    }

    @Override
    public NavigableSet<E> headSet( E toElement, boolean inclusive )
    {
        return new BTreeKeySet<E>( m.headMap( toElement, inclusive ), hasValues );
    }

    @Override
    public NavigableSet<E> tailSet( E fromElement, boolean inclusive )
    {
        return new BTreeKeySet<E>( m.tailMap( fromElement, inclusive ), hasValues );
    }

    @Override
    public NavigableSet<E> subSet( E fromElement, E toElement )
    {
        return subSet( fromElement, true, toElement, false );
    }

    @Override
    public NavigableSet<E> headSet( E toElement )
    {
        return headSet( toElement, false );
    }

    @Override
    public NavigableSet<E> tailSet( E fromElement )
    {
        return tailSet( fromElement, true );
    }

    @Override
    public NavigableSet<E> descendingSet()
    {
        return new BTreeKeySet( m.descendingMap(), hasValues );
    }

    @Override
    public boolean add( E k )
    {
        if( hasValues )
        {
            throw new UnsupportedOperationException();
        }
        else
        {
            return m.put( k, BTreeMapImpl.EMPTY ) == null;
        }
    }
}
