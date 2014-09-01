package org.mapdb.impl.btree;

import java.util.AbstractMap;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;

class BTreeDescendingMap<K, V> extends AbstractMap<K, V> implements ConcurrentNavigableMap<K, V>
{

    protected final BTreeMapImpl<K, V> m;

    protected final K lo;
    protected final boolean loInclusive;

    protected final K hi;
    protected final boolean hiInclusive;

    public BTreeDescendingMap( BTreeMapImpl<K, V> m, K lo, boolean loInclusive, K hi, boolean hiInclusive )
    {
        this.m = m;
        this.lo = lo;
        this.loInclusive = loInclusive;
        this.hi = hi;
        this.hiInclusive = hiInclusive;
        if( lo != null && hi != null && m.keySerializer.getComparator().compare( lo, hi ) > 0 )
        {
            throw new IllegalArgumentException();
        }
    }


/* ----------------  Map API methods -------------- */

    @Override
    public boolean containsKey( Object key )
    {
        if( key == null )
        {
            throw new NullPointerException();
        }
        K k = (K) key;
        return inBounds( k ) && m.containsKey( k );
    }

    @Override
    public V get( Object key )
    {
        if( key == null )
        {
            throw new NullPointerException();
        }
        K k = (K) key;
        return ( ( !inBounds( k ) ) ? null : m.get( k ) );
    }

    @Override
    public V put( K key, V value )
    {
        checkKeyBounds( key );
        return m.put( key, value );
    }

    @Override
    public V remove( Object key )
    {
        K k = (K) key;
        return ( !inBounds( k ) ) ? null : m.remove( k );
    }

    @Override
    public int size()
    {
        Iterator<K> i = keyIterator();
        int counter = 0;
        while( i.hasNext() )
        {
            counter++;
            i.next();
        }
        return counter;
    }

    @Override
    public boolean isEmpty()
    {
        return !keyIterator().hasNext();
    }

    @Override
    public boolean containsValue( Object value )
    {
        if( value == null )
        {
            throw new NullPointerException();
        }
        Iterator<V> i = valueIterator();
        while( i.hasNext() )
        {
            if( value.equals( i.next() ) )
            {
                return true;
            }
        }
        return false;
    }

    @Override
    public void clear()
    {
        Iterator<K> i = keyIterator();
        while( i.hasNext() )
        {
            i.next();
            i.remove();
        }
    }


    /* ----------------  ConcurrentMap API methods -------------- */

    @Override
    public V putIfAbsent( K key, V value )
    {
        checkKeyBounds( key );
        return m.putIfAbsent( key, value );
    }

    @Override
    public boolean remove( Object key, Object value )
    {
        K k = (K) key;
        return inBounds( k ) && m.remove( k, value );
    }

    @Override
    public boolean replace( K key, V oldValue, V newValue )
    {
        checkKeyBounds( key );
        return m.replace( key, oldValue, newValue );
    }

    @Override
    public V replace( K key, V value )
    {
        checkKeyBounds( key );
        return m.replace( key, value );
    }

    /* ----------------  SortedMap API methods -------------- */

    @Override
    public Comparator<? super K> comparator()
    {
        return m.comparator();
    }

    /* ----------------  Relational methods -------------- */

    @Override
    public Entry<K, V> higherEntry( K key )
    {
        if( key == null )
        {
            throw new NullPointerException();
        }
        if( tooLow( key ) )
        {
            return null;
        }

        if( tooHigh( key ) )
        {
            return firstEntry();
        }

        Entry<K, V> r = m.lowerEntry( key );
        return r != null && !tooLow( r.getKey() ) ? r : null;
    }

    @Override
    public K lowerKey( K key )
    {
        Entry<K, V> n = lowerEntry( key );
        return ( n == null ) ? null : n.getKey();
    }

    @Override
    public Entry<K, V> ceilingEntry( K key )
    {
        if( key == null )
        {
            throw new NullPointerException();
        }
        if( tooLow( key ) )
        {
            return null;
        }

        if( tooHigh( key ) )
        {
            return firstEntry();
        }

        Entry<K, V> ret = m.floorEntry( key );
        if( ret != null && tooLow( ret.getKey() ) )
        {
            return null;
        }
        return ret;
    }

    @Override
    public K floorKey( K key )
    {
        Entry<K, V> n = floorEntry( key );
        return ( n == null ) ? null : n.getKey();
    }

    @Override
    public Entry<K, V> floorEntry( K key )
    {
        if( key == null )
        {
            throw new NullPointerException();
        }
        if( tooHigh( key ) )
        {
            return null;
        }

        if( tooLow( key ) )
        {
            return lastEntry();
        }

        Entry<K, V> ret = m.ceilingEntry( key );
        if( ret != null && tooHigh( ret.getKey() ) )
        {
            return null;
        }
        return ret;
    }

    @Override
    public K ceilingKey( K key )
    {
        Entry<K, V> k = ceilingEntry( key );
        return k != null ? k.getKey() : null;
    }

    @Override
    public Entry<K, V> lowerEntry( K key )
    {
        Entry<K, V> r = m.higherEntry( key );
        return r != null && inBounds( r.getKey() ) ? r : null;
    }

    @Override
    public K higherKey( K key )
    {
        Entry<K, V> k = higherEntry( key );
        return k != null ? k.getKey() : null;
    }

    @Override
    public K firstKey()
    {
        Entry<K, V> e = firstEntry();
        if( e == null )
        {
            throw new NoSuchElementException();
        }
        return e.getKey();
    }

    @Override
    public K lastKey()
    {
        Entry<K, V> e = lastEntry();
        if( e == null )
        {
            throw new NoSuchElementException();
        }
        return e.getKey();
    }

    @Override
    public Entry<K, V> lastEntry()
    {
        Entry<K, V> k =
            lo == null ?
            m.firstEntry() :
            m.findLarger( lo, loInclusive );
        return k != null && inBounds( k.getKey() ) ? k : null;
    }

    @Override
    public Entry<K, V> firstEntry()
    {
        Entry<K, V> k =
            hi == null ?
            m.lastEntry() :
            m.findSmaller( hi, hiInclusive );

        return k != null && inBounds( k.getKey() ) ? k : null;
    }

    @Override
    public Entry<K, V> pollFirstEntry()
    {
        while( true )
        {
            Entry<K, V> e = firstEntry();
            if( e == null || remove( e.getKey(), e.getValue() ) )
            {
                return e;
            }
        }
    }

    @Override
    public Entry<K, V> pollLastEntry()
    {
        while( true )
        {
            Entry<K, V> e = lastEntry();
            if( e == null || remove( e.getKey(), e.getValue() ) )
            {
                return e;
            }
        }
    }

    /**
     * Utility to create submaps, where given bounds override
     * unbounded(null) ones and/or are checked against bounded ones.
     */
    private BTreeDescendingMap<K, V> newSubMap(
        K toKey,
        boolean toInclusive,
        K fromKey,
        boolean fromInclusive
    )
    {

//            if(fromKey!=null && toKey!=null){
//                int comp = m.comparator.compare(fromKey, toKey);
//                if((fromInclusive||!toInclusive) && comp==0)
//                    throw new IllegalArgumentException();
//            }

        if( lo != null )
        {
            if( fromKey == null )
            {
                fromKey = lo;
                fromInclusive = loInclusive;
            }
            else
            {
                int c = m.keySerializer.getComparator().compare( fromKey, lo );
                if( c < 0 || ( c == 0 && !loInclusive && fromInclusive ) )
                {
                    throw new IllegalArgumentException( "key out of range" );
                }
            }
        }
        if( hi != null )
        {
            if( toKey == null )
            {
                toKey = hi;
                toInclusive = hiInclusive;
            }
            else
            {
                int c = m.keySerializer.getComparator().compare( toKey, hi );
                if( c > 0 || ( c == 0 && !hiInclusive && toInclusive ) )
                {
                    throw new IllegalArgumentException( "key out of range" );
                }
            }
        }
        return new BTreeDescendingMap<K, V>( m, fromKey, fromInclusive,
                                        toKey, toInclusive );
    }

    @Override
    public BTreeDescendingMap<K, V> subMap( K fromKey,
                                       boolean fromInclusive,
                                       K toKey,
                                       boolean toInclusive
    )
    {
        if( fromKey == null || toKey == null )
        {
            throw new NullPointerException();
        }
        return newSubMap( fromKey, fromInclusive, toKey, toInclusive );
    }

    @Override
    public BTreeDescendingMap<K, V> headMap( K toKey,
                                        boolean inclusive
    )
    {
        if( toKey == null )
        {
            throw new NullPointerException();
        }
        return newSubMap( null, false, toKey, inclusive );
    }

    @Override
    public BTreeDescendingMap<K, V> tailMap( K fromKey,
                                        boolean inclusive
    )
    {
        if( fromKey == null )
        {
            throw new NullPointerException();
        }
        return newSubMap( fromKey, inclusive, null, false );
    }

    @Override
    public BTreeDescendingMap<K, V> subMap( K fromKey, K toKey )
    {
        return subMap( fromKey, true, toKey, false );
    }

    @Override
    public BTreeDescendingMap<K, V> headMap( K toKey )
    {
        return headMap( toKey, false );
    }

    @Override
    public BTreeDescendingMap<K, V> tailMap( K fromKey )
    {
        return tailMap( fromKey, true );
    }

    @Override
    public ConcurrentNavigableMap<K, V> descendingMap()
    {
        if( lo == null && hi == null )
        {
            return m;
        }
        return m.subMap( lo, loInclusive, hi, hiInclusive );
    }

    @Override
    public NavigableSet<K> navigableKeySet()
    {
        return new BTreeKeySet<K>( (ConcurrentNavigableMap<K, Object>) this, m.hasValues );
    }


    /* ----------------  Utilities -------------- */

    private boolean tooLow( K key )
    {
        if( lo != null )
        {
            int c = m.keySerializer.getComparator().compare( key, lo );
            if( c < 0 || ( c == 0 && !loInclusive ) )
            {
                return true;
            }
        }
        return false;
    }

    private boolean tooHigh( K key )
    {
        if( hi != null )
        {
            int c = m.keySerializer.getComparator().compare( key, hi );
            if( c > 0 || ( c == 0 && !hiInclusive ) )
            {
                return true;
            }
        }
        return false;
    }

    private boolean inBounds( K key )
    {
        return !tooLow( key ) && !tooHigh( key );
    }

    private void checkKeyBounds( K key )
        throws IllegalArgumentException
    {
        if( key == null )
        {
            throw new NullPointerException();
        }
        if( !inBounds( key ) )
        {
            throw new IllegalArgumentException( "key out of range" );
        }
    }

    @Override
    public NavigableSet<K> keySet()
    {
        return new BTreeKeySet<K>( (ConcurrentNavigableMap<K, Object>) this, m.hasValues );
    }

    @Override
    public NavigableSet<K> descendingKeySet()
    {
        return new BTreeKeySet<K>( (ConcurrentNavigableMap<K, Object>) descendingMap(), m.hasValues );
    }

    @Override
    public Set<Entry<K, V>> entrySet()
    {
        return new BTreeEntrySet<K, V>( this );
    }


    /*
     * ITERATORS
     */

    abstract class Iter<E> implements Iterator<E>
    {
        Entry<K, V> current = BTreeDescendingMap.this.firstEntry();
        Entry<K, V> last = null;

        @Override
        public boolean hasNext()
        {
            return current != null;
        }

        public void advance()
        {
            if( current == null )
            {
                throw new NoSuchElementException();
            }
            last = current;
            current = BTreeDescendingMap.this.higherEntry( current.getKey() );
        }

        @Override
        public void remove()
        {
            if( last == null )
            {
                throw new IllegalStateException();
            }
            BTreeDescendingMap.this.remove( last.getKey() );
            last = null;
        }
    }

    Iterator<K> keyIterator()
    {
        return new Iter<K>()
        {
            @Override
            public K next()
            {
                advance();
                return last.getKey();
            }
        };
    }

    Iterator<V> valueIterator()
    {
        return new Iter<V>()
        {

            @Override
            public V next()
            {
                advance();
                return last.getValue();
            }
        };
    }

    Iterator<Entry<K, V>> entryIterator()
    {
        return new Iter<Entry<K, V>>()
        {
            @Override
            public Entry<K, V> next()
            {
                advance();
                return last;
            }
        };
    }
}
