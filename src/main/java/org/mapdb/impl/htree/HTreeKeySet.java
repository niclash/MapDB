package org.mapdb.impl.htree;

import java.util.AbstractSet;
import java.util.Iterator;
import org.mapdb.HTreeMap;
import org.mapdb.impl.btree.BTreeMapImpl;

public class HTreeKeySet<K,V> extends AbstractSet<K>
{

    private HTreeMapImpl<K,V> hTreeMap;

    public HTreeKeySet( HTreeMapImpl<K,V> hTreeMap )
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
        return hTreeMap.containsKey( o );
    }

    @Override
    public Iterator<K> iterator()
    {
        return new HTreeKeyIterator<K,V>(hTreeMap);
    }

    @Override
    public boolean add( K k )
    {
        if( hTreeMap.hasValues )
        {
            throw new UnsupportedOperationException();
        }
        else
        {
            return hTreeMap.put( k, (V) BTreeMapImpl.EMPTY ) == null;
        }
    }

    @Override
    public boolean remove( Object o )
    {
//            if(o instanceof Entry){
//                Entry e = (Entry) o;
//                return HTreeMapImpl.this.remove(((Entry) o).getKey(),((Entry) o).getValue());
//            }
        return hTreeMap.remove( o ) != null;
    }

    @Override
    public void clear()
    {
        hTreeMap.clear();
    }

    public HTreeMap<K, V> parent()
    {
        return hTreeMap;
    }

    @Override
    public int hashCode()
    {
        int result = 0;
        for( K k : this )
        {
            result += hTreeMap.hasher.hashCode( k );
        }
        return result;
    }
}
