package org.mapdb.impl.binaryserializer;

import java.util.Arrays;

/**
 * Utility class similar to ArrayList, but with fast identity search.
 */
public final class FastArrayList<K>
{

    public int size;
    public K[] data;
    public boolean forwardRefs = false;

    public FastArrayList()
    {
        size = 0;
        data = (K[]) new Object[ 1 ];
    }

    public void add( K o )
    {
        if( data.length == size )
        {
            //grow array if necessary
            data = Arrays.copyOf( data, data.length * 2 );
        }

        data[ size ] = o;
        size++;
    }

    /**
     * This method is reason why ArrayList is not used.
     * Search an item in list and returns its index.
     * It uses identity rather than 'equalsTo'
     * One could argue that TreeMap should be used instead,
     * but we do not expect large object trees.
     * This search is VERY FAST compared to Maps, it does not allocate
     * new instances or uses method calls.
     *
     * @param obj to find in list
     *
     * @return index of object in list or -1 if not found
     */
    public int identityIndexOf( Object obj )
    {
        for( int i = 0; i < size; i++ )
        {
            if( obj == data[ i ] )
            {
                forwardRefs = true;
                return i;
            }
        }
        return -1;
    }
}
