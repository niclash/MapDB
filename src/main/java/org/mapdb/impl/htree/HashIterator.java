package org.mapdb.impl.htree;

import java.util.Arrays;
import java.util.concurrent.locks.Lock;

abstract class HashIterator<K,V>
{

    private HTreeMapImpl<K,V> hTreeMap;
    protected HTreeLinkedNode<K,V>[] currentLinkedList;
    protected int currentLinkedListPos = 0;

    private K lastReturnedKey = null;

    private int lastSegment = 0;

    HashIterator( HTreeMapImpl<K,V> hTreeMap )
    {
        this.hTreeMap = hTreeMap;
        currentLinkedList = findNextLinkedNode( 0 );
    }

    public void remove()
    {
        final K keyToRemove = lastReturnedKey;
        if( lastReturnedKey == null )
        {
            throw new IllegalStateException();
        }

        lastReturnedKey = null;
        hTreeMap.remove( keyToRemove );
    }

    public boolean hasNext()
    {
        return currentLinkedList != null && currentLinkedListPos < currentLinkedList.length;
    }

    protected void moveToNext()
    {
        lastReturnedKey = currentLinkedList[ currentLinkedListPos ].key;
        currentLinkedListPos += 1;
        if( currentLinkedListPos == currentLinkedList.length )
        {
            final int lastHash = hTreeMap.hash( lastReturnedKey );
            currentLinkedList = advance( lastHash );
            currentLinkedListPos = 0;
        }
    }

    private HTreeLinkedNode<K,V>[] advance( int lastHash )
    {

        int segment = lastHash >>> 28;

        //two phases, first find old item and increase hash
        try
        {
            hTreeMap.segmentLocks[ segment ].readLock().lock();

            long dirRecid = hTreeMap.segmentRecids[ segment ];
            int level = 3;
            //dive into tree, finding last hash position
            while( true )
            {
                long[][] dir = hTreeMap.engine.get( dirRecid, HTreeMapImpl.DIR_SERIALIZER );
                int pos = ( lastHash >>> ( 7 * level ) ) & 0x7F;

                //check if we need to expand deeper
                if( dir[ pos >>> HTreeMapImpl.DIV8 ] == null || dir[ pos >>> HTreeMapImpl.DIV8 ][ pos & HTreeMapImpl.MOD8 ] == 0 || ( dir[ pos >>> HTreeMapImpl.DIV8 ][ pos & HTreeMapImpl.MOD8 ] & 1 ) == 1 )
                {
                    //increase hash by 1
                    if( level != 0 )
                    {
                        lastHash = ( ( lastHash >>> ( 7 * level ) ) + 1 ) << ( 7 * level ); //should use mask and XOR
                    }
                    else
                    {
                        lastHash += 1;
                    }
                    if( lastHash == 0 )
                    {
                        return null;
                    }
                    break;
                }

                //reference is dir, move to next level
                dirRecid = dir[ pos >>> HTreeMapImpl.DIV8 ][ pos & HTreeMapImpl.MOD8 ] >>> 1;
                level--;
            }
        }
        finally
        {
            hTreeMap.segmentLocks[ segment ].readLock().unlock();
        }
        return findNextLinkedNode( lastHash );
    }

    private HTreeLinkedNode<K,V>[] findNextLinkedNode( int hash )
    {
        //second phase, start search from increased hash to find next items
        for( int segment = Math.max( hash >>> 28, lastSegment ); segment < 16; segment++ )
        {
            final Lock lock = hTreeMap.expireAccessFlag ? hTreeMap.segmentLocks[ segment ].writeLock() : hTreeMap.segmentLocks[ segment ].readLock();
            lock.lock();
            try
            {
                lastSegment = Math.max( segment, lastSegment );
                long dirRecid = hTreeMap.segmentRecids[ segment ];
                HTreeLinkedNode<K,V> ret[] = findNextLinkedNodeRecur( dirRecid, hash, 3 );
                if( ret != null )
                {
                    for( HTreeLinkedNode<K,V> ln : ret )
                    {
                        assert hTreeMap.hash( ln.key ) >>> 28 == segment;
                    }
                }
                //System.out.println(Arrays.asList(ret));
                if( ret != null )
                {
                    if( hTreeMap.expireAccessFlag )
                    {
                        for( HTreeLinkedNode<K,V> ln : ret )
                        {
                            hTreeMap.expireLinkBump( segment, ln.expireLinkNodeRecid, true );
                        }
                    }
                    return ret;
                }
                hash = 0;
            }
            finally
            {
                lock.unlock();
            }
        }

        return null;
    }

    private HTreeLinkedNode<K,V>[] findNextLinkedNodeRecur( long dirRecid, int newHash, int level )
    {
        long[][] dir = hTreeMap.engine.get( dirRecid, HTreeMapImpl.DIR_SERIALIZER );
        if( dir == null )
        {
            return null;
        }
        int pos = ( newHash >>> ( level * 7 ) ) & 0x7F;
        boolean first = true;
        while( pos < 128 )
        {
            if( dir[ pos >>> HTreeMapImpl.DIV8 ] != null )
            {
                long recid = dir[ pos >>> HTreeMapImpl.DIV8 ][ pos & HTreeMapImpl.MOD8 ];
                if( recid != 0 )
                {
                    if( ( recid & 1 ) == 1 )
                    {
                        recid = recid >> 1;
                        //found linked list, load it into array and return
                        @SuppressWarnings( "unchecked" )
                        HTreeLinkedNode<K,V>[] array = new HTreeLinkedNode[ 1 ];
                        int arrayPos = 0;
                        while( recid != 0 )
                        {
                            HTreeLinkedNode<K,V> ln = hTreeMap.engine.get( recid, hTreeMap.LN_SERIALIZER );
                            if( ln == null )
                            {
                                recid = 0;
                                continue;
                            }
                            //increase array size if needed
                            if( arrayPos == array.length )
                            {
                                array = Arrays.copyOf( array, array.length + 1 );
                            }
                            array[ arrayPos++ ] = ln;
                            recid = ln.next;
                        }
                        return array;
                    }
                    else
                    {
                        //found another dir, continue dive
                        recid = recid >> 1;
                        HTreeLinkedNode<K,V>[] ret = findNextLinkedNodeRecur( recid, first ? newHash : 0, level - 1 );
                        if( ret != null )
                        {
                            return ret;
                        }
                    }
                }
            }
            first = false;
            pos++;
        }
        return null;
    }
}
