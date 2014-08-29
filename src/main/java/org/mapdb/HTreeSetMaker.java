package org.mapdb;

import java.util.Set;
import java.util.concurrent.TimeUnit;

public interface HTreeSetMaker
{
    /**
     * by default collection does not have counter, without counter updates are faster, but entire collection needs to be traversed to count items.
     */
    HTreeSetMaker counterEnable();

    /**
     * keySerializer used to convert keys into/from binary form.
     */
    HTreeSetMaker serializer( ValueSerializer<?> serializer );

    /**
     * maximal number of entries in this map. Less used entries will be expired and removed to make collection smaller
     */
    HTreeSetMaker expireMaxSize( long maxSize );

    /**
     * maximal size of store in GB, if store is larger entries will start expiring
     */
    HTreeSetMaker expireStoreSize( double maxStoreSize );

    /**
     * Specifies that each entry should be automatically removed from the map once a fixed duration has elapsed after the entry's creation, or the most recent replacement of its value.
     */
    HTreeSetMaker expireAfterWrite( long interval, TimeUnit timeUnit );

    /**
     * Specifies that each entry should be automatically removed from the map once a fixed duration has elapsed after the entry's creation, or the most recent replacement of its value.
     */
    HTreeSetMaker expireAfterWrite( long interval );

    /**
     * Specifies that each entry should be automatically removed from the map once a fixed duration has elapsed after the entry's creation, the most recent replacement of its value, or its last access. Access time is reset by all map read and write operations
     */
    HTreeSetMaker expireAfterAccess( long interval, TimeUnit timeUnit );

    /**
     * Specifies that each entry should be automatically removed from the map once a fixed duration has elapsed after the entry's creation, the most recent replacement of its value, or its last access. Access time is reset by all map read and write operations
     */
    HTreeSetMaker expireAfterAccess( long interval );

    HTreeSetMaker hasher( Hasher<?> hasher );

    <K> Set<K> make();

    <K> Set<K> makeOrGet();
}
