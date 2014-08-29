package org.mapdb;

import java.util.concurrent.TimeUnit;
import org.mapdb.impl.Fun;

public interface HTreeMapMaker
{
    /**
     * by default collection does not have counter, without counter updates are faster, but entire collection needs to be traversed to count items.
     */
    HTreeMapMaker counterEnable();

    /**
     * keySerializer used to convert keys into/from binary form.
     */
    HTreeMapMaker keySerializer( ValueSerializer<?> keySerializer );

    /**
     * valueSerializer used to convert values into/from binary form.
     */
    HTreeMapMaker valueSerializer( ValueSerializer<?> valueSerializer );

    /**
     * maximal number of entries in this map. Less used entries will be expired and removed to make collection smaller
     */
    HTreeMapMaker expireMaxSize( long maxSize );

    /**
     * Specifies that each entry should be automatically removed from the map once a fixed duration has elapsed after the entry's creation, or the most recent replacement of its value.
     */
    HTreeMapMaker expireAfterWrite( long interval, TimeUnit timeUnit );

    /**
     * Specifies that each entry should be automatically removed from the map once a fixed duration has elapsed after the entry's creation, or the most recent replacement of its value.
     */
    HTreeMapMaker expireAfterWrite( long interval );

    /**
     * Specifies that each entry should be automatically removed from the map once a fixed duration has elapsed after the entry's creation, the most recent replacement of its value, or its last access. Access time is reset by all map read and write operations
     */
    HTreeMapMaker expireAfterAccess( long interval, TimeUnit timeUnit );

    /**
     * Specifies that each entry should be automatically removed from the map once a fixed duration has elapsed after the entry's creation, the most recent replacement of its value, or its last access. Access time is reset by all map read and write operations
     */
    HTreeMapMaker expireAfterAccess( long interval );

    HTreeMapMaker expireStoreSize( double maxStoreSize );

    /**
     * If value is not found, HTreeMap can fetch and insert default value. `valueCreator` is used to return new value.
     * This way `HTreeMap.get()` never returns null
     */
    HTreeMapMaker valueCreator( Fun.Function1<?, ?> valueCreator );

    HTreeMapMaker hasher( Hasher<?> hasher );

    <K, V> HTreeMap<K, V> make();

    <K, V> HTreeMap<K, V> makeOrGet();
}
