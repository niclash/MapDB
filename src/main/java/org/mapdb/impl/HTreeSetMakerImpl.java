package org.mapdb.impl;

import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.mapdb.HTreeSetMaker;
import org.mapdb.Hasher;
import org.mapdb.ValueSerializer;

public class HTreeSetMakerImpl implements HTreeSetMaker
{
    private DbImpl db;
    protected final String name;

    public HTreeSetMakerImpl( DbImpl db, String name )
    {
        this.db = db;
        this.name = name;
    }

    protected boolean counter = false;
    protected ValueSerializer<?> serializer = null;
    protected long expireMaxSize = 0L;
    protected long expireStoreSize = 0L;
    protected long expire = 0L;
    protected long expireAccess = 0L;
    protected Hasher<?> hasher = null;

    /**
     * by default collection does not have counter, without counter updates are faster, but entire collection needs to be traversed to count items.
     */
    @Override
    public HTreeSetMaker counterEnable()
    {
        this.counter = true;
        return this;
    }

    /**
     * keySerializer used to convert keys into/from binary form.
     */
    @Override
    public HTreeSetMaker serializer( ValueSerializer<?> serializer )
    {
        this.serializer = serializer;
        return this;
    }

    /**
     * maximal number of entries in this map. Less used entries will be expired and removed to make collection smaller
     */
    @Override
    public HTreeSetMaker expireMaxSize( long maxSize )
    {
        this.expireMaxSize = maxSize;
        this.counter = true;
        return this;
    }

    /**
     * maximal size of store in GB, if store is larger entries will start expiring
     */
    @Override
    public HTreeSetMaker expireStoreSize( double maxStoreSize )
    {
        this.expireStoreSize = (long) ( maxStoreSize * 1024L * 1024L * 1024L );
        return this;
    }

    /**
     * Specifies that each entry should be automatically removed from the map once a fixed duration has elapsed after the entry's creation, or the most recent replacement of its value.
     */
    @Override
    public HTreeSetMaker expireAfterWrite( long interval, TimeUnit timeUnit )
    {
        this.expire = timeUnit.toMillis( interval );
        return this;
    }

    /**
     * Specifies that each entry should be automatically removed from the map once a fixed duration has elapsed after the entry's creation, or the most recent replacement of its value.
     */
    @Override
    public HTreeSetMaker expireAfterWrite( long interval )
    {
        this.expire = interval;
        return this;
    }

    /**
     * Specifies that each entry should be automatically removed from the map once a fixed duration has elapsed after the entry's creation, the most recent replacement of its value, or its last access. Access time is reset by all map read and write operations
     */
    @Override
    public HTreeSetMaker expireAfterAccess( long interval, TimeUnit timeUnit )
    {
        this.expireAccess = timeUnit.toMillis( interval );
        return this;
    }

    /**
     * Specifies that each entry should be automatically removed from the map once a fixed duration has elapsed after the entry's creation, the most recent replacement of its value, or its last access. Access time is reset by all map read and write operations
     */
    @Override
    public HTreeSetMaker expireAfterAccess( long interval )
    {
        this.expireAccess = interval;
        return this;
    }

    @Override
    public HTreeSetMaker hasher( Hasher<?> hasher )
    {
        this.hasher = hasher;
        return this;
    }

    @Override
    public <K> Set<K> make()
    {
        if( expireMaxSize != 0 )
        {
            counter = true;
        }
        return db.createHashSet( HTreeSetMakerImpl.this );
    }

    @Override
    public <K> Set<K> makeOrGet()
    {
        synchronized( db )
        {
            //TODO add parameter check
            return (Set<K>) ( db.catGet( name + ".type" ) == null ?
                              make() : db.getHashSet( name ) );
        }
    }
}
