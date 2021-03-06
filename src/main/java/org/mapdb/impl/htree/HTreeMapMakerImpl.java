package org.mapdb.impl.htree;

import java.util.concurrent.TimeUnit;
import org.mapdb.HTreeMap;
import org.mapdb.HTreeMapMaker;
import org.mapdb.Hasher;
import org.mapdb.ValueSerializer;
import org.mapdb.impl.engine.DbImpl;
import org.mapdb.impl.Fun;

public class HTreeMapMakerImpl implements HTreeMapMaker
{
    public final String name;
    private final DbImpl db;

    public HTreeMapMakerImpl( String name, DbImpl db )
    {
        this.name = name;
        this.db = db;
    }

    public boolean counter = false;
    public ValueSerializer<?> keySerializer = null;
    public ValueSerializer<?> valueSerializer = null;
    public long expireMaxSize = 0L;
    public long expire = 0L;
    public long expireAccess = 0L;
    public long expireStoreSize;
    public Hasher<?> hasher = null;

    public Fun.Function1<?, ?> valueCreator = null;

    @Override
    public HTreeMapMaker counterEnable()
    {
        this.counter = true;
        return this;
    }

    @Override
    public HTreeMapMaker keySerializer( ValueSerializer<?> keySerializer )
    {
        this.keySerializer = keySerializer;
        return this;
    }

    @Override
    public HTreeMapMaker valueSerializer( ValueSerializer<?> valueSerializer )
    {
        this.valueSerializer = valueSerializer;
        return this;
    }

    @Override
    public HTreeMapMaker expireMaxSize( long maxSize )
    {
        this.expireMaxSize = maxSize;
        this.counter = true;
        return this;
    }

    @Override
    public HTreeMapMaker expireAfterWrite( long interval, TimeUnit timeUnit )
    {
        this.expire = timeUnit.toMillis( interval );
        return this;
    }

    @Override
    public HTreeMapMaker expireAfterWrite( long interval )
    {
        this.expire = interval;
        return this;
    }

    @Override
    public HTreeMapMaker expireAfterAccess( long interval, TimeUnit timeUnit )
    {
        this.expireAccess = timeUnit.toMillis( interval );
        return this;
    }

    @Override
    public HTreeMapMaker expireAfterAccess( long interval )
    {
        this.expireAccess = interval;
        return this;
    }

    @Override
    public HTreeMapMaker expireStoreSize( double maxStoreSize )
    {
        this.expireStoreSize = (long) ( maxStoreSize * 1024L * 1024L * 1024L );
        return this;
    }

    @Override
    public HTreeMapMaker valueCreator( Fun.Function1<?, ?> valueCreator )
    {
        this.valueCreator = valueCreator;
        return this;
    }

    @Override
    public HTreeMapMaker hasher( Hasher<?> hasher )
    {
        this.hasher = hasher;
        return this;
    }

    @Override
    public <K, V> HTreeMap<K, V> make()
    {
        if( expireMaxSize != 0 )
        {
            counter = true;
        }
        return db.createHashMap( HTreeMapMakerImpl.this );
    }

    @Override
    public <K, V> HTreeMap<K, V> makeOrGet()
    {
        synchronized( db )
        {
            //TODO add parameter check
            return (HTreeMap<K, V>) ( db.catGet( name + ".type" ) == null ?
                                      make() : db.getHashMap( name ) );
        }
    }
}
