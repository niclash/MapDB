package org.mapdb;

import java.io.Closeable;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

public interface HTreeMap<K,V> extends ConcurrentMap<K, V>, Bind.MapWithModificationListener<K, V>, Closeable
{
    /**
     * Return given value, without updating cache statistics if `expireAccess()` is true
     * It also does not use `valueCreator` if value is not found (always returns null if not found)
     *
     * @param key key to lookup
     *
     * @return value associated with key or null
     */
    V getPeek( Object key );

    /**
     * Returns maximal (newest) expiration timestamp
     */
    long getMaxExpireTime();

    /**
     * Returns minimal (oldest) expiration timestamp
     */
    long getMinExpireTime();

    /**
     * Make readonly snapshot view of current Map. Snapshot is immutable and not affected by modifications made by other threads.
     * Useful if you need consistent view on Map.
     * <p>
     * Maintaining snapshot have some overhead, underlying Engine is closed after Map view is GCed.
     * Please make sure to release reference to this Map view, so snapshot view can be garbage collected.
     *
     * @return snapshot
     */
    Map<K, V> snapshot();

    Engine getEngine();
}
