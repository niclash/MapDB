package org.mapdb;

import java.io.Closeable;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentNavigableMap;

public interface BTreeMap<K, V> extends ConcurrentNavigableMap<K,V>, Bind.MapWithModificationListener<K, V>,Closeable
{
    /**
     * Make readonly snapshot view of current Map. Snapshot is immutable and not affected by modifications made by other threads.
     * Useful if you need consistent view on Map.
     *
     * Maintaining snapshot have some overhead, underlying Engine is closed after Map view is GCed.
     * Please make sure to release reference to this Map view, so snapshot view can be garbage collected.
     *
     * @return snapshot
     */
    NavigableMap<K,V> snapshot();

    Engine getEngine();
}
