package org.mapdb;

import java.util.Comparator;
import java.util.Iterator;
import org.mapdb.impl.Fun;

public interface BTreeMapMaker
{
    /**
     * nodeSize maximal size of node, larger node causes overflow and creation of new BTree node. Use large number for small keys, use small number for large keys.
     */
    BTreeMapMaker nodeSize( int nodeSize );

    /**
     * by default values are stored inside BTree Nodes. Large values should be stored outside of BTreeNodes
     */
    BTreeMapMaker valuesOutsideNodesEnable();

    /**
     * by default collection does not have counter, without counter updates are faster, but entire collection needs to be traversed to count items.
     */
    BTreeMapMaker counterEnable();

    /**
     * keySerializer used to convert keys into/from binary form.
     */
    BTreeMapMaker keySerializer( KeySerializer<?> keySerializer );

    /**
     * keySerializer used to convert keys into/from binary form.
     * This wraps ordinary serializer, with no delta packing used
     */
    BTreeMapMaker keySerializerWrap( ValueSerializer<?> serializer );

    /**
     * valueSerializer used to convert values into/from binary form.
     */
    BTreeMapMaker valueSerializer( ValueSerializer<?> valueSerializer );

    /**
     * comparator used to sort keys.
     */
    BTreeMapMaker comparator( Comparator<?> comparator );

    <K, V> BTreeMapMaker pumpSource( Iterator<K> keysSource, Fun.Function1<V, K> valueExtractor );

    <K, V> BTreeMapMaker pumpSource( Iterator<Fun.Tuple2<K, V>> entriesSource );

    BTreeMapMaker pumpPresort( int batchSize );

    /**
     * If source iteretor contains an duplicate key, exception is thrown.
     * This options will only use firts key and ignore any consequentive duplicates.
     */
    <K> BTreeMapMaker pumpIgnoreDuplicates();

    <K, V> BTreeMap<K, V> make();

    <K, V> BTreeMap<K, V> makeOrGet();

    /**
     * creates map optimized for using `String` keys
     */
    <V> BTreeMap<String, V> makeStringMap();

    /**
     * creates map optimized for using zero or positive `Long` keys
     */
    <V> BTreeMap<Long, V> makeLongMap();
}
