package org.mapdb;

import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableSet;
import org.mapdb.impl.BTreeKeySerializer;

public interface BTreeSetMaker
{
    /**
     * nodeSize maximal size of node, larger node causes overflow and creation of new BTree node. Use large number for small keys, use small number for large keys.
     */
    BTreeSetMaker nodeSize( int nodeSize );

    /**
     * by default collection does not have counter, without counter updates are faster, but entire collection needs to be traversed to count items.
     */
    BTreeSetMaker counterEnable();

    /**
     * keySerializer used to convert keys into/from binary form.
     */
    BTreeSetMaker serializer( BTreeKeySerializer<?> serializer );

    /**
     * comparator used to sort keys.
     */
    BTreeSetMaker comparator( Comparator<?> comparator );

    BTreeSetMaker pumpSource( Iterator<?> source );

    /**
     * If source iteretor contains an duplicate key, exception is thrown.
     * This options will only use firts key and ignore any consequentive duplicates.
     */
    <K> BTreeSetMaker pumpIgnoreDuplicates();

    BTreeSetMaker pumpPresort( int batchSize );

    <K> NavigableSet<K> make();

    <K> NavigableSet<K> makeOrGet();

    /**
     * creates set optimized for using `String`
     */
    NavigableSet<String> makeStringSet();

    /**
     * creates set optimized for using zero or positive `Long`
     */
    NavigableSet<Long> makeLongSet();
}
