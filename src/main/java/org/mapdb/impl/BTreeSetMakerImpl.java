package org.mapdb.impl;

import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableSet;
import org.mapdb.BTreeSetMaker;
import org.mapdb.KeySerializer;

public class BTreeSetMakerImpl implements org.mapdb.BTreeSetMaker
{
    private DbImpl db;
    protected final String name;

    public BTreeSetMakerImpl( DbImpl db, String name )
    {
        this.db = db;
        this.name = name;
    }

    protected int nodeSize = 32;
    protected boolean counter = false;
    protected KeySerializer<?> serializer;
    protected Comparator<?> comparator;

    protected Iterator<?> pumpSource;
    protected int pumpPresortBatchSize = -1;
    protected boolean pumpIgnoreDuplicates = false;

    /**
     * nodeSize maximal size of node, larger node causes overflow and creation of new BTree node. Use large number for small keys, use small number for large keys.
     */
    @Override
    public BTreeSetMaker nodeSize( int nodeSize )
    {
        this.nodeSize = nodeSize;
        return this;
    }

    /**
     * by default collection does not have counter, without counter updates are faster, but entire collection needs to be traversed to count items.
     */
    @Override
    public BTreeSetMaker counterEnable()
    {
        this.counter = true;
        return this;
    }

    /**
     * keySerializer used to convert keys into/from binary form.
     */
    @Override
    public BTreeSetMaker serializer( BTreeKeySerializer<?> serializer )
    {
        this.serializer = serializer;
        return this;
    }

    /**
     * comparator used to sort keys.
     */
    @Override
    public BTreeSetMaker comparator( Comparator<?> comparator )
    {
        this.comparator = comparator;
        return this;
    }

    @Override
    public BTreeSetMaker pumpSource( Iterator<?> source )
    {
        this.pumpSource = source;
        return this;
    }

    /**
     * If source iteretor contains an duplicate key, exception is thrown.
     * This options will only use firts key and ignore any consequentive duplicates.
     */
    @Override
    public <K> BTreeSetMaker pumpIgnoreDuplicates()
    {
        this.pumpIgnoreDuplicates = true;
        return this;
    }

    @Override
    public BTreeSetMaker pumpPresort( int batchSize )
    {
        this.pumpPresortBatchSize = batchSize;
        return this;
    }

    @Override
    public <K> NavigableSet<K> make()
    {
        return db.createTreeSet( BTreeSetMakerImpl.this );
    }

    @Override
    public <K> NavigableSet<K> makeOrGet()
    {
        synchronized( db )
        {
            //TODO add parameter check
            return (NavigableSet<K>) ( db.catGet( name + ".type" ) == null ?
                                       make() : db.getTreeSet( name ) );
        }
    }

    /**
     * creates set optimized for using `String`
     */
    @Override
    public NavigableSet<String> makeStringSet()
    {
        serializer = BTreeKeySerializer.STRING;
        return make();
    }

    /**
     * creates set optimized for using zero or positive `Long`
     */
    @Override
    public NavigableSet<Long> makeLongSet()
    {
        serializer = BTreeKeySerializer.ZERO_OR_POSITIVE_LONG;
        return make();
    }
}
