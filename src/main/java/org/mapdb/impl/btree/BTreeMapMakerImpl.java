package org.mapdb.impl.btree;

import java.util.Comparator;
import java.util.Iterator;
import org.mapdb.BTreeMap;
import org.mapdb.BTreeMapMaker;
import org.mapdb.KeySerializer;
import org.mapdb.ValueSerializer;
import org.mapdb.impl.engine.DbImpl;
import org.mapdb.impl.Fun;
import org.mapdb.impl.binaryserializer.BTreeKeySerializer;

public class BTreeMapMakerImpl
    implements BTreeMapMaker
{
    private DbImpl db;
    public final String name;

    public BTreeMapMakerImpl( DbImpl db, String name )
    {
        this.db = db;
        this.name = name;
    }

    public int nodeSize = 32;
    public boolean valuesOutsideNodes = false;
    public boolean counter = false;
    public KeySerializer keySerializer;
    public ValueSerializer valueSerializer;
    public Comparator comparator;

    public Iterator pumpSource;
    public Fun.Function1 pumpKeyExtractor;
    public Fun.Function1 pumpValueExtractor;
    public int pumpPresortBatchSize = -1;
    public boolean pumpIgnoreDuplicates = false;

    /**
     * nodeSize maximal size of node, larger node causes overflow and creation of new BTree node. Use large number for small keys, use small number for large keys.
     */
    @Override
    public BTreeMapMaker nodeSize( int nodeSize )
    {
        this.nodeSize = nodeSize;
        return this;
    }

    /**
     * by default values are stored inside BTree Nodes. Large values should be stored outside of BTreeNodes
     */
    @Override
    public BTreeMapMaker valuesOutsideNodesEnable()
    {
        this.valuesOutsideNodes = true;
        return this;
    }

    /**
     * by default collection does not have counter, without counter updates are faster, but entire collection needs to be traversed to count items.
     */
    @Override
    public BTreeMapMaker counterEnable()
    {
        this.counter = true;
        return this;
    }

    /**
     * keySerializer used to convert keys into/from binary form.
     */
    @Override
    public BTreeMapMaker keySerializer( KeySerializer<?> keySerializer )
    {
        this.keySerializer = keySerializer;
        return this;
    }

    /**
     * keySerializer used to convert keys into/from binary form.
     * This wraps ordinary serializer, with no delta packing used
     */
    @Override
    public BTreeMapMaker keySerializerWrap( ValueSerializer<?> serializer )
    {
        this.keySerializer = new BTreeKeySerializer.BasicKeySerializer( serializer );
        return this;
    }

    /**
     * valueSerializer used to convert values into/from binary form.
     */
    @Override
    public BTreeMapMaker valueSerializer( ValueSerializer<?> valueSerializer )
    {
        this.valueSerializer = valueSerializer;
        return this;
    }

    /**
     * comparator used to sort keys.
     */
    @Override
    public BTreeMapMaker comparator( Comparator<?> comparator )
    {
        this.comparator = comparator;
        return this;
    }

    @Override
    public <K, V> BTreeMapMaker pumpSource( Iterator<K> keysSource, Fun.Function1<V, K> valueExtractor )
    {
        this.pumpSource = keysSource;
        this.pumpKeyExtractor = Fun.extractNoTransform();
        this.pumpValueExtractor = valueExtractor;
        return this;
    }

    @Override
    public <K, V> BTreeMapMaker pumpSource( Iterator<Fun.Tuple2<K, V>> entriesSource )
    {
        this.pumpSource = entriesSource;
        this.pumpKeyExtractor = Fun.extractKey();
        this.pumpValueExtractor = Fun.extractValue();
        return this;
    }

    @Override
    public BTreeMapMaker pumpPresort( int batchSize )
    {
        this.pumpPresortBatchSize = batchSize;
        return this;
    }

    /**
     * If source iteretor contains an duplicate key, exception is thrown.
     * This options will only use firts key and ignore any consequentive duplicates.
     */
    @Override
    public <K> BTreeMapMaker pumpIgnoreDuplicates()
    {
        this.pumpIgnoreDuplicates = true;
        return this;
    }

    @Override
    public <K, V> BTreeMap<K, V> make()
    {
        return db.createTreeMap( BTreeMapMakerImpl.this );
    }

    @Override
    public <K, V> BTreeMap<K, V> makeOrGet()
    {
        synchronized( db )
        {
            //TODO add parameter check
            return (BTreeMap<K, V>) ( db.catGet( name + ".type" ) == null ?
                                      make() : db.getTreeMap( name ) );
        }
    }

    /**
     * creates map optimized for using `String` keys
     */
    @Override
    public <V> BTreeMap<String, V> makeStringMap()
    {
        keySerializer = BTreeKeySerializer.STRING;
        return make();
    }

    /**
     * creates map optimized for using zero or positive `Long` keys
     */
    @Override
    public <V> BTreeMap<Long, V> makeLongMap()
    {
        keySerializer = BTreeKeySerializer.ZERO_OR_POSITIVE_LONG;
        return make();
    }
}
