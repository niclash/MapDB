package org.mapdb.impl.btree;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.mapdb.KeySerializer;
import org.mapdb.ValueSerializer;
import org.mapdb.impl.CC;
import org.mapdb.impl.DataInput2;
import org.mapdb.impl.DataOutput2;
import org.mapdb.impl.binaryserializer.SerializerBase;

public final class BTreeNodeSerializer<A, B> implements ValueSerializer<BTreeNode<A,B>>
{

    protected final boolean hasValues;
    protected final boolean valsOutsideNodes;
    protected final KeySerializer<A> keySerializer;
    protected final ValueSerializer<B> valueSerializer;
    protected final int numberOfNodeMetas;

    public BTreeNodeSerializer( boolean valsOutsideNodes,
                                KeySerializer<A> keySerializer,
                                ValueSerializer<B> valueSerializer,
                                int numberOfNodeMetas
    )
    {
        assert ( keySerializer != null );
        this.hasValues = valueSerializer != null;
        this.valsOutsideNodes = valsOutsideNodes;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.numberOfNodeMetas = numberOfNodeMetas;
    }

    @Override
    public void serialize( DataOutput out, BTreeNode<A,B> value )
        throws IOException
    {
        final boolean isLeaf = value.isLeaf();

        //first byte encodes if is leaf (first bite) and length (last seven bites)
        assert ( value.keys().length <= 255 );
        assert ( !( !isLeaf && value.child().length != value.keys().length ) );
        assert ( !( isLeaf && hasValues && value.vals().length != value.keys().length - 2 ) );
        assert ( !( !isLeaf && value.highKey() != null && value.child()[ value.child().length - 1 ] == 0 ) );

        //check node integrity in paranoid mode
        if( CC.PARANOID )
        {
            int len = value.keys().length;
            for( int i = value.keys()[ 0 ] == null ? 2 : 1;
                 i < ( value.keys()[ len - 1 ] == null ? len - 1 : len );
                 i++ )
            {
                int comp = keySerializer.getComparator().compare( value.keys()[ i - 1 ], value.keys()[ i ] );
                int limit = i == len - 1 ? 1 : 0;
                if( comp >= limit )
                {
                    throw new AssertionError( "BTreeNode format error, wrong key order at #" + i + "\n" + value );
                }
            }
        }

        final boolean left = value.keys()[ 0 ] == null;
        final boolean right = value.keys()[ value.keys().length - 1 ] == null;

        final int header = makeHeader( isLeaf, left, right );

        out.write( header );
        out.write( value.keys().length );

        //write node metas, right now this is ignored, but in future it could be used for counted btrees or aggregations
        for( int i = 0; i < numberOfNodeMetas; i++ )
        {
            DataOutput2.packLong( out, 0 );
        }

        //longs go first, so it is possible to reconstruct tree without serializer
        if( isLeaf )
        {
            DataOutput2.packLong( out, ( (BTreeLeafNode) value ).next );
        }
        else
        {
            for( long child : ( (BTreeDirNode) value ).child )
            {
                DataOutput2.packLong( out, child );
            }
        }

        keySerializer.serialize( out, left ? 1 : 0,
                                 right ? value.keys().length - 1 : value.keys().length,
                                 value.keys() );

        if( isLeaf )
        {
            if( hasValues )
            {
                for( B val : value.vals() )
                {
                    assert ( val != null );
                    if( valsOutsideNodes )
                    {
                        long recid = ( (BTreeValRef) val ).recid;
                        DataOutput2.packLong( out, recid );
                    }
                    else
                    {
                        valueSerializer.serialize( out, val );
                    }
                }
            }
            else
            {
                //write bits if values are null
                boolean[] bools = new boolean[ value.vals().length ];
                for( int i = 0; i < bools.length; i++ )
                {
                    bools[ i ] = value.vals()[ i ] != null;
                }
                //pack
                byte[] bb = SerializerBase.booleanToByteArray( bools );
                out.write( bb );
            }
        }
    }

    private int makeHeader( final boolean isLeaf, final boolean left, final boolean right )
    {
        if( isLeaf )
        {
            if( right )
            {
                if( left )
                {
                    return BTreeMapImpl.B_TREE_NODE_LEAF_LR;
                }
                else
                {
                    return BTreeMapImpl.B_TREE_NODE_LEAF_R;
                }
            }
            else
            {
                if( left )
                {
                    return BTreeMapImpl.B_TREE_NODE_LEAF_L;
                }
                else
                {
                    return BTreeMapImpl.B_TREE_NODE_LEAF_C;
                }
            }
        }
        else
        {
            if( right )
            {
                if( left )
                {
                    return BTreeMapImpl.B_TREE_NODE_DIR_LR;
                }
                else
                {
                    return BTreeMapImpl.B_TREE_NODE_DIR_R;
                }
            }
            else
            {
                if( left )
                {
                    return BTreeMapImpl.B_TREE_NODE_DIR_L;
                }
                else
                {
                    return BTreeMapImpl.B_TREE_NODE_DIR_C;
                }
            }
        }
    }

    @Override
    public BTreeNode<A,B> deserialize( DataInput in )
        throws IOException
    {
        final int header = in.readUnsignedByte();
        final int size = in.readUnsignedByte();

        //read node metas, right now this is ignored, but in future it could be used for counted btrees or aggregations
        for( int i = 0; i < numberOfNodeMetas; i++ )
        {
            DataInput2.unpackLong( in );
        }

        //first bite indicates leaf
        final boolean isLeaf =
            header == BTreeMapImpl.B_TREE_NODE_LEAF_C || header == BTreeMapImpl.B_TREE_NODE_LEAF_L ||
            header == BTreeMapImpl.B_TREE_NODE_LEAF_LR || header == BTreeMapImpl.B_TREE_NODE_LEAF_R;
        final int start =
            ( header == BTreeMapImpl.B_TREE_NODE_LEAF_L || header == BTreeMapImpl.B_TREE_NODE_LEAF_LR || header == BTreeMapImpl.B_TREE_NODE_DIR_L || header == BTreeMapImpl.B_TREE_NODE_DIR_LR ) ?
            1 : 0;

        final int end =
            ( header == BTreeMapImpl.B_TREE_NODE_LEAF_R || header == BTreeMapImpl.B_TREE_NODE_LEAF_LR || header == BTreeMapImpl.B_TREE_NODE_DIR_R || header == BTreeMapImpl.B_TREE_NODE_DIR_LR ) ?
            size - 1 : size;

        if( isLeaf )
        {
            return deserializeLeaf( in, size, start, end );
        }
        else
        {
            return deserializeDir( in, size, start, end );
        }
    }

    private BTreeNode<A,B> deserializeDir( final DataInput in, final int size, final int start, final int end )
        throws IOException
    {
        final long[] child = new long[ size ];
        for( int i = 0; i < size; i++ )
        {
            child[ i ] = DataInput2.unpackLong( in );
        }
        final A[] keys = keySerializer.deserialize( in, start, end, size );
        assert ( keys.length == size );
        return new BTreeDirNode<A,B>( keys, child );
    }

    private BTreeNode<A,B> deserializeLeaf( final DataInput in, final int size, final int start, final int end )
        throws IOException
    {
        final long next = DataInput2.unpackLong( in );
        final Object[] keys = keySerializer.deserialize( in, start, end, size );
        assert ( keys.length == size );
        Object[] vals = new Object[ size - 2 ];
        if( hasValues )
        {
            for( int i = 0; i < vals.length; i++ )
            {
                if( valsOutsideNodes )
                {
                    long recid = DataInput2.unpackLong( in );
                    vals[ i ] = recid == 0 ? null : new BTreeValRef( recid );
                }
                else
                {
                    vals[ i ] = valueSerializer.deserialize( in );
                }
            }
        }
        else
        {
            //restore values which were deleted
            boolean[] bools = SerializerBase.readBooleanArray( vals.length, in );
            for( int i = 0; i < bools.length; i++ )
            {
                if( bools[ i ] )
                {
                    vals[ i ] = BTreeMapImpl.EMPTY;
                }
            }
        }
        return new BTreeLeafNode( keys, vals, next );
    }

    @Override
    public int fixedSize()
    {
        return -1;
    }
}
