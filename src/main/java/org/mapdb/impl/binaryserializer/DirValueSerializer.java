package org.mapdb.impl.binaryserializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.mapdb.ValueSerializer;
import org.mapdb.impl.DataInput2;
import org.mapdb.impl.DataOutput2;

public class DirValueSerializer implements ValueSerializer<long[][]>
{
    @Override
    public void serialize( DataOutput out, long[][] value )
        throws IOException
    {
        assert ( value.length == 16 );

        //first write mask which indicate subarray nullability
        int nulls = 0;
        for( int i = 0; i < 16; i++ )
        {
            if( value[ i ] != null )
            {
                for( long l : value[ i ] )
                {
                    if( l != 0 )
                    {
                        nulls |= 1 << i;
                        break;
                    }
                }
            }
        }
        out.writeShort( nulls );

        //write non null subarrays
        for( int i = 0; i < 16; i++ )
        {
            if( value[ i ] != null )
            {
                assert ( value[ i ].length == 8 );
                for( long l : value[ i ] )
                {
                    DataOutput2.packLong( out, l );
                }
            }
        }
    }

    @Override
    public long[][] deserialize( DataInput in )
        throws IOException
    {

        final long[][] ret = new long[ 16 ][];

        //there are 16  subarrays, each bite indicates if subarray is null
        int nulls = in.readUnsignedShort();
        for( int i = 0; i < 16; i++ )
        {
            if( ( nulls & 1 ) != 0 )
            {
                final long[] subarray = new long[ 8 ];
                for( int j = 0; j < 8; j++ )
                {
                    subarray[ j ] = DataInput2.unpackLong( in );
                }
                ret[ i ] = subarray;
            }
            nulls = nulls >>> 1;
        }

        return ret;
    }

    @Override
    public int fixedSize()
    {
        return -1;
    }
}
