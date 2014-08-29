package org.mapdb;

import examples.Bidi_Map;
import examples.CacheEntryExpiry;
import examples.Compression;
import examples.Custom_Value;
import examples.Histogram;
import examples.Huge_Insert;
import examples.Lazily_Loaded_Records;
import examples.Map_Size_Counter;
import examples.MultiMap;
import examples.SQL_Auto_Incremental_Unique_Key;
import examples.Secondary_Key;
import examples.Secondary_Map;
import examples.Secondary_Values;
import examples.Transactions;
import examples.Transactions2;
import examples.TreeMap_Composite_Key;
import examples._HelloWorld;
import examples._TempMap;
import java.io.IOException;
import org.junit.Test;
import org.mapdb.impl.Pump_InMemory_Import_Then_Save_To_Disk;

public class ExamplesTest
{

    static final String[] args = new String[ 0 ];

    @Test
    public void _HelloWorld()
        throws IOException
    {
        _HelloWorld.main( args );
    }

    @Test
    public void _TempMap()
        throws IOException
    {
        _TempMap.main( args );
    }

    @Test
    public void Cache()
    {
        CacheEntryExpiry.main( args );
    }

    @Test
    public void Compression()
    {
        Compression.main( args );
    }

    @Test
    public void Huge_Insert()
        throws IOException
    {
        Huge_Insert.main( args );
    }

    @Test
    public void Custom_Value()
        throws IOException
    {
        Custom_Value.main( args );
    }

    @Test
    public void Bidi_Map()
    {
        Bidi_Map.main( args );
    }

    @Test
    public void Histogram()
    {
        Histogram.main( args );
    }

    @Test
    public void Lazily_Loaded_Records()
        throws IOException
    {
        Lazily_Loaded_Records.main( args );
    }

    @Test
    public void Map_Size_Counter()
    {
        Map_Size_Counter.main( args );
    }

    @Test
    public void MultiMap()
        throws IOException
    {
        MultiMap.main( args );
    }

    @Test
    public void Secondary_Key()
    {
        Secondary_Key.main( args );
    }

    @Test
    public void Secondary_Map()
    {
        Secondary_Map.main( args );
    }

    @Test
    public void Secondary_Values()
    {
        Secondary_Values.main( args );
    }

    @Test
    public void SQL_Auto_Incremental_Unique_Key()
    {
        SQL_Auto_Incremental_Unique_Key.main( args );
    }

    @Test
    public void Transactions()
        throws IOException
    {
        Transactions.main( args );
    }

    @Test
    public void Transactions2()
        throws IOException
    {
        Transactions2.main( args );
    }

    @Test
    public void TreeMap_Composite_Key()
    {
        TreeMap_Composite_Key.main( args );
    }

    @Test
    public void Pump_InMemory_Import_Than_Save_To_Disk()
        throws IOException
    {
        Pump_InMemory_Import_Then_Save_To_Disk.main( args );
    }
}
