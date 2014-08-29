package examples;

import java.util.Map;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.ValueSerializer;
import org.mapdb.impl.CompressionWrapper;

/**
 * Demonstrates how-to apply compression in various modes.
 * <p/>
 * MapDB uses LZF compression, there is discussion to support other compression alghorithms
 */
public class Compression
{

    public static void main( String[] args )
    {
        /*
         * first case, just enable storage wide compression for all records.
         */
        DB db = DBMaker.newMemoryDB()
            .compressionEnable() //this settings enables compression
            .make();
        //and now create and use map as usual
        Map map = db.getTreeMap( "test" );
        map.put( "some", "stuff" );



        /*
         * Other option is to use compression only for specific part. For example if
         * you have large values, you may want to compress them. It may make sense
         * not to compress BTree Nodes and Keys.
         */
        DB db2 = DBMaker.newMemoryDB().make(); //no store wide compression this time

        //construct value serializier, use default serializier
        ValueSerializer valueSerializer = db2.getDefaultSerializer();
        //but wrap it, to compress its output
        valueSerializer = new CompressionWrapper( valueSerializer );

        //now construct map, with additional options
        Map map2 = db2.createTreeMap( "test" )
            .valuesOutsideNodesEnable() // store values outside of BTree Nodes. Faster reads if values are large.
            .valueSerializer( valueSerializer ) //set our value serializer.
            .make();

        map2.put( "some", "stuff" );
    }
}
