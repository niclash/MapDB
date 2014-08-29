package org.mapdb;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import org.junit.Test;
import org.mapdb.impl.UtilsTest;

import static org.junit.Assert.assertTrue;

public class Issue162Test
{

    public static class MyValue implements Serializable
    {
        private String string;

        public MyValue( String string )
        {
            this.string = string;
        }

        @Override
        public String toString()
        {
            return "MyValue{" + "string='" + string + '\'' + '}';
        }

        @Override
        public boolean equals( Object o )
        {
            if( this == o )
            {
                return true;
            }
            if( !( o instanceof MyValue ) )
            {
                return false;
            }

            MyValue myValue = (MyValue) o;
            if( !string.equals( myValue.string ) )
            {
                return false;
            }
            return true;
        }

        @Override
        public int hashCode()
        {
            return string.hashCode();
        }
    }

    public static class MyValueSerializer implements Serializable, ValueSerializer<MyValue>
    {

        @Override
        public void serialize( DataOutput out, MyValue value )
            throws IOException
        {
            assertTrue( value != null );
            System.out.println( "Custom serializer called with '" + value + "'" );
            out.writeUTF( value.string );
        }

        @Override
        public MyValue deserialize( DataInput in, int available )
            throws IOException
        {
            String s = in.readUTF();
            return new MyValue( s );
        }

        @Override
        public int fixedSize()
        {
            return -1;
        }
    }

    private static void printEntries( Map<Long, MyValue> map )
    {
        System.out.println( "Reading back data" );
        for( Map.Entry<Long, MyValue> entry : map.entrySet() )
        {
            System.out.println( "Entry id = " + entry.getKey() + ", contents = " + entry.getValue().toString() );
        }
    }

    File path = UtilsTest.tempDbFile();

    @Test
    public void testHashMap()
        throws IOException
    {
        System.out.println( "--- Testing HashMap with custom serializer" );

        DB db = DBMaker.newFileDB( path ).make();
        Map<Long, MyValue> map = db.createHashMap( "map" )
            .valueSerializer( new MyValueSerializer() )
            .make();
        db.commit();

        System.out.println( "Putting and committing data" );
        map.put( 1L, new MyValue( "one" ) );
        map.put( 2L, new MyValue( "two" ) );
        db.commit();

        System.out.println( "Closing and reopening db" );
        db.close();
        map = null;

        db = DBMaker.newFileDB( path ).make();
        map = db.getHashMap( "map" );

        printEntries( map );
    }

    @Test
    public void testBTreeMap()
        throws IOException
    {
        System.out.println( "--- Testing BTreeMap with custom serializer" );

        DB db = DBMaker.newFileDB( path ).make();
        Map<Long, MyValue> map = db.createTreeMap( "map" )
            .valueSerializer( new MyValueSerializer() )
            .make();
        db.commit();

        System.out.println( "Putting and committing data" );
        map.put( 1L, new MyValue( "one" ) );
        map.put( 2L, new MyValue( "two" ) );
        db.commit();

        System.out.println( "Closing and reopening db" );
        db.close();
        map = null;

        db = DBMaker.newFileDB( path ).make();
        map = db.getTreeMap( "map" );

        printEntries( map );
    }
}
