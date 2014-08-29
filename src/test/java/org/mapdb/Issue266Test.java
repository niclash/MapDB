package org.mapdb;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;
import org.mapdb.impl.UtilsTest;

import static org.junit.Assert.assertEquals;

enum AdvancedEnum
{
    A()
        {
            @Override
            public void dummy()
            {
                System.out.println( "dummy1" );
            }
        },
    B()
        {
            @Override
            public void dummy()
            {
                System.out.println( "dummy2" );
            }
        },
    C()
        {
            @Override
            public void dummy()
            {
                System.out.println( "dummy3" );
            }
        };

    public abstract void dummy();

}

public class Issue266Test
{
    @Test
    public void testEnum()
        throws IOException
    {

        File f = File.createTempFile( "mapdb", "asdas" );
        DB db = DBMaker.newFileDB( f ).make();

        AdvancedEnum testEnumValue = AdvancedEnum.C;

        Set<Object> set = db.createTreeSet( "set" ).makeOrGet();
        set.clear();

        set.add( testEnumValue );
        db.commit();

        db.close();

        db = DBMaker.newFileDB( f ).make();

        set = db.createTreeSet( "set" ).makeOrGet();
        AdvancedEnum enumValue = (AdvancedEnum) set.iterator().next();

        Assert.assertNotNull( enumValue );

        assertEquals( "Invalid Enum.name()", enumValue.name(), testEnumValue.name() );
        assertEquals( "Invalid Enum.ordinal()", enumValue.ordinal(), testEnumValue.ordinal() );
    }

    @Test
    public void testEnum2()
    {
        assertEquals( AdvancedEnum.A, AdvancedEnum.class.getEnumConstants()[ 0 ] );

        DB db = DBMaker.newMemoryDB().make();
        AdvancedEnum a = (AdvancedEnum) UtilsTest.clone( AdvancedEnum.A, db.getDefaultSerializer() );
        assertEquals( a.toString(), AdvancedEnum.A.toString() );
        assertEquals( a.ordinal(), AdvancedEnum.A.ordinal() );
    }
}