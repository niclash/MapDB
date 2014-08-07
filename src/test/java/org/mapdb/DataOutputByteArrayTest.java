package org.mapdb;

import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class DataOutputByteArrayTest {

    //TODO more tests here for compability between DataOutput2 and other DataInputs

    DataIO.DataOutputByteArray out = new DataIO.DataOutputByteArray();

    DataIO.DataInputByteBuffer in(){
        return new DataIO.DataInputByteBuffer(out.buf);
    }

    @Test
    public void testWriteFloat() throws Exception {
        float f = 12.1239012093e-19F;
        out.writeFloat(f);
        DataIO.DataInputByteBuffer in = in();
        assertEquals(Float.floatToIntBits(f),Float.floatToIntBits(in.readFloat()));
        assertEquals(4,in.pos);
    }

    @Test
    public void testWriteDouble() throws Exception {
        double f = 12.123933423523012093e-199;
        out.writeDouble(f);
        DataIO.DataInputByteBuffer in = in();
        assertEquals(Double.doubleToLongBits(f),Double.doubleToLongBits(in.readDouble()));
        assertEquals(8,in.pos);
    }
}
