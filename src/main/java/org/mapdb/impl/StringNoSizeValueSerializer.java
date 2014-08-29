package org.mapdb.impl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.Charset;
import org.mapdb.ValueSerializer;

public class StringNoSizeValueSerializer implements ValueSerializer<String>
{

    private final Charset UTF8_CHARSET = Charset.forName("UTF8");

    @Override
    public void serialize(DataOutput out, String value) throws IOException
    {
        final byte[] bytes = value.getBytes(UTF8_CHARSET);
        out.write(bytes);
    }

    @Override
    public String deserialize(DataInput in, int available) throws IOException {
        if(available==-1) throw new IllegalArgumentException("STRING_NOSIZE does not work with collections.");
        byte[] bytes = new byte[available];
        in.readFully(bytes);
        return new String(bytes, UTF8_CHARSET);
    }

    @Override
    public int fixedSize() {
        return -1;
    }
}
