package org.mapdb.impl.binaryserializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import org.mapdb.ValueSerializer;

public class JavaValueSerializer implements ValueSerializer<Object>
{
    @Override
    public void serialize(DataOutput out, Object value) throws IOException
    {
        ObjectOutputStream out2 = new ObjectOutputStream((OutputStream) out);
        out2.writeObject(value);
        out2.flush();
    }

    @Override
    public Object deserialize(DataInput in, int available) throws IOException {
        try {
            ObjectInputStream in2 = new ObjectInputStream((InputStream) in);
            return in2.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException(e);
        }
    }

    @Override
    public int fixedSize() {
        return -1;
    }
}
