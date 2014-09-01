package org.mapdb.impl.binaryserializer;

import org.mapdb.ValueSerializer;

public interface InternalSerializers
{
    /**
     * Serializes strings using UTF8 encoding.
     * Stores string size so can be used as collection serializer.
     * Does not handle null values
     */
    ValueSerializer<String> STRING = new StringValueSerializer();

    /**
     * Serializes strings using UTF8 encoding.
     * Deserialized String is interned {@link String#intern()},
     * so it could save some memory.
     *
     * Stores string size so can be used as collection serializer.
     * Does not handle null values
     */
    ValueSerializer<String> STRING_INTERN = new StringInternValueSerializer();

    /**
     * Serializes strings using ASCII encoding (8 bit character).
     * Is faster compared to UTF8 encoding.
     * Stores string size so can be used as collection serializer.
     * Does not handle null values
     */
    ValueSerializer<String> STRING_ASCII = new StringAsciiValueSerializer();

    /**
     * Serializes strings using UTF8 encoding.
     * Used mainly for testing.
     * Does not handle null values.
     */
    ValueSerializer<String> STRING_NOSIZE = new StringNoSizeValueSerializer();

    /**
     * Serializes Long into 8 bytes, used mainly for testing.
     * Does not handle null values.
     */
    ValueSerializer<Long> LONG = new LongValueSerializer();

    /**
     * Serializes Integer into 4 bytes.
     * Does not handle null values.
     */
    ValueSerializer<Integer> INTEGER = new IntegerValueSerializer();

    ValueSerializer<Boolean> BOOLEAN = new BooleanValueSerializer();

    /**
     * Always throws {@link IllegalAccessError} when invoked. Useful for testing and assertions.
     */
    ValueSerializer<Object> ILLEGAL_ACCESS = new IllegalAccessValueSerializer();

    /**
     * Basic serializer for most classes in 'java.lang' and 'java.util' packages.
     * It does not handle custom POJO classes. It also does not handle classes which
     * require access to `DB` itself.
     */
    @SuppressWarnings( "unchecked" )
    ValueSerializer<Object> BASIC = new SerializerBase();

    /**
     * Serializes `byte[]` it adds header which contains size information
     */
    ValueSerializer<byte[]> BYTE_ARRAY = new ByteValueSerializer();

    /**
     * Serializes `byte[]` directly into underlying store
     * It does not store size, so it can not be used in Maps and other collections.
     */
    ValueSerializer<byte[]> BYTE_ARRAY_NOSIZE = new ByteArrayValueSerializer();

    /**
     * Serializes `char[]` it adds header which contains size information
     */
    ValueSerializer<char[]> CHAR_ARRAY = new CharArrayValueSerializer();

    /**
     * Serializes `int[]` it adds header which contains size information
     */
    ValueSerializer<int[]> INT_ARRAY = new IntArrayValueSerializer();

    /**
     * Serializes `long[]` it adds header which contains size information
     */
    ValueSerializer<long[]> LONG_ARRAY = new LongArrayValueSerializer();

    /**
     * Serializes `double[]` it adds header which contains size information
     */
    ValueSerializer<double[]> DOUBLE_ARRAY = new DoubleArrayValueSerializer();

    /**
     * Serializer which uses standard Java Serialization with {@link java.io.ObjectInputStream} and {@link java.io.ObjectOutputStream}
     */
    ValueSerializer<Object> JAVA = new JavaValueSerializer();

    /**
     * Serializers {@link java.util.UUID} class
     */
    ValueSerializer<java.util.UUID> UUID = new UuidValueSerializer();
}
