package org.mapdb;

public interface SerializerFactory
{
    <T> KeySerializer<T> createKeySerializer( Class<T> type );

    <T> ValueSerializer<T> createValueSerializer( Class<T> type );
}
