package org.mapdb;

public interface SerializerFactory
{
    <T> KeySerializer<T> createKeySerializer( String structureName, Class<T> type );

    <T> ValueSerializer<T> createValueSerializer( String structureName, Class<T> type );
}
