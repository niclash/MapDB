package org.mapdb.impl;

import org.mapdb.DBBuilder;

public class DbFactoryImpl implements org.mapdb.DBFactory
{
    @Override
    public DBBuilder createBuilder()
    {
        return new DbBuilderImpl();
    }
}
