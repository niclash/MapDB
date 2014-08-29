package org.mapdb.impl;

public class StoreHeapTest extends EngineTest<StoreHeap>
{

    @Override
    protected StoreHeap openEngine()
    {
        return new StoreHeap();
    }

    @Override
    boolean canReopen()
    {
        return false;
    }

    @Override
    boolean canRollback()
    {
        return false;
    }
}
