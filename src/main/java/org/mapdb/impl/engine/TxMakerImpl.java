/*
 *  Copyright (c) 2012 Jan Kotek
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.mapdb.impl.engine;

import java.io.IOException;
import org.mapdb.DB;
import org.mapdb.Engine;
import org.mapdb.TxBlock;
import org.mapdb.TxMaker;
import org.mapdb.TxRollbackException;
import org.mapdb.impl.Fun;

/**
 * Transaction factory
 *
 * @author Jan Kotek
 */
public class TxMakerImpl implements TxMaker
{

    /**
     * marker for deleted records
     */
    protected static final Object DELETED = new Object();
    private final boolean txSnapshotsEnabled;
    private final boolean strictDBGet;

    /**
     * parent engine under which modifications are stored
     */
    protected Engine engine;

    public TxMakerImpl( Engine engine )
    {
        this( engine, false, false );
    }

    public TxMakerImpl( Engine engine, boolean strictDBGet, boolean txSnapshotsEnabled )
    {
        if( engine == null )
        {
            throw new IllegalArgumentException();
        }
        if( !engine.canSnapshot() )
        {
            throw new IllegalArgumentException( "Snapshot must be enabled for TxMaker" );
        }
        if( engine.isReadOnly() )
        {
            throw new IllegalArgumentException( "TxMaker can not be used with read-only Engine" );
        }
        this.engine = engine;
        this.strictDBGet = strictDBGet;
        this.txSnapshotsEnabled = txSnapshotsEnabled;
    }

    @Override
    public DB makeTx()
    {
        Engine snapshot = engine.snapshot();
        if( txSnapshotsEnabled )
        {
            snapshot = new TxEngine( snapshot, false );
        }
        return new DbImpl( snapshot, strictDBGet, false );
    }

    @Override
    public void close()
    {
        engine.close();
        engine = null;
    }

    /**
     * Executes given block within a single transaction.
     * If block throws {@code TxRollbackException} execution is repeated until it does not fail.
     *
     * @param txBlock
     */
    @Override
    public void execute( TxBlock txBlock )
    {
        for(; ; )
        {
            DB tx = makeTx();
            try
            {
                txBlock.tx( tx );
                if( !tx.isClosed() )
                {
                    tx.commit();
                }
                return;
            }
            catch( TxRollbackException e )
            {
                //failed, so try again
                close( tx );
            }
        }
    }

    /**
     * Executes given block withing single transaction.
     * If block throws {@code TxRollbackException} execution is repeated until it does not fail.
     *
     * This method returns result returned by txBlock.
     *
     * @param txBlock
     */
    @Override
    public <A> A execute( Fun.Function1<A, DB> txBlock )
    {
        for(; ; )
        {
            DB tx = makeTx();
            try
            {
                A a = txBlock.run( tx );
                if( !tx.isClosed() )
                {
                    tx.commit();
                }
                return a;
            }
            catch( TxRollbackException e )
            {
                //failed, so try again
                close(tx);
            }
        }
    }

    private void close( DB tx )
    {
        if( !tx.isClosed() )
        {
            try
            {
                tx.close();
            }
            catch( IOException e )
            {
                // IGNORE
            }
        }
    }
}
