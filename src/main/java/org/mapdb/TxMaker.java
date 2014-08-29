package org.mapdb;

import java.io.Closeable;
import org.mapdb.impl.Fun;

public interface TxMaker extends Closeable
{
    DB makeTx();

    void close();

    /**
     * Executes given block within a single transaction.
     * If block throws {@code TxRollbackException} execution is repeated until it does not fail.
     *
     * @param txBlock
     */
    void execute( TxBlock txBlock );

    /**
     * Executes given block withing single transaction.
     * If block throws {@code TxRollbackException} execution is repeated until it does not fail.
     *
     * This method returns result returned by txBlock.
     *
     * @param txBlock
     */
    <A> A execute( Fun.Function1<A, DB> txBlock );
}
