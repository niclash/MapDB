package org.mapdb.impl.queue;

import org.mapdb.Atomic;
import org.mapdb.Engine;
import org.mapdb.ValueSerializer;

/**
 * First in first out lock-free queue
 *
 * @param <E>
 */
public class FifoQueue<E> extends SimpleQueue<E> {

    protected final Atomic.Long tail;

    public FifoQueue( Engine engine, ValueSerializer<E> serializer, long headerRecid,
                      long nextTailRecid, boolean useLocks
    ) {
        super(engine, serializer,headerRecid,useLocks);
        tail = new Atomic.Long(engine,nextTailRecid);
    }

    @Override
    public boolean add(E e) {
        long nextTail = engine.put((Node<E>) Node.EMPTY,nodeSerializer);
        long tail2 = tail.get();
        while(!tail.compareAndSet(tail2,nextTail)){
            tail2 = tail.get();
        }
        //now we have tail2 just for us
        Node<E> n = new Node(nextTail,e);
        engine.update(tail2,n,nodeSerializer);
        return true;
    }



}
