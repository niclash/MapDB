package org.mapdb.impl.queue;

import org.mapdb.Engine;
import org.mapdb.ValueSerializer;

/**
 * Last in first out lock-free queue
 *
 * @param <E>
 */
public class Stack<E> extends SimpleQueue<E> {



    public Stack( Engine engine, ValueSerializer<E> serializer, long headerRecidRef, boolean useLocks ) {
        super(engine, serializer, headerRecidRef, useLocks);
    }

    @Override
    public boolean add(E e) {
        long head2 = head.get();
        Node<E> n = new Node<E>(head2, e);
        long recid = engine.put(n, nodeSerializer);
        while(!head.compareAndSet(head2, recid)){
            //failed to update head, so read new value and start over
            head2 = head.get();
            n = new Node<E>(head2, e);
            engine.update(recid, n, nodeSerializer);
        }
        return true;
    }
}
