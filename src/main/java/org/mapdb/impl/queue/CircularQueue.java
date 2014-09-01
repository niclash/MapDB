package org.mapdb.impl.queue;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.mapdb.Atomic;
import org.mapdb.Engine;
import org.mapdb.ValueSerializer;
import org.mapdb.impl.CC;

public class CircularQueue<E> extends SimpleQueue<E> {

    protected final Atomic.Long headInsert;
    //TODO is there a way to implement this without global locks?
    protected final Lock lock = new ReentrantLock( CC.FAIR_LOCKS);
    protected final long size;

    public CircularQueue( Engine engine,
                          ValueSerializer<E> serializer,
                          long headRecid,
                          long headInsertRecid,
                          long size
    ) {
        super(engine, serializer, headRecid,false);
        headInsert = new Atomic.Long(engine, headInsertRecid);
        this.size = size;
    }

    @Override
    public boolean add(Object o) {
        lock.lock();
        try{
            long nRecid = headInsert.get();
            Node<E> n = engine.get(nRecid, nodeSerializer);
            n = new Node<E>(n.next, (E) o);
            engine.update(nRecid, n, nodeSerializer);
            headInsert.set(n.next);
            //move 'poll' head if it points to currently replaced item
            head.compareAndSet(nRecid, n.next);
            return true;
        }finally {
            lock.unlock();
        }
    }

    @Override
    public void clear() {
        // praise locking
        lock.lock();
        try {
            for (int i = 0; i < size; i++) {
                poll();
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public E poll() {
        lock.lock();
        try{
            long nRecid = head.get();
            Node<E> n = engine.get(nRecid, nodeSerializer);
            engine.update(nRecid, new Node<E>(n.next, null), nodeSerializer);
            head.set(n.next);
            return n.value;
        }finally {
            lock.unlock();
        }
    }

    @Override
    public E peek() {
        lock.lock();
        try{
            long nRecid = head.get();
            Node<E> n = engine.get(nRecid, nodeSerializer);
            return n.value;
        }finally {
            lock.unlock();
        }
    }

}
