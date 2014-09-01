package org.mapdb.impl.btree;

/**
 * common interface for BTree node
 */
public interface BTreeNode<K,V>
{
    boolean isLeaf();

    K[] keys();

    V[] vals();

    K highKey();

    long[] child();

    long next();
}
