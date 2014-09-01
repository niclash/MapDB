package org.mapdb.impl.btree;

/**
 * if <code>valsOutsideNodes</code> is true, this class is used instead of values.
 * It contains reference to actual value. It also supports assertions from preventing it to leak outside of Map
 */
public final class BTreeValRef
{
    /**
     * reference to actual value
     */
    final long recid;

    public BTreeValRef( long recid )
    {
        this.recid = recid;
    }

    @Override
    public boolean equals( Object obj )
    {
        throw new IllegalAccessError();
    }

    @Override
    public int hashCode()
    {
        throw new IllegalAccessError();
    }

    @Override
    public String toString()
    {
        return "BTreeMap-ValRer[" + recid + "]";
    }
}
