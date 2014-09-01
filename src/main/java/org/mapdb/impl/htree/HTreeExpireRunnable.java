package org.mapdb.impl.htree;

import java.lang.ref.WeakReference;
import java.util.logging.Level;
import org.mapdb.impl.CC;
import org.mapdb.impl.Store;

class HTreeExpireRunnable implements Runnable
{

    //use weak referece to prevent memory leak
    final WeakReference<HTreeMapImpl> mapRef;

    public HTreeExpireRunnable( HTreeMapImpl map )
    {
        this.mapRef = new WeakReference<HTreeMapImpl>( map );
    }

    @Override
    public void run()
    {
        if( CC.LOG_HTREEMAP && HTreeMapImpl.LOG.isLoggable( Level.FINE ) )
        {
            HTreeMapImpl.LOG.log( Level.FINE, "HTreeMap expirator thread started" );
        }
        boolean pause = false;
        try
        {
            while( true )
            {

                if( pause )
                {
                    Thread.sleep( 1000 );
                }

                HTreeMapImpl map = mapRef.get();
                if( map == null || map.engine.isClosed() || map.closeLatch.getCount() < 2 )
                {
                    return;
                }

                //TODO what if store gets closed while working on this?
                map.expirePurge();

                if( map.engine.isClosed() || map.closeLatch.getCount() < 2 )
                {
                    return;
                }

                pause = ( ( !map.expireMaxSizeFlag || map.size() < map.expireMaxSize )
                          && ( map.expireStoreSize == 0L ||
                               Store.forEngine( map.engine ).getCurrSize() - Store.forEngine( map.engine )
                                   .getFreeSize() < map.expireStoreSize ) );
            }
        }
        catch( Throwable e )
        {
            HTreeMapImpl.LOG.log( Level.SEVERE, "HTreeMap expirator thread failed", e );
        }
        finally
        {
            HTreeMapImpl m = mapRef.get();
            if( m != null )
            {
                m.closeLatch.countDown();
            }
            mapRef.clear();
            if( CC.LOG_HTREEMAP && HTreeMapImpl.LOG.isLoggable( Level.FINE ) )
            {
                HTreeMapImpl.LOG.log( Level.FINE, "HTreeMap expirator finished" );
            }
        }
    }
}
