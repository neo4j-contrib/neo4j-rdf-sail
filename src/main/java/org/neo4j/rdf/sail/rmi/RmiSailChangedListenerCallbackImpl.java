package org.neo4j.rdf.sail.rmi;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashSet;
import java.util.Set;

import org.openrdf.sail.Sail;
import org.openrdf.sail.SailChangedEvent;
import org.openrdf.sail.SailChangedListener;

class RmiSailChangedListenerCallbackImpl extends UnicastRemoteObject implements
    RmiSailChangedListenerCallback
{
    private static class Event implements SailChangedEvent
    {
        private final Sail sail;
        private final boolean statementsAdded;
        private final boolean statementsRemoved;
        
        Event( Sail sail, boolean statementsAdded, boolean statementsRemoved )
        {
            this.sail = sail;
            this.statementsAdded = statementsAdded;
            this.statementsRemoved = statementsRemoved;
        }
        
        public Sail getSail()
        {
            return sail;
        }
        
        public boolean statementsAdded()
        {
            return statementsAdded;
        }
        
        public boolean statementsRemoved()
        {
            return statementsRemoved;
        }
    }
    
    final Set<SailChangedListener> listeners =
        new HashSet<SailChangedListener>();
    private final RmiSailClient sail;
    
    RmiSailChangedListenerCallbackImpl( RmiSailClient sail )
        throws RemoteException
    {
        super();
        this.sail = sail;
    }
    
    public boolean sailChanged( boolean statementsAdded,
        boolean statementsRemoved )
    {
        SailChangedEvent event = new Event( sail, statementsAdded,
            statementsRemoved );
        for ( SailChangedListener listener : listeners )
        {
            listener.sailChanged( event );
        }
        return sail.alive;
    }
}
