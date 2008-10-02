package org.neo4j.rdf.sail.rmi;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashSet;
import java.util.Set;

import org.openrdf.model.Statement;
import org.openrdf.sail.SailConnectionListener;

class RmiSailConnectionListenerCallbackImpl extends UnicastRemoteObject
    implements RmiSailConnectionListenerCallback
{
    final Set<SailConnectionListener> listeners =
        new HashSet<SailConnectionListener>();
    
    RmiSailConnectionListenerCallbackImpl() throws RemoteException
    {
        super();
    }
    
    public void statementAdded( Statement statement )
    {
        for ( SailConnectionListener listener : listeners )
        {
            listener.statementAdded( statement );
        }
    }
    
    public void statementRemoved( Statement statement )
    {
        for ( SailConnectionListener listener : listeners )
        {
            listener.statementRemoved( statement );
        }
    }
}
