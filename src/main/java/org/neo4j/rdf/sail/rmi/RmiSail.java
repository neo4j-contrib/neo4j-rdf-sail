package org.neo4j.rdf.sail.rmi;

import java.io.File;
import java.rmi.Remote;
import java.rmi.RemoteException;

import org.openrdf.sail.Sail;
import org.openrdf.sail.SailException;

/**
 * Basically an RMI version of the {@link Sail} interface but with a
 * change listener callback which is more optimized for use with RMI.
 */
interface RmiSail extends Remote
{
    RmiSailConnection connect( RmiSailConnectionListenerCallback listener )
        throws SailException, RemoteException;
    
    File getDataDir() throws RemoteException;
    
    void initialize() throws SailException, RemoteException;
    
    boolean isWritable() throws SailException, RemoteException;
    
    void setDataDir( File file ) throws RemoteException;
    
    void addCallback( RmiSailChangedListenerCallback callback )
        throws RemoteException;
}
