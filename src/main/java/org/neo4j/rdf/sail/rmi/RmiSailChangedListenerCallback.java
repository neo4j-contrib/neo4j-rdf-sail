package org.neo4j.rdf.sail.rmi;

import java.rmi.Remote;
import java.rmi.RemoteException;

interface RmiSailChangedListenerCallback extends Remote
{
	boolean sailChanged( boolean statementsAdded, boolean statementsRemoved )
	    throws RemoteException;
}
