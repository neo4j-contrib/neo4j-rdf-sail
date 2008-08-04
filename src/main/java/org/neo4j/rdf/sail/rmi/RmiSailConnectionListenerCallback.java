package org.neo4j.rdf.sail.rmi;

import java.rmi.Remote;
import java.rmi.RemoteException;

import org.openrdf.model.Statement;

interface RmiSailConnectionListenerCallback extends Remote
{
	void statementAdded( Statement statement ) throws RemoteException;

	void statementRemoved( Statement statement ) throws RemoteException;
}
