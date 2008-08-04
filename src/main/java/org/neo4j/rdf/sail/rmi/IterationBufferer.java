package org.neo4j.rdf.sail.rmi;

import info.aduna.iteration.CloseableIteration;

import java.rmi.RemoteException;
import java.rmi.server.RMIClientSocketFactory;
import java.rmi.server.RMIServerSocketFactory;
import java.rmi.server.UnicastRemoteObject;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

class IterationBufferer<E, X extends Exception> extends UnicastRemoteObject
    implements RmiIterationBuffer<E, X>
{
	private static final int CHUNK_SIZE = 10;
	private final CloseableIteration<E, X> iter;
	private boolean open = true;

	IterationBufferer( CloseableIteration<E, X> iter ) throws RemoteException
	{
		super();
		this.iter = iter;
	}

	IterationBufferer( CloseableIteration<E, X> iter, int port )
	    throws RemoteException
	{
		super( port );
		this.iter = iter;
	}

	IterationBufferer( CloseableIteration<E, X> iter, int port,
	    RMIClientSocketFactory csf, RMIServerSocketFactory ssf )
	    throws RemoteException
	{
		super( port, csf, ssf );
		this.iter = iter;
	}

	public void close() throws X
	{
		open = false;
		iter.close();
	}

	public Collection<E> getChunk() throws X, RemoteException
	{
		if ( !open ) return null;
		List<E> data = new LinkedList<E>();
		for ( int count = CHUNK_SIZE; count > 0 && iter.hasNext(); count-- )
		{
			data.add( iter.next() );
		}
		return data;
	}
}
