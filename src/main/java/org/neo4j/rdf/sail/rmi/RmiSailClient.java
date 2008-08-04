package org.neo4j.rdf.sail.rmi;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URI;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;

import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.sail.Sail;
import org.openrdf.sail.SailChangedListener;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;

public class RmiSailClient implements Sail
{
	static final String RMI_CONNECTION_FAILED = "RMI connection failed.";
	static final ValueFactory valueFactory = new ValueFactoryImpl();
	private final RmiSail rmi;
	private volatile RmiSailChangedListenerCallbackImpl callback = null;
	boolean alive = true;

	private synchronized RmiSailChangedListenerCallbackImpl callback()
	    throws RemoteException
	{
		if ( callback == null )
		{
			rmi.addCallback( callback = new RmiSailChangedListenerCallbackImpl(
			    this ) );
		}
		return callback;
	}

	public RmiSailClient( URI resourceUri ) throws MalformedURLException,
	    RemoteException, NotBoundException
	{
		rmi = ( RmiSail ) Naming.lookup( resourceUri.toString() );
	}

	public SailConnection getConnection() throws SailException
	{
		try
		{
			RmiSailConnectionListenerCallbackImpl callback = new RmiSailConnectionListenerCallbackImpl();
			return new LocalSailConnection( rmi.connect( callback ), callback );
		}
		catch ( RemoteException ex )
		{
			throw new SailException( RMI_CONNECTION_FAILED, ex );
		}
	}

	public void addSailChangedListener( SailChangedListener listener )
	{
		try
		{
			callback().listeners.add( listener );
		}
		catch ( RemoteException ex )
		{
			throw new RuntimeException( RMI_CONNECTION_FAILED, ex );
		}
	}

	public void removeSailChangedListener( SailChangedListener listener )
	{
		if ( callback != null )
		{
			callback.listeners.remove( listener );
		}
	}

	public File getDataDir()
	{
		try
		{
			return rmi.getDataDir();
		}
		catch ( RemoteException ex )
		{
			throw new RuntimeException( RMI_CONNECTION_FAILED, ex );
		}
	}

	public ValueFactory getValueFactory()
	{
		return valueFactory;
	}

	public void initialize() throws SailException
	{
		try
		{
			rmi.initialize();
		}
		catch ( RemoteException ex )
		{
			throw new SailException( RMI_CONNECTION_FAILED, ex );
		}
	}

	public boolean isWritable() throws SailException
	{
		try
		{
			return rmi.isWritable();
		}
		catch ( RemoteException ex )
		{
			throw new SailException( RMI_CONNECTION_FAILED, ex );
		}
	}

	public void setDataDir( File file )
	{
		try
		{
			rmi.setDataDir( file );
		}
		catch ( RemoteException ex )
		{
			throw new RuntimeException( RMI_CONNECTION_FAILED, ex );
		}
	}

	public void shutDown() throws SailException
	{
		alive = false;
	}
}
