package org.neo4j.rdf.sail.rmi;

import info.aduna.iteration.CloseableIteration;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URI;
import java.rmi.AlreadyBoundException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.RMIClientSocketFactory;
import java.rmi.server.RMIServerSocketFactory;
import java.rmi.server.UnicastRemoteObject;

import org.neo4j.rdf.sail.GraphDatabaseSailConnection;
import org.openrdf.model.Statement;
import org.openrdf.sail.Sail;
import org.openrdf.sail.SailChangedEvent;
import org.openrdf.sail.SailChangedListener;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailConnectionListener;
import org.openrdf.sail.SailException;

/**
 * Here you can register {@link Sail} instances so that other JVM instances
 * can connect to them via RMI using {@link RmiSailClient}.
 */
public class RmiSailServer extends UnicastRemoteObject implements RmiSail
{
    interface RmiSailConnectionFactory
    {
        RmiSailConnectionImpl connect( SailConnection connection )
            throws RemoteException;
        
        <E, X extends Exception> IterationBufferer<E, X> buffer(
            CloseableIteration<E, X> iter ) throws RemoteException;
    }
    
    private final Sail sail;
    private final RmiSailConnectionFactory factory;
    
    private RmiSailServer( Sail sail ) throws RemoteException
    {
        super();
        this.sail = sail;
        this.factory = new RmiSailConnectionFactory()
        {
            public RmiSailConnectionImpl connect( SailConnection connection )
                throws RemoteException
            {
                return new RmiSailConnectionImpl( connection, this );
            }
            
            public <E, X extends Exception> IterationBufferer<E, X> buffer(
                CloseableIteration<E, X> iter ) throws RemoteException
            {
                return new IterationBufferer<E, X>( iter );
            }
        };
    }
    
    private RmiSailServer( Sail sail, final int port ) throws RemoteException
    {
        super( port );
        this.sail = sail;
        this.factory = new RmiSailConnectionFactory()
        {
            public RmiSailConnectionImpl connect( SailConnection connection )
                throws RemoteException
            {
                return new RmiSailConnectionImpl( connection, this, port );
            }
            
            public <E, X extends Exception> IterationBufferer<E, X> buffer(
                CloseableIteration<E, X> iter ) throws RemoteException
            {
                return new IterationBufferer<E, X>( iter, port );
            }
        };
    }
    
    private RmiSailServer( Sail sail, final int port,
        final RMIClientSocketFactory csf, final RMIServerSocketFactory ssf )
        throws RemoteException
    {
        super( port, csf, ssf );
        this.sail = sail;
        this.factory = new RmiSailConnectionFactory()
        {
            public RmiSailConnectionImpl connect( SailConnection connection )
                throws RemoteException
            {
                return new RmiSailConnectionImpl( connection, this, port, csf,
                    ssf );
            }
            
            public <E, X extends Exception> IterationBufferer<E, X> buffer(
                CloseableIteration<E, X> iter ) throws RemoteException
            {
                return new IterationBufferer<E, X>( iter, port, csf, ssf );
            }
        };
    }
    
    /**
     * Registers {@code sail} at the URI given by {@code resourceUri}.
     * The URI must have the "rmi" protocol. The registry for the supplied port
     * (the port in the URI) must be created before calling this method.
     * F.ex. with {@link LocateRegistry#createRegistry(int)}.
     * 
     * @param sail the {@link Sail} to register at the URI.
     * @param resourceUri the URI to register the sail at.
     * @throws MalformedURLException if the URI is malformed.
     * @throws RemoteException if an RMI problem occurs.
     * @throws AlreadyBoundException if the given URI is already bound.
     */
    public static void register( Sail sail, URI resourceUri )
        throws MalformedURLException, RemoteException, AlreadyBoundException
    {
        Naming.rebind( resourceUri.toString(), new RmiSailServer( sail ) );
    }
    
    /**
     * Registers {@code sail} at the URI given by {@code resourceUri}.
     * The URI must have the "rmi" protocol. The registry for the supplied port
     * (the port in the URI) must be created before calling this method.
     * F.ex. with {@link LocateRegistry#createRegistry(int)}.
     * 
     * @param sail the {@link Sail} to register at the URI.
     * @param resourceUri the URI to register the sail at.
     * @param port the port where the method invocation communication will
     * occur. see {@link UnicastRemoteObject} for more details.
     * @throws MalformedURLException if the URI is malformed.
     * @throws RemoteException if an RMI problem occurs.
     * @throws AlreadyBoundException if the given URI is already bound.
     */
    public static void register( Sail sail, URI resourceUri, int port )
        throws MalformedURLException, RemoteException, AlreadyBoundException
    {
        Naming.rebind( resourceUri.toString(),
            new RmiSailServer( sail, port ) );
    }
    
    /**
     * Registers {@code sail} at the URI given by {@code resourceUri}.
     * The URI must have the "rmi" protocol. The registry for the supplied port
     * (the port in the URI) must be created before calling this method.
     * F.ex. with {@link LocateRegistry#createRegistry(int)}.
     * 
     * @param sail the {@link Sail} to register at the URI.
     * @param resourceUri the URI to register the sail at.
     * @param port the port where the method invocation communication will
     * occur. see {@link UnicastRemoteObject} for more details.
     * @param csf the {@link RMIClientSocketFactory} to use.
     * See {@link UnicastRemoteObject} for more details.
     * @param ssf the {@link RMIServerSocketFactory} to use.
     * See {@link UnicastRemoteObject} for more details.
     * @throws MalformedURLException if the URI is malformed.
     * @throws RemoteException if an RMI problem occurs.
     * @throws AlreadyBoundException if the given URI is already bound.
     */
    public static void register( Sail sail, URI resourceUri, int port,
        RMIClientSocketFactory csf, RMIServerSocketFactory ssf )
        throws MalformedURLException, RemoteException, AlreadyBoundException
    {
        Naming.rebind( resourceUri.toString(), new RmiSailServer( sail, port,
            csf, ssf ) );
    }
    
    /**
     * Used by the client to get a connection to the sail.
     * 
     * @param callback acts as {@link SailConnectionListener} over RMI.
     * @throws RemoteException if an RMI problem occurs.
     * @throws SailException if a connection couldn't be retrieved.
     * @return an RMI version of a {@link GraphDatabaseSailConnection}.
     */
    public RmiSailConnection connect(
        final RmiSailConnectionListenerCallback callback )
        throws RemoteException, SailException
    {
        final SailConnection connection = sail.getConnection();
        final RmiSailConnectionImpl remote = factory.connect(connection);
        connection.addConnectionListener( new SailConnectionListener()
        {
            public void statementAdded( Statement statement )
            {
                try
                {
                    callback.statementAdded( statement );
                }
                catch ( RemoteException e )
                {
                    connection.removeConnectionListener( this );
                    remote.callbackConnectionLost();
                }
            }
            
            public void statementRemoved( Statement statement )
            {
                try
                {
                    callback.statementRemoved( statement );
                }
                catch ( RemoteException e )
                {
                    connection.removeConnectionListener( this );
                    remote.callbackConnectionLost();
                }
            }
        } );
        return remote;
    }
    
    /**
     * @return the underlying sail's {@link Sail#getDataDir()}.
     */
    public File getDataDir()
    {
        return sail.getDataDir();
    }
    
    /**
     * Calls the underlying sail's {@link Sail#initialize()}.
     */
    public void initialize() throws SailException
    {
        sail.initialize();
    }
    
    /**
     * @return the underlying sail's {@link Sail#isWritable()}.
     */
    public boolean isWritable() throws SailException
    {
        return sail.isWritable();
    }
    
    /**
     * Calls the underlying sail's {@link Sail#setDataDir(File)}.
     * @param file the data directory.
     */
    public void setDataDir( File file )
    {
        sail.setDataDir( file );
    }
    
    /**
     * Calls the underlying sail's
     * {@link Sail#addSailChangedListener(SailChangedListener)}. TODO.
     * 
     * @param callback the listener which receives "change" events.
     */
    public void addCallback( final RmiSailChangedListenerCallback callback )
    {
        sail.addSailChangedListener( new SailChangedListener()
        {
            public void sailChanged( SailChangedEvent event )
            {
                boolean alive = true;
                try
                {
                    alive = callback.sailChanged( event.statementsAdded(),
                        event.statementsRemoved() );
                }
                catch ( RemoteException ex )
                {
                    alive = false;
                }
                if ( !alive )
                {
                    sail.removeSailChangedListener( this );
                }
            }
        } );
    }
}
