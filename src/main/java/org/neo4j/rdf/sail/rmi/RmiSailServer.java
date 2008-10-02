package org.neo4j.rdf.sail.rmi;

import info.aduna.iteration.CloseableIteration;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URI;
import java.rmi.AlreadyBoundException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.server.RMIClientSocketFactory;
import java.rmi.server.RMIServerSocketFactory;
import java.rmi.server.UnicastRemoteObject;

import org.openrdf.model.Statement;
import org.openrdf.sail.Sail;
import org.openrdf.sail.SailChangedEvent;
import org.openrdf.sail.SailChangedListener;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailConnectionListener;
import org.openrdf.sail.SailException;

public class RmiSailServer extends UnicastRemoteObject implements RmiSail
{
    interface RmiSailConnectionFactory
    {
        RmiSailConnection connect( SailConnection connection )
            throws RemoteException;
        
        <E, X extends Exception> IterationBufferer<E, X> buffer(
            CloseableIteration<E, X> iter ) throws RemoteException;
    }
    
    private final Sail sail;
    private final RmiSailConnectionFactory factory;
    
    RmiSailServer( Sail sail ) throws RemoteException
    {
        super();
        this.sail = sail;
        this.factory = new RmiSailConnectionFactory()
        {
            public RmiSailConnection connect( SailConnection connection )
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
            public RmiSailConnection connect( SailConnection connection )
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
            public RmiSailConnection connect( SailConnection connection )
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
    
    public static void register( Sail sail, URI resourceUri )
        throws MalformedURLException, RemoteException, AlreadyBoundException
    {
        Naming.rebind( resourceUri.toString(), new RmiSailServer( sail ) );
    }
    
    public static void register( Sail sail, URI resourceUri, int port )
        throws MalformedURLException, RemoteException, AlreadyBoundException
    {
        Naming.rebind( resourceUri.toString(), new RmiSailServer( sail, port ) );
    }
    
    public static void register( Sail sail, URI resourceUri, int port,
        RMIClientSocketFactory csf, RMIServerSocketFactory ssf )
        throws MalformedURLException, RemoteException, AlreadyBoundException
    {
        Naming.rebind( resourceUri.toString(), new RmiSailServer( sail, port,
            csf, ssf ) );
    }
    
    // Implementation
    public RmiSailConnection connect(
        final RmiSailConnectionListenerCallback callback )
        throws RemoteException, SailException
    {
        final SailConnection connection = sail.getConnection();
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
                }
            }
        } );
        return factory.connect( connection );
    }
    
    public File getDataDir()
    {
        return sail.getDataDir();
    }
    
    public void initialize() throws SailException
    {
        sail.initialize();
    }
    
    public boolean isWritable() throws SailException
    {
        return sail.isWritable();
    }
    
    public void setDataDir( File file )
    {
        sail.setDataDir( file );
    }
    
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
