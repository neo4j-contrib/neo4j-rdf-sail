package org.neo4j.rdf.sail;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.rdf.sail.utils.MutatingLogger;
import org.neo4j.rdf.store.RdfStore;
import org.neo4j.rdf.store.RdfStoreImpl;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.sail.NotifyingSail;
import org.openrdf.sail.NotifyingSailConnection;
import org.openrdf.sail.SailChangedListener;
import org.openrdf.sail.SailException;

/**
 * Author: josh
 * Date: Apr 25, 2008
 * Time: 5:32:22 PM
 */
public class GraphDatabaseSail implements NotifyingSail {
    // TODO: is there such thing as a read-only Sail?
    private static final boolean IS_WRITABLE = true;

    private final GraphDatabaseService graphDb;
    private final RdfStore store;
    private final ValueFactory valueFactory = new ValueFactoryImpl();
    private final Set<SailChangedListener> listeners = new HashSet<SailChangedListener>();
    private final AtomicInteger connectionCounter = new AtomicInteger();
    
    private final Map<Integer, GraphDatabaseSailConnectionImpl> activeConnections =
        Collections.synchronizedMap(
            new HashMap<Integer, GraphDatabaseSailConnectionImpl>() );
    
    public GraphDatabaseSail(final GraphDatabaseService graphDb, final RdfStore store,
            boolean shutdownNeo4jInstancesUponShutdown ) {
        this.graphDb = graphDb;
        this.store = store;
        ((RdfStoreImpl)this.store).setShutdownNeo4jInstancesUponShutdown( 
                shutdownNeo4jInstancesUponShutdown );
    }

    public GraphDatabaseSail(final GraphDatabaseService graphDb, final RdfStore store) {
        this( graphDb, store, false );
    }
    
    public void setDataDir(final File file) {
        // Not used.
    }

    public File getDataDir() {
        // Not used.
        return null;
    }
    
    public void initialize() throws SailException {
        // Not used.
    }

    public void shutDown() throws SailException {
        printActiveConnections();
//      System.out.println( "Number of history connections: " +
//          connectionCounter.get() );
        store.shutDown();
    }
    
    private void printActiveConnections()
    {
        for ( Map.Entry<Integer, GraphDatabaseSailConnectionImpl> entry :
            this.activeConnections.entrySet() )
        {
            String logString = entry.getValue().getClass().getSimpleName() + "[" +
                entry.getKey() + "] still open when shutting down sail, closing";
            MutatingLogger.getLogger().warn( logString );
            try
            {
                entry.getValue().close();
            }
            catch ( Exception e )
            {
                MutatingLogger.getLogger().warn( e );
            }
        }
    }

    public boolean isWritable() throws SailException {
        return IS_WRITABLE;
    }

    public NotifyingSailConnection getConnection() throws SailException {
        connectionCounter.incrementAndGet();
        GraphDatabaseSailConnectionImpl connection =
            new GraphDatabaseSailConnectionImpl(graphDb, store, this, valueFactory, listeners);
        this.activeConnections.put( connection.getIdentifier(), connection );
        return connection;
    }
    
    void connectionEnded( int identifier, GraphDatabaseSailConnectionImpl connection )
    {
        this.activeConnections.remove( identifier );
    }

    public ValueFactory getValueFactory() {
        return valueFactory;
    }

    public void addSailChangedListener(final SailChangedListener listener) {
        listeners.add(listener);
    }

    public void removeSailChangedListener(final SailChangedListener listener) {
        listeners.remove(listener);
    }
}
