package org.neo4j.rdf.sail;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.api.core.NeoService;
import org.neo4j.rdf.sail.utils.MutatingLogger;
import org.neo4j.rdf.store.RdfStore;
import org.neo4j.rdf.util.TemporaryLogger;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.sail.Sail;
import org.openrdf.sail.SailChangedListener;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;

/**
 * Author: josh
 * Date: Apr 25, 2008
 * Time: 5:32:22 PM
 */
public class NeoSail implements Sail {
    // TODO: is there such thing as a read-only NeoSail?
    private static final boolean IS_WRITABLE = true;

    private final NeoService neo;
    private final RdfStore store;
    private final ValueFactory valueFactory = new ValueFactoryImpl();
    private final Set<SailChangedListener> listeners = new HashSet<SailChangedListener>();
    private final AtomicInteger connectionCounter = new AtomicInteger();
    
    private final Map<Integer, NeoSailConnection> activeConnections =
        Collections.synchronizedMap(
            new HashMap<Integer, NeoSailConnection>() );
    
    public NeoSail(final NeoService neo, final RdfStore store) {
//System.out.println("we're creating a NeoSail: " + neo + ", " + store);
        this.neo = neo;
        this.store = store;
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
        TemporaryLogger.getLogger().info( getClass().getName() +
            " shutDown called", new Exception() );
        printActiveConnections();
//      System.out.println( "Number of history connections: " +
//          connectionCounter.get() );
        store.shutDown();
    }
    
    private void printActiveConnections()
    {
        for ( Map.Entry<Integer, NeoSailConnection> entry :
            this.activeConnections.entrySet() )
        {
            String logString = "NeoSailConnection[" +
                entry.getKey() + "] still open when shutting down sail, closing";
            TemporaryLogger.getLogger().warn( logString );
            MutatingLogger.getLogger().warn( logString );
            try
            {
                entry.getValue().close();
            }
            catch ( Exception e )
            {
                TemporaryLogger.getLogger().warn( e );
                MutatingLogger.getLogger().warn( e );
            }
        }
    }

    public boolean isWritable() throws SailException {
        return IS_WRITABLE;
    }

    public SailConnection getConnection() throws SailException {
        connectionCounter.incrementAndGet();
        NeoSailConnection connection =
            new NeoSailConnection(neo, store, this, valueFactory, listeners);
        this.activeConnections.put( connection.getIdentifier(), connection );
        return connection;
    }
    
    void connectionEnded( int identifier, NeoSailConnection connection )
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
