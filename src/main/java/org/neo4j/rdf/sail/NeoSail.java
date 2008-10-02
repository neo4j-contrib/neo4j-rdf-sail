package org.neo4j.rdf.sail;

import org.openrdf.sail.Sail;
import org.openrdf.sail.SailException;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailChangedListener;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.neo4j.api.core.NeoService;
import org.neo4j.rdf.store.RdfStore;

import java.io.File;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicInteger;

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
//      System.out.println( "Number of history connections: " +
//          connectionCounter.get() );
        store.shutDown();
    }

    public boolean isWritable() throws SailException {
        return IS_WRITABLE;
    }

    public SailConnection getConnection() throws SailException {
        connectionCounter.incrementAndGet();
        NeoSailConnection connection =
            new NeoSailConnection(neo, store, this, valueFactory, listeners);
        return connection;
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
