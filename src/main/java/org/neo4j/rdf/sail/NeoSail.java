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
    private final ValueFactory valueFactory;

    public NeoSail(final NeoService neo, final RdfStore store) {
        this.neo = neo;
        this.store = store;
        this.valueFactory = new ValueFactoryImpl();
    }

    public void setDataDir(final File file) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public File getDataDir() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public void initialize() throws SailException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void shutDown() throws SailException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public boolean isWritable() throws SailException {
        return IS_WRITABLE;
    }

    public SailConnection getConnection() throws SailException {
        return new NeoSailConnection(neo, store, valueFactory);
    }

    public ValueFactory getValueFactory() {
        return valueFactory;
    }

    public void addSailChangedListener(final SailChangedListener sailChangedListener) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void removeSailChangedListener(final SailChangedListener sailChangedListener) {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}
