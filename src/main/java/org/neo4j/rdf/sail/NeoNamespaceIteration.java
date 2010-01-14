package org.neo4j.rdf.sail;

import info.aduna.iteration.CloseableIteration;
import org.openrdf.model.Namespace;
import org.openrdf.model.impl.NamespaceImpl;
import org.openrdf.sail.SailException;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.GraphDatabaseService;

import java.util.Iterator;
import java.util.Collection;
import java.util.LinkedList;

/**
 * Author: josh
 * Date: Apr 25, 2008
 * Time: 7:10:31 PM
 */
public class NeoNamespaceIteration implements CloseableIteration<Namespace, SailException> {
    private final Iterator<Namespace> iterator;

    public NeoNamespaceIteration(final Node node, final GraphDatabaseService neo) {

        Transaction tx = neo.beginTx();
        try {
            Collection<Namespace> namespaces = new LinkedList<Namespace>();
            Iterator<String> keys = node.getPropertyKeys().iterator();
            while (keys.hasNext()) {
                String key = keys.next();
                String uri = (String) node.getProperty(key);
                namespaces.add(new NamespaceImpl(key, uri));
            }
            this.iterator = namespaces.iterator();
        } finally {
            tx.finish();
        }
    }

    public void close() throws SailException {
        // Not needed
    }

    public boolean hasNext() throws SailException {
        return iterator.hasNext();
    }

    public Namespace next() throws SailException {
        return iterator.next();
    }

    public void remove() throws SailException {
        // TODO: decide whether remove() should be supported
    }
}