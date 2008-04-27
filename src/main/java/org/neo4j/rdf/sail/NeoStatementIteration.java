package org.neo4j.rdf.sail;

import info.aduna.iteration.CloseableIteration;
import org.openrdf.model.Statement;
import org.openrdf.sail.SailException;
import org.neo4j.rdf.model.CompleteStatement;

import java.util.Iterator;

/**
 * Author: josh
 * Date: Apr 25, 2008
 * Time: 7:10:31 PM
 */
public class NeoStatementIteration implements CloseableIteration<Statement, SailException> {
    private final Iterator<org.neo4j.rdf.model.CompleteStatement> iterator;

    public NeoStatementIteration(final Iterator<org.neo4j.rdf.model.CompleteStatement> iterator) {
        this.iterator = iterator;
    }

    public void close() throws SailException {
        // Not needed
    }

    public boolean hasNext() throws SailException {
        return iterator.hasNext();
    }

    public Statement next() throws SailException {
        org.neo4j.rdf.model.CompleteStatement statement = iterator.next();
        return (null == statement)
                // TODO: would be better here if iterator were an Iterator<CompleteStatement>
                ? null : NeoSesameMapper.createStatement((CompleteStatement) statement);
    }

    public void remove() throws SailException {
        // TODO: decide whether remove() should be supported
    }
}
