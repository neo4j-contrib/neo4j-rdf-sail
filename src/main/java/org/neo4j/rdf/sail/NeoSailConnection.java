package org.neo4j.rdf.sail;

import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;
import org.openrdf.sail.SailConnectionListener;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.Namespace;
import org.openrdf.model.ValueFactory;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.Dataset;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.evaluation.impl.EvaluationStrategyImpl;
import org.openrdf.query.algebra.evaluation.TripleSource;
import org.neo4j.rdf.store.RdfStore;
import org.neo4j.rdf.sail.utils.SailConnectionTripleSource;
import org.neo4j.api.core.NeoService;
import info.aduna.iteration.CloseableIteration;

/**
 * Author: josh
 * Date: Apr 25, 2008
 * Time: 5:36:36 PM
 */
public class NeoSailConnection implements SailConnection {
    private final NeoService neo;
    private final RdfStore store;
    private ValueFactory valueFactory;
    private boolean open;

    NeoSailConnection(final NeoService neo,
                      final RdfStore store,
                      final ValueFactory valueFactory) {
        this.neo = neo;
        this.store = store;
        this.valueFactory = valueFactory;
        this.open = true;
    }

    public boolean isOpen() throws SailException {
        return open;
    }

    public void close() throws SailException {
        // TODO: anything else to do here?
        open = false;
    }

    public CloseableIteration<? extends BindingSet, QueryEvaluationException> evaluate(final TupleExpr tupleExpr,
                                                                                       final Dataset dataset,
                                                                                       final BindingSet bindingSet,
                                                                                       final boolean includeInferred) throws SailException {
		try
		{
			TripleSource tripleSource = new SailConnectionTripleSource(this, valueFactory, includeInferred);
			EvaluationStrategyImpl strategy = new EvaluationStrategyImpl(tripleSource, dataset);

			return strategy.evaluate(tupleExpr, bindingSet);
		}

        catch (QueryEvaluationException e)
		{
			throw new SailException(e);
		}
    }

    public CloseableIteration<? extends Resource, SailException> getContextIDs() throws SailException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public CloseableIteration<? extends Statement, SailException> getStatements(final Resource subject,
                                                                                final URI predicate,
                                                                                final Value object,
                                                                                final boolean includeInferred,
                                                                                final Resource... contexts) throws SailException {
        try {
            org.neo4j.rdf.model.WildcardStatement statement
                    = SesameNeoMapper.createWildcardStatement(subject, predicate, object, contexts);
            Iterable<org.neo4j.rdf.model.CompleteStatement> iterator = store.getStatements(statement, includeInferred);

            return new NeoStatementIteration(iterator.iterator());
        } catch (RuntimeException e) {
            throw new SailException(e);
        }
    }

    public long size(final Resource... contexts) throws SailException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public void commit() throws SailException {

    }

    public void rollback() throws SailException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void addStatement(final Resource subject,
                             final URI predicate,
                             final Value object,
                             final Resource... contexts) throws SailException {
        try {
            org.neo4j.rdf.model.CompleteStatement statement
                    = SesameNeoMapper.createCompleteStatement(subject, predicate, object, contexts);

            store.addStatements(statement);
        } catch (RuntimeException e) {
            throw new SailException(e);
        }
    }

    public void removeStatements(final Resource subject,
                                 final URI predicate,
                                 final Value object,
                                 final Resource... contexts) throws SailException {
        try {
            org.neo4j.rdf.model.WildcardStatement statement
                    = SesameNeoMapper.createWildcardStatement(subject, predicate, object, contexts);

            store.removeStatements(statement);
        } catch (RuntimeException e) {
            throw new SailException(e);
        }
    }

    public void clear(final Resource... contexts) throws SailException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public CloseableIteration<? extends Namespace, SailException> getNamespaces() throws SailException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public String getNamespace(final String prefix) throws SailException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public void setNamespace(final String prefix, final String uri) throws SailException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void removeNamespace(final String s) throws SailException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void clearNamespaces() throws SailException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void addConnectionListener(final SailConnectionListener sailConnectionListener) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public void removeConnectionListener(final SailConnectionListener sailConnectionListener) {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}
