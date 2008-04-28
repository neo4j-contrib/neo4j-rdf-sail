package org.neo4j.rdf.sail;

import info.aduna.iteration.CloseableIteration;

import java.util.LinkedList;
import java.util.Set;
import java.util.HashSet;

import org.neo4j.api.core.NeoService;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.RelationshipType;
import org.neo4j.api.core.Transaction;
import org.neo4j.rdf.model.CompleteStatement;
import org.neo4j.rdf.sail.utils.SailConnectionTripleSource;
import org.neo4j.rdf.store.RdfStore;
import org.neo4j.util.NeoUtil;
import org.openrdf.model.Namespace;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.evaluation.TripleSource;
import org.openrdf.query.algebra.evaluation.impl.EvaluationStrategyImpl;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailConnectionListener;
import org.openrdf.sail.SailException;

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
    private final Set<SailConnectionListener> listeners = new HashSet<SailConnectionListener>();

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
        try {
            TripleSource tripleSource = new SailConnectionTripleSource(this, valueFactory, includeInferred);
            EvaluationStrategyImpl strategy = new EvaluationStrategyImpl(tripleSource, dataset);

            return strategy.evaluate(tupleExpr, bindingSet);
        }

        catch (QueryEvaluationException e) {
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
            if (contexts.length == 0) {
                org.neo4j.rdf.model.WildcardStatement statement
                        = SesameNeoMapper.createWildcardStatement(subject, predicate, object);
                Iterable<org.neo4j.rdf.model.CompleteStatement> iterator = store.getStatements(statement, includeInferred);
                return new NeoStatementIteration(iterator.iterator());
            } else {
                LinkedList<CompleteStatement> result = new LinkedList<CompleteStatement>();
                for (Resource context : contexts) {
                    org.neo4j.rdf.model.WildcardStatement statement
                            = SesameNeoMapper.createWildcardStatement(subject, predicate, object, context);
                    Iterable<org.neo4j.rdf.model.CompleteStatement> iterator = store.getStatements(statement, includeInferred);
                    for (CompleteStatement resultStatement : iterator) {
                        result.add(resultStatement);
                    }
                }
                return new NeoStatementIteration(result.iterator());
            }
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
            if (contexts.length == 0) {
                org.neo4j.rdf.model.CompleteStatement statement = SesameNeoMapper.createCompleteStatement(subject, predicate, object, (Resource) null);
                store.addStatements(statement);
            } else {
                for (Resource context : contexts) {
                    org.neo4j.rdf.model.CompleteStatement statement =
                            SesameNeoMapper.createCompleteStatement(subject, predicate, object, context);
                    store.addStatements(statement);
                }
            }

        } catch (RuntimeException e) {
            throw new SailException(e);
        }

        if (listeners.size() > 0) {
            for (SailConnectionListener l : listeners) {
                if (0 == contexts.length) {
                    l.statementAdded(valueFactory.createStatement(subject, predicate, object));
                } else {
                    for (Resource context : contexts) {
                        l.statementAdded(valueFactory.createStatement(subject, predicate, object, context));
                    }
                }
            }
        }
    }

    public void removeStatements(final Resource subject,
                                 final URI predicate,
                                 final Value object,
                                 final Resource... contexts) throws SailException {
        try {
            if (contexts.length == 0) {
                org.neo4j.rdf.model.WildcardStatement statement
                        = SesameNeoMapper.createWildcardStatement(subject, predicate, object);
                store.removeStatements(statement);
            } else {
                for (Resource context : contexts) {
                    org.neo4j.rdf.model.WildcardStatement statement
                            = SesameNeoMapper.createWildcardStatement(subject, predicate, object, context);
                    store.removeStatements(statement);
                }
            }
        } catch (RuntimeException e) {
            throw new SailException(e);
        }

        // TODO: wildcard statements are not allowed by ValueFactoryImpl -- either create a new ValueFactory class
        // which does allow them, or don't worry about it...
/*
        if (listeners.size() > 0) {
            for (SailConnectionListener l : listeners) {
                if (0 == contexts.length) {
                    l.statementRemoved(valueFactory.createStatement(subject, predicate, object));
                } else {
                    for (Resource context : contexts) {
                        l.statementRemoved(valueFactory.createStatement(subject, predicate, object, context));
                    }
                }
            }
        }*/
    }

    public void clear(final Resource... contexts) throws SailException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public CloseableIteration<? extends Namespace, SailException> getNamespaces() throws SailException {
        return new NeoNamespaceIteration(getNamespaceNode(), neo);
    }

    public String getNamespace(final String prefix) throws SailException {
        Transaction tx = neo.beginTx();
        try {
            String uri = (String) getNamespaceNode().getProperty(prefix, null);
            tx.success();
            return uri;
        } finally {
            tx.finish();
        }
    }

    public void setNamespace(final String prefix, final String uri) throws SailException {
        Transaction tx = neo.beginTx();
        try {
            getNamespaceNode().setProperty(prefix, uri);
            tx.success();
        } finally {
            tx.finish();
        }
    }

    private Node getNamespaceNode() {

        return new NeoUtil(neo).getOrCreateSubReferenceNode(NeoSailRelTypes.REF_TO_NAMESPACE);
    }

    private enum NeoSailRelTypes implements RelationshipType { REF_TO_NAMESPACE }

    public void removeNamespace(final String prefix) throws SailException {
        Transaction tx = neo.beginTx();
        try {
            getNamespaceNode().removeProperty(prefix);
            tx.success();
        } finally {
            tx.finish();
        }
    }

    public void clearNamespaces() throws SailException {
        Transaction tx = neo.beginTx();
        try {
            getNamespaceNode().delete();
            tx.success();
        } finally {
            tx.finish();
        }
    }

    public void addConnectionListener(final SailConnectionListener listener) {
        synchronized (listeners) {
            listeners.add(listener);
        }
    }

    public void removeConnectionListener(final SailConnectionListener listener) {
        synchronized (listeners) {
            listeners.remove(listener);
        }
    }
}
