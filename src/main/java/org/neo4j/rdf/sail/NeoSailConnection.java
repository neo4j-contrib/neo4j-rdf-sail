package org.neo4j.rdf.sail;

import info.aduna.iteration.CloseableIteration;

import java.util.LinkedList;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicInteger;

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
 * Author: josh Date: Apr 25, 2008 Time: 5:36:36 PM
 */
public class NeoSailConnection implements SailConnection
{
    private final NeoService neo;
    private final RdfStore store;
    private final ValueFactory valueFactory;
    private final Set<SailConnectionListener> listeners = new HashSet<SailConnectionListener>();
    private final int batchSize;
    private boolean open;
    private Transaction currentTransaction;
    private final AtomicInteger writeOperationCount = new AtomicInteger();

    private enum NeoSailRelTypes implements RelationshipType
    {
        REF_TO_NAMESPACE
    }

    NeoSailConnection( final NeoService neo, final RdfStore store,
        final ValueFactory valueFactory )
    {
        this( neo, store, valueFactory, 5000 );
    }

    NeoSailConnection( final NeoService neo, final RdfStore store,
        final ValueFactory valueFactory, int batchSize )
    {
        this.neo = neo;
        this.store = store;
        this.valueFactory = valueFactory;
        this.open = true;
        this.batchSize = batchSize;
    }

    public boolean isOpen() throws SailException
    {
        return open;
    }

    public void close() throws SailException
    {
        open = false;
        rollback();
    }

    public CloseableIteration<? extends BindingSet, QueryEvaluationException> evaluate(
        final TupleExpr tupleExpr, final Dataset dataset,
        final BindingSet bindingSet, final boolean includeInferred )
        throws SailException
    {
        try
        {
            TripleSource tripleSource = new SailConnectionTripleSource( this,
                valueFactory, includeInferred );
            EvaluationStrategyImpl strategy = new EvaluationStrategyImpl(
                tripleSource, dataset );
            return strategy.evaluate( tupleExpr, bindingSet );
        }
        catch ( QueryEvaluationException e )
        {
            throw new SailException( e );
        }
    }

    public CloseableIteration<? extends Resource, SailException> getContextIDs()
        throws SailException
    {
        return null;
    }

    public CloseableIteration<? extends Statement, SailException> getStatements(final Resource subject,
                                                                                final URI predicate,
                                                                                final Value object,
                                                                                final boolean includeInferred,
                                                                                final Resource... contexts) throws SailException {
//System.out.println("getStatements(" + subject + ", " + predicate + ", " + object + ", " + includeInferred + ", " + contexts );
        try {
            if (contexts.length == 0) {
                org.neo4j.rdf.model.WildcardStatement statement
                        = SesameNeoMapper.createWildcardStatement(subject, predicate, object);
                Iterable<org.neo4j.rdf.model.CompleteStatement> iterator = store.getStatements(statement, includeInferred);
                return new NeoStatementIteration(iterator.iterator());
            } else {
                LinkedList<CompleteStatement> result = new LinkedList<CompleteStatement>();
                for ( Resource context : contexts )
                {
                    org.neo4j.rdf.model.WildcardStatement statement = SesameNeoMapper
                        .createWildcardStatement( subject, predicate, object,
                            context );
                    Iterable<org.neo4j.rdf.model.CompleteStatement> iterator = store
                        .getStatements( statement, includeInferred );
                    for ( CompleteStatement resultStatement : iterator )
                    {
                        result.add( resultStatement );
                    }
                }
                return new NeoStatementIteration( result.iterator() );
            }
        }
        catch ( RuntimeException e )
        {
            throw new SailException( e );
        }
    }

    public long size( final Resource... contexts ) throws SailException
    {
        return 0;
    }

    public void addStatement( final Resource subject, final URI predicate,
        final Value object, final Resource... contexts ) throws SailException
    {
//System.out.println("addStatement(" + subject + ", " + predicate + ", " + object + ", " + contexts);
        ensureOpenTransaction();
        try
        {
            if ( contexts.length == 0 )
            {
                org.neo4j.rdf.model.CompleteStatement statement = SesameNeoMapper
                    .createCompleteStatement( subject, predicate, object,
                        ( Resource ) null );
                store.addStatements( statement );
            }
            else
            {
                for ( Resource context : contexts )
                {
                    org.neo4j.rdf.model.CompleteStatement statement = SesameNeoMapper
                        .createCompleteStatement( subject, predicate, object,
                            context );
                    store.addStatements( statement );
                }
            }
            checkBatchCommit();
        }
        catch ( RuntimeException e )
        {
            throw new SailException( e );
        }
        if ( listeners.size() > 0 )
        {
            for ( SailConnectionListener l : listeners )
            {
                if ( 0 == contexts.length )
                {
                    l.statementAdded( valueFactory.createStatement( subject,
                        predicate, object ) );
                }
                else
                {
                    for ( Resource context : contexts )
                    {
                        l.statementAdded( valueFactory.createStatement(
                            subject, predicate, object, context ) );
                    }
                }
            }
        }
    }

    public void removeStatements( final Resource subject, final URI predicate,
        final Value object, final Resource... contexts ) throws SailException
    {
        ensureOpenTransaction();
        try
        {
            if ( contexts.length == 0 )
            {
                org.neo4j.rdf.model.WildcardStatement statement = SesameNeoMapper
                    .createWildcardStatement( subject, predicate, object );
                store.removeStatements( statement );
            }
            else
            {
                for ( Resource context : contexts )
                {
                    org.neo4j.rdf.model.WildcardStatement statement = SesameNeoMapper
                        .createWildcardStatement( subject, predicate, object,
                            context );
                    store.removeStatements( statement );
                }
            }
            checkBatchCommit();
        }
        catch ( RuntimeException e )
        {
            throw new SailException( e );
        }
        // TODO: wildcard statements are not allowed by ValueFactoryImpl --
        // either create a new ValueFactory class
        // which does allow them, or don't worry about it...
        /*
         * if (listeners.size() > 0) { for (SailConnectionListener l :
         * listeners) { if (0 == contexts.length) {
         * l.statementRemoved(valueFactory.createStatement(subject, predicate,
         * object)); } else { for (Resource context : contexts) {
         * l.statementRemoved(valueFactory.createStatement(subject, predicate,
         * object, context)); } } } }
         */
    }

    public synchronized void commit() throws SailException
    {
        tx().success();
        tx().finish();
        clearBatchCommit();
        currentTransaction = null;
    }

    public synchronized void rollback() throws SailException
    {
        tx().finish();
        clearBatchCommit();
        currentTransaction = null;
    }

    private synchronized void ensureOpenTransaction()
    {
        if ( tx() == null )
        {
            clearBatchCommit();
            currentTransaction = neo.beginTx();            
        }
    }
    
    private synchronized void checkBatchCommit() throws SailException
    {
        if ( writeOperationCount.incrementAndGet() >= batchSize )
        {
            commit(); // will invoke clearBatchCommit            
        }
    }
    
    private synchronized void clearBatchCommit()
    {
        writeOperationCount.set( 0 );
    }
    
    private Transaction tx()
    {
        return currentTransaction;
    }

    public void clear( final Resource... contexts ) throws SailException
    {
    }

    public CloseableIteration<? extends Namespace, SailException> getNamespaces()
        throws SailException
    {
        return new NeoNamespaceIteration( getNamespaceNode(), neo );
    }

    public String getNamespace( final String prefix ) throws SailException
    {
        Transaction tx = neo.beginTx();
        try
        {
            String uri = ( String ) getNamespaceNode().getProperty( prefix,
                null );
            tx.success();
            return uri;
        }
        finally
        {
            tx.finish();
        }
    }

    public void setNamespace( final String prefix, final String uri )
        throws SailException
    {
        Transaction tx = neo.beginTx();
        try
        {
            getNamespaceNode().setProperty( prefix, uri );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    private Node getNamespaceNode()
    {
        return new NeoUtil( neo )
            .getOrCreateSubReferenceNode( NeoSailRelTypes.REF_TO_NAMESPACE );
    }

    public void removeNamespace( final String prefix ) throws SailException
    {
        Transaction tx = neo.beginTx();
        try
        {
            getNamespaceNode().removeProperty( prefix );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    public void clearNamespaces() throws SailException
    {
        Transaction tx = neo.beginTx();
        try
        {
            getNamespaceNode().delete();
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    public void addConnectionListener( final SailConnectionListener listener )
    {
        synchronized ( listeners )
        {
            listeners.add( listener );
        }
    }

    public void removeConnectionListener( final SailConnectionListener listener )
    {
        synchronized ( listeners )
        {
            listeners.remove( listener );
        }
    }
}
