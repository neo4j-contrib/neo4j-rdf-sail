package org.neo4j.rdf.sail;

import info.aduna.iteration.CloseableIteration;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.api.core.NeoService;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.RelationshipType;
import org.neo4j.api.core.Transaction;
import org.neo4j.rdf.model.CompleteStatement;
import org.neo4j.rdf.sail.utils.SailConnectionTripleSource;
import org.neo4j.rdf.store.RdfStore;
import org.neo4j.util.CombiningIterable;
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
import org.openrdf.sail.Sail;
import org.openrdf.sail.SailChangedListener;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailConnectionListener;
import org.openrdf.sail.SailException;
import org.openrdf.sail.helpers.DefaultSailChangedEvent;

/**
 * Author: josh Date: Apr 25, 2008 Time: 5:36:36 PM
 */
public class NeoSailConnection implements SailConnection
{
    private static final int DEFAULT_BATCHSIZE = 5000;

    private final NeoService neo;
    private final RdfStore store;
    private final ValueFactory valueFactory;
    private final Set<SailConnectionListener> sailConnectionListeners = new HashSet<SailConnectionListener>();
    private final Collection<SailChangedListener> sailChangedListeners;
    private final int batchSize;
    private boolean open;
    private Transaction currentTransaction;
    private final AtomicInteger writeOperationCount = new AtomicInteger();
    private final Sail sail;
    private final AtomicInteger totalAddCount = new AtomicInteger();
    private boolean iterateResults;

    private enum NeoSailRelTypes implements RelationshipType
    {
        REF_TO_NAMESPACE
    }

    NeoSailConnection( final NeoService neo, final RdfStore store, final Sail sail,
        final ValueFactory valueFactory, final Collection<SailChangedListener> sailChangedListeners )
    {
        this( neo, store, sail, valueFactory, DEFAULT_BATCHSIZE, sailChangedListeners );
    }

    NeoSailConnection( final NeoService neo, final RdfStore store, final Sail sail,
        final ValueFactory valueFactory, int batchSize, final Collection<SailChangedListener> sailChangedListeners )
    {
        this.neo = neo;
        this.store = store;
        this.sail = sail;
        this.valueFactory = valueFactory;
        this.open = true;
        this.batchSize = batchSize;
        this.sailChangedListeners = sailChangedListeners;
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
    
    public void setIterateResults( boolean iterateResults )
    {
        // When running playback tests I don't think the PlaybackSail
        // iterates through all the results and since the Iterator returned
        // from this method is a true iterator which will find the results
        // on the fly it isn't a fair result to just return the iterator.
    	this.iterateResults = iterateResults;
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

    // TODO
    public CloseableIteration<? extends Resource, SailException> getContextIDs()
        throws SailException
    {
        return null;
    }

    public CloseableIteration<? extends Statement, SailException> getStatements(final Resource subject,
                                                                                final URI predicate,
                                                                                final Value object,
                                                                                boolean includeInferred,
                                                                                final Resource... contexts) throws SailException {
        if (includeInferred) {
            // TODO: change this to a warning
//            System.err.println("Warning: inference is not yet supported");
            includeInferred = false;
        }

        ensureOpenTransaction();
//System.out.println("getStatements(" + subject + ", " + predicate + ", " + object + ", " + includeInferred + ", " + contexts );
        try {
        	Iterable<CompleteStatement> result = null;
            if (contexts.length == 0) {
                org.neo4j.rdf.model.WildcardStatement statement
                        = SesameNeoMapper.createWildcardStatement(subject, predicate, object);
                Iterable<org.neo4j.rdf.model.CompleteStatement> iterator = store.getStatements(statement, includeInferred);
                result = iterator;
            } else {
                LinkedList<Iterable<CompleteStatement>> allQueries =
                	new LinkedList<Iterable<CompleteStatement>>();
                for ( Resource context : contexts )
                {
                    org.neo4j.rdf.model.WildcardStatement statement = SesameNeoMapper
                        .createWildcardStatement( subject, predicate, object,
                            context );
                    Iterable<CompleteStatement> iterator = store
                        .getStatements( statement, includeInferred );
                    allQueries.add( iterator );
                }
                result = new CombiningIterable<CompleteStatement>( allQueries );
            }
            
            if ( this.iterateResults )
            {
	            LinkedList<CompleteStatement> statements = new LinkedList<CompleteStatement>();
	            for ( CompleteStatement stmt : result )
	            {
	            	statements.add( stmt );
	            }
	            result = statements;
            }
            return new NeoStatementIteration( result.iterator() );
        }
        catch ( RuntimeException e )
        {
            throw new SailException( e );
        }
    }

    // TODO
    public long size( final Resource... contexts ) throws SailException
    {
        return -1;
    }

    public void addStatement( final Resource subject, final URI predicate,
        final Value object, final Resource... contexts ) throws SailException
    {
    	totalAddCount.incrementAndGet();
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
            e.printStackTrace();
            throw new SailException( e );
        }

        if ( sailConnectionListeners.size() > 0 )
        {
            for ( SailConnectionListener l : sailConnectionListeners)
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

        if (sailChangedListeners.size() > 0) {
            DefaultSailChangedEvent event = new DefaultSailChangedEvent(sail);
            event.setStatementsAdded(true);

            for (SailChangedListener listener : sailChangedListeners) {
                listener.sailChanged(event);
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
            e.printStackTrace();
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

        // Note: doesn't check whether any statements were actually removed,
        // only that this method was called.
        if (sailChangedListeners.size() > 0) {
            DefaultSailChangedEvent event = new DefaultSailChangedEvent(sail);
            event.setStatementsRemoved(true);

            for (SailChangedListener listener : sailChangedListeners) {
                listener.sailChanged(event);
            }
        }
    }

    public synchronized void commit() throws SailException
    {
//        System.out.println( "NeoSailConnection: commit invoked, at " +
//            writeOperationCount.get() + " op count (total of " +
//            totalAddCount.get() + ")" );
        if ( openTransaction() )
        {
            tx().success();
            tx().finish();
            clearBatchCommit();
            currentTransaction = null;
        }
    }

    public synchronized void rollback() throws SailException
    {
//        System.out.println( "NeoSailConnection: ROLLBACK invoked, at " +
//            writeOperationCount.get() + " op count" );        
        if ( openTransaction() )
        {
            tx().finish();
            clearBatchCommit();
            currentTransaction = null;
        }
    }

    private synchronized void ensureOpenTransaction()
    {
        if ( !openTransaction() )
        {
            clearBatchCommit();
            currentTransaction = neo.beginTx();            
        }
    }
    
    private synchronized boolean openTransaction()
    {
        return tx() != null;
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
        // TODO
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
        synchronized (sailConnectionListeners)
        {
            sailConnectionListeners.add( listener );
        }
    }

    public void removeConnectionListener( final SailConnectionListener listener )
    {
        synchronized (sailConnectionListeners)
        {
            sailConnectionListeners.remove( listener );
        }
    }
}
