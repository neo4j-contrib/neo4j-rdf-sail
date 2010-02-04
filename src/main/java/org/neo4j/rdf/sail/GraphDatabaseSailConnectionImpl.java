package org.neo4j.rdf.sail;

import info.aduna.iteration.CloseableIteration;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.impl.transaction.DeadlockDetectedException;
import org.neo4j.rdf.fulltext.FulltextIndex;
import org.neo4j.rdf.fulltext.QueryResult;
import org.neo4j.rdf.model.CompleteStatement;
import org.neo4j.rdf.model.StatementMetadata;
import org.neo4j.rdf.model.WildcardStatement;
import org.neo4j.rdf.sail.utils.ContextHandling;
import org.neo4j.rdf.sail.utils.MutatingLogger;
import org.neo4j.rdf.sail.utils.SailConnectionTripleSource;
import org.neo4j.rdf.store.RdfStore;
import org.neo4j.rdf.store.RdfStoreImpl;
import org.neo4j.rdf.util.TemporaryLogger;
import org.neo4j.commons.iterator.CombiningIterable;
import org.neo4j.util.NeoUtil;
import org.openrdf.model.Literal;
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
import org.openrdf.sail.SailConnectionListener;
import org.openrdf.sail.SailException;
import org.openrdf.sail.helpers.DefaultSailChangedEvent;

/**
 * Author: josh Date: Apr 25, 2008 Time: 5:36:36 PM
 */
public class GraphDatabaseSailConnectionImpl implements GraphDatabaseSailConnection
{
    private static final int DEFAULT_BATCHSIZE = 5000;
    // number of times to retry work in a transaction if deadlock detected
    private static final int NUMBER_OF_RETRIES = 5;
    
    private static final AtomicInteger connectionIdentifier = 
        new AtomicInteger( 0 );
    
    private final GraphDatabaseService graphDb;
    private final TransactionManager tm;
    private final RdfStore store;
    private final ValueFactory valueFactory;
    private final Set<SailConnectionListener> sailConnectionListeners = new HashSet<SailConnectionListener>();
    private final Collection<SailChangedListener> sailChangedListeners;
    private final int batchSize;
    private Transaction transaction;
    private boolean open;
    private final AtomicInteger writeOperationCount = new AtomicInteger();
    private final Sail sail;
    private final AtomicInteger totalAddCount = new AtomicInteger();
    private final List<Command> commands = new ArrayList<Command>();
    private final int identifier;
    
    private enum SailRelTypes implements RelationshipType
    {
        REF_TO_NAMESPACE
    }

    GraphDatabaseSailConnectionImpl( final GraphDatabaseService graphDb, final RdfStore store, final Sail sail,
        final ValueFactory valueFactory, final Collection<SailChangedListener> sailChangedListeners )
    {
        this( graphDb, store, sail, valueFactory, DEFAULT_BATCHSIZE, sailChangedListeners );
    }

    GraphDatabaseSailConnectionImpl( final GraphDatabaseService graphDb, final RdfStore store, final Sail sail,
        final ValueFactory valueFactory, int batchSize, final Collection<SailChangedListener> sailChangedListeners )
    {
        this.graphDb = graphDb;
        this.store = store;
        this.sail = sail;
        this.valueFactory = valueFactory;
        this.open = true;
        this.batchSize = batchSize;
        this.sailChangedListeners = sailChangedListeners;
        this.tm = (( EmbeddedGraphDatabase ) graphDb).getConfig().getTxModule().getTxManager();
        // setupTransaction();
        this.identifier = connectionIdentifier.incrementAndGet();
        log( "connection created" );
    }
    
    int getIdentifier()
    {
        return this.identifier;
    }
    
    private void log( String msg )
    {
        if ( transaction != null )
        {
            MutatingLogger.getLogger().info( getClass().getSimpleName() + "[" +
                identifier + "] running with tx[" + transaction.hashCode() +
                "]: " + msg );
        }
        else
        {
            MutatingLogger.getLogger().info( getClass().getSimpleName() + "[" +
                identifier + "] running with tx[" + null + "]: " + msg );
        }
    }

/*    private void setupTransaction() 
    {
        try
        {
            Transaction otherTx = tm.getTransaction();
            if ( otherTx != null )
            {
                tm.suspend();
            }
            tm.begin();
            transaction = tm.getTransaction();
            tm.suspend();
            if ( otherTx != null )
            {
                tm.resume( otherTx );
            }
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            throw new RuntimeException( e );
        }
    }*/
    
    private void beginTransaction()
    {
        try
        {
            tm.begin();
            transaction = tm.getTransaction();
            if ( transaction == null )
            {
                System.out.println( "GAHHHHHHH" );
            }
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            throw new RuntimeException( e );
        }
    }
    
    Transaction suspendOtherAndResumeThis()
    {
        try
        {
            Transaction otherTx = tm.getTransaction();
            if ( otherTx != null && otherTx == transaction )
            {
                return null;
            }
            else
            {
                if ( otherTx != null )
                {
                    tm.suspend();
                }
                if ( transaction == null )
                {
                    beginTransaction();
                }
                else
                {
                    tm.resume( transaction );
                }
                return otherTx;
            }
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            throw new RuntimeException( e );
        }
    }
    
    void suspendThisAndResumeOther( Transaction otherTx )
    {
        try
        {
            tm.suspend();
            if ( otherTx != null )
            {
                tm.resume( otherTx );
            }
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            throw new RuntimeException( e );
        }
    }
    
    private void resumeOther( Transaction otherTx )
    {
        try
        {
            if ( otherTx != null )
            {
                tm.resume( otherTx );
            }
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }

	public synchronized boolean isOpen() throws SailException
    {
        return open;
    }

    public synchronized void close() throws SailException
    {
        if ( !open )
        {
            return;
        }
        open = false;
        if ( commands.size() != 0 )
        {
            log( "close(): there are " + commands.size() + 
                " non commited operations that will be rolled back on close" );
        }
        commands.clear();
        Transaction otherTx = suspendOtherAndResumeThis();
        try
        {
            tm.rollback();
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            throw new RuntimeException( e );
        }
        finally
        {
            log( "connection closed" );
            ( ( GraphDatabaseSail ) this.sail ).connectionEnded( this.identifier, this );
            transaction = null;
            if ( otherTx != null )
            {
                resumeOther( otherTx );
            }
        }
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
    
    public synchronized CloseableIteration<FulltextQueryResult, SailException>
        evaluate( String query )
    {
        Transaction otherTx = suspendOtherAndResumeThis();
        try
        {
            Iterable<QueryResult> queryResult =
                this.store.searchFulltext( query );
            return new QueryResultIteration( queryResult.iterator(), this );
        }
        finally
        {
            suspendThisAndResumeOther( otherTx );
        }
    }
    
    public synchronized CloseableIteration<FulltextQueryResult, SailException>
        evaluateWithSnippets( String query, int snippetCountLimit )
    {
        Transaction otherTx = suspendOtherAndResumeThis();
        try
        {
            Iterable<QueryResult> queryResult = this.store.
                searchFulltextWithSnippets( query, snippetCountLimit );
            return new QueryResultIteration( queryResult.iterator(), this );
        }
        finally
        {
            suspendThisAndResumeOther( otherTx );
        }
    }
    
    public synchronized void reindexFulltextIndex()
    {
        log( "reindexFulltextIndex() called" );
        Transaction otherTx = suspendOtherAndResumeThis();
        try
        {
            FulltextIndex fulltextIndex =
                ( ( RdfStoreImpl ) store ).getFulltextIndex();
            if ( fulltextIndex == null )
            {
                throw new RuntimeException( "Fulltext index not used, please " +
                    "supply it in the RdfStore constructor" );
            }
            
            ( ( RdfStoreImpl ) store ).reindexFulltextIndex();
            log( "reindexFulltextIndex() completed" );
        }
        finally
        {
            suspendThisAndResumeOther( otherTx );
        }
    }

    // TODO
    public CloseableIteration<? extends Resource, SailException> getContextIDs()
        throws SailException
    {
        return null;
    }
    
    protected synchronized Iterator<CompleteStatement> internalGetStatements(
        final Resource subject, final URI predicate, final Value object,
        boolean includeInferred, final Resource... contexts )
        throws SailException
    {
        if ( includeInferred )
        {
            // TODO: change this to a warning
//            System.err.println("Warning: inference is not yet supported");
            includeInferred = false;
        }

        try
        {
            Iterable<CompleteStatement> result = null;
            if ( contexts.length == 0 ) {
                WildcardStatement statement = SesameGraphDatabaseMapper.
                    createWildcardStatement( subject, predicate, object );
                Iterable<CompleteStatement> iterator =
                    store.getStatements( statement, includeInferred );
                result = iterator;
            }
            else
            {
                LinkedList<Iterable<CompleteStatement>> allQueries =
                    new LinkedList<Iterable<CompleteStatement>>();
                for ( Resource context : contexts )
                {
                    WildcardStatement statement = SesameGraphDatabaseMapper
                        .createWildcardStatement( subject, predicate, object,
                            context );
                    Iterable<CompleteStatement> iterator = store
                        .getStatements( statement, includeInferred );
                    allQueries.add( iterator );
                }
                result = new CombiningIterable<CompleteStatement>( allQueries );
            }
            return result.iterator();
        }
        catch ( RuntimeException e )
        {
            throw new SailException( e );
        }
    }

    public synchronized CloseableIteration<? extends Statement, SailException> getStatements(final Resource subject,
                                                                                final URI predicate,
                                                                                final Value object,
                                                                                boolean includeInferred,
                                                                                final Resource... contexts) throws SailException {
        
        Transaction otherTx = suspendOtherAndResumeThis();
        try
        {
            return new GraphDatabaseStatementIteration( internalGetStatements( subject,
                predicate, object, includeInferred, contexts ), this );
        }
        finally
        {
            suspendThisAndResumeOther( otherTx );
        }
    }

    public synchronized long size( final Resource... contexts ) throws SailException
    {
        Transaction otherTx = suspendOtherAndResumeThis();
        try
        {
            return store.size( ContextHandling.createContexts( contexts ) );
        }
        finally
        {
            suspendThisAndResumeOther( otherTx );
        }
    }
    
    private void innerAddStatement( final Resource subject, 
        final URI predicate, final Value object, final Resource... contexts )
        throws SailException
    {
        commands.add( new Command( CommandType.ADD_STATEMENT, subject, 
            predicate, object, contexts ) );
        try
        {
            internalAddStatement( subject, predicate, object, contexts );
            totalAddCount.incrementAndGet();
            checkBatchCommit();
        }
        catch ( DeadlockDetectedException e )
        {
            handleDeadlockDetected( e );
        }
        catch ( RuntimeException e )
        {
            e.printStackTrace();
            throw new SailException( e );
        }
    }
    
    private String spogString( final Resource subject, 
        final URI predicate, final Value object, final Resource... contexts )
    {
        StringBuffer spog = new StringBuffer( "S[" );
        spog.append(  subject ).append( "] P[" ).append( predicate ).append( 
            "] O[" ).append( object ).append( "] G[" ); 
        for ( Resource r : contexts )
        {
            spog.append( "[" + r + "]" );
        }
        spog.append( "]" );
        return spog.toString();
    }

    public synchronized void addStatement( final Resource subject, 
        final URI predicate, final Value object, final Resource... contexts ) 
        throws SailException
    {
        Transaction otherTx = suspendOtherAndResumeThis();
        try
        {
            log( "addStatement: " + spogString( subject, predicate, object, 
                contexts ) );
            innerAddStatement( subject, predicate, object, contexts );
        }
        finally
        {
            suspendThisAndResumeOther( otherTx );
        }
        sendEventsToListeners( subject, predicate, object, contexts );
    }
    
    private void sendEventsToListeners( final Resource subject,
        final URI predicate, final Value object, final Resource... contexts )
    {
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
    
    public synchronized Statement addStatement(
        final Map<String, Literal> metadata, final Resource subject,
        final URI predicate, final Value object, final Resource... contexts )
        throws SailException
    {
        Statement result = null;
        Transaction otherTx = suspendOtherAndResumeThis();
        try
        {
            log( "addStatement with metadata: " + spogString( subject, 
                predicate, object, contexts ) );
            innerAddStatement( subject, predicate, object, contexts );
            CompleteStatement statement = internalGetStatements( subject,
                predicate, object, false, contexts ).next();
            setStatementMetadata( statement, metadata );
            result = GraphDatabaseSesameMapper.createStatement( statement, true );
        }
        finally
        {
            suspendThisAndResumeOther( otherTx );
        }
        sendEventsToListeners( subject, predicate, object, contexts );
        return result;
    }
    
    public synchronized void setStatementMetadata( Statement statement,
        Map<String, Literal> metadata ) throws SailException
    {
        Transaction otherTx = suspendOtherAndResumeThis();
        try
        {
            log( "setStatementMetadata: " + statement );
            CompleteStatement resultStatement = internalGetStatements(
                statement.getSubject(), statement.getPredicate(),
                statement.getObject(), false, statement.getContext() ).next();
            setStatementMetadata( resultStatement, metadata );
        }
        finally
        {
            suspendThisAndResumeOther( otherTx );
        }
    }
    
    private void setStatementMetadata( CompleteStatement statement,
        Map<String, Literal> metadata ) throws SailException
    {
        try
        {
            Collection<String> allKeys = new HashSet<String>();
            allKeys.addAll( metadata.keySet() );
            StatementMetadata existingMetadata = statement.getMetadata();
            for ( String key : existingMetadata.getKeys() )
            {
                allKeys.add( key );
            }
            for ( String key : allKeys )
            {
                Literal value = metadata.get( key );
                if ( value != null && existingMetadata.has( key ) )
                {
                    // Update
                    if ( !existingMetadata.get( key ).equals( value ) )
                    {
                        existingMetadata.set( key,
                            SesameGraphDatabaseMapper.createLiteral( value ) );
                    }
                }
                else if ( value != null )
                {
                    // Add
                    existingMetadata.set( key,
                        SesameGraphDatabaseMapper.createLiteral( value ) );
                }
                else
                {
                    // Remove
                    existingMetadata.remove( key );
                }
            }
        }
        catch ( RuntimeException e )
        {
            throw new SailException( e );
        }
    }
    
    private void internalAddStatement( final Resource subject, 
        final URI predicate, final Value object, final Resource... contexts ) 
    {
        if ( contexts.length == 0 )
        {
            org.neo4j.rdf.model.CompleteStatement statement = SesameGraphDatabaseMapper
                .createCompleteStatement( subject, predicate, object,
                    ( Resource ) null );
            store.addStatements( statement );
        }
        else
        {
            for ( Resource context : contexts )
            {
                org.neo4j.rdf.model.CompleteStatement statement = SesameGraphDatabaseMapper
                    .createCompleteStatement( subject, predicate, object,
                        context );
                store.addStatements( statement );
            }
        }
    }

    public synchronized void removeStatements( final Resource subject, 
    	final URI predicate, final Value object, final Resource... contexts ) 
    		throws SailException
    {
    	Transaction otherTx = suspendOtherAndResumeThis();
        try
        {
            log( "removeStatements: " + spogString( subject, predicate, object, 
                contexts ) );
            commands.add( new Command( CommandType.REMOVE_STATEMENT, subject, 
                predicate, object, contexts ) );
            internalRemoveStatements( subject, predicate, object, contexts );
            checkBatchCommit();
        }
        catch ( DeadlockDetectedException e )
        {
            handleDeadlockDetected( e );
        }
        catch ( RuntimeException e )
        {
            e.printStackTrace();
            throw new SailException( e );
        }
        finally
        {
        	suspendThisAndResumeOther( otherTx );
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

    private void internalRemoveStatements( final Resource subject, 
        final URI predicate, final Value object, final Resource... contexts ) 
    {
        if ( contexts.length == 0 )
        {
            org.neo4j.rdf.model.WildcardStatement statement = SesameGraphDatabaseMapper
                .createWildcardStatement( subject, predicate, object );
            store.removeStatements( statement );
        }
        else
        {
            for ( Resource context : contexts )
            {
                org.neo4j.rdf.model.WildcardStatement statement = SesameGraphDatabaseMapper
                    .createWildcardStatement( subject, predicate, object,
                        context );
                store.removeStatements( statement );
            }
        }
    }
    
    private void handleDeadlockDetected( DeadlockDetectedException dde )
    {
        for ( int i = 0; i < NUMBER_OF_RETRIES; i++ )
        {
            try
            {
                int txId = getTxId();
                transaction.rollback();
                commitFulltextIndex( txId, false );
                tm.begin();
                transaction = tm.getTransaction();
            }
            catch( Exception e )
            {
                dde.printStackTrace();
                throw new RuntimeException( 
                    "Problem during rollback/begin transaction handling DDE", 
                    e );
            }
            try
            {
                for ( Command c : commands )
                {
                    if ( c.getType() == CommandType.ADD_STATEMENT )
                    {
                        internalAddStatement( c.getSubject(), c.getPredicate(), 
                            c.getObject(), c.getContexts() );
                    }
                    else if ( c.getType() == CommandType.REMOVE_STATEMENT )
                    {
                        internalRemoveStatements( c.getSubject(), 
                            c.getPredicate(), c.getObject(), c.getContexts() );
                    }
                }
                // success
                return;
            }
            catch ( DeadlockDetectedException e )
            {
                // ok we failed again
            }
        }
        throw new RuntimeException( "Failed to handle DDE", dde );
    }
    
    private void commitFulltextIndex( int txId, boolean commit )
    {
        // TODO Just a temporary hack now
        FulltextIndex fulltextIndex =
            ( ( RdfStoreImpl ) store ).getFulltextIndex();
        if ( fulltextIndex != null )
        {
            fulltextIndex.end( txId, commit );
        }
    }
    
    private int getTxId() throws Exception
    {
        return transaction.hashCode();
    }
    
    public synchronized void commit() throws SailException
    {
        TemporaryLogger.getLogger().info( this.getClass().getName() +
            " commit called" );
//        System.out.println( "NeoSailConnection: commit invoked, at " +
//            writeOperationCount.get() + " op count (total of " +
//            totalAddCount.get() + ")" );
        Transaction otherTx = suspendOtherAndResumeThis();
        try
        {
            int txId = getTxId();
            tm.commit();
            transaction = null;
            commitFulltextIndex( txId, true );
//            tm.begin();
//            transaction = tm.getTransaction();
            log( "commit() called on tx[" + txId + "] " + 
                commands.size() + " operations committed" ); 
            clearBatchCommit();
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            throw new RuntimeException( e );
        }
        finally
        {
            resumeOther( otherTx );
        }
    }

    public synchronized void rollback() throws SailException
    {
//        System.out.println( "NeoSailConnection: ROLLBACK invoked, at " +
//            writeOperationCount.get() + " op count" );        
        Transaction otherTx = suspendOtherAndResumeThis();
        try
        {
            int txId = getTxId();
            tm.rollback();
            commitFulltextIndex( txId, false );
            transaction = null;
//            tm.begin();
//            transaction = tm.getTransaction();
            log( "rollback() called on tx[" + txId + "] " + 
                commands.size() + " operations rolled back" ); 
            clearBatchCommit();
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            throw new RuntimeException( e );
        }
        finally
        {
            resumeOther( otherTx );
        }
    }

    private synchronized void checkBatchCommit() throws SailException
    {
        if ( writeOperationCount.incrementAndGet() >= batchSize )
        {
            try
            {
                int txId = getTxId();
                tm.commit();
                
                commitFulltextIndex( txId, true );
                tm.begin();
                transaction = tm.getTransaction();
                log( "<- new tx, old tx[" + txId + "] commited " + 
                    commands.size() + " operations" ); 
                clearBatchCommit();
            }
            catch ( Exception e )
            {
                throw new RuntimeException( e );
            }
        }
    }
    
    private synchronized void clearBatchCommit()
    {
        commands.clear();
        writeOperationCount.set( 0 );
    }
    
    public void clear( final Resource... contexts ) throws SailException
    {
        removeStatements( null, null, null, contexts );
    }

    public CloseableIteration<? extends Namespace, SailException> getNamespaces()
        throws SailException
    {
        return new GraphDatabaseNamespaceIteration( getNamespaceNode(), graphDb );
    }

    public synchronized String getNamespace( final String prefix ) 
    	throws SailException
    {
    	Transaction otherTx = suspendOtherAndResumeThis();
        try
        {
            String uri = ( String ) getNamespaceNode().getProperty( prefix,
                null );
            return uri;
        }
        finally
        {
        	suspendThisAndResumeOther( otherTx );
        }
    }

    public synchronized void setNamespace( final String prefix, final String uri )
        throws SailException
    {
    	Transaction otherTx = suspendOtherAndResumeThis();
        try
        {
            getNamespaceNode().setProperty( prefix, uri );
        }
        finally
        {
        	suspendThisAndResumeOther( otherTx );
        }
    }

    private Node getNamespaceNode()
    {
        return new NeoUtil( graphDb )
            .getOrCreateSubReferenceNode( SailRelTypes.REF_TO_NAMESPACE );
    }

    public synchronized void removeNamespace( final String prefix ) 
    	throws SailException
    {
    	Transaction otherTx = suspendOtherAndResumeThis();
        try
        {
            getNamespaceNode().removeProperty( prefix );
        }
        finally
        {
        	suspendThisAndResumeOther( otherTx );
        }
    }

    public synchronized void clearNamespaces() throws SailException
    {
    	Transaction otherTx = suspendOtherAndResumeThis();
        try
        {
            getNamespaceNode().delete();
        }
        finally
        {
        	suspendThisAndResumeOther( otherTx );
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
    
    private static enum CommandType
    {
        ADD_STATEMENT,
        REMOVE_STATEMENT
    }
    
    private static class Command
    {
        private final CommandType type;
        private final Resource subject; 
        private final URI predicate;
        private final Value object;
        private final Resource[] contexts;
        
        Command( final CommandType type, final Resource subject, 
            final URI predicate, final Value object, 
            final Resource... contexts  )
        {
            this.type = type;
            this.subject = subject;
            this.predicate = predicate;
            this.object = object;
            this.contexts = contexts;
        }
        
        CommandType getType()
        {
            return type;
        }
        
        Resource getSubject()
        {
            return subject;
        }
        
        URI getPredicate()
        {
            return predicate;
        }
        
        Value getObject()
        {
            return object;
        }
        
        Resource[] getContexts()
        {
            return contexts;
        }
    }
}