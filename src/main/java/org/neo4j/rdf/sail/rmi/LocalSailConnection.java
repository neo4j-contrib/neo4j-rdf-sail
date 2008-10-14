package org.neo4j.rdf.sail.rmi;

import static org.neo4j.rdf.sail.rmi.RmiSailClient.RMI_CONNECTION_FAILED;
import static org.neo4j.rdf.sail.rmi.RmiSailClient.valueFactory;
import info.aduna.iteration.CloseableIteration;

import java.rmi.RemoteException;
import java.util.Map;

import org.neo4j.rdf.sail.FulltextQueryResult;
import org.neo4j.rdf.sail.NeoRdfSailConnection;
import org.neo4j.rdf.sail.utils.SailConnectionTripleSource;
import org.openrdf.model.Literal;
import org.openrdf.model.Namespace;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.evaluation.TripleSource;
import org.openrdf.query.algebra.evaluation.impl.EvaluationStrategyImpl;
import org.openrdf.sail.SailConnectionListener;
import org.openrdf.sail.SailException;

class LocalSailConnection implements NeoRdfSailConnection
{
    private final RmiSailConnection connection;
    private final RmiSailConnectionListenerCallbackImpl callback;
    
    LocalSailConnection( RmiSailConnection connection,
        RmiSailConnectionListenerCallbackImpl callback )
    {
        this.connection = connection;
        this.callback = callback;
    }
    
    public void addConnectionListener( SailConnectionListener listener )
    {
        callback.listeners.add( listener );
    }
    
    public void removeConnectionListener( SailConnectionListener listener )
    {
        callback.listeners.remove( listener );
    }
    
    public void addStatement( Resource subj, URI pred, Value obj,
        Resource... contexts ) throws SailException
    {
        try
        {
            connection.addStatement( subj, pred, obj, contexts );
        }
        catch ( RemoteException ex )
        {
            throw new SailException( RMI_CONNECTION_FAILED, ex );
        }
    }
    
    public Statement addStatement( Map<String, Literal> metadata, Resource subj,
        URI pred, Value obj, Resource... contexts ) throws SailException
    {
        try
        {
            return connection.addStatement( metadata, subj, pred, obj,
                contexts );
        }
        catch ( RemoteException ex )
        {
            throw new SailException( RMI_CONNECTION_FAILED, ex );
        }
    }
    
    public void clear( Resource... contexts ) throws SailException
    {
        try
        {
            connection.clear( contexts );
        }
        catch ( RemoteException ex )
        {
            throw new SailException( RMI_CONNECTION_FAILED, ex );
        }
    }
    
    public void clearNamespaces() throws SailException
    {
        try
        {
            connection.clearNamespaces();
        }
        catch ( RemoteException ex )
        {
            throw new SailException( RMI_CONNECTION_FAILED, ex );
        }
    }
    
    public void close() throws SailException
    {
        try
        {
            connection.close();
        }
        catch ( RemoteException ex )
        {
            throw new SailException( RMI_CONNECTION_FAILED, ex );
        }
    }
    
    public void commit() throws SailException
    {
        try
        {
            connection.commit();
        }
        catch ( RemoteException ex )
        {
            throw new SailException( RMI_CONNECTION_FAILED, ex );
        }
    }
    
    public CloseableIteration<? extends BindingSet, QueryEvaluationException> evaluate(
        TupleExpr tupleExpr, Dataset dataset, BindingSet bindings,
        boolean includeInferred ) throws SailException
    {
        // NOTE: due to problems with serialization of the arguments for this
        // method, the evaluation is done on the client side.
        try
        {
            TripleSource tripleSource = new SailConnectionTripleSource( this,
                valueFactory, includeInferred );
            EvaluationStrategyImpl strategy = new EvaluationStrategyImpl(
                tripleSource, dataset );
            return strategy.evaluate( tupleExpr, bindings );
        }
        catch ( QueryEvaluationException e )
        {
            throw new SailException( e );
        }
    }
    
    public CloseableIteration<? extends Resource, SailException> getContextIDs()
        throws SailException
    {
        try
        {
            return IterationUnbufferer.unbuffer( connection.getContextIDs() );
        }
        catch ( RemoteException ex )
        {
            throw new SailException( RMI_CONNECTION_FAILED, ex );
        }
    }
    
    public CloseableIteration<? extends Namespace, SailException> getNamespaces()
        throws SailException
    {
        try
        {
            return IterationUnbufferer.unbuffer( connection.getNamespaces() );
        }
        catch ( RemoteException ex )
        {
            throw new SailException( RMI_CONNECTION_FAILED, ex );
        }
    }
    
    public CloseableIteration<? extends Statement, SailException> getStatements(
        Resource subj, URI pred, Value obj, boolean includeInferred,
        Resource... contexts ) throws SailException
    {
        try
        {
            return IterationUnbufferer.unbuffer( connection.getStatements(
                subj, pred, obj, includeInferred, contexts ) );
        }
        catch ( RemoteException ex )
        {
            throw new SailException( RMI_CONNECTION_FAILED, ex );
        }
    }
    
    public String getNamespace( String prefix ) throws SailException
    {
        try
        {
            return connection.getNamespace( prefix );
        }
        catch ( RemoteException ex )
        {
            throw new SailException( RMI_CONNECTION_FAILED, ex );
        }
    }
    
    public boolean isOpen() throws SailException
    {
        try
        {
            return connection.isOpen();
        }
        catch ( RemoteException ex )
        {
            throw new SailException( RMI_CONNECTION_FAILED, ex );
        }
    }
    
    public void removeNamespace( String prefix ) throws SailException
    {
        try
        {
            connection.removeNamespace( prefix );
        }
        catch ( RemoteException ex )
        {
            throw new SailException( RMI_CONNECTION_FAILED, ex );
        }
    }
    
    public void removeStatements( Resource subj, URI pred, Value obj,
        Resource... contexts ) throws SailException
    {
        try
        {
            connection.removeStatements( subj, pred, obj, contexts );
        }
        catch ( RemoteException ex )
        {
            throw new SailException( RMI_CONNECTION_FAILED, ex );
        }
    }
    
    public void rollback() throws SailException
    {
        try
        {
            connection.rollback();
        }
        catch ( RemoteException ex )
        {
            throw new SailException( RMI_CONNECTION_FAILED, ex );
        }
    }
    
    public void setNamespace( String prefix, String name ) throws SailException
    {
        try
        {
            connection.setNamespace( prefix, name );
        }
        catch ( RemoteException ex )
        {
            throw new SailException( RMI_CONNECTION_FAILED, ex );
        }
    }
    
    public long size( Resource... contexts ) throws SailException
    {
        try
        {
            return connection.size( contexts );
        }
        catch ( RemoteException ex )
        {
            throw new SailException( RMI_CONNECTION_FAILED, ex );
        }
    }
    
    public CloseableIteration<? extends FulltextQueryResult, SailException>
        evaluate( String query ) throws SailException
    {
        try
        {
            return IterationUnbufferer.unbuffer( connection.evaluate( query ) );
        }
        catch ( RemoteException ex )
        {
            throw new SailException( RMI_CONNECTION_FAILED, ex );
        }
    }
    
    public void setStatementMetadata( Statement statement,
        Map<String, Literal> metadata ) throws SailException
    {
        try
        {
            connection.setStatementMetadata( statement, metadata );
        }
        catch ( RemoteException e )
        {
            throw new SailException( RMI_CONNECTION_FAILED, e );
        }
    }
    
    public void reindexFulltextIndex() throws SailException
    {
        try
        {
            connection.reindexFulltextIndex();
        }
        catch ( RemoteException e )
        {
            throw new SailException( RMI_CONNECTION_FAILED, e );
        }
    }
}
