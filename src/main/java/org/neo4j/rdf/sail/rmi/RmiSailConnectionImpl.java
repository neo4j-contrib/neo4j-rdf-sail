package org.neo4j.rdf.sail.rmi;

import java.rmi.RemoteException;
import java.rmi.server.RMIClientSocketFactory;
import java.rmi.server.RMIServerSocketFactory;
import java.rmi.server.UnicastRemoteObject;
import java.util.Map;

import org.neo4j.rdf.sail.FulltextQueryResult;
import org.neo4j.rdf.sail.NeoRdfSailConnection;
import org.neo4j.rdf.sail.rmi.RmiSailServer.RmiSailConnectionFactory;
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
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;

class RmiSailConnectionImpl extends UnicastRemoteObject
    implements RmiSailConnection
{
    private final SailConnection connection;
    private final RmiSailConnectionFactory factory;
    
    RmiSailConnectionImpl( SailConnection connection,
        RmiSailConnectionFactory factory ) throws RemoteException
    {
        super();
        this.connection = connection;
        this.factory = factory;
    }
    
    RmiSailConnectionImpl( SailConnection connection,
        RmiSailConnectionFactory factory, int port ) throws RemoteException
    {
        super( port );
        this.connection = connection;
        this.factory = factory;
    }
    
    RmiSailConnectionImpl( SailConnection connection,
        RmiSailConnectionFactory factory, int port, RMIClientSocketFactory csf,
        RMIServerSocketFactory ssf ) throws RemoteException
    {
        super( port, csf, ssf );
        this.connection = connection;
        this.factory = factory;
    }
    
    // Implementation
    public void addStatement( Resource subj, URI pred, Value obj,
        Resource[] contexts ) throws SailException
    {
        connection.addStatement( subj, pred, obj, contexts );
    }
    
    public Statement addStatement( Map<String, Literal> metadata, Resource subj,
        URI pred, Value obj, Resource[] contexts ) throws SailException
    {
        return getNeoRdfConnection().addStatement( metadata, subj, pred, obj,
            contexts );
    }
    
    public void clear( Resource[] contexts ) throws SailException
    {
        connection.clear( contexts );
    }
    
    public void clearNamespaces() throws SailException
    {
        connection.clearNamespaces();
    }
    
    public void close() throws SailException
    {
        connection.close();
    }
    
    public void commit() throws SailException
    {
        connection.commit();
    }
    
    public String getNamespace( String prefix ) throws SailException
    {
        return connection.getNamespace( prefix );
    }
    
    public boolean isOpen() throws SailException
    {
        return connection.isOpen();
    }
    
    public void removeNamespace( String prefix ) throws SailException
    {
        connection.removeNamespace( prefix );
    }
    
    public void removeStatements( Resource subj, URI pred, Value obj,
        Resource[] contexts ) throws SailException
    {
        connection.removeStatements( subj, pred, obj, contexts );
    }
    
    public void rollback() throws SailException
    {
        connection.rollback();
    }
    
    public void setNamespace( String prefix, String name ) throws SailException
    {
        connection.setNamespace( prefix, name );
    }
    
    public long size( Resource[] contexts ) throws SailException
    {
        return connection.size( contexts );
    }
    
    public RmiIterationBuffer<? extends BindingSet, QueryEvaluationException> evaluate(
        TupleExpr tupleExpr, Dataset dataset, BindingSet bindings,
        boolean includeInferred ) throws SailException, RemoteException
    {
        return factory.buffer( connection.evaluate( tupleExpr, dataset,
            bindings, includeInferred ) );
    }
    
    public RmiIterationBuffer<? extends Resource, SailException> getContextIDs()
        throws SailException, RemoteException
    {
        return factory.buffer( connection.getContextIDs() );
    }
    
    public RmiIterationBuffer<? extends Namespace, SailException> getNamespaces()
        throws SailException, RemoteException
    {
        return factory.buffer( connection.getNamespaces() );
    }
    
    public RmiIterationBuffer<? extends Statement, SailException> getStatements(
        Resource subj, URI pred, Value obj, boolean includeInferred,
        Resource[] contexts ) throws SailException, RemoteException
    {
        return factory.buffer( connection.getStatements( subj, pred, obj,
            includeInferred, contexts ) );
    }
    
    private NeoRdfSailConnection getNeoRdfConnection()
    {
        if ( !( connection instanceof NeoRdfSailConnection ) )
        {
            throw new RuntimeException( "Only available for connections " +
                "implementing " + NeoRdfSailConnection.class );
        }
        return ( NeoRdfSailConnection ) connection;
    }
    
    public RmiIterationBuffer<? extends FulltextQueryResult, SailException> evaluate(
        String query ) throws SailException, RemoteException
    {
        NeoRdfSailConnection neoRdfSailCollection = getNeoRdfConnection();
        return factory.buffer( neoRdfSailCollection.evaluate( query ) );
    }
    
    public RmiIterationBuffer<? extends FulltextQueryResult, SailException>
        evaluateWithSnippets( String query, int snippetCountLimit )
        throws SailException, RemoteException
    {
        NeoRdfSailConnection neoRdfSailCollection = getNeoRdfConnection();
        return factory.buffer( neoRdfSailCollection.evaluateWithSnippets(
            query, snippetCountLimit ) );
    }
    
    public void setStatementMetadata( Statement statement,
        Map<String, Literal> metadata ) throws SailException, RemoteException
    {
        NeoRdfSailConnection neoRdfSailCollection = getNeoRdfConnection();
        neoRdfSailCollection.setStatementMetadata( statement, metadata );
    }
    
    public void reindexFulltextIndex() throws SailException, RemoteException
    {
        getNeoRdfConnection().reindexFulltextIndex();
    }
}
