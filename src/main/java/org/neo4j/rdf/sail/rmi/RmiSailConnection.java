package org.neo4j.rdf.sail.rmi;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.Map;

import org.neo4j.rdf.sail.FulltextQueryResult;
import org.neo4j.rdf.sail.GraphDatabaseSailConnection;
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

/**
 * An RMI interface for the {@link GraphDatabaseSailConnection} which is a
 * {@link SailConnection} with some added functionality for a graph db sail.
 */
interface RmiSailConnection extends Remote
{
    void addStatement( Resource subj, URI pred, Value obj, Resource[] contexts )
        throws SailException, RemoteException;
    
    Statement addStatement( Map<String, Literal> metadata, Resource subj,
        URI pred, Value obj, Resource[] contexts )
        throws SailException, RemoteException;
    
    void clear( Resource[] contexts ) throws SailException, RemoteException;
    
    void clearNamespaces() throws SailException, RemoteException;
    
    void close() throws SailException, RemoteException;
    
    void commit() throws SailException, RemoteException;
    
    String getNamespace( String prefix ) throws SailException, RemoteException;
    
    boolean isOpen() throws SailException, RemoteException;
    
    void removeNamespace( String prefix ) throws SailException, RemoteException;
    
    void removeStatements( Resource subj, URI pred, Value obj,
        Resource[] contexts ) throws SailException, RemoteException;
    
    void rollback() throws SailException, RemoteException;
    
    void setNamespace( String prefix, String name ) throws SailException,
        RemoteException;
    
    long size( Resource[] contexts ) throws SailException, RemoteException;
    
    RmiIterationBuffer<? extends BindingSet, QueryEvaluationException> evaluate(
        TupleExpr tupleExpr, Dataset dataset, BindingSet bindings,
        boolean includeInferred ) throws SailException, RemoteException;
    
    void setStatementMetadata( Statement statement,
        Map<String, Literal> metadata ) throws SailException, RemoteException;
    
    RmiIterationBuffer<? extends FulltextQueryResult, SailException>
        evaluate( String query ) throws SailException, RemoteException;
    
    RmiIterationBuffer<? extends FulltextQueryResult, SailException>
        evaluateWithSnippets( String query, int snippetCountLimit )
        throws SailException, RemoteException;
    
    RmiIterationBuffer<? extends Resource, SailException> getContextIDs()
        throws SailException, RemoteException;
    
    RmiIterationBuffer<? extends Namespace, SailException> getNamespaces()
        throws SailException, RemoteException;
    
    RmiIterationBuffer<? extends Statement, SailException> getStatements(
        Resource subj, URI pred, Value obj, boolean includeInferred,
        Resource[] contexts ) throws SailException, RemoteException;
    
    void reindexFulltextIndex() throws SailException, RemoteException;
}
