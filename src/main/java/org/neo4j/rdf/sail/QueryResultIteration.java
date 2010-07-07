package org.neo4j.rdf.sail;

import info.aduna.iteration.CloseableIteration;

import java.util.Iterator;

import javax.transaction.Transaction;

import org.neo4j.helpers.collection.IteratorWrapper;
import org.neo4j.rdf.fulltext.QueryResult;
import org.openrdf.sail.SailException;

public class QueryResultIteration
    extends IteratorWrapper<FulltextQueryResult, QueryResult>
    implements CloseableIteration<FulltextQueryResult, SailException>
{
    private GraphDatabaseSailConnectionImpl connection;
    
    QueryResultIteration( Iterator<QueryResult> queryResult,
        GraphDatabaseSailConnectionImpl connection )
    {
        super( queryResult );
        this.connection = connection;
    }
    
    public void close() throws SailException
    {
        // Not needed
    }
    
    @Override
    public boolean hasNext()
    {
        synchronized ( connection )
        {
            Transaction otherTx = connection.suspendOtherAndResumeThis();
            try
            {
                return super.hasNext();
            }
            finally
            {
                connection.suspendThisAndResumeOther( otherTx );
            }
        }
    }

    @Override
    public FulltextQueryResult next()
    {
        synchronized ( connection )
        {
            Transaction otherTx = connection.suspendOtherAndResumeThis();
            try
            {
                return super.next();
            }
            finally
            {
                connection.suspendThisAndResumeOther( otherTx );
            }
        }
    }
    
    @Override
    protected FulltextQueryResult underlyingObjectToObject(
        QueryResult object )
    {
        return new FulltextQueryResult( object );
    }
}
