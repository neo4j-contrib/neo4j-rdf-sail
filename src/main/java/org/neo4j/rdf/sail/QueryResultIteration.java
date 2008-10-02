package org.neo4j.rdf.sail;

import info.aduna.iteration.CloseableIteration;

import java.util.Iterator;

import org.neo4j.rdf.fulltext.QueryResult;
import org.neo4j.util.IteratorWrapper;
import org.openrdf.sail.SailException;

public class QueryResultIteration
    extends IteratorWrapper<FulltextQueryResult, QueryResult>
    implements CloseableIteration<FulltextQueryResult, SailException>
{
    private NeoSailConnection connection;
    
    QueryResultIteration( Iterator<QueryResult> queryResult,
        NeoSailConnection connection )
    {
        super( queryResult );
        this.connection = connection;
    }
    
    public void close() throws SailException
    {
        // Not needed
    }
    
    @Override
    protected FulltextQueryResult underlyingObjectToObject(
        QueryResult object )
    {
        connection.suspendOtherAndResumeThis();
        try
        {
            return new FulltextQueryResult( object );
        }
        finally
        {
            connection.suspendThisAndResumeOther();
        }
    }
}
