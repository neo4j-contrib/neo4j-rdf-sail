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
	QueryResultIteration( Iterator<QueryResult> queryResult )
	{
		super( queryResult );
	}

	public void close() throws SailException
    {
		// Not needed
    }

	@Override
    protected FulltextQueryResult underlyingObjectToObject(
        QueryResult object )
    {
		return new FulltextQueryResult( object );
    }
}
