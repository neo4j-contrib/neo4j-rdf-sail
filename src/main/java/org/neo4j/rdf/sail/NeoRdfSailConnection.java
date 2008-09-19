package org.neo4j.rdf.sail;

import info.aduna.iteration.CloseableIteration;

import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;

/**
 * An extended {@link SailConnection} with a method for fulltext search.
 * The format is very basic word search with AND logics between them, f.ex:
 * (without quotation marks)
 * 
 * "computer science" will return statements with computer and science in them
 * "comput* science" will return statement with computer, computational (and
 * other words beginning with comput) and science in them.
 * 
 */
public interface NeoRdfSailConnection extends SailConnection
{
    CloseableIteration<? extends FulltextQueryResult, SailException>
    	evaluate( String query ) throws SailException;
}
