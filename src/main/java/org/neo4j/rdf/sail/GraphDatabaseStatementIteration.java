package org.neo4j.rdf.sail;

import info.aduna.iteration.CloseableIteration;
import org.openrdf.model.Statement;
import org.openrdf.sail.SailException;
import org.neo4j.rdf.model.CompleteStatement;

import java.util.Iterator;

import javax.transaction.Transaction;

/**
 * Author: josh
 * Date: Apr 25, 2008
 * Time: 7:10:31 PM
 */
public class GraphDatabaseStatementIteration implements CloseableIteration<Statement, SailException> {

    private final Iterator<org.neo4j.rdf.model.CompleteStatement> iterator;
    private final GraphDatabaseSailConnectionImpl connection;
    private Statement nextStatement;
    
    public GraphDatabaseStatementIteration(final Iterator<org.neo4j.rdf.model.CompleteStatement> iterator, 
        final GraphDatabaseSailConnectionImpl connection ) {
        this.iterator = iterator;
        this.connection = connection;
    }

    public void close() throws SailException {
        // Not needed
    }

    public boolean hasNext() throws SailException {
        if ( nextStatement != null )
        {
            return true;
        }
        synchronized ( connection )
        {
            Transaction otherTx = connection.suspendOtherAndResumeThis();
            try
            {
                if ( iterator.hasNext() )
                {
                	nextStatement = fetchNextStatement();
                }
                return nextStatement != null;
            }
            finally
            {
                connection.suspendThisAndResumeOther( otherTx );
            }
        }
    }

    public Statement next() throws SailException 
    {
        if ( !hasNext() )
        {
            throw new IllegalStateException();
        }
        Statement result = nextStatement;
        nextStatement = null;
        return result;
    }
    
    private Statement fetchNextStatement() throws SailException
    {
        org.neo4j.rdf.model.CompleteStatement statement = iterator.next();
        //System.out.println("retrieved a statement: " + statement);
                return (null == statement)
                        // TODO: would be better here if iterator were an Iterator<CompleteStatement>
                        ? null : GraphDatabaseSesameMapper.createStatement((CompleteStatement) statement, true);
    }

    public void remove() throws SailException {
        // TODO: decide whether remove() should be supported
    }
}
