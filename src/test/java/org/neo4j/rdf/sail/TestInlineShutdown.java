package org.neo4j.rdf.sail;

import static org.junit.Assert.fail;

import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.index.IndexService;
import org.neo4j.index.lucene.LuceneIndexService;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.rdf.store.RdfStore;
import org.neo4j.rdf.store.VerboseQuadStore;
import org.openrdf.sail.Sail;

public class TestInlineShutdown
{
    @Test
    public void testInlineShutdown() throws Exception
    {
        GraphDatabaseService graphDb = new EmbeddedGraphDatabase(
                "target/var-shutdown" );
        IndexService index = new LuceneIndexService( graphDb );
        RdfStore store = new VerboseQuadStore( graphDb, index );
        Sail sail = new GraphDatabaseSail( graphDb, store, true );
        sail.shutDown();
        try
        {
            Transaction tx = graphDb.beginTx();
            try
            {
                graphDb.createNode();
                tx.success();
            }
            finally
            {
                tx.finish();
            }
            fail( "Shouldn't be able to create a node in graph db which is shut down" );
        }
        catch ( Exception e )
        {
            // OK
        }
    }

    @Test
    public void testNormalShutdown() throws Exception
    {
        GraphDatabaseService graphDb = new EmbeddedGraphDatabase(
                "target/var-shutdown" );
        IndexService index = new LuceneIndexService( graphDb );
        RdfStore store = new VerboseQuadStore( graphDb, index );
        Sail sail = new GraphDatabaseSail( graphDb, store );
        sail.shutDown();
        Transaction tx = graphDb.beginTx();
        try
        {
            graphDb.createNode();
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }
}
