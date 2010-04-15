package examples;

import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.index.IndexService;
import org.neo4j.index.lucene.LuceneIndexService;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.rdf.sail.GraphDatabaseSail;
import org.neo4j.rdf.store.RdfStore;
import org.neo4j.rdf.store.VerboseQuadStore;
import org.openrdf.sail.Sail;

public class SiteExamples
{
    @Test
    // START SNIPPET: setUpSail
    public void setUpSail() throws Exception
    {
        // Start up the graph database and RDF store and wrap it in a Sail
        GraphDatabaseService graphDb = new EmbeddedGraphDatabase(
                "target/var/examples" );
        IndexService indexService = new LuceneIndexService( graphDb );
        RdfStore rdfStore = new VerboseQuadStore( graphDb, indexService );
        Sail sail = new GraphDatabaseSail( graphDb, rdfStore );
        
        // ...
        
        // Shut down in correct order
        sail.shutDown();
        indexService.shutdown();
        graphDb.shutdown();
    }
    // END SNIPPET: setUpSail
}
